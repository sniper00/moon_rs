use dashmap::DashMap;
use lazy_static::lazy_static;
use moon_lua::{
    self, cstr,
    ffi::{self},
    laux::{self, LuaState, LuaTable, LuaValue},
    lreg, lreg_null, luaL_newlib,
};
use moon_runtime::{
    actor::LuaActor,
    context::{self, ActorId, CONTEXT},
};
use percent_encoding::percent_decode;
use reqwest::ClientBuilder;
use reqwest::{Method, Version, header::HeaderMap};
use std::{error::Error, ffi::c_int, str::FromStr, time::Duration};
use url::form_urlencoded::{self};

lazy_static! {
    static ref HTTP_CLIENTS: DashMap<String, reqwest::Client> = DashMap::new();
}

struct HttpRequest {
    id: ActorId,
    session: i64,
    method: String,
    url: String,
    body: Vec<u8>,
    headers: HeaderMap,
    timeout: u64,
    proxy: String,
}

struct HttpResponse {
    version: Version,
    status_code: i32,
    headers: HeaderMap,
    body: bytes::Bytes,
}

/// Returns a cached `reqwest::Client` for the given proxy.
///
/// Clients are keyed by **proxy only** — the proxy is the sole setting baked into
/// the client at build time. The per-call timeout is *not* part of the key; it is
/// applied on the `RequestBuilder` in `http_request` instead. As a result every
/// request sharing a proxy reuses a single connection pool regardless of its
/// individual timeout, and the cache is bounded by the number of distinct proxies
/// (typically just one, the empty/no-proxy case) rather than growing with every
/// distinct timeout value.
pub fn get_http_client(proxy: &str) -> Result<reqwest::Client, Box<dyn Error>> {
    if let Some(client) = HTTP_CLIENTS.get(proxy) {
        return Ok(client.clone());
    }

    let builder = ClientBuilder::new().use_rustls_tls().tcp_nodelay(true);

    // Surface invalid proxy / TLS-builder configuration to the caller instead
    // of panicking or silently falling back to a default client.
    let client = if proxy.is_empty() {
        builder.build()?
    } else {
        let parsed = reqwest::Proxy::all(proxy)
            .map_err(|e| format!("invalid http proxy '{}': {}", proxy, e))?;
        builder.proxy(parsed).build()?
    };

    // A concurrent builder for the same proxy may race us here; last write wins
    // and the redundant client is simply dropped (its pool is never used).
    HTTP_CLIENTS.insert(proxy.to_string(), client.clone());
    Ok(client)
}

fn version_to_string(version: &reqwest::Version) -> &str {
    match *version {
        reqwest::Version::HTTP_09 => "HTTP/0.9",
        reqwest::Version::HTTP_10 => "HTTP/1.0",
        reqwest::Version::HTTP_11 => "HTTP/1.1",
        reqwest::Version::HTTP_2 => "HTTP/2.0",
        reqwest::Version::HTTP_3 => "HTTP/3.0",
        _ => "Unknown",
    }
}

/// Read a response body into memory, refusing to buffer more than
/// `crate::LIMITS.network_read_bytes` bytes. The advertised `Content-Length` (when
/// present) is rejected up-front; the streamed total is also enforced because
/// the header may be absent or untruthful (e.g. chunked transfer).
async fn read_body_capped(mut response: reqwest::Response) -> Result<bytes::Bytes, Box<dyn Error>> {
    let limit = crate::LIMITS.network_read_bytes;
    if let Some(len) = response.content_length() {
        if len > limit as u64 {
            return Err(format!(
                "http response body too large: {} bytes (limit {})",
                len, limit
            )
            .into());
        }
    }

    let mut buf: Vec<u8> = Vec::new();
    while let Some(chunk) = response.chunk().await? {
        if buf.len() + chunk.len() > limit {
            return Err(format!("http response body exceeds limit of {} bytes", limit).into());
        }
        buf.extend_from_slice(&chunk);
    }
    Ok(bytes::Bytes::from(buf))
}

async fn http_request(req: HttpRequest) -> Result<(), Box<dyn Error>> {
    let http_client = get_http_client(&req.proxy)?;

    if req.timeout > crate::LIMITS.http_client_timeout_ms {
        log::warn!("http request timeout {}ms is too long", req.timeout);
    }

    let response = http_client
        .request(Method::from_str(req.method.as_str())?, req.url)
        .headers(req.headers)
        .timeout(Duration::from_millis(req.timeout))
        .body(req.body)
        .send()
        .await?;

    let version = response.version();
    let status_code = response.status().as_u16() as i32;
    let headers = response.headers().clone();
    let body = read_body_capped(response).await?;

    let _ = CONTEXT.send_value(
        context::PTYPE_HTTPC,
        req.id,
        req.session,
        HttpResponse {
            version,
            status_code,
            headers,
            body,
        },
    );

    Ok(())
}

fn extract_headers(state: LuaState, index: i32) -> Result<HeaderMap, String> {
    let mut headers = HeaderMap::with_capacity(8); // Pre-allocate reasonable size

    let table = LuaTable::from_stack(state, index);
    let header_table = table.rawget("headers");

    match &header_table.value {
        LuaValue::Table(header_table) => {
            header_table
                .iter()
                .try_for_each(|(key, value)| {
                    let key_str = key.to_string();
                    let value_str = value.to_string();

                    // Parse header name and value
                    let name = key_str
                        .parse::<reqwest::header::HeaderName>()
                        .map_err(|e| format!("Invalid header name '{}': {}", key_str, e))?;

                    let value = value_str
                        .parse::<reqwest::header::HeaderValue>()
                        .map_err(|e| format!("Invalid header value '{}': {}", value_str, e))?;

                    headers.insert(name, value);
                    Ok(())
                })
                .map_err(|e: String| e)?;
        }
        _ => return Ok(headers), // Empty headers if not a table
    }

    Ok(headers)
}

extern "C-unwind" fn lua_http_request(state: LuaState) -> i32 {
    laux::lua_checktype(state, 1, ffi::LUA_TTABLE);

    let headers = match extract_headers(state, 1) {
        Ok(headers) => headers,
        Err(err) => {
            return crate::lua_push_error(state, &err);
        }
    };

    let actor = LuaActor::from_lua_state(state);

    let id = unsafe { (*actor).id };
    let session = unsafe { (*actor).next_session() };

    // Read the (optional) request body as raw bytes so binary payloads are
    // preserved, and cap it so a single request can't buffer an unbounded
    // amount of memory before it is even sent.
    let body: Vec<u8> = match laux::opt_field::<&[u8]>(state, 1, "body") {
        Some(b) if b.len() > crate::LIMITS.network_read_bytes => {
            return crate::lua_push_error(
                state,
                &format!(
                    "http request body too large: {} bytes (max {})",
                    b.len(),
                    crate::LIMITS.network_read_bytes
                ),
            );
        }
        Some(b) => b.to_vec(),
        None => Vec::new(),
    };

    let req = HttpRequest {
        id,
        session,
        method: laux::opt_field(state, 1, "method").unwrap_or("GET".to_string()),
        url: laux::opt_field(state, 1, "url").unwrap_or_default(),
        body,
        headers,
        timeout: laux::opt_field(state, 1, "timeout").unwrap_or(5000),
        proxy: laux::opt_field(state, 1, "proxy").unwrap_or_default(),
    };

    CONTEXT.io_runtime().spawn(async move {
        if let Err(err) = http_request(req).await {
            let _ = CONTEXT.send_value(
                context::PTYPE_HTTPC,
                id,
                session,
                HttpResponse {
                    version: Version::HTTP_11,
                    status_code: -1,
                    headers: HeaderMap::new(),
                    body: err.to_string().into(),
                },
            );
        }
    });

    laux::lua_push(state, session);
    1
}

fn push_http_response(state: LuaState, response: HttpResponse) -> i32 {
    LuaTable::new(state, 0, 6)
        .insert("version", version_to_string(&response.version))
        .insert("status_code", response.status_code)
        .insert("body", response.body.as_ref())
        .rawset_x("headers", || {
            let headers = LuaTable::new(state, 0, response.headers.len());
            for (key, value) in response.headers.iter() {
                headers.insert(key.as_str(), value.to_str().unwrap_or("").trim());
            }
        });
    1
}

extern "C-unwind" fn lua_http_form_urlencode(state: LuaState) -> i32 {
    laux::lua_checktype(state, 1, ffi::LUA_TTABLE);

    let mut result = String::with_capacity(64);
    for (key, value) in LuaTable::from_stack(state, 1).iter() {
        if !result.is_empty() {
            result.push('&');
        }
        result.push_str(
            form_urlencoded::byte_serialize(key.to_vec().as_ref())
                .collect::<String>()
                .as_str(),
        );
        result.push('=');
        result.push_str(
            form_urlencoded::byte_serialize(value.to_vec().as_ref())
                .collect::<String>()
                .as_str(),
        );
    }
    laux::lua_push(state, result);
    1
}

extern "C-unwind" fn lua_http_form_urldecode(state: LuaState) -> i32 {
    let query_string = unsafe { laux::lua_check_str(state, 1) };

    let decoded: Vec<(String, String)> = form_urlencoded::parse(query_string.as_bytes())
        .into_owned()
        .collect();

    let table = LuaTable::new(state, 0, decoded.len());

    for (key, value) in decoded {
        table.insert(key, value);
    }
    1
}

extern "C-unwind" fn lua_http_parse_response(state: LuaState) -> c_int {
    let raw_response = unsafe { laux::lua_check_lstring(state, 1) };

    let mut lines = raw_response.split(|&x| x == b'\n');
    let version_line = match lines.next() {
        Some(version_line) => version_line,
        None => {
            return crate::lua_push_error(state, "No input");
        }
    };

    let mut parts = version_line.splitn(3, |&x| x == b' ');
    let version = match parts.next() {
        Some(part) if part.len() >= 5 => &part[5..],
        Some(_) => {
            return crate::lua_push_error(state, "Invalid HTTP version");
        }
        None => {
            return crate::lua_push_error(state, "No version");
        }
    };

    let status_code = match parts.next() {
        Some(part) => part,
        None => {
            return crate::lua_push_error(state, "No status code");
        }
    };

    let response = LuaTable::new(state, 0, 6);
    response.insert("version", version);
    response.insert(
        "status_code",
        i32::from_str(String::from_utf8_lossy(status_code).as_ref()).unwrap_or(200),
    );

    response.rawset_x("headers", || {
        let headers = LuaTable::new(state, 0, 16);
        for line in lines {
            let mut parts = line.splitn(2, |&x| x == b':');
            let key = match parts.next() {
                Some(part) => String::from_utf8_lossy(part),
                None => continue,
            };

            let value = match parts.next() {
                Some(part) => String::from_utf8_lossy(part),
                None => continue,
            };
            headers.insert(key.to_lowercase(), value.trim());
        }
    });

    1
}

extern "C-unwind" fn lua_http_parse_request(state: LuaState) -> c_int {
    let raw_request = unsafe { laux::lua_check_lstring(state, 1) };
    let mut headers = [httparse::EMPTY_HEADER; 32];
    let mut req = httparse::Request::new(&mut headers);

    match req.parse(raw_request) {
        Ok(httparse::Status::Complete(_)) => {
            let method = req.method.unwrap_or("GET");

            let path = percent_decode(req.path.unwrap_or("/").as_bytes()).decode_utf8_lossy();

            let mut query_string = "";
            let path = if let Some(index) = path.find('?') {
                query_string = &path[index + 1..];
                &path[..index]
            } else {
                &path
            };

            LuaTable::new(state, 0, 6)
                .insert("method", method)
                .insert("path", path)
                .insert("query_string", query_string)
                .rawset_x("headers", || {
                    let headers = LuaTable::new(state, 0, req.headers.len());
                    for header in req.headers.iter() {
                        headers.insert(header.name.to_lowercase(), header.value);
                    }
                });
            1
        }
        Ok(httparse::Status::Partial) => crate::lua_push_error(state, "Incomplete request"),
        Err(err) => crate::lua_push_error(state, &err.to_string()),
    }
}

pub unsafe extern "C-unwind" fn decode_httpc_message(
    state: LuaState,
    m: *mut moon_runtime::context::Message,
) -> c_int {
    match unsafe { crate::message_decode::take_boxed::<HttpResponse>(m) } {
        Ok(response) => push_http_response(state, response),
        Err(e) => crate::lua_push_error(state, &e),
    }
}

pub extern "C-unwind" fn luaopen_httpc(state: LuaState) -> c_int {
    let l = [
        lreg!("request", lua_http_request),
        lreg!("form_urlencode", lua_http_form_urlencode),
        lreg!("form_urldecode", lua_http_form_urldecode),
        lreg!("parse_response", lua_http_parse_response),
        lreg!("parse_request", lua_http_parse_request),
        lreg_null!(),
    ];

    luaL_newlib!(state, l);

    1
}
