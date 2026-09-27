use dashmap::DashMap;
use lazy_static::lazy_static;
use moon_base::{
    self, cstr,
    ffi::{self},
    laux::{self, LuaStack, LuaState, LuaTable},
    lreg_null, lreg_try, luaL_newlib,
};
use moon_runtime::{
    actor::LuaActor,
    context::{self, ActorId, CONTEXT},
};
use percent_encoding::percent_decode;
use reqwest::ClientBuilder;
use reqwest::{Method, Version, header::HeaderMap};
use std::{error::Error, ffi::c_int, pin::Pin, str::FromStr, time::Duration};
use tokio::sync::oneshot;
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
    read_timeout: u64,
    proxy: String,
}

struct HttpResponse {
    version: Version,
    status_code: i32,
    headers: HeaderMap,
    body: bytes::Bytes,
}

enum HttpcResponse {
    Complete(HttpResponse),
    StreamHeaders {
        version: Version,
        status_code: i32,
        headers: HeaderMap,
        next: oneshot::Sender<HttpStreamCommand>,
    },
    StreamChunk {
        body: bytes::Bytes,
        next: oneshot::Sender<HttpStreamCommand>,
    },
    StreamEnd,
    StreamError(String),
}

enum HttpStreamCommand {
    Next { owner: ActorId, session: i64 },
    Close,
}

struct HttpStreamHandle(Option<oneshot::Sender<HttpStreamCommand>>);

struct IdleReadTimer {
    sleep: Pin<Box<tokio::time::Sleep>>,
    timeout: Duration,
}

impl IdleReadTimer {
    fn new(timeout: Duration) -> Self {
        Self {
            sleep: Box::pin(tokio::time::sleep(timeout)),
            timeout,
        }
    }

    async fn next_chunk(
        &mut self,
        response: &mut reqwest::Response,
    ) -> Result<Option<bytes::Bytes>, String> {
        if self.timeout.is_zero() {
            return response.chunk().await.map_err(|err| err.to_string());
        }

        self.sleep
            .as_mut()
            .reset(tokio::time::Instant::now() + self.timeout);
        tokio::select! {
            result = response.chunk() => result.map_err(|err| err.to_string()),
            _ = self.sleep.as_mut() => Err(format!(
                "http response stream idle timeout after {}ms",
                self.timeout.as_millis()
            )),
        }
    }
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
/// `crate::LIMITS.max_network_read_bytes` bytes. The advertised `Content-Length` (when
/// present) is rejected up-front; the streamed total is also enforced because
/// the header may be absent or untruthful (e.g. chunked transfer).
async fn read_body_capped(mut response: reqwest::Response) -> Result<bytes::Bytes, Box<dyn Error>> {
    let limit = crate::LIMITS.max_network_read_bytes;
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
        HttpcResponse::Complete(HttpResponse {
            version,
            status_code,
            headers,
            body,
        }),
    );

    Ok(())
}

async fn http_stream_request(req: HttpRequest) -> Result<(), Box<dyn Error>> {
    let http_client = get_http_client(&req.proxy)?;

    if req.timeout > crate::LIMITS.http_client_timeout_ms {
        log::warn!("http stream timeout {}ms is too long", req.timeout);
    }
    if req.read_timeout > crate::LIMITS.http_client_timeout_ms {
        log::warn!(
            "http stream read timeout {}ms is too long",
            req.read_timeout
        );
    }

    let mut builder = http_client
        .request(Method::from_str(req.method.as_str())?, req.url)
        .headers(req.headers)
        .body(req.body);
    // Reqwest's request timeout covers the entire response body. Streaming
    // callers normally disable it and use read_timeout for the gap between
    // chunks instead.
    if req.timeout > 0 {
        builder = builder.timeout(Duration::from_millis(req.timeout));
    }
    let mut response = builder.send().await?;

    let version = response.version();
    let status_code = response.status().as_u16() as i32;
    let headers = response.headers().clone();
    let limit = crate::LIMITS.max_network_read_bytes;
    if let Some(len) = response.content_length()
        && len > limit as u64
    {
        return Err(format!(
            "http response body too large: {} bytes (limit {})",
            len, limit
        )
        .into());
    }

    let (next_tx, mut next_rx) = oneshot::channel();
    if CONTEXT
        .send_value(
            context::PTYPE_HTTPC,
            req.id,
            req.session,
            HttpcResponse::StreamHeaders {
                version,
                status_code,
                headers,
                next: next_tx,
            },
        )
        .is_some()
    {
        return Ok(());
    }

    let mut timer = IdleReadTimer::new(Duration::from_millis(req.read_timeout));
    let mut total = 0usize;
    loop {
        let command = match next_rx.await {
            Ok(command) => command,
            Err(_) => return Ok(()),
        };
        let (owner, session) = match command {
            HttpStreamCommand::Next { owner, session } => (owner, session),
            HttpStreamCommand::Close => return Ok(()),
        };

        match timer.next_chunk(&mut response).await {
            Ok(Some(chunk)) => {
                total = total
                    .checked_add(chunk.len())
                    .ok_or_else(|| std::io::Error::other("http response body size overflow"))?;
                if total > limit {
                    let _ = CONTEXT.send_value(
                        context::PTYPE_HTTPC,
                        owner,
                        session,
                        HttpcResponse::StreamError(format!(
                            "http response body exceeds limit of {} bytes",
                            limit
                        )),
                    );
                    return Ok(());
                }

                let (next_tx, new_next_rx) = oneshot::channel();
                if CONTEXT
                    .send_value(
                        context::PTYPE_HTTPC,
                        owner,
                        session,
                        HttpcResponse::StreamChunk {
                            body: chunk,
                            next: next_tx,
                        },
                    )
                    .is_some()
                {
                    return Ok(());
                }
                next_rx = new_next_rx;
            }
            Ok(None) => {
                let _ = CONTEXT.send_value(
                    context::PTYPE_HTTPC,
                    owner,
                    session,
                    HttpcResponse::StreamEnd,
                );
                return Ok(());
            }
            Err(err) => {
                let _ = CONTEXT.send_value(
                    context::PTYPE_HTTPC,
                    owner,
                    session,
                    HttpcResponse::StreamError(err),
                );
                return Ok(());
            }
        }
    }
}

fn extract_headers(lua: &mut LuaStack<'_>, index: i32) -> Result<HeaderMap, String> {
    let mut headers = HeaderMap::with_capacity(8); // Pre-allocate reasonable size

    let Some(cursor) = lua.table_field_cursor(index, "headers") else {
        return Ok(headers);
    };
    for entry in cursor {
        let key = entry.key();
        let value = entry.value();
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
    }

    Ok(headers)
}

fn parse_http_request(
    lua: &mut LuaStack<'_>,
    id: ActorId,
    session: i64,
    default_timeout: u64,
) -> Result<HttpRequest, String> {
    let headers = extract_headers(lua, 1)?;

    // Read the optional request body as raw bytes so binary payloads are
    // preserved, and cap it before spawning an IO task.
    let body: Vec<u8> = match lua.opt_field::<Vec<u8>>(1, "body") {
        Some(b) if b.len() > crate::LIMITS.max_network_read_bytes => {
            return Err(format!(
                "http request body too large: {} bytes (max {})",
                b.len(),
                crate::LIMITS.max_network_read_bytes
            ));
        }
        Some(b) => b,
        None => Vec::new(),
    };

    Ok(HttpRequest {
        id,
        session,
        method: lua.opt_field(1, "method").unwrap_or("GET".to_string()),
        url: lua.opt_field(1, "url").unwrap_or_default(),
        body,
        headers,
        timeout: lua.opt_field(1, "timeout").unwrap_or(default_timeout),
        read_timeout: lua.opt_field(1, "read_timeout").unwrap_or(0),
        proxy: lua.opt_field(1, "proxy").unwrap_or_default(),
    })
}

fn lua_http_request(lua: &mut LuaStack<'_>) -> Result<i32, String> {
    let state = lua.state();
    if lua.value(1).kind() != laux::LuaType::Table {
        return Err("bad argument #1 (table expected)".to_string());
    }

    let actor = LuaActor::from_lua_state(state);

    let id = unsafe { (*actor).id };
    let session = unsafe { (*actor).next_session() };
    let req = match parse_http_request(lua, id, session, 5000) {
        Ok(req) => req,
        Err(error) => return Ok(crate::lua_push_error_tuple(state, &error)),
    };

    CONTEXT.io_runtime().spawn(async move {
        if let Err(err) = http_request(req).await {
            let _ = CONTEXT.send_value(
                context::PTYPE_HTTPC,
                id,
                session,
                HttpcResponse::Complete(HttpResponse {
                    version: Version::HTTP_11,
                    status_code: -1,
                    headers: HeaderMap::new(),
                    body: err.to_string().into(),
                }),
            );
        }
    });

    lua.push(session);
    Ok(1)
}

fn lua_http_stream_request(lua: &mut LuaStack<'_>) -> Result<i32, String> {
    let state = lua.state();
    if lua.value(1).kind() != laux::LuaType::Table {
        return Err("bad argument #1 (table expected)".to_string());
    }

    let actor = LuaActor::from_lua_state(state);
    let id = unsafe { (*actor).id };
    let session = unsafe { (*actor).next_session() };
    let mut req = match parse_http_request(lua, id, session, 0) {
        Ok(req) => req,
        Err(error) => return Ok(crate::lua_push_error_tuple(state, &error)),
    };
    req.read_timeout = lua.opt_field(1, "read_timeout").unwrap_or(10000);

    CONTEXT.io_runtime().spawn(async move {
        if let Err(err) = http_stream_request(req).await {
            let _ = CONTEXT.send_value(
                context::PTYPE_HTTPC,
                id,
                session,
                HttpcResponse::StreamError(err.to_string()),
            );
        }
    });

    lua.push(session);
    Ok(1)
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

fn push_http_stream_headers(
    state: LuaState,
    version: Version,
    status_code: i32,
    headers: HeaderMap,
) -> i32 {
    LuaTable::new(state, 0, 6)
        .insert("version", version_to_string(&version))
        .insert("status_code", status_code)
        .insert("stream", true)
        .rawset_x("headers", || {
            let table = LuaTable::new(state, 0, headers.len());
            for (key, value) in &headers {
                table.insert(key.as_str(), value.to_str().unwrap_or("").trim());
            }
        });
    1
}

fn http_stream_next(lua: &mut LuaStack<'_>) -> Result<c_int, String> {
    let state = lua.state();
    let mut handle_ptr = lua
        .value(1)
        .as_userdata::<HttpStreamHandle>()
        .ok_or_else(|| "invalid http stream handle".to_string())?;
    let handle = unsafe { handle_ptr.as_mut() };
    let Some(tx) = handle.0.take() else {
        return Ok(crate::lua_push_error_tuple(
            state,
            "http stream: cursor already consumed or closed",
        ));
    };

    let actor = LuaActor::from_lua_state(state);
    let owner = unsafe { (*actor).id };
    let session = unsafe { (*actor).next_session() };
    if tx.send(HttpStreamCommand::Next { owner, session }).is_err() {
        return Ok(crate::lua_push_error_tuple(
            state,
            "http stream: worker is gone",
        ));
    }
    laux::lua_push(state, session);
    Ok(1)
}

fn http_stream_close(lua: &mut LuaStack<'_>) -> Result<c_int, String> {
    let mut handle_ptr = lua
        .value(1)
        .as_userdata::<HttpStreamHandle>()
        .ok_or_else(|| "invalid http stream handle".to_string())?;
    let handle = unsafe { handle_ptr.as_mut() };
    if let Some(tx) = handle.0.take() {
        let _ = tx.send(HttpStreamCommand::Close);
    }
    Ok(0)
}

fn push_http_stream_handle(state: LuaState, next: Option<oneshot::Sender<HttpStreamCommand>>) {
    let Some(next) = next else {
        laux::lua_pushnil(state);
        return;
    };
    let methods = [
        lreg_try!("next", http_stream_next),
        lreg_try!("close", http_stream_close),
        lreg_null!(),
    ];
    laux::lua_newuserdata(
        state,
        HttpStreamHandle(Some(next)),
        cstr!("http_stream_handle"),
        &methods,
    );
}

fn push_httpc_response(state: LuaState, response: HttpcResponse) -> c_int {
    match response {
        HttpcResponse::Complete(response) => push_http_response(state, response),
        HttpcResponse::StreamHeaders {
            version,
            status_code,
            headers,
            next,
        } => {
            push_http_stream_headers(state, version, status_code, headers);
            push_http_stream_handle(state, Some(next));
            2
        }
        HttpcResponse::StreamChunk { body, next } => {
            laux::lua_push(state, body.as_ref());
            push_http_stream_handle(state, Some(next));
            2
        }
        HttpcResponse::StreamEnd => {
            laux::lua_pushnil(state);
            laux::lua_pushnil(state);
            2
        }
        HttpcResponse::StreamError(error) => crate::lua_push_error_tuple(state, &error),
    }
}

fn lua_http_form_urlencode(lua: &mut LuaStack<'_>) -> Result<i32, String> {
    if lua.value(1).kind() != laux::LuaType::Table {
        return Err("bad argument #1 (table expected)".to_string());
    }

    let mut result = String::with_capacity(64);
    {
        for entry in lua.table_cursor(1) {
            let key = entry.key();
            let value = entry.value();
            if !result.is_empty() {
                result.push('&');
            }
            let encoded_key = match key.as_bytes() {
                Some(bytes) => form_urlencoded::byte_serialize(bytes).collect::<String>(),
                None => {
                    let text = key.to_string();
                    form_urlencoded::byte_serialize(text.as_bytes()).collect::<String>()
                }
            };
            result.push_str(&encoded_key);
            result.push('=');
            let encoded_value = match value.as_bytes() {
                Some(bytes) => form_urlencoded::byte_serialize(bytes).collect::<String>(),
                None => {
                    let text = value.to_string();
                    form_urlencoded::byte_serialize(text.as_bytes()).collect::<String>()
                }
            };
            result.push_str(&encoded_value);
        }
    }
    lua.push(result);
    Ok(1)
}

fn lua_http_form_urldecode(lua: &mut LuaStack<'_>) -> Result<i32, String> {
    let query_string = lua
        .value(1)
        .as_str()
        .ok_or_else(|| "bad argument #1 (valid UTF-8 string expected)".to_string())?;

    let decoded: Vec<(String, String)> = form_urlencoded::parse(query_string.as_bytes())
        .into_owned()
        .collect();

    let table = LuaTable::new(lua.state(), 0, decoded.len());

    for (key, value) in decoded {
        table.insert(key, value);
    }
    Ok(1)
}

fn lua_http_parse_response(lua: &mut LuaStack<'_>) -> Result<c_int, String> {
    // SAFETY: argument 1 remains rooted for the whole parser call. The parser
    // only reads its bytes while the result tables append values above it; no
    // operation pops, replaces, or reorders the source slot.
    let raw_response = unsafe {
        lua.value_bytes_append_only(1)
            .ok_or_else(|| "bad argument #1 (string expected)".to_string())?
    };

    let mut lines = raw_response.split(|&x| x == b'\n');
    let version_line = match lines.next() {
        Some(version_line) => version_line,
        None => {
            return Ok(crate::lua_push_error_tuple(lua.state(), "No input"));
        }
    };

    let mut parts = version_line.splitn(3, |&x| x == b' ');
    let version = match parts.next() {
        Some(part) if part.len() >= 5 => &part[5..],
        Some(_) => {
            return Ok(crate::lua_push_error_tuple(
                lua.state(),
                "Invalid HTTP version",
            ));
        }
        None => {
            return Ok(crate::lua_push_error_tuple(lua.state(), "No version"));
        }
    };

    let status_code = match parts.next() {
        Some(part) => part,
        None => {
            return Ok(crate::lua_push_error_tuple(lua.state(), "No status code"));
        }
    };

    let response = LuaTable::new(lua.state(), 0, 6);
    response.insert("version", version);
    response.insert(
        "status_code",
        i32::from_str(String::from_utf8_lossy(status_code).as_ref()).unwrap_or(200),
    );

    response.rawset_x("headers", || {
        let headers = LuaTable::new(lua.state(), 0, 16);
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

    Ok(1)
}

fn lua_http_parse_request(lua: &mut LuaStack<'_>) -> Result<c_int, String> {
    // SAFETY: argument 1 remains rooted while httparse's borrowed fields are
    // copied into the result table. All Lua operations only append above it.
    let raw_request = unsafe {
        lua.value_bytes_append_only(1)
            .ok_or_else(|| "bad argument #1 (string expected)".to_string())?
    };
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

            LuaTable::new(lua.state(), 0, 6)
                .insert("method", method)
                .insert("path", path)
                .insert("query_string", query_string)
                .rawset_x("headers", || {
                    let headers = LuaTable::new(lua.state(), 0, req.headers.len());
                    for header in req.headers.iter() {
                        headers.insert(header.name.to_lowercase(), header.value);
                    }
                });
            Ok(1)
        }
        Ok(httparse::Status::Partial) => Ok(crate::lua_push_error_tuple(
            lua.state(),
            "Incomplete request",
        )),
        Err(err) => Ok(crate::lua_push_error_tuple(lua.state(), &err.to_string())),
    }
}

pub unsafe extern "C-unwind" fn decode_httpc_message(
    state: LuaState,
    m: *mut moon_runtime::context::Message,
) -> c_int {
    match unsafe { crate::message_decode::take_boxed::<HttpcResponse>(m) } {
        Ok(response) => push_httpc_response(state, response),
        Err(e) => crate::lua_push_error_tuple(state, &e),
    }
}

pub extern "C-unwind" fn luaopen_httpc(state: LuaState) -> c_int {
    let l = [
        lreg_try!("request", lua_http_request),
        lreg_try!("request_stream", lua_http_stream_request),
        lreg_try!("form_urlencode", lua_http_form_urlencode),
        lreg_try!("form_urldecode", lua_http_form_urldecode),
        lreg_try!("parse_response", lua_http_parse_response),
        lreg_try!("parse_request", lua_http_parse_request),
        lreg_null!(),
    ];

    luaL_newlib!(state, l);

    1
}
