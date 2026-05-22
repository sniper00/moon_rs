use dashmap::DashMap;
use lazy_static::lazy_static;
use actor::{
    context::{self, CONTEXT},
};
use luars::{LuaResult, LuaState, LuaValue};
use percent_encoding::percent_decode;
use reqwest::ClientBuilder;
use reqwest::{header::HeaderMap, Method, Version};

use crate::lua_actor::ActorRef;
use std::{error::Error, str::FromStr, time::Duration};
use url::form_urlencoded::{self};

use crate::{lua_check_bytes, lua_check_str, lua_push_error, lua_take_typed_lightuserdata, opt_field_bytes, opt_field_int, opt_field_str};

lazy_static! {
    static ref HTTP_CLIENTS: DashMap<String, reqwest::Client> = DashMap::new();
}

struct HttpRequest {
    id: i64,
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

pub fn get_http_client(timeout: u64, proxy: &String) -> reqwest::Client {
    let name = format!("{}_{}", timeout, proxy);
    if let Some(client) = HTTP_CLIENTS.get(&name) {
        return client.clone();
    }

    if timeout > 100 {
        log::warn!("http client timeout {} is too long", timeout);
    }

    let builder = ClientBuilder::new()
        .timeout(Duration::from_secs(timeout))
        .use_rustls_tls()
        .tcp_nodelay(true);

    let client = if proxy.is_empty() {
        builder.build().unwrap_or_default()
    } else {
        builder
            .proxy(reqwest::Proxy::all(proxy).unwrap())
            .build()
            .unwrap_or_default()
    };

    HTTP_CLIENTS.insert(name.to_string(), client.clone());
    client
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

async fn http_request(req: HttpRequest) -> Result<(), Box<dyn Error>> {
    let http_client = &get_http_client(req.timeout, &req.proxy);

    let response = http_client
        .request(Method::from_str(req.method.as_str())?, req.url)
        .headers(req.headers)
        .body(req.body)
        .send()
        .await?;

    CONTEXT.send_value(
        context::PTYPE_HTTP,
        req.id,
        req.session,
        HttpResponse {
        version: response.version(),
        status_code: response.status().as_u16() as i32,
        headers: response.headers().clone(),
        body: response.bytes().await?,
        },
    );

    Ok(())
}

fn extract_headers(state: &mut LuaState, table: &LuaValue) -> Result<HeaderMap, String> {
    let mut headers = HeaderMap::with_capacity(8);

    let key = state
        .create_string("headers")
        .map_err(|e| format!("{}", e))?;
    let header_table = match state.raw_get(table, &key) {
        Some(ht) if ht.is_table() => ht,
        _ => return Ok(headers),
    };

    let lua_table = header_table
        .as_table()
        .ok_or("headers field is not a table")?;
    let mut current_key = LuaValue::nil();
    loop {
        match lua_table.next(&current_key) {
            Ok(Some((k, v))) => {
                let key_str = k.as_str().unwrap_or("").to_string();
                let value_str = v.as_str().unwrap_or("").to_string();

                let name = key_str
                    .parse::<reqwest::header::HeaderName>()
                    .map_err(|e| format!("Invalid header name '{}': {}", key_str, e))?;

                let value = value_str
                    .parse::<reqwest::header::HeaderValue>()
                    .map_err(|e| format!("Invalid header value '{}': {}", value_str, e))?;

                headers.insert(name, value);
                current_key = k;
            }
            Ok(None) => break,
            Err(_) => return Err("invalid key during header iteration".to_string()),
        }
    }

    Ok(headers)
}

fn lua_http_request(state: &mut LuaState) -> LuaResult<usize> {
    let table = state
        .get_arg(1)
        .filter(|v| v.is_table())
        .ok_or_else(|| state.error("bad argument #1 (table expected)".to_string()))?;

    let headers = match extract_headers(state, &table) {
        Ok(headers) => headers,
        Err(err) => return lua_push_error(state, &err),
    };

    let actor = ActorRef::from_state(state);
    let id = actor.id();
    let session = actor.next_session();

    let req = HttpRequest {
        id,
        session,
        method: opt_field_str(state, &table, "method").unwrap_or("GET".to_string()),
        url: opt_field_str(state, &table, "url").unwrap_or_default(),
        body: opt_field_bytes(state, &table, "body").unwrap_or_default(),
        headers,
        timeout: opt_field_int(state, &table, "timeout").unwrap_or(5) as u64,
        proxy: opt_field_str(state, &table, "proxy").unwrap_or_default(),
    };

    tokio::spawn(async move {
        if let Err(err) = http_request(req).await {
            CONTEXT.send_value(
                context::PTYPE_HTTP,
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

    state.push_value(LuaValue::integer(session))?;
    Ok(1)
}

fn decode(state: &mut LuaState) -> LuaResult<usize> {
    let response = lua_take_typed_lightuserdata::<HttpResponse>(state, 1)?;

    let table = state.create_table(0, 6)?;

    let k = state.create_string("version")?;
    let v = state.create_string(version_to_string(&response.version))?;
    state.raw_set(&table, k, v);

    let k = state.create_string("status_code")?;
    state.raw_set(&table, k, LuaValue::integer(response.status_code as i64));

    let k = state.create_string("body")?;
    let v = state.create_bytes(response.body.as_ref())?;
    state.raw_set(&table, k, v);

    let headers_table = state.create_table(0, response.headers.len())?;
    for (key, value) in response.headers.iter() {
        let k = state.create_string(key.as_str())?;
        let v = state.create_string(value.to_str().unwrap_or("").trim())?;
        state.raw_set(&headers_table, k, v);
    }
    let k = state.create_string("headers")?;
    state.raw_set(&table, k, headers_table);

    state.push_value(table)?;
    Ok(1)
}

fn lua_http_form_urlencode(state: &mut LuaState) -> LuaResult<usize> {
    let table = state
        .get_arg(1)
        .filter(|v| v.is_table())
        .ok_or_else(|| state.error("bad argument #1 (table expected)".to_string()))?;

    let lua_table = table
        .as_table()
        .ok_or_else(|| state.error("bad argument #1 (table expected)".to_string()))?;

    let mut result = String::with_capacity(64);
    let mut current_key = LuaValue::nil();
    loop {
        match lua_table.next(&current_key) {
            Ok(Some((k, v))) => {
                if !result.is_empty() {
                    result.push('&');
                }
                let key_bytes = k.as_bytes().unwrap_or(&[]);
                let val_bytes = v.as_bytes().unwrap_or(&[]);
                result.push_str(
                    &form_urlencoded::byte_serialize(key_bytes).collect::<String>(),
                );
                result.push('=');
                result.push_str(
                    &form_urlencoded::byte_serialize(val_bytes).collect::<String>(),
                );
                current_key = k;
            }
            Ok(None) => break,
            Err(_) => return Err(state.error("http: invalid table key during iteration".to_string())),
        }
    }

    let val = state.create_string(&result)?;
    state.push_value(val)?;
    Ok(1)
}

fn lua_http_form_urldecode(state: &mut LuaState) -> LuaResult<usize> {
    let query_string = lua_check_str(state, 1)?;

    let decoded: Vec<(String, String)> = form_urlencoded::parse(query_string.as_bytes())
        .into_owned()
        .collect();

    let table = state.create_table(0, decoded.len())?;
    for (key, value) in decoded {
        let k = state.create_string(&key)?;
        let v = state.create_string(&value)?;
        state.raw_set(&table, k, v);
    }
    state.push_value(table)?;
    Ok(1)
}

fn lua_http_parse_response(state: &mut LuaState) -> LuaResult<usize> {
    let raw_response = lua_check_bytes(state, 1)?.to_vec();

    let mut lines = raw_response.split(|&x| x == b'\n');
    let version_line = match lines.next() {
        Some(version_line) => version_line,
        None => return lua_push_error(state, "http: empty response"),
    };

    let mut parts = version_line.splitn(3, |&x| x == b' ');
    let version = match parts.next() {
        Some(part) => part
            .strip_prefix(b"HTTP/")
            .ok_or_else(|| state.error("http: invalid version prefix".to_string()))?,
        None => return lua_push_error(state, "http: missing version"),
    };

    let status_code = match parts.next() {
        Some(part) => part,
        None => return lua_push_error(state, "http: missing status code"),
    };

    let response = state.create_table(0, 6)?;

    let k = state.create_string("version")?;
    let v = state.create_bytes(version)?;
    state.raw_set(&response, k, v);

    let k = state.create_string("status_code")?;
    let status = i32::from_str(String::from_utf8_lossy(status_code).as_ref()).unwrap_or(200);
    state.raw_set(&response, k, LuaValue::integer(status as i64));

    let headers_table = state.create_table(0, 16)?;
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

        let k = state.create_string(&key.to_lowercase())?;
        let v = state.create_string(value.trim())?;
        state.raw_set(&headers_table, k, v);
    }
    let k = state.create_string("headers")?;
    state.raw_set(&response, k, headers_table);

    state.push_value(response)?;
    Ok(1)
}

fn lua_http_parse_request(state: &mut LuaState) -> LuaResult<usize> {
    let raw_request = lua_check_bytes(state, 1)?.to_vec();

    let mut headers = [httparse::EMPTY_HEADER; 32];
    let mut req = httparse::Request::new(&mut headers);

    match req.parse(&raw_request) {
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

            let table = state.create_table(0, 6)?;

            let k = state.create_string("method")?;
            let v = state.create_string(method)?;
            state.raw_set(&table, k, v);

            let k = state.create_string("path")?;
            let v = state.create_string(path)?;
            state.raw_set(&table, k, v);

            let k = state.create_string("query_string")?;
            let v = state.create_string(query_string)?;
            state.raw_set(&table, k, v);

            let headers_table = state.create_table(0, req.headers.len())?;
            for header in req.headers.iter() {
                let k = state.create_string(&header.name.to_lowercase())?;
                let v = state.create_bytes(header.value)?;
                state.raw_set(&headers_table, k, v);
            }
            let k = state.create_string("headers")?;
            state.raw_set(&table, k, headers_table);

            state.push_value(table)?;
            Ok(1)
        }
        Ok(httparse::Status::Partial) => lua_push_error(state, "http: incomplete request"),
        Err(err) => lua_push_error(state, &err.to_string()),
    }
}

pub fn register_http() -> luars::LibraryModule {
    luars::lua_module!("http.core", {
        "request" => lua_http_request,
        "decode" => decode,
        "form_urlencode" => lua_http_form_urlencode,
        "form_urldecode" => lua_http_form_urldecode,
        "parse_response" => lua_http_parse_response,
        "parse_request" => lua_http_parse_request,
    })
}
