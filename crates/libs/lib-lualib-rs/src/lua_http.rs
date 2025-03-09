use lib_core::{
    actor::LuaActor,
    context::{self, CONTEXT},
};
use lib_lua::{
    self, cstr,
    ffi::{self, luaL_Reg},
    laux::{self, LuaTable, LuaValue},
    lreg, lreg_null, luaL_newlib,
};
use percent_encoding::percent_decode;
use reqwest::{header::HeaderMap, Method, Version};
use std::{error::Error, ffi::c_int, str::FromStr};
use url::form_urlencoded::{self};

struct HttpRequest {
    id: i64,
    session: i64,
    method: String,
    url: String,
    body: String,
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
    let http_client = &CONTEXT.get_http_client(req.timeout, &req.proxy);

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

fn extract_headers(state: *mut ffi::lua_State, index: i32) -> Result<HeaderMap, String> {
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

extern "C-unwind" fn lua_http_request(state: *mut ffi::lua_State) -> c_int {
    laux::lua_checktype(state, 1, ffi::LUA_TTABLE);

    let headers = match extract_headers(state, 1) {
        Ok(headers) => headers,
        Err(err) => {
            laux::lua_push(state, false);
            laux::lua_push(state, err);
            return 2;
        }
    };

    let actor = LuaActor::from_lua_state(state);

    let id = actor.id;
    let session = actor.next_session();

    let req = HttpRequest {
        id,
        session,
        method: laux::opt_field(state, 1, "method").unwrap_or("GET".to_string()),
        url: laux::opt_field(state, 1, "url").unwrap_or_default(),
        body: laux::opt_field(state, 1, "body").unwrap_or_default(),
        headers,
        timeout: laux::opt_field(state, 1, "timeout").unwrap_or(5),
        proxy: laux::opt_field(state, 1, "proxy").unwrap_or_default(),
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

    laux::lua_push(state, session);
    1
}

extern "C-unwind" fn decode(state: *mut ffi::lua_State) -> c_int {
    laux::luaL_checkstack(state, 4, std::ptr::null());
    let p_as_isize: isize = laux::lua_get(state, 1);
    let response = unsafe { Box::from_raw(p_as_isize as *mut HttpResponse) };

    LuaTable::new(state, 0, 6)
        .rawset("version", version_to_string(&response.version))
        .rawset("status_code", response.status_code)
        .rawset("body", response.body.as_ref())
        .rawset_x("headers", || {
            let headers = LuaTable::new(state, 0, response.headers.len());
            for (key, value) in response.headers.iter() {
                headers.rawset(key.as_str(), value.to_str().unwrap_or("").trim());
            }
        });
    1
}

extern "C-unwind" fn lua_http_form_urlencode(state: *mut ffi::lua_State) -> c_int {
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

extern "C-unwind" fn lua_http_form_urldecode(state: *mut ffi::lua_State) -> c_int {
    let query_string = laux::lua_get::<&str>(state, 1);

    let decoded: Vec<(String, String)> = form_urlencoded::parse(query_string.as_bytes())
        .into_owned()
        .collect();

    let table = LuaTable::new(state, 0, decoded.len());

    for (key, value) in decoded {
        table.rawset(key, value);
    }
    1
}

extern "C-unwind" fn lua_http_parse_response(state: *mut ffi::lua_State) -> c_int {
    let raw_response = laux::lua_get::<&[u8]>(state, 1);

    let mut lines = raw_response.split(|&x| x == b'\n');
    let version_line = match lines.next() {
        Some(version_line) => version_line,
        None => {
            laux::lua_push(state, false);
            laux::lua_push(state, "No input");
            return 2;
        }
    };

    let mut parts = version_line.splitn(3, |&x| x == b' ');
    let version = match parts.next() {
        Some(part) => &part[5..],
        None => {
            laux::lua_push(state, false);
            laux::lua_push(state, "No version");
            return 2;
        }
    };

    let status_code = match parts.next() {
        Some(part) => part,
        None => {
            laux::lua_push(state, false);
            laux::lua_push(state, "No status code");
            return 2;
        }
    };

    let response = LuaTable::new(state, 0, 6);
    response.rawset("version", version);
    response.rawset(
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
            headers.rawset(key.to_lowercase(), value.trim());
        }
    });

    1
}

extern "C-unwind" fn lua_http_parse_request(state: *mut ffi::lua_State) -> c_int {
    let raw_request = laux::lua_get::<&[u8]>(state, 1);
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
                .rawset("method", method)
                .rawset("path", path)
                .rawset("query_string", query_string)
                .rawset_x("headers", || {
                    let headers = LuaTable::new(state, 0, req.headers.len());
                    for header in req.headers.iter() {
                        headers.rawset(header.name.to_lowercase(), header.value);
                    }
                });
            1
        }
        Ok(httparse::Status::Partial) => {
            laux::lua_push(state, false);
            laux::lua_push(state, "Incomplete request");
            2
        }
        Err(err) => {
            laux::lua_push(state, false);
            laux::lua_push(state, err.to_string());
            2
        }
    }
}

pub unsafe extern "C-unwind" fn luaopen_http(state: *mut ffi::lua_State) -> c_int {
    let l = [
        lreg!("request", lua_http_request),
        lreg!("decode", decode),
        lreg!("form_urlencode", lua_http_form_urlencode),
        lreg!("form_urldecode", lua_http_form_urldecode),
        lreg!("parse_response", lua_http_parse_response),
        lreg!("parse_request", lua_http_parse_request),
        lreg_null!(),
    ];


    luaL_newlib!(state, l);

    1
}
