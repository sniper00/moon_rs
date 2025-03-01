use lib_core::{
    actor::LuaActor,
    context::{self, Message, MessageData, CONTEXT},
};
use lib_lua::{self, cstr, ffi, ffi::luaL_Reg, laux, lreg, lreg_null, lua_rawsetfield};
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

    let response = HttpResponse {
        version: response.version(),
        status_code: response.status().as_u16() as i32,
        headers: response.headers().clone(),
        body: response.bytes().await?,
    };

    let ptr = Box::into_raw(Box::new(response));

    CONTEXT.send(Message {
        ptype: context::PTYPE_HTTP,
        from: 0,
        to: req.id,
        session: req.session,
        data: MessageData::ISize(ptr as isize),
    });

    Ok(())
}

fn extract_headers(state: *mut ffi::lua_State, index: i32) -> Result<HeaderMap, String> {
    let mut headers = HeaderMap::new();

    laux::push_c_string(state, cstr!("headers"));
    if laux::lua_rawget(state, index) == ffi::LUA_TTABLE {
        // [+1]
        laux::lua_pushnil(state);
        while laux::lua_next(state, -2) {
            let key: &str = laux::lua_opt(state, -2).unwrap_or_default();
            let value: &str = laux::lua_opt(state, -1).unwrap_or_default();
            match key.parse::<reqwest::header::HeaderName>() {
                Ok(name) => match value.parse::<reqwest::header::HeaderValue>() {
                    Ok(value) => {
                        headers.insert(name, value);
                    }
                    Err(err) => return Err(err.to_string()),
                },
                Err(err) => return Err(err.to_string()),
            }
            laux::lua_pop(state, 1);
        }
        laux::lua_pop(state, 1); //pop headers table
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
            let ptr = Box::into_raw(Box::new(HttpResponse{
                version: Version::HTTP_11,
                status_code: -1,
                headers: HeaderMap::new(),
                body: err.to_string().into(),
            }));

            CONTEXT.send(Message {
                ptype: context::PTYPE_HTTP,
                from: 0,
                to: id,
                session,
                data: MessageData::ISize(ptr as isize),
            });
        }
    });

    laux::lua_push(state, session);
    1
}

extern "C-unwind" fn decode(state: *mut ffi::lua_State) -> c_int {
    laux::luaL_checkstack(state, 4, std::ptr::null());
    let p_as_isize: isize = laux::lua_get(state, 1);
    let response = unsafe { Box::from_raw(p_as_isize as *mut HttpResponse) };

    unsafe {
        ffi::lua_createtable(state, 0, 6);
        lua_rawsetfield!(
            state,
            -1,
            "version",
            laux::lua_push(state, version_to_string(&response.version))
        );
        lua_rawsetfield!(
            state,
            -1,
            "status_code",
            laux::lua_push(state, response.status_code as i32)
        );

        ffi::lua_pushstring(state, cstr!("headers"));
        ffi::lua_createtable(state, 0, 16);

        for (key, value) in response.headers.iter() {
            laux::lua_push(state, key.to_string().to_lowercase());
            laux::lua_push(state, value.to_str().unwrap_or("").trim());
            ffi::lua_rawset(state, -3);
        }
        ffi::lua_rawset(state, -3);

        lua_rawsetfield!(
            state,
            -1,
            "body",
            laux::lua_push(state, response.body.as_ref())
        );
    }
    1
}

extern "C-unwind" fn lua_http_form_urlencode(state: *mut ffi::lua_State) -> c_int {
    laux::lua_checktype(state, 1, ffi::LUA_TTABLE);
    laux::lua_pushnil(state);
    let mut result = String::new();
    while laux::lua_next(state, 1) {
        if !result.is_empty() {
            result.push('&');
        }
        let key = laux::to_string_unchecked(state, -2);
        let value = laux::to_string_unchecked(state, -1);
        result.push_str(
            form_urlencoded::byte_serialize(key.as_bytes())
                .collect::<String>()
                .as_str(),
        );
        result.push('=');
        result.push_str(
            form_urlencoded::byte_serialize(value.as_bytes())
                .collect::<String>()
                .as_str(),
        );
        laux::lua_pop(state, 1);
    }
    laux::lua_push(state, result);
    1
}

extern "C-unwind" fn lua_http_form_urldecode(state: *mut ffi::lua_State) -> c_int {
    let query_string = laux::lua_get::<&str>(state, 1);

    unsafe { ffi::lua_createtable(state, 0, 8) };

    let decoded: Vec<(String, String)> = form_urlencoded::parse(query_string.as_bytes())
        .into_owned()
        .collect();

    for pair in decoded {
        laux::lua_push(state, pair.0);
        laux::lua_push(state, pair.1);
        unsafe {
            ffi::lua_rawset(state, -3);
        }
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

    unsafe {
        ffi::lua_createtable(state, 0, 6);
        lua_rawsetfield!(state, -1, "version", laux::lua_push(state, version));
        lua_rawsetfield!(
            state,
            -1,
            "status_code",
            laux::lua_push(
                state,
                i32::from_str(String::from_utf8_lossy(status_code).as_ref()).unwrap_or(200)
            )
        );

        ffi::lua_pushstring(state, cstr!("headers"));
        ffi::lua_createtable(state, 0, 16);
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

            laux::lua_push(state, key.as_ref().to_lowercase());
            laux::lua_push(state, value.as_ref().trim());
            ffi::lua_rawset(state, -3);
        }
        ffi::lua_rawset(state, -3);
        1
    }
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

            unsafe {
                ffi::lua_createtable(state, 0, 6);
                lua_rawsetfield!(state, -1, "method", laux::lua_push(state, method));
                lua_rawsetfield!(state, -1, "path", laux::lua_push(state, path));
                lua_rawsetfield!(
                    state,
                    -1,
                    "query_string",
                    laux::lua_push(state, query_string)
                );

                ffi::lua_pushstring(state, cstr!("headers"));
                ffi::lua_createtable(state, 0, req.headers.len() as c_int);

                for header in req.headers.iter() {
                    laux::lua_push(state, header.name.to_lowercase());
                    laux::lua_push(state, header.value);
                    ffi::lua_rawset(state, -3);
                }

                ffi::lua_rawset(state, -3);
                1
            }
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

    ffi::lua_createtable(state, 0, l.len() as c_int);
    ffi::luaL_setfuncs(state, l.as_ptr(), 0);

    1
}
