use lib_core::{
    actor::LuaActor,
    buffer::Buffer,
    c_str,
    context::{self, Message, CONTEXT},
    laux::{self},
    lreg, lreg_null, lua_rawsetfield,
};
use lib_lua::{ffi, ffi::luaL_Reg};
use percent_encoding::percent_decode;
use reqwest::{header::HeaderMap, Method};
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

    let mut buffer = Buffer::with_capacity(256);

    //reserve 4 bytes for store length

    buffer.commit(std::mem::size_of::<u32>());

    buffer.write_str(
        format!(
            "{} {} {}\r\n",
            version_to_string(&response.version()),
            response.status().as_u16(),
            response.status().canonical_reason().unwrap_or("")
        )
        .as_str(),
    );

    for (key, value) in response.headers().iter() {
        buffer.write_str(
            format!(
                "{}: {}\r\n",
                key.to_string().to_lowercase(),
                value.to_str().unwrap_or("")
            )
            .as_str(),
        );
    }

    buffer.write_str("\r\n\r\n");

    buffer.seek(std::mem::size_of::<u32>() as isize);
    buffer.write_front((buffer.len() as u32).to_le_bytes().as_ref());

    let body = response.bytes().await?;
    buffer.write_slice(body.as_ref());

    CONTEXT.send(Message {
        ptype: context::PTYPE_HTTP,
        from: 0,
        to: req.id,
        session: req.session,
        data: Some(Box::new(buffer)),
    });
    Ok(())
}

extern "C-unwind" fn lua_http_request(state: *mut ffi::lua_State) -> c_int {
    laux::lua_checktype(state, 1, ffi::LUA_TTABLE);

    let mut headers = HeaderMap::new();

    {
        laux::push_c_string(state, c_str!("headers"));
        if laux::lua_rawget(state, 1) == ffi::LUA_TTABLE {
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
                        Err(err) => {
                            laux::lua_push(state, false);
                            laux::lua_push(state, err.to_string());
                            return 2;
                        }
                    },
                    Err(err) => {
                        laux::lua_push(state, false);
                        laux::lua_push(state, err.to_string());
                        return 2;
                    }
                }
                laux::lua_pop(state, 1);
            }
            laux::lua_pop(state, 1); //pop headers table
        }
    }

    let actor = LuaActor::from_lua_state(state);

    let id = actor.id;
    let session = actor.next_uuid();

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
            CONTEXT.response_error(0, id, -session, err.to_string());
        }
    });

    laux::lua_push(state, session);
    1
}

extern "C-unwind" fn lua_http_parse_response(state: *mut ffi::lua_State) -> c_int {
    let raw_response = laux::lua_get::<&[u8]>(state, 1);
    let mut headers = [httparse::EMPTY_HEADER; 32];
    let mut res = httparse::Response::new(&mut headers);

    match res.parse(raw_response) {
        Ok(httparse::Status::Complete(_)) => {
            let version = res.version.unwrap_or(1);
            let status_code = res.code.unwrap_or(200);
            unsafe {
                ffi::lua_createtable(state, 0, 6);
                lua_rawsetfield!(state, -3, "version", laux::lua_push(state, version));
                lua_rawsetfield!(
                    state,
                    -3,
                    "status_code",
                    laux::lua_push(state, status_code as u32)
                );
                ffi::lua_pushstring(state, c_str!("headers"));
                ffi::lua_createtable(state, 0, res.headers.len() as c_int);
                for header in res.headers.iter() {
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
            laux::lua_push(state, "Incomplete response");
            2
        }
        Err(err) => {
            laux::lua_push(state, false);
            laux::lua_push(state, err.to_string());
            2
        }
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
                lua_rawsetfield!(state, -3, "method", laux::lua_push(state, method));
                lua_rawsetfield!(state, -3, "path", laux::lua_push(state, path));
                lua_rawsetfield!(
                    state,
                    -3,
                    "query_string",
                    laux::lua_push(state, query_string)
                );
                ffi::lua_pushstring(state, c_str!("headers"));
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

extern "C-unwind" fn lua_http_encode_query_string(state: *mut ffi::lua_State) -> c_int {
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

extern "C-unwind" fn lua_http_parse_query_string(state: *mut ffi::lua_State) -> c_int {
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

pub unsafe extern "C-unwind" fn luaopen_http(state: *mut ffi::lua_State) -> c_int {
    let l = [
        lreg!("request", lua_http_request),
        lreg!("parse_response", lua_http_parse_response),
        lreg!("parse_request", lua_http_parse_request),
        lreg!("encode_query_string", lua_http_encode_query_string),
        lreg!("parse_query_string", lua_http_parse_query_string),
        lreg_null!(),
    ];

    ffi::lua_createtable(state, 0, l.len() as c_int);
    ffi::luaL_setfuncs(state, l.as_ptr(), 0);

    1
}
