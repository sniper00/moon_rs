use lib_core::actor::LuaActor;
use lib_core::buffer::Buffer;
use lib_core::lua_rawsetfield;
use lib_lua::{ffi, ffi::luaL_Reg};
use reqwest::header::HeaderMap;
use reqwest::Method;
use std::error::Error;
use std::str::FromStr;
use std::{collections::HashMap, ffi::c_int};

use lib_core::{
    c_str,
    context::{self, Message, CONTEXT},
    laux::{self, LuaScopePop, LuaValue},
    lreg, lreg_null,
};

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

async fn http_request(
    id: i64,
    session: i64,
    method: String,
    uri: String,
    content: String,
    headers: HeaderMap,
) -> Result<(), Box<dyn Error>> {
    let http_client = &CONTEXT.http_client;

    let response = http_client
        .request(Method::from_str(method.as_str())?, uri)
        .headers(headers)
        .body(content)
        .send()
        .await?;

    let mut buffer = Buffer::with_head_reserve(256, 4);
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

    buffer.write_front((buffer.len() as u32).to_le_bytes().as_ref());

    let body = response.bytes().await?;
    buffer.write_slice(body.as_ref());

    if let Some(sender) = CONTEXT.get(id) {
        let _ = sender.send(Message {
            ptype: context::PTYPE_HTTP,
            from: 0,
            to: id,
            session,
            data: Some(Box::new(buffer)),
        });
    }
    Ok(())
}

extern "C-unwind" fn lua_http_request(state: *mut ffi::lua_State) -> c_int {
    unsafe {
        ffi::luaL_checktype(state, 1, ffi::LUA_TTABLE);
    }

    let method: String = laux::opt_field(state, 1, "method").unwrap_or("GET".to_string());
    let uri: String = laux::opt_field(state, 1, "uri").unwrap_or_default();
    let content: String = laux::opt_field(state, 1, "content").unwrap_or_default();

    let actor = LuaActor::from_lua_state(state);
    let id = actor.id;
    let session = actor.next_uuid();

    let mut headers = HeaderMap::new();

    unsafe {
        let _scope = LuaScopePop::new(state);
        ffi::lua_pushstring(state, c_str!("headers"));
        if ffi::lua_rawget(state, 1) == ffi::LUA_TTABLE {
            ffi::lua_pushnil(state);
            while ffi::lua_next(state, -2) != 0 {
                let key = laux::opt_str(state, -2, "");
                let value = laux::opt_str(state, -1, "");
                match key.parse::<reqwest::header::HeaderName>() {
                    Ok(name) => match value.parse::<reqwest::header::HeaderValue>() {
                        Ok(value) => {
                            headers.insert(name, value);
                        }
                        Err(e) => {
                            println!("http_request error: {}", e);
                        }
                    },
                    Err(e) => {
                        println!("http_request error: {}", e);
                        ffi::lua_pop(state, 1);
                    }
                }
                ffi::lua_pop(state, 1);
            }
        }
    }

    tokio::spawn(async move {
        match http_request(id, session, method, uri, content, headers).await {
            Ok(_) => {}
            Err(e) => {
                if let Some(sender) = CONTEXT.get(id) {
                    let _ = sender.send(Message {
                        ptype: context::PTYPE_ERROR,
                        from: 0,
                        to: id,
                        session,
                        data: Some(Box::new(e.to_string().into())),
                    });
                }
            }
        }
    });

    i64::push_lua(state, session);
    1
}

pub struct ResponseParser;

impl ResponseParser {
    pub fn parse(sv: &str) -> Result<(String, String, HashMap<String, String>), &'static str> {
        let mut lines = sv.lines();
        let line = match lines.next() {
            Some(line) => line,
            None => return Err("No input"),
        };

        let mut parts = line.splitn(3, ' ');
        let version = match parts.next() {
            Some(part) => part[5..].to_string(),
            None => return Err("No version"),
        };

        let status_code = match parts.next() {
            Some(part) => part.to_string(),
            None => return Err("No status code"),
        };

        let mut header = HashMap::new();
        for line in lines {
            let mut parts = line.splitn(2, ':');
            let key = match parts.next() {
                Some(part) => part.trim().to_string(),
                None => continue,
            };

            let value = match parts.next() {
                Some(part) => part.trim().to_string(),
                None => continue,
            };

            header.insert(key, value);
        }

        Ok((version, status_code, header))
    }
}

extern "C-unwind" fn lua_http_parse_response(state: *mut ffi::lua_State) -> c_int {
    let raw_response = laux::check_str(state, 1);
    if let Ok((version, status_code, header)) = ResponseParser::parse(raw_response) {
        unsafe {
            ffi::lua_createtable(state, 0, 6);
            lua_rawsetfield!(state, -3, "version", laux::push_string(state, &version));
            lua_rawsetfield!(
                state,
                -3,
                "status_code",
                i32::push_lua(state, i32::from_str(status_code.as_str()).unwrap_or(-2))
            );
            ffi::lua_pushliteral(state, "headers");
            ffi::lua_createtable(state, 0, header.len() as c_int);
            for (key, value) in header {
                laux::push_string(state, &key.to_lowercase());
                laux::push_string(state, &value);
                ffi::lua_rawset(state, -3);
            }
            ffi::lua_rawset(state, -3);
            1
        }
    } else {
        unsafe {
            ffi::lua_pushboolean(state, 0);
            ffi::lua_pushstring(state, c_str!("parse response error"));
            2
        }
    }
}

pub unsafe extern "C-unwind" fn luaopen_http(state: *mut ffi::lua_State) -> c_int {
    let l = [
        lreg!("request", lua_http_request),
        lreg!("parse_response", lua_http_parse_response),
        lreg_null!(),
    ];

    ffi::lua_createtable(state, 0, l.len() as c_int);
    ffi::luaL_setfuncs(state, l.as_ptr(), 0);

    1
}
