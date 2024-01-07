use lib_lua::{ffi, ffi::luaL_Reg};
use reqwest::header::HeaderMap;
use reqwest::Method;
use serde::{Deserialize, Serialize};
use std::ffi::c_int;
use std::str::FromStr;
use std::{collections::HashMap, error::Error};

use lib_core::{
    c_str,
    context::{self, Message, CONTEXT},
    laux::{self, LuaScopePop},
    lreg, lreg_null,
};

#[derive(Serialize, Deserialize)]
struct HttpResponse {
    status_code: u16,
    version: String,
    headers: HashMap<String, String>,
    body: String,
}

fn header_map_to_hash_map(header_map: &HeaderMap) -> HashMap<String, String> {
    let mut hash_map = HashMap::new();
    for (key, value) in header_map.iter() {
        hash_map.insert(key.to_string(), value.to_str().unwrap_or("").to_string());
    }
    hash_map
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

    let response = HttpResponse {
        status_code: response.status().as_u16(),
        version: version_to_string(&response.version()).to_string(),
        headers: header_map_to_hash_map(response.headers()),
        body: response.text().await?,
    };

    let json_str = serde_json::to_string(&response)?;

    if let Some(sender) = CONTEXT.get(id) {
        let _ = sender.send(Message {
            ptype: context::PTYPE_HTTP,
            from: 0,
            to: id,
            session,
            data: Some(Box::new(json_str.into())),
        });
    }
    Ok(())
}

extern "C-unwind" fn lua_http_request(state: *mut ffi::lua_State) -> c_int {
    unsafe {
        ffi::luaL_checktype(state, 1, ffi::LUA_TTABLE);
    }

    let id: i64 = laux::opt_field(state, 1, "id", 0);
    let session: i64 = laux::opt_field(state, 1, "session", 0);
    let method: String = laux::opt_field(state, 1, "method", "GET".to_string());
    let uri: String = laux::opt_field(state, 1, "uri", "".to_string());
    let content: String = laux::opt_field(state, 1, "content", "".to_string());

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

    1
}

pub unsafe extern "C-unwind" fn luaopen_http(state: *mut ffi::lua_State) -> c_int {
    let l = [lreg!("request", lua_http_request), lreg_null!()];

    ffi::lua_createtable(state, 0, l.len() as c_int);
    ffi::luaL_setfuncs(state, l.as_ptr(), 0);

    1
}
