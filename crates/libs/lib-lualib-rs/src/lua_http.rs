use lib_core::{
    actor::LuaActor,
    buffer::Buffer,
    c_str,
    context::{self, Message, CONTEXT},
    laux::{self},
    lreg, lreg_null, lua_rawsetfield,
};
use lib_lua::{ffi, ffi::luaL_Reg};
use reqwest::{header::HeaderMap, Method};
use std::{collections::HashMap, error::Error, ffi::c_int, str::FromStr};

pub type RequestResult = Result<(String, String, String, HashMap<String, String>), &'static str>;

pub type ResponseResult = Result<(String, String, HashMap<String, String>), &'static str>;

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

    buffer.seek(std::mem::size_of::<u32>() as isize);
    buffer.write_front((buffer.len() as u32).to_le_bytes().as_ref());

    let body = response.bytes().await?;
    buffer.write_slice(body.as_ref());

    CONTEXT.send(Message {
        ptype: context::PTYPE_HTTP,
        from: 0,
        to: id,
        session,
        data: Some(Box::new(buffer)),
    });
    Ok(())
}

extern "C-unwind" fn lua_http_request(state: *mut ffi::lua_State) -> c_int {
    // let lua = laux::LuaStateRef::new(state);

    laux::lua_checktype(state, 1, ffi::LUA_TTABLE);

    let method: String = laux::opt_field(state, 1, "method").unwrap_or("GET".to_string());
    let uri: String = laux::opt_field(state, 1, "uri").unwrap_or_default();
    let content: String = laux::opt_field(state, 1, "content").unwrap_or_default();

    let actor = LuaActor::from_lua_state(state);
    let id = actor.id;
    let session = actor.next_uuid();

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

    tokio::spawn(async move {
        if let Err(err) = http_request(id, session, method, uri, content, headers).await {
            CONTEXT.response_error(0, id, -session, err.to_string());
        }
    });

    laux::lua_push(state, session);
    1
}

pub struct HttpParser;

impl HttpParser {
    pub fn parse_response(sv: &str) -> ResponseResult {
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

    pub fn parse_request(sv: &str) -> RequestResult {
        let mut lines = sv.lines();
        let line = match lines.next() {
            Some(line) => line,
            None => return Err("No input"),
        };

        let mut parts = line.splitn(3, ' ');
        let method = match parts.next() {
            Some(part) => part.to_string(),
            None => return Err("No method"),
        };

        let mut path = match parts.next() {
            Some(part) => part.to_string(),
            None => return Err("No path"),
        };

        let mut query_string = String::new();
        if let Some(index) = path.find('?') {
            query_string = path[index + 1..].to_string();
            path = path[..index].to_string();
        }

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

        Ok((method, path, query_string, header))
    }

    fn percent_encode(value: &str) -> String {
        let mut result = String::new();
        result.reserve(value.len()); // Minimum size of result
        for chr in value.chars() {
            if !(chr.is_ascii_digit()
                || chr.is_ascii_uppercase()
                || chr.is_ascii_lowercase()
                || chr == '-'
                || chr == '.'
                || chr == '_'
                || chr == '~')
            {
                result.push('%');
                result.push_str(&format!("{:02X}", chr as u8));
            } else {
                result.push(chr);
            }
        }
        result
    }

    fn percent_decode(value: &str) -> String {
        let mut result = String::new();
        result.reserve(value.len() / 3 + (value.len() % 3)); // Minimum size of result
        let mut iter = value.chars();
        while let Some(chr) = iter.next() {
            if chr == '%' {
                if let Some(hex) = iter.next() {
                    if let Some(hex2) = iter.next() {
                        let decoded_chr =
                            u8::from_str_radix(&format!("{}{}", hex, hex2), 16).unwrap_or(0);
                        result.push(decoded_chr as char);
                    }
                }
            } else if chr == '+' {
                result.push(' ');
            } else {
                result.push(chr);
            }
        }
        result
    }

    fn encode_query(query: &HashMap<String, String>) -> String {
        let mut result = String::new();
        for (key, value) in query {
            if !result.is_empty() {
                result.push('&');
            }
            result.push_str(&HttpParser::percent_encode(key.as_str()));
            result.push('=');
            result.push_str(&HttpParser::percent_encode(value.as_str()));
        }
        result
    }

    /// Parses a query string into a `HashMap`.
    ///
    /// # Examples
    ///
    /// ```
    /// use your_crate::HttpParser;
    /// use std::collections::HashMap;
    ///
    /// let query_string = "key%3Dvalue=value%26value";
    /// let mut expected = HashMap::new();
    /// expected.insert("key=value".to_string(), "value&value".to_string());
    ///
    /// assert_eq!(HttpParser::parse_query(query_string), expected);
    ///
    /// assert_eq!(HttpParser::parse_query(query_string), expected);
    /// ```
    ///
    /// # Panics
    ///
    /// This function does not panic.
    ///
    /// # Errors
    ///
    /// This function does not return any errors.
    fn parse_query(query_string: &str) -> HashMap<String, String> {
        let mut query = HashMap::new();
        for pair in query_string.split('&') {
            let mut parts = pair.splitn(2, '=');
            let key = match parts.next() {
                Some(part) => part.to_string(),
                None => continue,
            };

            let value = match parts.next() {
                Some(part) => part.to_string(),
                None => continue,
            };

            query.insert(
                HttpParser::percent_decode(key.as_str()),
                HttpParser::percent_decode(value.as_str()),
            );
        }
        query
    }
}

extern "C-unwind" fn lua_http_parse_response(state: *mut ffi::lua_State) -> c_int {
    let raw_response = laux::lua_get::<&str>(state, 1);
    if let Ok((version, status_code, header)) = HttpParser::parse_response(raw_response) {
        unsafe {
            ffi::lua_createtable(state, 0, 6);
            lua_rawsetfield!(state, -3, "version", laux::lua_push(state, version));
            lua_rawsetfield!(
                state,
                -3,
                "status_code",
                laux::lua_push(state, i32::from_str(status_code.as_str()).unwrap_or(-2))
            );
            ffi::lua_pushliteral(state, "headers");
            ffi::lua_createtable(state, 0, header.len() as c_int);
            for (key, value) in header {
                laux::lua_push(state, key.to_lowercase());
                laux::lua_push(state, value);
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

extern "C-unwind" fn lua_http_parse_request(state: *mut ffi::lua_State) -> c_int {
    let raw_request = laux::lua_get::<&str>(state, 1);
    if let Ok((method, path, query_string, header)) = HttpParser::parse_request(raw_request) {
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
            ffi::lua_pushliteral(state, "headers");
            ffi::lua_createtable(state, 0, header.len() as c_int);
            for (key, value) in header {
                laux::lua_push(state, key.to_lowercase());
                laux::lua_push(state, value);
                ffi::lua_rawset(state, -3);
            }
            ffi::lua_rawset(state, -3);
            1
        }
    } else {
        unsafe {
            ffi::lua_pushboolean(state, 0);
            ffi::lua_pushstring(state, c_str!("parse request error"));
            2
        }
    }
}

extern "C-unwind" fn lua_http_encode_query_string(state: *mut ffi::lua_State) -> c_int {
    laux::lua_checktype(state, 1, ffi::LUA_TTABLE);
    laux::lua_pushnil(state);
    let mut query = HashMap::new();
    while laux::lua_next(state, 1) {
        let key = laux::lua_get::<&str>(state, -2);
        let value = laux::lua_get::<&str>(state, -1);
        query.insert(key.to_string(), value.to_string());
        laux::lua_pop(state, 1);
    }
    let query_string = HttpParser::encode_query(&query);
    laux::lua_push(state, query_string);
    1
}

extern "C-unwind" fn lua_http_parse_query_string(state: *mut ffi::lua_State) -> c_int {
    let query_string = laux::lua_get::<&str>(state, 1);
    let query = HttpParser::parse_query(query_string);
    unsafe {
        ffi::lua_createtable(state, 0, query.len() as c_int);
        for (key, value) in query {
            laux::lua_push(state, key);
            laux::lua_push(state, value);
            ffi::lua_rawset(state, -3);
        }
        1
    }
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_response() {
        let response = "HTTP/1.1 200 OK\r\nContent-Type: text/html\r\n\r\n";
        let result = HttpParser::parse_response(response).unwrap();
        assert_eq!(result.0, "1.1");
        assert_eq!(result.1, "200");
        assert_eq!(result.2.get("Content-Type").unwrap(), "text/html");
    }

    #[test]
    fn test_parse_request() {
        let request = "GET /path?query=value HTTP/1.1\r\nHost: example.com\r\n\r\n";
        let result = HttpParser::parse_request(request).unwrap();
        assert_eq!(result.0, "GET");
        assert_eq!(result.1, "/path");
        assert_eq!(result.2, "query=value");
        assert_eq!(result.3.get("Host").unwrap(), "example.com");
    }

    #[test]
    fn test_percent_encode() {
        let value = "value with spaces";
        let result = HttpParser::percent_encode(value);
        assert_eq!(result, "value%20with%20spaces");
    }

    #[test]
    fn test_percent_decode() {
        let value = "value%20with%20spaces";
        let result = HttpParser::percent_decode(value);
        assert_eq!(result, "value with spaces");
    }

    #[test]
    fn test_encode_query() {
        let mut query = HashMap::new();
        query.insert(
            "key with spaces".to_string(),
            "value with spaces".to_string(),
        );
        let result = HttpParser::encode_query(&query);
        assert_eq!(result, "key%20with%20spaces=value%20with%20spaces");
    }

    #[test]
    fn test_parse_query() {
        let query_string = "key%20with%20spaces=value%20with%20spaces";
        let result = HttpParser::parse_query(query_string);
        assert_eq!(result.get("key with spaces").unwrap(), "value with spaces");
    }
}
