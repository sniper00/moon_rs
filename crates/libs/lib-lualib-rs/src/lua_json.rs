use lib_lua::{ffi, ffi::luaL_Reg};
use serde_json::Value;
use std::error::Error;
use std::ffi::{c_char, c_int, c_void};

use lib_core::{
    buffer::Buffer,
    c_str,
    laux::{self},
    lreg, lreg_null,
};

const JSON_NULL: &str = "null";
const JSON_TRUE: &str = "true";
const JSON_FALSE: &str = "false";
const CHAR2ESCAPE: [u8; 256] = [
    b'u', b'u', b'u', b'u', b'u', b'u', b'u', b'u', b'b', b't', b'n', b'u', b'f', b'r', b'u', b'u',
    b'u', b'u', b'u', b'u', // 0~19
    b'u', b'u', b'u', b'u', b'u', b'u', b'u', b'u', b'u', b'u', b'u', b'u', 0, 0, b'"', 0, 0, 0, 0,
    0, // 20~39
    0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, // 40~59
    0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, // 60~79
    0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, b'\\', 0, 0, 0, 0, 0, 0, 0, // 80~99
    0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, // 100~119
    0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, // 120~139
    0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, // 140~159
    0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, // 160~179
    0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, // 180~199
    0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, // 200~219
    0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, // 220~239
    0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
];

const HEX_DIGITS: [u8; 16] = [
    b'0', b'1', b'2', b'3', b'4', b'5', b'6', b'7', b'8', b'9', b'A', b'B', b'C', b'D', b'E', b'F',
];

unsafe fn encode_one(
    state: *mut ffi::lua_State,
    writer: &mut Vec<u8>,
    idx: i32,
    depth: i32,
    fmt: bool,
) -> Result<(), Box<dyn Error>> {
    let t = ffi::lua_type(state, idx);
    match t {
        ffi::LUA_TBOOLEAN => {
            if ffi::lua_toboolean(state, idx) != 0 {
                writer.extend_from_slice(JSON_TRUE.as_bytes());
            } else {
                writer.extend_from_slice(JSON_FALSE.as_bytes());
            }
        }
        ffi::LUA_TNUMBER => {
            if ffi::lua_isinteger(state, idx) != 0 {
                let n = ffi::lua_tointeger(state, idx);
                writer.extend_from_slice(n.to_string().as_bytes());
            } else {
                let n = ffi::lua_tonumber(state, idx);
                writer.extend_from_slice(n.to_string().as_bytes());
            }
        }
        ffi::LUA_TSTRING => {
            let mut len = 0;
            let str = ffi::lua_tolstring(state, idx, &mut len);
            writer.reserve(len * 6 + 2);
            writer.push(b'\"');
            for i in 0..len {
                let ch = *str.add(i) as u8 as usize;
                let esc = CHAR2ESCAPE[ch];
                if esc == 0 {
                    writer.push(ch as u8);
                } else {
                    writer.push(b'\\');
                    writer.push(esc);
                    if esc == b'u' {
                        writer.push(b'0');
                        writer.push(b'0');
                        writer.push(HEX_DIGITS[(ch >> 4) & 0xF]);
                        writer.push(HEX_DIGITS[ch & 0xF]);
                    }
                }
            }
            writer.push(b'\"');
        }
        ffi::LUA_TTABLE => {
            encode_table(state, writer, idx, depth, fmt)?;
        }
        ffi::LUA_TNIL => {
            writer.extend_from_slice(JSON_NULL.as_bytes());
        }
        ffi::LUA_TLIGHTUSERDATA => {
            if ffi::lua_touserdata(state, idx).is_null() {
                writer.extend_from_slice(JSON_NULL.as_bytes());
            }
        }
        _ => {
            let tname = std::ffi::CStr::from_ptr(ffi::lua_typename(state, t))
                .to_str()
                .unwrap_or_default();
            return Err(Box::new(std::io::Error::new(
                std::io::ErrorKind::Other,
                format!("json encode: unsupport value type :{}", tname),
            )));
        }
    }

    Ok(())
}

fn format_new_line(writer: &mut Vec<u8>, fmt: bool) {
    if fmt {
        writer.push(b'\n');
    }
}

fn format_space(writer: &mut Vec<u8>, fmt: bool, n: i32) {
    if fmt {
        for _ in 0..n {
            writer.push(b' ');
            writer.push(b' ');
        }
    }
}

unsafe fn encode_table_array(
    state: *mut ffi::lua_State,
    size: usize,
    writer: &mut Vec<u8>,
    idx: i32,
    depth: i32,
    fmt: bool,
) -> Result<(), Box<dyn Error>> {
    let bsize = writer.len();
    writer.push(b'[');
    for i in 1..=size {
        if i == 1 {
            format_new_line(writer, fmt);
        }
        format_space(writer, fmt, depth);
        ffi::lua_rawgeti(state, idx, i as ffi::lua_Integer);
        if ffi::lua_isnil(state, -1) != 0 {
            ffi::lua_pop(state, 1);
            writer.truncate(bsize);
            return encode_table_object(state, writer, idx, depth, fmt);
        }
        encode_one(state, writer, -1, depth, fmt)?;
        ffi::lua_pop(state, 1);
        if i != size {
            writer.push(b',');
        }
        format_new_line(writer, fmt)
    }
    format_space(writer, fmt, depth - 1);
    writer.push(b']');
    Ok(())
}

unsafe fn encode_table_object(
    state: *mut ffi::lua_State,
    writer: &mut Vec<u8>,
    idx: i32,
    depth: i32,
    fmt: bool,
) -> Result<(), Box<dyn Error>> {
    let mut i = 0;
    writer.push(b'{');
    ffi::lua_pushnil(state);
    while ffi::lua_next(state, idx) != 0 {
        if i > 0 {
            writer.push(b',');
        }
        i += 1;
        format_new_line(writer, fmt);
        let key_type = ffi::lua_type(state, -2);
        match key_type {
            ffi::LUA_TSTRING => {
                format_space(writer, fmt, depth);
                writer.push(b'\"');
                writer.extend_from_slice(laux::check_str(state, -2).as_bytes());
                writer.extend_from_slice(b"\":");
                if fmt {
                    writer.push(b' ');
                }
                encode_one(state, writer, -1, depth, fmt)?;
            }
            ffi::LUA_TNUMBER => {
                if ffi::lua_isinteger(state, -2) != 0 {
                    format_space(writer, fmt, depth);
                    let key = ffi::lua_tointeger(state, -2);
                    writer.push(b'\"');
                    writer.extend_from_slice(key.to_string().as_bytes());
                    writer.extend_from_slice(b"\":");
                    if fmt {
                        writer.push(b' ');
                    }
                    encode_one(state, writer, -1, depth, fmt)?;
                } else {
                    return Err(Box::new(std::io::Error::new(
                        std::io::ErrorKind::Other,
                        "json encode: unsupport number key type.",
                    )));
                }
            }
            _ => {}
        }
        ffi::lua_pop(state, 1);
    }

    if i == 0 {
        writer.pop();
        writer.extend_from_slice(b"[]");
    } else {
        format_new_line(writer, fmt);
        format_space(writer, fmt, depth - 1);
        writer.push(b'}');
    }

    Ok(())
}

unsafe fn encode_table(
    state: *mut ffi::lua_State,
    writer: &mut Vec<u8>,
    mut idx: i32,
    depth: i32,
    fmt: bool,
) -> Result<(), Box<dyn Error>> {
    let depth = depth + 1;
    if depth > 64 {
        return Err(Box::new(std::io::Error::new(
            std::io::ErrorKind::Other,
            "json encode: too depth",
        )));
    }

    if idx < 0 {
        idx = ffi::lua_gettop(state) + idx + 1;
    }

    ffi::luaL_checkstack(state, 6, c_str!("json.encode.table"));
    let arr_size = lua_array_size(state, idx);
    if arr_size > 0 {
        encode_table_array(state, arr_size, writer, idx, depth, fmt)?;
    } else {
        encode_table_object(state, writer, idx, depth, fmt)?;
    }

    Ok(())
}

unsafe fn lua_array_size(state: *mut ffi::lua_State, idx: i32) -> usize {
    ffi::lua_pushnil(state);
    if ffi::lua_next(state, idx) == 0 {
        return 0;
    }

    let first_key = if ffi::lua_isinteger(state, -2) != 0 {
        ffi::lua_tointeger(state, -2)
    } else {
        0
    };

    ffi::lua_pop(state, 2);

    if first_key <= 0 {
        return 0;
    } else if first_key == 1 {
        /*
         * https://www.lua.org/manual/5.4/manual.html#3.4.7
         * The length operator applied on a table returns a border in that table.
         * A border in a table t is any natural number that satisfies the following condition :
         * (border == 0 or t[border] ~= nil) and t[border + 1] == nil
         */
        let len = ffi::lua_rawlen(state, idx) as ffi::lua_Integer;
        ffi::lua_pushinteger(state, len);
        if ffi::lua_next(state, idx) != 0 {
            ffi::lua_pop(state, 2);
            return 0;
        }
        return len as usize;
    }

    let len = ffi::lua_rawlen(state, idx) as ffi::lua_Integer;
    if first_key > len {
        return 0;
    }

    ffi::lua_pushnil(state);
    while ffi::lua_next(state, idx) != 0 {
        if ffi::lua_isinteger(state, -2) != 0 {
            let x = ffi::lua_tointeger(state, -2);
            if x > 0 && x <= len {
                ffi::lua_pop(state, 1);
                continue;
            }
        }
        ffi::lua_pop(state, 2);
        return 0;
    }

    len as usize
}

unsafe extern "C-unwind" fn encode(state: *mut ffi::lua_State) -> c_int {
    let mut writer = Vec::new();
    let fmt = ffi::lua_toboolean(state, 2) != 0;
    match encode_one(state, &mut writer, 1, 0, fmt) {
        Ok(_) => {
            ffi::lua_pushlstring(state, writer.as_ptr() as *const c_char, writer.len());
            1
        }
        Err(err) => {
            unsafe {
                ffi::lua_pushnil(state);
                ffi::lua_pushstring(state, err.to_string().as_ptr() as *const c_char);
            }
            2
        }
    }
}

unsafe fn decode_one(state: *mut ffi::lua_State, val: &Value) -> Result<(), Box<dyn Error>> {
    match val {
        Value::Object(map) => {
            ffi::luaL_checkstack(state, 6, c_str!("json.decode.object"));
            ffi::lua_createtable(state, 0, map.len() as i32);
            for (k, v) in map {
                if !k.is_empty() {
                    let c = k.as_bytes()[0];
                    if c.is_ascii_digit() || c == b'-' {
                        // k to integer
                        match k.parse::<ffi::lua_Integer>() {
                            Ok(n) => {
                                ffi::lua_pushinteger(state, n);
                            }
                            Err(_) => {
                                ffi::lua_pushlstring(state, k.as_ptr() as *const c_char, k.len());
                            }
                        }
                    } else {
                        ffi::lua_pushlstring(state, k.as_ptr() as *const c_char, k.len());
                    }
                    decode_one(state, v)?;
                    ffi::lua_rawset(state, -3);
                }
            }
        }
        Value::Array(arr) => {
            ffi::luaL_checkstack(state, 6, c_str!("json.decode.array"));
            ffi::lua_createtable(state, arr.len() as i32, 0);
            for (i, v) in arr.iter().enumerate() {
                decode_one(state, v)?;
                ffi::lua_rawseti(state, -2, (i + 1) as ffi::lua_Integer);
            }
        }
        Value::Bool(b) => {
            ffi::lua_pushboolean(
                state,
                match b {
                    true => 1,
                    false => 0,
                },
            );
        }
        Value::Number(n) => {
            if n.is_f64() {
                ffi::lua_pushnumber(state, n.as_f64().unwrap_or_default());
            } else {
                ffi::lua_pushinteger(state, n.as_i64().unwrap_or_default());
            }
        }
        Value::Null => {
            ffi::lua_pushnil(state);
        }
        Value::String(s) => {
            ffi::lua_pushlstring(state, s.as_ptr() as *const c_char, s.len());
        }
    }

    Ok(())
}

extern "C-unwind" fn decode(state: *mut ffi::lua_State) -> c_int {
    let str = laux::check_str(state, 1);
    match serde_json::from_str::<Value>(str) {
        Ok(val) => {
            unsafe {
                decode_one(state, &val).unwrap_or_default();
            }
            1
        }
        Err(e) => {
            unsafe {
                ffi::lua_pushnil(state);
                ffi::lua_pushstring(state, e.to_string().as_ptr() as *const c_char);
            }
            2
        }
    }
}

unsafe extern "C-unwind" fn concat(state: *mut ffi::lua_State) -> c_int {
    let ltype = laux::lua_type(state, 1);
    if ltype == ffi::LUA_TSTRING {
        let slc = laux::check_slice(state, 1);
        let mut buf = Box::new(Buffer::with_reserve(slc.len()));
        buf.write_slice(slc);
        ffi::lua_pushlightuserdata(state, Box::into_raw(buf) as *mut c_void);
        return 1;
    }
    ffi::luaL_checktype(state, 1, ffi::LUA_TTABLE);

    ffi::lua_settop(state, 1);

    let mut writer = Box::new(Buffer::new());
    let array_len = ffi::lua_rawlen(state, 1);

    for i in 1..=array_len {
        ffi::lua_rawgeti(state, 1, i as ffi::lua_Integer);
        let ltype = ffi::lua_type(state, -1);
        match ltype {
            ffi::LUA_TSTRING => {
                let slc = laux::check_slice(state, -1);
                writer.write_slice(slc);
            }
            ffi::LUA_TNUMBER => {
                if unsafe { ffi::lua_isinteger(state, -1) } != 0 {
                    writer.write_chars(ffi::lua_tointeger(state, -1));
                } else {
                    writer.write_chars(ffi::lua_tonumber(state, -1));
                }
            }
            ffi::LUA_TBOOLEAN => {
                if ffi::lua_toboolean(state, -1) != 0 {
                    writer.write_slice(JSON_TRUE.as_bytes());
                } else {
                    writer.write_slice(JSON_FALSE.as_bytes());
                }
            }
            ffi::LUA_TTABLE => {
                if let Err(err) = encode_one(state, writer.as_mut_vec(), -1, 0, false) {
                    drop(writer);
                    laux::lua_error(state, &err.to_string());
                }
            }
            _ => {}
        }
    }

    ffi::lua_pushlightuserdata(state, Box::into_raw(writer) as *mut c_void);

    1
}

pub unsafe extern "C-unwind" fn luaopen_json(state: *mut ffi::lua_State) -> c_int {
    let l = [
        lreg!("decode", ffi::lua_json_decode),
        lreg!("decode_v2", decode),
        lreg!("encode", encode),
        lreg!("concat", concat),
        lreg_null!(),
    ];

    ffi::lua_createtable(state, 0, l.len() as c_int);
    ffi::luaL_setfuncs(state, l.as_ptr(), 0);

    1
}
