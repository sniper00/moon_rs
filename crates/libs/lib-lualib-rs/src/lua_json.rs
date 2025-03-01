use lib_lua::{
    self, cstr,
    ffi::{self, luaL_Reg},
    laux::{self, LuaStateRaw, LuaType},
    lreg, lreg_null,
};
use serde::de::Error;
use serde_json::Value;
use std::{
    ffi::{c_char, c_int, c_void},
    fs::File,
    io::Read,
};

use lib_core::buffer::Buffer;

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

pub struct JsonOptions {
    empty_as_array: bool,
    enable_number_key: bool,
    enable_sparse_array: bool,
}

impl Default for JsonOptions {
    fn default() -> Self {
        Self {
            empty_as_array: true,
            enable_number_key: true,
            enable_sparse_array: true,
        }
    }
}

extern "C-unwind" fn set_options(state: LuaStateRaw) -> i32 {
    let options = fetch_options(state);
    let key = laux::lua_get::<&str>(state, 1);
    match key {
        "encode_empty_as_array" => {
            let v = options.empty_as_array;
            options.empty_as_array = laux::lua_opt(state, 2).unwrap_or(true);
            laux::lua_push(state, v);
        }
        "enable_number_key" => {
            let v = options.enable_number_key;
            options.enable_number_key = laux::lua_opt(state, 2).unwrap_or(true);
            laux::lua_push(state, v);
        }
        "enable_sparse_array" => {
            let v = options.enable_sparse_array;
            options.enable_sparse_array = laux::lua_opt(state, 2).unwrap_or(false);
            laux::lua_push(state, v);
        }
        _ => {
            laux::lua_error(state, format!("invalid json option key: {}", key).as_str());
        }
    }

    1
}

pub fn fetch_options(state: LuaStateRaw) -> &'static mut JsonOptions {
    let opts = laux::lua_touserdata::<JsonOptions>(state, ffi::lua_upvalueindex(1));
    if opts.is_none() {
        laux::lua_error(state, "expect json options");
    }
    opts.unwrap()
}

#[allow(clippy::not_unsafe_ptr_arg_deref)]
pub fn encode_one(
    state: *mut ffi::lua_State,
    writer: &mut Vec<u8>,
    idx: i32,
    depth: i32,
    fmt: bool,
    options: &JsonOptions,
) -> Result<(), String> {
    let t = laux::lua_type(state, idx);
    match t {
        LuaType::Boolean => {
            if laux::lua_to::<bool>(state, idx) {
                writer.extend_from_slice(JSON_TRUE.as_bytes());
            } else {
                writer.extend_from_slice(JSON_FALSE.as_bytes());
            }
        }
        LuaType::Number => {
            if laux::lua_isinteger(state, idx) {
                let n = laux::lua_to::<i64>(state, idx);
                writer.extend_from_slice(n.to_string().as_bytes());
            } else {
                let n = laux::lua_to::<f64>(state, idx);
                writer.extend_from_slice(n.to_string().as_bytes());
            }
        }
        LuaType::String => {
            let str = laux::lua_to::<&[u8]>(state, idx);
            writer.reserve(str.len() * 6 + 2);
            writer.push(b'\"');
            for ch in str {
                let esc = CHAR2ESCAPE[*ch as usize];
                if esc == 0 {
                    writer.push(*ch);
                } else {
                    writer.push(b'\\');
                    writer.push(esc);
                    if esc == b'u' {
                        writer.push(b'0');
                        writer.push(b'0');
                        writer.push(HEX_DIGITS[(*ch >> 4) as usize & 0xF]);
                        writer.push(HEX_DIGITS[*ch as usize & 0xF]);
                    }
                }
            }

            writer.push(b'\"');
        }
        LuaType::Table => {
            encode_table(state, writer, idx, depth, fmt, options)?;
        }
        LuaType::Nil => {
            writer.extend_from_slice(JSON_NULL.as_bytes());
        }
        LuaType::LightUserData => unsafe {
            if ffi::lua_touserdata(state, idx).is_null() {
                writer.extend_from_slice(JSON_NULL.as_bytes());
            }
        },
        ltype => {
            return Err(format!(
                "json encode: unsupport value type :{}",
                laux::type_name(state, ltype.into())
            ));
        }
    }

    Ok(())
}

#[inline]
fn format_new_line(writer: &mut Vec<u8>, fmt: bool) {
    if fmt {
        writer.push(b'\n');
    }
}

#[inline]
fn format_space(writer: &mut Vec<u8>, fmt: bool, n: i32) {
    if fmt {
        for _ in 0..n {
            writer.push(b' ');
            writer.push(b' ');
        }
    }
}

unsafe fn encode_array(
    state: *mut ffi::lua_State,
    size: usize,
    writer: &mut Vec<u8>,
    idx: i32,
    depth: i32,
    fmt: bool,
    options: &JsonOptions,
) -> Result<(), String> {
    let bsize = writer.len();
    writer.push(b'[');
    for i in 1..=size {
        if i == 1 {
            format_new_line(writer, fmt);
        }
        format_space(writer, fmt, depth);
        ffi::lua_rawgeti(state, idx, i as ffi::lua_Integer);
        if ffi::lua_isnil(state, -1) != 0 && !options.enable_sparse_array {
            ffi::lua_pop(state, 1);
            writer.truncate(bsize);
            return encode_object(state, writer, idx, depth, fmt, options);
        }
        encode_one(state, writer, -1, depth, fmt, options)?;
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

unsafe fn encode_object(
    state: *mut ffi::lua_State,
    writer: &mut Vec<u8>,
    idx: i32,
    depth: i32,
    fmt: bool,
    options: &JsonOptions,
) -> Result<(), String> {
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
                writer.extend_from_slice(laux::lua_get::<&str>(state, -2).as_bytes());
                writer.extend_from_slice(b"\":");
                if fmt {
                    writer.push(b' ');
                }
                encode_one(state, writer, -1, depth, fmt, options)?;
            }
            ffi::LUA_TNUMBER => {
                if ffi::lua_isinteger(state, -2) != 0 && options.enable_number_key {
                    format_space(writer, fmt, depth);
                    let key = ffi::lua_tointeger(state, -2);
                    writer.push(b'\"');
                    writer.extend_from_slice(key.to_string().as_bytes());
                    writer.extend_from_slice(b"\":");
                    if fmt {
                        writer.push(b' ');
                    }
                    encode_one(state, writer, -1, depth, fmt, options)?;
                } else {
                    return Err("json encode: unsupport number key type.".to_string());
                }
            }
            _ => {}
        }
        ffi::lua_pop(state, 1);
    }

    if i == 0 && options.empty_as_array {
        writer.pop();
        writer.extend_from_slice(b"[]");
    } else {
        if i > 0 {
            format_new_line(writer, fmt);
            format_space(writer, fmt, depth - 1);
        }
        writer.push(b'}');
    }

    Ok(())
}

fn encode_table(
    state: *mut ffi::lua_State,
    writer: &mut Vec<u8>,
    mut idx: i32,
    depth: i32,
    fmt: bool,
    options: &JsonOptions,
) -> Result<(), String> {
    let depth = depth + 1;
    if depth > 64 {
        return Err("json encode: too depth".to_string());
    }

    if idx < 0 {
        idx = laux::lua_top(state) + idx + 1;
    }

    unsafe {
        ffi::luaL_checkstack(state, 6, cstr!("json.encode.table"));
        let arr_size = lua_array_size(state, idx);
        if arr_size > 0 {
            encode_array(state, arr_size, writer, idx, depth, fmt, options)?;
        } else {
            encode_object(state, writer, idx, depth, fmt, options)?;
        }
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
    ffi::luaL_checkany(state, 1);

    {
        let options = fetch_options(state);
        let fmt: bool = laux::lua_opt(state, 2).unwrap_or_default();
        let mut writer = Vec::new();
        match encode_one(state, &mut writer, 1, 0, fmt, options) {
            Ok(_) => {
                laux::lua_push(state, writer.as_slice());
                return 1;
            }
            Err(err) => {
                laux::lua_push(state, err.to_string());
            }
        }
    }

    laux::throw_error(state)
}

#[inline]
unsafe fn decode_one(state: *mut ffi::lua_State, val: &Value, options: &JsonOptions) {
    match val {
        Value::Object(map) => {
            ffi::luaL_checkstack(state, 6, cstr!("json.decode.object"));
            ffi::lua_createtable(state, 0, map.len() as i32);
            for (k, v) in map {
                if !k.is_empty() {
                    let c = k.as_bytes()[0];
                    if (c.is_ascii_digit() || c == b'-') && options.enable_number_key {
                        if let Ok(n) = k.parse::<ffi::lua_Integer>() {
                            //try convert k to integer
                            ffi::lua_pushinteger(state, n);
                        } else {
                            ffi::lua_pushlstring(state, k.as_ptr() as *const c_char, k.len());
                        }
                    } else {
                        ffi::lua_pushlstring(state, k.as_ptr() as *const c_char, k.len());
                    }
                    decode_one(state, v, options);
                    ffi::lua_rawset(state, -3);
                }
            }
        }
        Value::Array(arr) => {
            ffi::luaL_checkstack(state, 6, cstr!("json.decode.array"));
            ffi::lua_createtable(state, arr.len() as i32, 0);
            for (i, v) in arr.iter().enumerate() {
                decode_one(state, v, options);
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
}

extern "C-unwind" fn decode(state: *mut ffi::lua_State) -> c_int {
    let options = fetch_options(state);
    let str: &[u8] = laux::lua_get(state, 1);

    // Handle JSON decoding errors
    fn handle_error(state: *mut ffi::lua_State, e: serde_json::Error) -> c_int {
        laux::lua_pushnil(state);
        laux::lua_push(state, e.to_string());
        2
    }

    // Decode JSON data
    let result = if !str.is_empty() && str[0] == b'@' {
        match std::str::from_utf8(&str[1..]) {
            Ok(path) => {
                let mut file = match File::open(path) {
                    Ok(file) => file,
                    Err(e) => return handle_error(state, serde_json::Error::custom(e.to_string())),
                };
                let mut contents = Vec::new();
                if let Err(e) = file.read_to_end(&mut contents) {
                    return handle_error(state, serde_json::Error::custom(e.to_string()));
                }
                serde_json::from_slice::<Value>(&contents)
            }
            Err(e) => return handle_error(state, serde_json::Error::custom(e.to_string())),
        }
    } else {
        serde_json::from_slice::<Value>(str)
    };

    match result {
        Ok(val) => {
            unsafe {
                decode_one(state, &val, options);
            }
            1
        }
        Err(e) => handle_error(state, e),
    }
}

unsafe extern "C-unwind" fn concat(state: *mut ffi::lua_State) -> c_int {
    let options = fetch_options(state);

    if laux::lua_type(state, 1) == LuaType::String {
        let slc = laux::lua_get::<&[u8]>(state, 1);
        let mut buf = Box::new(Buffer::with_capacity(slc.len()));
        buf.write_slice(slc);
        laux::lua_pushlightuserdata(state, Box::into_raw(buf) as *mut c_void);
        return 1;
    }
    ffi::luaL_checktype(state, 1, ffi::LUA_TTABLE);

    ffi::lua_settop(state, 1);

    let mut writer = Box::new(Buffer::new());
    let array_len = ffi::lua_rawlen(state, 1);
    let mut has_error = false;

    for i in 1..=array_len {
        ffi::lua_rawgeti(state, 1, i as ffi::lua_Integer);
        let ltype = ffi::lua_type(state, -1);
        match ltype {
            ffi::LUA_TSTRING => {
                let slc = laux::lua_get::<&[u8]>(state, -1);
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
                if let Err(err) = encode_one(state, writer.as_mut_vec(), -1, 0, false, options) {
                    has_error = true;
                    laux::lua_push(state, err);
                    break;
                }
            }
            _ => {
                has_error = true;
                laux::lua_push(
                    state,
                    format!(
                        "concat: unsupport value type :{}",
                        laux::type_name(state, ltype)
                    ),
                );
                break;
            }
        }
        laux::lua_pop(state, 1);
    }

    if has_error {
        drop(writer);
        laux::throw_error(state);
    }

    ffi::lua_pushlightuserdata(state, Box::into_raw(writer) as *mut c_void);

    1
}

pub fn hash_combine_u64(h: &mut u64, k: u64) {
    let m = 0xc6a4a7935bd1e995;
    let r = 47;

    let mut k = k;
    k = k.wrapping_mul(m);
    k ^= k >> r;
    k = k.wrapping_mul(m);

    *h ^= k;
    *h = h.wrapping_mul(m);

    // Completely arbitrary number, to prevent 0's
    // from hashing to 0.
    *h = h.wrapping_add(0xe6546b64);
}

fn hash_string(s: &str) -> u64 {
    let mut seed = 0;
    let mut basis: u64 = 14695981039346656037;
    for b in s.bytes() {
        basis ^= b as u64;
        basis = basis.wrapping_mul(1099511628211);
        hash_combine_u64(&mut seed, basis);
    }
    seed
}

#[inline]
fn write_resp(writer: &mut Buffer, cmd: &str) {
    writer.write_slice(b"\r\n$");
    writer.write_chars(cmd.len());
    writer.write_slice(b"\r\n");
    writer.write_str(cmd);
}

fn concat_resp_one(
    writer: &mut Buffer,
    state: *mut ffi::lua_State,
    index: i32,
    options: &JsonOptions,
) -> Result<(), String> {
    // let lua = LuaStateRef::new(state);
    let ltype = laux::lua_type(state, index);
    match ltype {
        LuaType::Nil => {
            writer.write_slice(b"\r\n$-1");
        }
        LuaType::Number => {
            if laux::is_integer(state, index) {
                write_resp(
                    writer,
                    laux::lua_to::<i64>(state, index).to_string().as_str(),
                );
            } else {
                write_resp(
                    writer,
                    laux::lua_to::<f64>(state, index).to_string().as_str(),
                );
            }
        }
        LuaType::Boolean => {
            if laux::lua_opt::<bool>(state, index).unwrap_or_default() {
                write_resp(writer, JSON_TRUE);
            } else {
                write_resp(writer, JSON_FALSE);
            }
        }
        LuaType::String => {
            let slc = laux::lua_to(state, index);
            write_resp(writer, slc);
        }
        LuaType::Table => {
            if unsafe { ffi::luaL_getmetafield(state, index, cstr!("__redis")) != ffi::LUA_TNIL } {
                laux::lua_pop(state, 1);
                let size = laux::lua_rawlen(state, index);
                for n in 1..=size {
                    laux::lua_rawgeti(state, index, n);
                    concat_resp_one(writer, state, -1, options)?;
                    laux::lua_pop(state, 1);
                }
            } else {
                let mut w = Buffer::new();
                encode_one(state, w.as_mut_vec(), index, 0, false, options)?;
                write_resp(writer, w.as_str());
            }
        }
        _ => {
            return Err(format!(
                "concat_resp_one: unsupport value type :{}",
                laux::type_name(state, ltype.into())
            ));
        }
    }

    Ok(())
}

extern "C-unwind" fn concat_resp(state: *mut ffi::lua_State) -> c_int {
    let n = laux::lua_top(state);
    if n == 0 {
        return 0;
    }

    let options = fetch_options(state);

    let mut writer = Box::new(Buffer::new());
    let mut has_error = false;
    let mut hash = 1;
    if laux::lua_type(state, 2) != LuaType::Table {
        if let Some(key) = laux::lua_opt::<&str>(state, 1) {
            if !key.is_empty() {
                let hash_part = if n > 2 && (key.starts_with('h') || key.starts_with('H')) {
                    laux::lua_opt::<&str>(state, 3)
                } else if n > 1 {
                    laux::lua_opt::<&str>(state, 2)
                } else {
                    None
                };

                if let Some(part) = hash_part {
                    hash = hash_string(part);
                }
            }
        }
    }

    writer.write(b'*');
    writer.write_chars(n);
    for i in 1..=n {
        if let Err(err) = concat_resp_one(&mut writer, state, i, options) {
            has_error = true;
            laux::lua_push(state, err);
            break;
        }
    }

    if has_error {
        drop(writer);
        laux::throw_error(state);
    }

    writer.write_slice(b"\r\n");

    laux::lua_pushlightuserdata(state, Box::into_raw(writer) as *mut c_void);
    laux::lua_push(state, (hash as ffi::lua_Integer).abs());

    2
}

/// # Safety
///
/// This function is unsafe because it dereferences a raw pointer `state`.
/// The caller must ensure that `state` is a valid pointer to a `lua_State`
/// and that it remains valid for the duration of the function call.
#[no_mangle]
#[allow(clippy::not_unsafe_ptr_arg_deref)]
pub unsafe extern "C-unwind" fn luaopen_json(state: *mut ffi::lua_State) -> c_int {
    let l = [
        lreg!("decode", decode),
        lreg!("encode", encode),
        lreg!("concat", concat),
        lreg!("concat_resp", concat_resp),
        lreg!("options", set_options),
        lreg_null!(),
    ];

    ffi::lua_createtable(state, 0, l.len() as c_int);
    laux::lua_newuserdata(
        state,
        JsonOptions {
            empty_as_array: true,
            enable_number_key: true,
            enable_sparse_array: false,
        },
        cstr!("json_options_meta"),
        &[lreg_null!()],
    );

    ffi::luaL_setfuncs(state, l.as_ptr(), 1);

    ffi::lua_pushlightuserdata(state, std::ptr::null_mut());
    ffi::lua_setfield(state, -2, cstr!("null"));

    1
}
