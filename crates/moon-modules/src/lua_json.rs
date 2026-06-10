use moon_lua::{
    self, cstr,
    ffi::{self, luaL_Reg},
    laux::{self, LuaState, LuaTable, LuaType, LuaValue},
    lreg, lreg_null,
};
use serde_json::Value;
use std::{
    ffi::{c_char, c_int, c_void},
    fs::File,
    io::Read,
};

use moon_runtime::buffer::{BUFFER_HEAD_RESERVE, Buffer};

const JSON_NULL: &[u8] = b"null";
const JSON_TRUE: &[u8] = b"true";
const JSON_FALSE: &[u8] = b"false";
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

const JSON_OBJECT_META: *const c_char = cstr!("__json_object");
const JSON_ARRAY_META: *const c_char = cstr!("__json_array");

const DEFAULT_CONCAT_BUFFER_SIZE: usize = 512;
const MIN_CONCAT_BUFFER_SIZE: usize = 16;

pub struct JsonOptions {
    empty_as_array: bool,
    enable_number_key: bool,
    enable_sparse_array: bool,
    has_metatfield: bool,
    concat_buffer_size: usize,
}

impl Default for JsonOptions {
    fn default() -> Self {
        Self {
            empty_as_array: true,
            enable_number_key: true,
            enable_sparse_array: true,
            has_metatfield: true,
            concat_buffer_size: DEFAULT_CONCAT_BUFFER_SIZE,
        }
    }
}

extern "C-unwind" fn set_options(state: LuaState) -> i32 {
    let options = fetch_options(state);
    let key = unsafe { laux::lua_check_str(state, 1) };
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
        "has_metatfield" => {
            let v = options.has_metatfield;
            options.has_metatfield = laux::lua_opt(state, 2).unwrap_or(true);
            laux::lua_push(state, v);
        }
        "concat_buffer_size" => {
            let new_size: usize = laux::lua_get(state, 2);
            if new_size < MIN_CONCAT_BUFFER_SIZE {
                laux::lua_error(
                    state,
                    format!(
                        "bad argument #2 (concat_buffer_size must be >= {})",
                        MIN_CONCAT_BUFFER_SIZE
                    ),
                );
            }
            let v = options.concat_buffer_size;
            options.concat_buffer_size = new_size;
            laux::lua_push(state, v as ffi::lua_Integer);
        }
        _ => {
            laux::lua_error(state, format!("invalid json option key: {}", key));
        }
    }

    1
}

pub fn fetch_options(state: LuaState) -> &'static mut JsonOptions {
    let opts = laux::lua_touserdata::<JsonOptions>(state, ffi::lua_upvalueindex(1));
    if opts.is_none() {
        laux::lua_error(state, "expect json options".to_string());
    }
    opts.unwrap()
}

/// Write `val` as a JSON string literal (surrounding quotes included), escaping
/// control characters and JSON metacharacters per `CHAR2ESCAPE`.
pub fn write_json_string(writer: &mut Vec<u8>, val: &[u8]) {
    writer.reserve(val.len() * 6 + 2);
    writer.push(b'\"');
    for ch in val {
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

pub fn encode_one(
    writer: &mut Vec<u8>,
    val: LuaValue,
    depth: i32,
    fmt: bool,
    options: &JsonOptions,
) -> Result<(), String> {
    match val {
        LuaValue::Boolean(val) => {
            if val {
                writer.extend_from_slice(JSON_TRUE);
            } else {
                writer.extend_from_slice(JSON_FALSE);
            }
        }
        LuaValue::Number(val) => {
            // NaN / +-Inf have no JSON representation; emitting `val.to_string()`
            // here would produce `NaN`/`inf`, i.e. invalid JSON that no decoder
            // accepts. Surface a hard error instead of writing garbage.
            if !val.is_finite() {
                return Err(
                    "json encode: cannot encode a non-finite number (NaN or Infinity)".to_string(),
                );
            }
            writer.extend_from_slice(val.to_string().as_bytes());
        }
        LuaValue::Integer(val) => writer.extend_from_slice(val.to_string().as_bytes()),
        LuaValue::String(val) => {
            write_json_string(writer, val);
        }
        LuaValue::Table(val) => {
            encode_table(writer, &val, depth, fmt, options)?;
        }
        LuaValue::Nil => {
            writer.extend_from_slice(JSON_NULL);
        }
        LuaValue::LightUserData(val) => {
            if val.is_null() {
                writer.extend_from_slice(JSON_NULL);
            } else {
                // A non-null lightuserdata has no JSON representation. Writing
                // nothing would desync the separators already emitted by the
                // array/object caller (e.g. produce `[1,,3]`), so fail instead.
                return Err("json encode: cannot encode a non-null lightuserdata value".to_string());
            }
        }
        val => {
            return Err(format!("json encode: unsupport value type :{}", val.name()));
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

fn encode_array(
    writer: &mut Vec<u8>,
    table: &LuaTable,
    size: usize,
    depth: i32,
    fmt: bool,
    options: &JsonOptions,
) -> Result<(), String> {
    let bsize = writer.len();
    writer.push(b'[');

    for (i, val) in table.expected_array_iter(size).enumerate() {
        if i == 0 {
            format_new_line(writer, fmt);
        } else {
            writer.push(b',');
        }
        format_space(writer, fmt, depth);

        if let LuaValue::Nil = val
            && !options.enable_sparse_array
        {
            writer.truncate(bsize);
            return encode_object(writer, table, depth, fmt, options, false);
        }
        encode_one(writer, val, depth, fmt, options)?;
        format_new_line(writer, fmt)
    }
    format_space(writer, fmt, depth - 1);
    writer.push(b']');
    Ok(())
}

fn encode_object(
    writer: &mut Vec<u8>,
    table: &LuaTable,
    depth: i32,
    fmt: bool,
    options: &JsonOptions,
    force_object: bool,
) -> Result<(), String> {
    let mut i = 0;
    writer.push(b'{');

    for (key, value) in table.iter() {
        if i > 0 {
            writer.push(b',');
        }
        i += 1;
        format_new_line(writer, fmt);

        match key {
            LuaValue::String(key) => {
                format_space(writer, fmt, depth);
                // Keys must be escaped exactly like string values: a key
                // containing `"`, `\`, or control characters would otherwise
                // produce invalid JSON.
                write_json_string(writer, key);
                writer.push(b':');
                if fmt {
                    writer.push(b' ');
                }
                encode_one(writer, value, depth, fmt, options)?;
            }
            LuaValue::Integer(key) => {
                if options.enable_number_key {
                    format_space(writer, fmt, depth);
                    writer.push(b'\"');
                    writer.extend_from_slice(key.to_string().as_bytes());
                    writer.extend_from_slice(b"\":");
                    if fmt {
                        writer.push(b' ');
                    }
                    encode_one(writer, value, depth, fmt, options)?;
                } else {
                    return Err("json encode: unsupport number key type.".to_string());
                }
            }
            // The separator/newline above are emitted before this match, so a
            // key we can't serialize would otherwise leave a dangling comma and
            // produce invalid JSON. Fail instead of silently skipping it.
            _ => {
                return Err("json encode: unsupported table key type (only string/integer keys are allowed).".to_string());
            }
        }
    }

    if i == 0 && options.empty_as_array && !force_object {
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

pub fn encode_table(
    writer: &mut Vec<u8>,
    table: &LuaTable,
    depth: i32,
    fmt: bool,
    options: &JsonOptions,
) -> Result<(), String> {
    let depth = depth + 1;
    if depth > 64 {
        return Err("json encode: too depth".to_string());
    }

    laux::lua_checkstack(table.lua_state(), 6, cstr!("json.encode.table"));

    if options.has_metatfield {
        if table.getmetafield(JSON_ARRAY_META).is_some() {
            let size = table.len();
            return encode_array(writer, table, size, depth, fmt, options);
        }

        if table.getmetafield(JSON_OBJECT_META).is_some() {
            return encode_object(writer, table, depth, fmt, options, true);
        }
    }

    let arr_size = table.array_len();
    if arr_size > 0 {
        encode_array(writer, table, arr_size, depth, fmt, options)?;
    } else {
        encode_object(writer, table, depth, fmt, options, false)?;
    }

    Ok(())
}

extern "C-unwind" fn encode(state: LuaState) -> i32 {
    unsafe { ffi::luaL_checkany(state.as_ptr(), 1) };

    {
        let options = fetch_options(state);
        let fmt: bool = laux::lua_opt(state, 2).unwrap_or_default();
        let mut writer = Vec::with_capacity(options.concat_buffer_size);
        match encode_one(&mut writer, LuaValue::from_stack(state, 1), 0, fmt, options) {
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

fn decode_one(
    state: LuaState,
    val: &Value,
    depth: usize,
    options: &JsonOptions,
) -> Result<(), String> {
    if depth > 64 {
        return Err("json decode: too deep".to_string());
    }
    match val {
        Value::Object(map) => {
            laux::lua_checkstack(state, 6, cstr!("json.decode.object"));
            let table = LuaTable::new(state, 0, map.len());
            for (k, v) in map {
                if !k.is_empty() {
                    let c = k.as_bytes()[0];
                    if (c.is_ascii_digit() || c == b'-') && options.enable_number_key {
                        if let Ok(n) = k.parse::<ffi::lua_Integer>() {
                            laux::lua_push(state, n);
                        } else {
                            laux::lua_push(state, k.as_str());
                        }
                    } else {
                        laux::lua_push(state, k.as_str());
                    }
                    decode_one(state, v, depth + 1, options)?;
                    table.insert_from_stack();
                }
            }
            if options.has_metatfield {
                set_json_metatable(state, -1, JSON_OBJECT_META);
            }
        }
        Value::Array(arr) => {
            laux::lua_checkstack(state, 6, cstr!("json.decode.array"));
            let table = LuaTable::new(state, arr.len(), 0);
            for (i, v) in arr.iter().enumerate() {
                decode_one(state, v, depth + 1, options)?;
                table.rawseti(i + 1);
            }
            if options.has_metatfield {
                set_json_metatable(state, -1, JSON_ARRAY_META);
            }
        }
        Value::Bool(b) => {
            laux::lua_push(state, *b);
        }
        Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                laux::lua_push(state, i);
            } else if let Some(f) = n.as_f64() {
                // `u64` values above `i64::MAX` (and genuine floats) don't fit a
                // Lua integer. Represent them as a float instead of silently
                // collapsing to 0 via `as_i64().unwrap_or_default()`.
                laux::lua_push(state, f);
            } else {
                return Err(format!("json decode: number out of range: {}", n));
            }
        }
        Value::Null => {
            laux::lua_pushlightuserdata(state, std::ptr::null_mut());
        }
        Value::String(s) => {
            laux::lua_push(state, s.as_str());
        }
    }
    Ok(())
}

/// Parse `data` as JSON and push the resulting Lua value.
///
/// Returns `Err` (never longjmps) so callers can drop owned resources (the parsed
/// `Value`, and for `decode_file` the `File`/`contents`) before raising the Lua
/// error. The parsed `Value` is local to this function and dropped on every path.
fn decode_bytes(state: LuaState, data: &[u8], options: &JsonOptions) -> Result<i32, String> {
    let val: Value = serde_json::from_slice(data).map_err(|e| format!("json.decode: {}", e))?;
    decode_one(state, &val, 0, options)?;
    Ok(1)
}

extern "C-unwind" fn decode(state: LuaState) -> i32 {
    let options = fetch_options(state);
    let str = unsafe { laux::lua_check_lstring(state, 1) };
    if str.is_empty() {
        laux::lua_pushnil(state);
        return 1;
    }

    match decode_bytes(state, str, options) {
        Ok(n) => n,
        Err(e) => {
            laux::lua_push(state, e);
            laux::throw_error(state)
        }
    }
}

/// Read the file at the given path and decode its contents as JSON.
///
/// File access is intentionally a *separate*, explicit API: `decode` only ever
/// parses in-memory bytes, so JSON arriving from a network/client source can
/// never be coerced into reading arbitrary files. Only call `decode_file` with
/// a trusted, caller-controlled path.
extern "C-unwind" fn decode_file(state: LuaState) -> i32 {
    let options = fetch_options(state);
    let path = unsafe { laux::lua_check_str(state, 1) };

    // Read + parse with all owned resources (the `File`, the `contents` Vec, and
    // the parsed `Value`) confined to this closure, so they are dropped *before*
    // any longjmp below — a longjmp would otherwise skip their `Drop` and leak the
    // file descriptor and buffers.
    let parsed: Result<Value, String> = (|| {
        let mut file = File::open(path).map_err(|e| format!("json.decode: {}", e))?;
        let mut contents = Vec::new();
        file.read_to_end(&mut contents)
            .map_err(|e| format!("json.decode: {}", e))?;
        serde_json::from_slice::<Value>(&contents).map_err(|e| format!("json.decode: {}", e))
    })();

    match parsed {
        Ok(val) => match decode_one(state, &val, 0, options) {
            Ok(()) => 1,
            Err(e) => {
                drop(val);
                laux::lua_push(state, e);
                laux::throw_error(state)
            }
        },
        Err(e) => {
            laux::lua_push(state, e);
            laux::throw_error(state)
        }
    }
}

extern "C-unwind" fn concat(state: LuaState) -> i32 {
    let options = fetch_options(state);

    if laux::lua_type(state, 1) == LuaType::String {
        let slc = unsafe { laux::lua_to_lstring(state, 1) };
        let mut buf = Box::new(Buffer::with_capacity(slc.len() + BUFFER_HEAD_RESERVE));
        let _ = buf.commit(BUFFER_HEAD_RESERVE);
        buf.write_slice(slc);
        buf.seek(BUFFER_HEAD_RESERVE as isize);
        laux::lua_pushlightuserdata(state, Box::into_raw(buf) as *mut c_void);
        return 1;
    }

    laux::lua_checktype(state, 1, ffi::LUA_TTABLE);
    laux::lua_settop(state, 1);

    let mut writer = Box::new(Buffer::new());
    let _ = writer.commit(BUFFER_HEAD_RESERVE);
    let mut has_error = false;

    for val in LuaTable::from_stack(state, 1).array_iter() {
        match val {
            LuaValue::String(val) => writer.write_slice(val),
            LuaValue::Number(val) => writer.write_chars(val),
            LuaValue::Integer(val) => writer.write_chars(val),
            LuaValue::Boolean(val) => {
                if val {
                    writer.write_slice(JSON_TRUE);
                } else {
                    writer.write_slice(JSON_FALSE);
                }
            }
            LuaValue::Table(val) => {
                if let Err(err) = encode_table(writer.as_mut_vec(), &val, 0, false, options) {
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
                        "json.concat: unsupport value type :{}",
                        laux::type_name(state, -1)
                    ),
                );
                break;
            }
        }
    }

    if has_error {
        drop(writer);
        laux::throw_error(state);
    }

    writer.seek(BUFFER_HEAD_RESERVE as isize);
    laux::lua_pushlightuserdata(state, Box::into_raw(writer) as *mut c_void);

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
fn write_resp(writer: &mut Buffer, cmd: &[u8]) {
    writer.write_slice(b"\r\n$");
    writer.write_chars(cmd.len());
    writer.write_slice(b"\r\n");
    writer.write_slice(cmd);
}

fn concat_resp_one(
    writer: &mut Buffer,
    value: LuaValue,
    options: &JsonOptions,
) -> Result<(), String> {
    match value {
        LuaValue::Nil => {
            writer.write_slice(b"\r\n$-1");
        }
        LuaValue::Number(val) => write_resp(writer, val.to_string().as_bytes()),
        LuaValue::Integer(val) => write_resp(writer, val.to_string().as_bytes()),
        LuaValue::Boolean(val) => write_resp(writer, if val { JSON_TRUE } else { JSON_FALSE }),
        LuaValue::String(val) => {
            writer.write_slice(b"\r\n$");
            writer.write_chars(val.len());
            writer.write_slice(b"\r\n");
            writer.write_slice(val);
        }

        LuaValue::Table(val) => {
            if val.getmetafield(cstr!("__redis")).is_some() {
                for value in val.array_iter() {
                    concat_resp_one(writer, value, options)?;
                }
            } else {
                let mut w = Buffer::new();
                encode_table(w.as_mut_vec(), &val, 0, false, options)?;
                write_resp(writer, w.as_slice());
            }
        }
        val => {
            return Err(format!(
                "concat_resp_one: unsupport value type :{}",
                val.name()
            ));
        }
    }

    Ok(())
}

extern "C-unwind" fn concat_resp(state: LuaState) -> i32 {
    let n = laux::lua_top(state);
    if n == 0 {
        return 0;
    }

    let options = fetch_options(state);

    let mut writer = Box::new(Buffer::new());
    let _ = writer.commit(BUFFER_HEAD_RESERVE);
    let mut has_error = false;
    let mut hash = 1;
    if laux::lua_type(state, 2) != LuaType::Table
        && let Some(key) = unsafe { laux::lua_opt_str(state, 1) }
        && !key.is_empty()
    {
        let hash_part = if n > 2 && (key.starts_with('h') || key.starts_with('H')) {
            unsafe { laux::lua_opt_str(state, 3) }
        } else if n > 1 {
            unsafe { laux::lua_opt_str(state, 2) }
        } else {
            None
        };

        if let Some(part) = hash_part {
            hash = hash_string(part);
        }
    }

    writer.write(b'*');
    writer.write_chars(n);
    for i in 1..=n {
        if let Err(err) = concat_resp_one(&mut writer, LuaValue::from_stack(state, i), options) {
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

    writer.seek(BUFFER_HEAD_RESERVE as isize);
    laux::lua_pushlightuserdata(state, Box::into_raw(writer) as *mut c_void);
    laux::lua_push(state, (hash as ffi::lua_Integer).abs());

    2
}

fn set_json_metatable(state: LuaState, table_idx: i32, meta_key: *const c_char) {
    unsafe {
        let abs_idx = if table_idx < 0 {
            ffi::lua_gettop(state.as_ptr()) + table_idx + 1
        } else {
            table_idx
        };
        if ffi::luaL_newmetatable(state.as_ptr(), meta_key) != 0 {
            laux::lua_push(state, true);
            ffi::lua_setfield(state.as_ptr(), -2, meta_key);
        }
        ffi::lua_setmetatable(state.as_ptr(), abs_idx);
    }
}

extern "C-unwind" fn json_object(state: LuaState) -> c_int {
    if laux::lua_type(state, 1) == LuaType::Table {
        set_json_metatable(state, 1, JSON_OBJECT_META);
        unsafe { ffi::lua_settop(state.as_ptr(), 1) };
    } else {
        let nrec: i32 = laux::lua_opt(state, 1).unwrap_or(0);
        unsafe { ffi::lua_createtable(state.as_ptr(), 0, nrec) };
        set_json_metatable(state, -1, JSON_OBJECT_META);
    }
    1
}

extern "C-unwind" fn json_array(state: LuaState) -> c_int {
    if laux::lua_type(state, 1) == LuaType::Table {
        set_json_metatable(state, 1, JSON_ARRAY_META);
        unsafe { ffi::lua_settop(state.as_ptr(), 1) };
    } else {
        let narr: i32 = laux::lua_opt(state, 1).unwrap_or(0);
        unsafe { ffi::lua_createtable(state.as_ptr(), narr, 0) };
        set_json_metatable(state, -1, JSON_ARRAY_META);
    }
    1
}

pub extern "C-unwind" fn luaopen_json(state: LuaState) -> i32 {
    let l = [
        lreg!("decode", decode),
        lreg!("decode_file", decode_file),
        lreg!("encode", encode),
        lreg!("concat", concat),
        lreg!("concat_resp", concat_resp),
        lreg!("options", set_options),
        lreg!("object", json_object),
        lreg!("array", json_array),
        lreg_null!(),
    ];

    unsafe {
        ffi::lua_createtable(state.as_ptr(), 0, l.len() as c_int);
        laux::lua_newuserdata(
            state,
            JsonOptions {
                empty_as_array: true,
                enable_number_key: true,
                enable_sparse_array: false,
                has_metatfield: true,
                concat_buffer_size: DEFAULT_CONCAT_BUFFER_SIZE,
            },
            cstr!("json_options_meta"),
            &[lreg_null!()],
        );

        ffi::luaL_setfuncs(state.as_ptr(), l.as_ptr() as *const luaL_Reg, 1);

        ffi::lua_pushlightuserdata(state.as_ptr(), std::ptr::null_mut());
        ffi::lua_setfield(state.as_ptr(), -2, cstr!("null"));
    }

    1
}
