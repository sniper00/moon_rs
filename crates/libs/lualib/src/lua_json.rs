use luars::{LuaApi, LuaResult, LuaState, LuaTable, LuaValue, LuaValueKind, Table};
use serde_json::Value;
use std::{cell::RefCell, ffi::c_void, fs::File, io::Read};

use actor::buffer::Buffer;

use crate::{
    lua_array_size, lua_check_bytes, lua_check_integer, lua_check_str, lua_check_value,
    lua_opt_boolean, lua_push_error,
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

pub struct JsonOptions {
    pub empty_as_array: bool,
    pub enable_number_key: bool,
    pub enable_sparse_array: bool,
    pub has_metatfield: bool,
    pub concat_buffer_size: usize,
}

const JSON_OBJECT_META: &str = "__json_object";
const JSON_ARRAY_META: &str = "__json_array";
const DEFAULT_CONCAT_BUFFER_SIZE: usize = 512;
const MIN_CONCAT_BUFFER_SIZE: usize = 16;

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

thread_local! {
    static JSON_OPTIONS: RefCell<JsonOptions> = const { RefCell::new(JsonOptions {
        empty_as_array: true,
        enable_number_key: true,
        enable_sparse_array: false,
        has_metatfield: true,
        concat_buffer_size: DEFAULT_CONCAT_BUFFER_SIZE,
    }) };
}

fn with_options<F, R>(f: F) -> R
where
    F: FnOnce(&JsonOptions) -> R,
{
    JSON_OPTIONS.with(|opts| f(&opts.borrow()))
}

fn with_options_mut<F, R>(f: F) -> R
where
    F: FnOnce(&mut JsonOptions) -> R,
{
    JSON_OPTIONS.with(|opts| f(&mut opts.borrow_mut()))
}

fn set_options(state: &mut LuaState) -> LuaResult<usize> {
    let key = lua_check_str(state, 1)?;

    let new_val_arg = lua_opt_boolean(state, 2);

    match key {
        "encode_empty_as_array" => {
            let old = with_options_mut(|opts| {
                let old = opts.empty_as_array;
                opts.empty_as_array = new_val_arg.unwrap_or(true);
                old
            });
            state.push_value(LuaValue::boolean(old))?;
        }
        "enable_number_key" => {
            let old = with_options_mut(|opts| {
                let old = opts.enable_number_key;
                opts.enable_number_key = new_val_arg.unwrap_or(true);
                old
            });
            state.push_value(LuaValue::boolean(old))?;
        }
        "enable_sparse_array" => {
            let old = with_options_mut(|opts| {
                let old = opts.enable_sparse_array;
                opts.enable_sparse_array = new_val_arg.unwrap_or(false);
                old
            });
            state.push_value(LuaValue::boolean(old))?;
        }
        "has_metatfield" => {
            let old = with_options_mut(|opts| {
                let old = opts.has_metatfield;
                opts.has_metatfield = new_val_arg.unwrap_or(true);
                old
            });
            state.push_value(LuaValue::boolean(old))?;
        }
        "concat_buffer_size" => {
            let new_size: usize = lua_check_integer(state, 2)?;
            if new_size < MIN_CONCAT_BUFFER_SIZE {
                return Err(state.error(format!(
                    "bad argument #2 (concat_buffer_size must be >= {})",
                    MIN_CONCAT_BUFFER_SIZE
                )));
            }
            let old = with_options_mut(|opts| {
                let old = opts.concat_buffer_size;
                opts.concat_buffer_size = new_size;
                old
            });
            state.push_value(LuaValue::integer(old as i64))?;
        }
        _ => {
            return Err(state.error(format!("json: invalid option key '{}'", key)));
        }
    }

    Ok(1)
}

pub fn encode_one(
    writer: &mut Vec<u8>,
    state: &mut LuaState,
    val: &LuaValue,
    depth: i32,
    fmt: bool,
    options: &JsonOptions,
) -> Result<(), String> {
    match val.kind() {
        LuaValueKind::Boolean => {
            if val.as_boolean().unwrap() {
                writer.extend_from_slice(JSON_TRUE.as_bytes());
            } else {
                writer.extend_from_slice(JSON_FALSE.as_bytes());
            }
        }
        LuaValueKind::Integer => {
            writer.extend_from_slice(val.as_integer().unwrap().to_string().as_bytes());
        }
        LuaValueKind::Float => {
            writer.extend_from_slice(val.as_number().unwrap().to_string().as_bytes());
        }
        LuaValueKind::String => {
            let s = val.as_bytes().unwrap();
            writer.reserve(s.len() * 6 + 2);
            writer.push(b'\"');
            for ch in s {
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
        LuaValueKind::Table => {
            encode_table(writer, state, val.as_table().unwrap(), depth, fmt, options)?;
        }
        LuaValueKind::Nil => {
            writer.extend_from_slice(JSON_NULL.as_bytes());
        }
        LuaValueKind::Userdata => {
            if let Some(p) = val.as_lightuserdata()
                && p.is_null()
            {
                writer.extend_from_slice(JSON_NULL.as_bytes());
            }
        }
        _ => {
            return Err(format!(
                "json encode: unsupport value type :{}",
                val.type_name()
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

fn encode_array(
    writer: &mut Vec<u8>,
    state: &mut LuaState,
    table: &LuaTable,
    size: usize,
    depth: i32,
    fmt: bool,
    options: &JsonOptions,
) -> Result<(), String> {
    let bsize = writer.len();
    writer.push(b'[');
    for i in 0..size {
        let val = table.raw_geti((i + 1) as i64).unwrap_or(LuaValue::nil());

        if i == 0 {
            format_new_line(writer, fmt);
        } else {
            writer.push(b',');
        }
        format_space(writer, fmt, depth);

        if val.is_nil() && !options.enable_sparse_array {
            writer.truncate(bsize);
            return encode_object(writer, state, table, depth, fmt, options);
        }
        encode_one(writer, state, &val, depth, fmt, options)?;
        format_new_line(writer, fmt);
    }
    format_space(writer, fmt, depth - 1);
    writer.push(b']');
    Ok(())
}

fn encode_object(
    writer: &mut Vec<u8>,
    state: &mut LuaState,
    table: &LuaTable,
    depth: i32,
    fmt: bool,
    options: &JsonOptions,
) -> Result<(), String> {
    let mut i = 0;
    writer.push(b'{');

    for (key, value) in &table.iter_all() {
        if let Some(key_str) = key.as_bytes() {
            if i > 0 {
                writer.push(b',');
            }
            i += 1;
            format_new_line(writer, fmt);
            format_space(writer, fmt, depth);
            writer.push(b'\"');
            writer.extend_from_slice(key_str);
            writer.extend_from_slice(b"\":");
            if fmt {
                writer.push(b' ');
            }
            encode_one(writer, state, value, depth, fmt, options)?;
        } else if let Some(key_int) = key.as_integer() {
            if options.enable_number_key {
                if i > 0 {
                    writer.push(b',');
                }
                i += 1;
                format_new_line(writer, fmt);
                format_space(writer, fmt, depth);
                writer.push(b'\"');
                writer.extend_from_slice(key_int.to_string().as_bytes());
                writer.extend_from_slice(b"\":");
                if fmt {
                    writer.push(b' ');
                }
                encode_one(writer, state, value, depth, fmt, options)?;
            } else {
                return Err("json encode: unsupport number key type.".to_string());
            }
        }
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

fn table_has_meta_key(state: &mut LuaState, table: &LuaTable, meta_key: &str) -> LuaResult<bool> {
    if let Some(mt) = table.get_metatable() {
        let mt = mt.as_table().unwrap();
        if let Some(_) = mt.raw_get(&state.create_string(meta_key)?) {
            return Ok(true);
        }
    }
    Ok(false)
}

pub fn encode_table(
    writer: &mut Vec<u8>,
    state: &mut LuaState,
    table: &LuaTable,
    depth: i32,
    fmt: bool,
    options: &JsonOptions,
) -> Result<(), String> {
    let depth = depth + 1;
    if depth > 64 {
        return Err("json encode: too depth".to_string());
    }

    if options.has_metatfield {
        if table_has_meta_key(state, table, JSON_ARRAY_META)
            .map_err(|e| state.get_error_message(e))?
        {
            encode_array(writer, state, table, table.len(), depth, fmt, options)?;
            return Ok(());
        }

        if table_has_meta_key(state, table, JSON_OBJECT_META)
            .map_err(|e| state.get_error_message(e))?
        {
            encode_object(writer, state, table, depth, fmt, options)?;
            return Ok(());
        }
    }

    let arr_size = lua_array_size(table);
    if arr_size > 0 {
        encode_array(writer, state, table, arr_size, depth, fmt, options)?;
    } else {
        encode_object(writer, state, table, depth, fmt, options)?;
    }

    Ok(())
}

fn encode(state: &mut LuaState) -> LuaResult<usize> {
    let val = lua_check_value(state, 1)?;
    let fmt: bool = lua_opt_boolean(state, 2).unwrap_or(false);
    encode_value(state, val, fmt)
}

fn pretty_encode(state: &mut LuaState) -> LuaResult<usize> {
    let val = lua_check_value(state, 1)?;
    encode_value(state, val, true)
}

fn encode_value(state: &mut LuaState, val: LuaValue, fmt: bool) -> LuaResult<usize> {
    let result = with_options(|options| {
        let mut writer = Vec::with_capacity(options.concat_buffer_size);
        match encode_one(&mut writer, state, &val, 0, fmt, options) {
            Ok(_) => Ok(writer),
            Err(err) => Err(err),
        }
    });

    match result {
        Ok(writer) => {
            let val = state.create_bytes(&writer)?;
            state.push_value(val)?;
            Ok(1)
        }
        Err(err) => Err(state.error(err)),
    }
}

fn decode_one(state: &mut LuaState, val: &Value, options: &JsonOptions) -> LuaResult<LuaValue> {
    match val {
        Value::Object(map) => {
            let table = state.create_table(0, map.len())?;
            if options.has_metatfield {
                set_json_metatable(state, table, JSON_OBJECT_META)?;
            }
            for (k, v) in map {
                if !k.is_empty() {
                    let c = k.as_bytes()[0];
                    let key = if (c.is_ascii_digit() || c == b'-') && options.enable_number_key {
                        if let Ok(n) = k.parse::<i64>() {
                            LuaValue::integer(n)
                        } else {
                            state.create_string(k.as_str())?
                        }
                    } else {
                        state.create_string(k.as_str())?
                    };
                    let child = decode_one(state, v, options)?;
                    state.raw_set(&table, key, child);
                }
            }
            Ok(table)
        }
        Value::Array(arr) => {
            let table = state.create_table(arr.len(), 0)?;
            if options.has_metatfield {
                set_json_metatable(state, table, JSON_ARRAY_META)?;
            }
            for (i, v) in arr.iter().enumerate() {
                let child = decode_one(state, v, options)?;
                state.raw_seti(&table, (i + 1) as i64, child);
            }
            Ok(table)
        }
        Value::Bool(b) => Ok(LuaValue::boolean(*b)),
        Value::Number(n) => {
            if n.is_f64() {
                Ok(LuaValue::float(n.as_f64().unwrap_or_default()))
            } else {
                Ok(LuaValue::integer(n.as_i64().unwrap_or_default()))
            }
        }
        Value::Null => Ok(LuaValue::lightuserdata(std::ptr::null_mut())),
        Value::String(s) => {
            let val = state.create_string(s.as_str())?;
            Ok(val)
        }
    }
}

fn decode(state: &mut LuaState) -> LuaResult<usize> {
    let str_data = lua_check_bytes(state, 1)?.to_vec();
    if str_data.is_empty() {
        return Ok(0);
    }

    let result = if str_data[0] == b'@' {
        match std::str::from_utf8(&str_data[1..]) {
            Ok(path) => {
                let mut file = match File::open(path) {
                    Ok(file) => file,
                    Err(e) => return lua_push_error(state, &e.to_string()),
                };
                let mut contents = Vec::new();
                if let Err(e) = file.read_to_end(&mut contents) {
                    return lua_push_error(state, &e.to_string());
                }
                if contents.is_empty() {
                    return Ok(0);
                }
                serde_json::from_slice::<Value>(&contents)
            }
            Err(e) => return lua_push_error(state, &e.to_string()),
        }
    } else {
        serde_json::from_slice::<Value>(&str_data)
    };

    match result {
        Ok(val) => {
            let lua_val = with_options(|opts| decode_one(state, &val, opts))?;
            state.push_value(lua_val)?;
            Ok(1)
        }
        Err(e) => Err(state.error(format!("json.decode: {}", e))),
    }
}

fn json_concat(state: &mut LuaState) -> LuaResult<usize> {
    let arg1 = state.get_arg(1).unwrap_or(LuaValue::nil());

    if arg1.is_string() {
        let slc = arg1.as_bytes().unwrap_or_default().to_vec();
        let mut buf = Box::new(Buffer::with_capacity(slc.len()));
        buf.write_slice(&slc);
        state.push_value(LuaValue::lightuserdata(Box::into_raw(buf) as *mut c_void))?;
        return Ok(1);
    }

    if !arg1.is_table() {
        return Err(state.error("bad argument #1 (table expected)".to_string()));
    }

    let t = arg1
        .as_table()
        .ok_or_else(|| state.error("bad argument #1 (table expected)".to_string()))?;
    let arr_len = t.len();

    let mut writer = Box::new(Buffer::new());

    for i in 1..=arr_len as i64 {
        let val = t.raw_geti(i).unwrap_or(LuaValue::nil());
        match val.kind() {
            LuaValueKind::String => {
                writer.write_slice(val.as_bytes().unwrap());
            }
            LuaValueKind::Integer => {
                writer.write_chars(val.as_integer().unwrap());
            }
            LuaValueKind::Float => {
                writer.write_chars(val.as_number().unwrap());
            }
            LuaValueKind::Boolean => {
                if val.as_boolean().unwrap() {
                    writer.write_slice(JSON_TRUE.as_bytes());
                } else {
                    writer.write_slice(JSON_FALSE.as_bytes());
                }
            }
            LuaValueKind::Table => {
                let result = with_options(|options| {
                    encode_table(writer.as_mut_vec(), state, &val.as_table().unwrap(), 0, false, options)
                });
                if let Err(err) = result {
                    return Err(state.error(err));
                }
            }
            LuaValueKind::Nil => {}
            _ => {
                return Err(state.error(format!(
                    "json.concat: unsupported value type '{}'",
                    val.type_name()
                )));
            }
        }
    }

    state.push_value(LuaValue::lightuserdata(Box::into_raw(writer) as *mut c_void))?;
    Ok(1)
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

#[allow(clippy::only_used_in_recursion)]
fn concat_resp_one(
    state: &mut LuaState,
    writer: &mut Buffer,
    value: &LuaValue,
    options: &JsonOptions,
) -> Result<(), String> {
    match value.kind() {
        LuaValueKind::Nil => {
            writer.write_slice(b"\r\n$-1");
        }
        LuaValueKind::Integer => {
            write_resp(writer, value.as_integer().unwrap().to_string().as_str());
        }
        LuaValueKind::Float => {
            write_resp(writer, value.as_number().unwrap().to_string().as_str());
        }
        LuaValueKind::Boolean => {
            let b = value.as_boolean().unwrap();
            write_resp(writer, if b { JSON_TRUE } else { JSON_FALSE });
        }
        LuaValueKind::String => {
            write_resp(writer, value.as_str().unwrap());
        }
        LuaValueKind::Table => {
            let t = value.as_table().unwrap();
            let has_redis_meta = t.get_metatable().is_some_and(|mt| {
                mt.as_table().is_some_and(|mtt| {
                    mtt.iter_all()
                        .iter()
                        .any(|(k, _)| k.as_str() == Some("__redis"))
                })
            });

            if has_redis_meta {
                let arr_len = t.len();
                for i in 1..=arr_len as i64 {
                    if let Some(v) = t.raw_geti(i) {
                        concat_resp_one(state, writer, &v, options)?;
                    }
                }
            } else {
                let mut w = Buffer::new();
                encode_table(w.as_mut_vec(), state, &t, 0, false, options)?;
                write_resp(writer, w.as_str());
            }
        }
        _ => {
            return Err(format!(
                "concat_resp_one: unsupport value type :{}",
                value.type_name()
            ));
        }
    }

    Ok(())
}

fn concat_resp(state: &mut LuaState) -> LuaResult<usize> {
    let n = state.arg_count();
    if n == 0 {
        return Ok(0);
    }

    let args: Vec<LuaValue> = (1..=n).filter_map(|i| state.get_arg(i)).collect();

    let mut writer = Box::new(Buffer::new());
    let mut hash: u64 = 1;

    let arg2_is_table = args.get(1).is_some_and(|v| v.is_table());
    if !arg2_is_table
        && let Some(key) = args.first().and_then(|v| v.as_str())
        && !key.is_empty()
    {
        let hash_part = if n > 2 && (key.starts_with('h') || key.starts_with('H')) {
            args.get(2).and_then(|v| v.as_str())
        } else if n > 1 {
            args.get(1).and_then(|v| v.as_str())
        } else {
            None
        };

        if let Some(part) = hash_part {
            hash = hash_string(part);
        }
    }

    writer.write(b'*');
    writer.write_chars(n);

    let encode_result = with_options(|options| {
        let mut err = None;
        for val in &args {
            if let Err(e) = concat_resp_one(state, &mut writer, val, options) {
                err = Some(e);
                break;
            }
        }
        err
    });

    if let Some(err) = encode_result {
        return Err(state.error(err));
    }

    writer.write_slice(b"\r\n");

    state.push_value(LuaValue::lightuserdata(Box::into_raw(writer) as *mut c_void))?;
    state.push_value(LuaValue::integer((hash as i64).abs()))?;

    Ok(2)
}

pub fn register_json() -> luars::LibraryModule {
    luars::lua_module!("json", {
        "decode" => decode,
        "encode" => encode,
        "pretty_encode" => pretty_encode,
        "concat" => json_concat,
        "concat_resp" => concat_resp,
        "options" => set_options,
        "object" => json_object,
        "array" => json_array,
    })
    .with_value("null", |_vm| {
        Ok(LuaValue::lightuserdata(std::ptr::null_mut()))
    })
}

fn json_object(state: &mut LuaState) -> LuaResult<usize> {
    json_typed_table(state, JSON_OBJECT_META, 0, 16)
}

fn json_array(state: &mut LuaState) -> LuaResult<usize> {
    json_typed_table(state, JSON_ARRAY_META, 16, 0)
}

fn json_typed_table(
    state: &mut LuaState,
    meta_key: &str,
    arr_hint: usize,
    hash_hint: usize,
) -> LuaResult<usize> {
    let table = if let Some(arg) = state.get_arg(1) {
        if arg.is_integer() {
            let n = arg.as_integer().unwrap_or(16) as usize;
            if arr_hint > 0 {
                state.create_table(n, 1)?
            } else {
                state.create_table(0, n)?
            }
        } else if arg.is_table() {
            arg
        } else {
            return Err(state.error("bad argument #1 (table or integer expected)".to_string()));
        }
    } else {
        state.create_table(arr_hint, hash_hint)?
    };

    set_json_metatable(state, table, meta_key)?;
    state.push_value(table)?;
    Ok(1)
}

fn set_json_metatable(state: &mut LuaState, table: LuaValue, meta_key: &str) -> LuaResult<()> {
    if let Some(meta_value) = state.registry_get::<Table>(meta_key)? {
        state.set_metatable(table, Some(&meta_value))?;
    } else {
        let mt = state.create_table(0, 0)?;
        let key = state.create_string(meta_key)?;
        state.raw_set(&mt, key, LuaValue::boolean(true));
        state.registry_set(meta_key, mt)?;
        let mt = state.registry_get::<Table>(meta_key)?.unwrap();
        state.set_metatable(table, Some(&mt))?;
    }
    Ok(())
}
