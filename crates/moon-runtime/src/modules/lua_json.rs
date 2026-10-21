use moon_lua_sys::{
    self, cstr,
    ffi::{self, luaL_Reg},
    laux::{self, LuaStack, LuaStackValue, LuaState, LuaTable, LuaType},
    lreg, lreg_null, lreg_raw, lreg_try,
};
use serde::de::{self, DeserializeSeed, Deserializer, IgnoredAny, MapAccess, SeqAccess, Visitor};
use std::{
    ffi::{c_char, c_int, c_void},
    fmt,
    fs::File,
    io::{Read, Write},
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

const JSON_OBJECT_META: *const c_char = cstr!("__object");
const JSON_ARRAY_META: *const c_char = cstr!("__array");

const DEFAULT_CONCAT_BUFFER_SIZE: usize = 512;
const MIN_CONCAT_BUFFER_SIZE: usize = 16;

/// `#[repr(C)]` so the C JSON decoder (`lualib/lua_json_decode.c`) can read
/// the same options struct out of the shared `json_options` userdata upvalue.
/// The field order/types MUST stay in sync with the `json_options` struct there.
#[repr(C)]
#[derive(Clone, Copy)]
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

fn set_options(lua: &mut laux::LuaStack<'_>) -> Result<c_int, String> {
    let state = lua.state();
    let mut options_ptr = fetch_options_ptr(lua)?;
    let options = unsafe { options_ptr.as_mut() };
    enum OptionKey {
        EmptyAsArray,
        NumberKey,
        SparseArray,
        MetaField,
        ConcatBufferSize,
    }
    let key = lua
        .value(1)
        .as_str()
        .ok_or_else(|| "json.options: UTF-8 string expected".to_string())?;
    let option = match key {
        "encode_empty_as_array" => OptionKey::EmptyAsArray,
        "enable_number_key" => OptionKey::NumberKey,
        "enable_sparse_array" => OptionKey::SparseArray,
        "has_metatfield" => OptionKey::MetaField,
        "concat_buffer_size" => OptionKey::ConcatBufferSize,
        _ => return Err(format!("invalid json option key: {key}")),
    };

    match option {
        OptionKey::EmptyAsArray => {
            let v = options.empty_as_array;
            options.empty_as_array = lua.opt_truthy(2).unwrap_or(true);
            laux::lua_push(state, v);
        }
        OptionKey::NumberKey => {
            let v = options.enable_number_key;
            options.enable_number_key = lua.opt_truthy(2).unwrap_or(true);
            laux::lua_push(state, v);
        }
        OptionKey::SparseArray => {
            let v = options.enable_sparse_array;
            options.enable_sparse_array = lua.opt_truthy(2).unwrap_or(false);
            laux::lua_push(state, v);
        }
        OptionKey::MetaField => {
            let v = options.has_metatfield;
            options.has_metatfield = lua.opt_truthy(2).unwrap_or(true);
            laux::lua_push(state, v);
        }
        OptionKey::ConcatBufferSize => {
            let new_size: usize = lua.get(2)?;
            if new_size < MIN_CONCAT_BUFFER_SIZE {
                return Err(format!(
                    "bad argument #2 (concat_buffer_size must be >= {})",
                    MIN_CONCAT_BUFFER_SIZE
                ));
            }
            let v = options.concat_buffer_size;
            options.concat_buffer_size = new_size;
            laux::lua_push(state, v as ffi::lua_Integer);
        }
    }

    Ok(1)
}

fn fetch_options_ptr(lua: &LuaStack<'_>) -> Result<std::ptr::NonNull<JsonOptions>, String> {
    lua.value(ffi::lua_upvalueindex(1))
        .as_userdata::<JsonOptions>()
        .ok_or_else(|| "expect json options".to_string())
}

pub fn fetch_options(lua: &LuaStack<'_>) -> Result<JsonOptions, String> {
    let options = fetch_options_ptr(lua)?;
    Ok(unsafe { *options.as_ref() })
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
    lua: &mut LuaStack<'_>,
    index: i32,
    depth: i32,
    fmt: bool,
    options: &JsonOptions,
) -> Result<(), String> {
    let value = lua.value(index);
    if value.kind() == LuaType::Table {
        return encode_table(writer, lua, index, depth, fmt, options);
    }
    encode_stack_value(writer, value)
}

fn encode_stack_value(writer: &mut Vec<u8>, value: LuaStackValue<'_, '_>) -> Result<(), String> {
    match value.kind() {
        LuaType::Boolean => {
            let val = value.as_bool().unwrap_or(false);
            if val {
                writer.extend_from_slice(JSON_TRUE);
            } else {
                writer.extend_from_slice(JSON_FALSE);
            }
        }
        LuaType::Number => {
            let val = value.as_number().unwrap_or_default();
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
        LuaType::Integer => writer.extend_from_slice(
            value
                .as_integer()
                .unwrap_or_default()
                .to_string()
                .as_bytes(),
        ),
        LuaType::String => {
            write_json_string(writer, value.as_bytes().unwrap_or_default());
        }
        LuaType::Table => unreachable!("table values are handled by encode_one"),
        LuaType::Nil => {
            writer.extend_from_slice(JSON_NULL);
        }
        LuaType::LightUserData => {
            let is_null = value.as_light_userdata().is_some_and(|ptr| ptr.is_null());
            if is_null {
                writer.extend_from_slice(JSON_NULL);
            } else {
                // A non-null lightuserdata has no JSON representation. Writing
                // nothing would desync the separators already emitted by the
                // array/object caller (e.g. produce `[1,,3]`), so fail instead.
                return Err("json encode: cannot encode a non-null lightuserdata value".to_string());
            }
        }
        _ => {
            return Err(format!(
                "json encode: unsupport value type :{}",
                value.name()
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
    lua: &mut LuaStack<'_>,
    index: i32,
    size: usize,
    depth: i32,
    fmt: bool,
    options: &JsonOptions,
) -> Result<(), String> {
    let bsize = writer.len();
    writer.push(b'[');

    let mut cursor = lua.array_cursor_len(index, size);
    let mut i = 0;
    while let Some(val) = cursor.next() {
        if i > 0 {
            writer.push(b',');
        }
        format_new_line(writer, fmt);
        format_space(writer, fmt, depth);

        let value_index = val.index();
        if val.kind() == LuaType::Nil && !options.enable_sparse_array {
            writer.truncate(bsize);
            drop(cursor);
            return encode_object(writer, lua, index, depth, fmt, options, false);
        }
        if val.kind() == LuaType::Table {
            unsafe { encode_table(writer, cursor.lua_mut(), value_index, depth, fmt, options) }?;
        } else {
            encode_stack_value(writer, val)?;
        }
        i += 1;
    }
    if size > 0 {
        format_new_line(writer, fmt);
        format_space(writer, fmt, depth - 1);
    }
    writer.push(b']');
    Ok(())
}

fn encode_object(
    writer: &mut Vec<u8>,
    lua: &mut LuaStack<'_>,
    index: i32,
    depth: i32,
    fmt: bool,
    options: &JsonOptions,
    force_object: bool,
) -> Result<(), String> {
    let mut i = 0;
    writer.push(b'{');

    for mut entry in lua.table_cursor(index) {
        if i > 0 {
            writer.push(b',');
        }
        i += 1;
        format_new_line(writer, fmt);

        let key = entry.key();
        match key.kind() {
            LuaType::String => {
                {
                    let key = key.as_bytes().unwrap_or_default();
                    format_space(writer, fmt, depth);
                    // Keys must be escaped exactly like string values: a key
                    // containing `"`, `\`, or control characters would otherwise
                    // produce invalid JSON.
                    write_json_string(writer, key);
                    writer.push(b':');
                    if fmt {
                        writer.push(b' ');
                    }
                }
                let value = entry.value();
                let value_index = value.index();
                if value.kind() == LuaType::Table {
                    unsafe {
                        encode_table(writer, entry.lua_mut(), value_index, depth, fmt, options)
                    }?;
                } else {
                    encode_stack_value(writer, value)?;
                }
            }
            LuaType::Integer => {
                if options.enable_number_key {
                    {
                        let key = key.as_integer().unwrap_or_default();
                        format_space(writer, fmt, depth);
                        writer.push(b'\"');
                        writer.extend_from_slice(key.to_string().as_bytes());
                        writer.extend_from_slice(b"\":");
                        if fmt {
                            writer.push(b' ');
                        }
                    }
                    let value = entry.value();
                    let value_index = value.index();
                    if value.kind() == LuaType::Table {
                        unsafe {
                            encode_table(writer, entry.lua_mut(), value_index, depth, fmt, options)
                        }?;
                    } else {
                        encode_stack_value(writer, value)?;
                    }
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

enum TableShape {
    Array(usize),
    Object(bool),
}

fn table_shape(lua: &mut LuaStack<'_>, index: i32, options: &JsonOptions) -> TableShape {
    if options.has_metatfield {
        if lua.has_metafield(index, JSON_ARRAY_META) {
            return TableShape::Array(lua.raw_len(index));
        }
        if lua.has_metafield(index, JSON_OBJECT_META) {
            return TableShape::Object(true);
        }
    }
    match lua.array_len(index) {
        0 => TableShape::Object(false),
        size => TableShape::Array(size),
    }
}

/// Convert SQLx's default JSON table representation without an intermediate
/// document string. Return None when the old text-to-Value step would fail
/// (e.g. invalid UTF-8), preserving its binary-parameter fallback.
#[cfg(feature = "sqlx")]
pub(crate) fn table_to_json_value(
    lua: &mut LuaStack<'_>,
    index: i32,
    depth: i32,
) -> Result<Option<serde_json::Value>, String> {
    use serde_json::Value;
    let depth = depth + 1;
    if depth > 64 {
        return Err("json encode: too depth".to_string());
    }
    laux::lua_checkstack(lua.state(), 6, cstr!("json.encode.table"))?;
    match table_shape(lua, index, &JsonOptions::default()) {
        TableShape::Array(size) => {
            let mut values = Vec::with_capacity(size);
            let mut cursor = lua.array_cursor_len(index, size);
            while let Some(value) = cursor.next() {
                let index = value.index();
                let Some(value) = (unsafe { stack_to_json_value(cursor.lua_mut(), index, depth) })?
                else {
                    return Ok(None);
                };
                values.push(value);
            }
            Ok(Some(Value::Array(values)))
        }
        TableShape::Object(force) => {
            let mut values = serde_json::Map::new();
            for mut entry in lua.table_cursor(index) {
                let key = entry.key();
                let key = match key.kind() {
                    LuaType::String => {
                        let Some(key) = key.as_str() else { return Ok(None) };
                        key.to_owned()
                    }
                    LuaType::Integer => key.as_integer().unwrap_or_default().to_string(),
                    _ => return Err("json encode: unsupported table key type (only string/integer keys are allowed).".to_string()),
                };
                let index = entry.value().index();
                let Some(value) = (unsafe { stack_to_json_value(entry.lua_mut(), index, depth) })?
                else {
                    return Ok(None);
                };
                values.insert(key, value);
            }
            Ok(Some(if values.is_empty() && !force {
                Value::Array(Vec::new())
            } else {
                Value::Object(values)
            }))
        }
    }
}

#[cfg(feature = "sqlx")]
fn stack_to_json_value(
    lua: &mut LuaStack<'_>,
    index: i32,
    depth: i32,
) -> Result<Option<serde_json::Value>, String> {
    use serde_json::Value;
    let value = lua.value(index);
    let converted = match value.kind() {
        LuaType::Table => return table_to_json_value(lua, index, depth),
        LuaType::Nil => Value::Null,
        LuaType::Boolean => Value::Bool(value.as_bool().unwrap_or(false)),
        LuaType::Integer => Value::from(value.as_integer().unwrap_or_default()),
        LuaType::String => {
            let Some(text) = value.as_str() else {
                return Ok(None);
            };
            Value::String(text.to_owned())
        }
        LuaType::LightUserData if value.as_light_userdata().is_some_and(|ptr| ptr.is_null()) => {
            Value::Null
        }
        // Keep the text encoder's number normalization (1.0 -> integer, -0.0
        // -> negative zero) and serde's rounding, using only a stack buffer.
        LuaType::Number => {
            use std::io::Write;
            let number = value.as_number().unwrap_or_default();
            if !number.is_finite() {
                return Err(
                    "json encode: cannot encode a non-finite number (NaN or Infinity)".to_string(),
                );
            }
            let mut text = std::io::Cursor::new([0u8; 384]);
            write!(text, "{number}")
                .map_err(|err| format!("json number formatting failed: {err}"))?;
            match serde_json::from_slice(&text.get_ref()[..text.position() as usize]) {
                Ok(value) => value,
                Err(_) => return Ok(None),
            }
        }
        _ => {
            // Reuse the encoder's errors for unsupported scalar values.
            encode_stack_value(&mut Vec::new(), value)?;
            return Ok(None);
        }
    };
    Ok(Some(converted))
}

pub fn encode_table(
    writer: &mut Vec<u8>,
    lua: &mut LuaStack<'_>,
    index: i32,
    depth: i32,
    fmt: bool,
    options: &JsonOptions,
) -> Result<(), String> {
    let depth = depth + 1;
    if depth > 64 {
        return Err("json encode: too depth".to_string());
    }

    laux::lua_checkstack(lua.state(), 6, cstr!("json.encode.table"))?;

    match table_shape(lua, index, options) {
        TableShape::Array(size) => encode_array(writer, lua, index, size, depth, fmt, options)?,
        TableShape::Object(force) => encode_object(writer, lua, index, depth, fmt, options, force)?,
    }

    Ok(())
}

fn encode(lua: &mut laux::LuaStack<'_>) -> Result<c_int, String> {
    let state = lua.state();
    if matches!(laux::lua_type(state, 1), LuaType::None | LuaType::Nil) {
        return Err("json.encode: value expected".to_string());
    }

    let options = fetch_options(lua)?;
    let fmt = lua.opt_truthy(2).unwrap_or_default();
    let mut writer = Vec::with_capacity(options.concat_buffer_size);
    encode_one(&mut writer, lua, 1, 0, fmt, &options).map_err(|err| err.to_string())?;
    laux::lua_push(state, writer.as_slice());
    Ok(1)
}

const MAX_DECODE_DEPTH: usize = 64;

/// A stateful serde seed that drives a streaming parse straight onto the Lua
/// stack: no intermediate `serde_json::Value` DOM is ever materialized. Each
/// JSON value is pushed onto the stack as the parser yields it, so there are no
/// per-key `String`, no `Map`, and no `Vec` allocations on the Rust side. The
/// `type Value = ()` because the decoded value lives on the Lua stack, not in a
/// returned Rust object.
struct LuaSeed<'o> {
    state: LuaState,
    options: &'o JsonOptions,
    depth: usize,
}

impl<'de, 'o> DeserializeSeed<'de> for LuaSeed<'o> {
    type Value = ();

    fn deserialize<D>(self, deserializer: D) -> Result<(), D::Error>
    where
        D: Deserializer<'de>,
    {
        // Recursion here grows the Rust call stack with the JSON nesting depth,
        // so the limit must be enforced before descending.
        if self.depth > MAX_DECODE_DEPTH {
            return Err(de::Error::custom("json decode: too deep"));
        }
        deserializer.deserialize_any(LuaVisitor {
            state: self.state,
            options: self.options,
            depth: self.depth,
        })
    }
}

struct LuaVisitor<'o> {
    state: LuaState,
    options: &'o JsonOptions,
    depth: usize,
}

impl<'de, 'o> Visitor<'de> for LuaVisitor<'o> {
    type Value = ();

    fn expecting(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.write_str("any valid JSON value")
    }

    fn visit_bool<E>(self, v: bool) -> Result<(), E> {
        laux::lua_push(self.state, v);
        Ok(())
    }

    fn visit_i64<E>(self, v: i64) -> Result<(), E> {
        laux::lua_push(self.state, v);
        Ok(())
    }

    fn visit_u64<E>(self, v: u64) -> Result<(), E> {
        // `u64` values above `i64::MAX` don't fit a Lua integer; represent them
        // as a float rather than silently wrapping to a negative integer.
        if v <= i64::MAX as u64 {
            laux::lua_push(self.state, v as i64);
        } else {
            laux::lua_push(self.state, v as f64);
        }
        Ok(())
    }

    fn visit_f64<E>(self, v: f64) -> Result<(), E> {
        laux::lua_push(self.state, v);
        Ok(())
    }

    /// String with no escapes: borrows directly from the input buffer, so there
    /// is no transient Rust `String` — Lua copies it once into its own GC heap.
    fn visit_borrowed_str<E>(self, v: &'de str) -> Result<(), E> {
        laux::lua_push(self.state, v);
        Ok(())
    }

    /// String containing escapes: `v` points at serde_json's reusable scratch
    /// buffer (still not a one-shot `String`); Lua copies it once.
    fn visit_str<E>(self, v: &str) -> Result<(), E> {
        laux::lua_push(self.state, v);
        Ok(())
    }

    /// JSON `null`: `deserialize_any` reports it via `visit_unit`.
    fn visit_unit<E>(self) -> Result<(), E> {
        laux::lua_pushlightuserdata(self.state, std::ptr::null_mut());
        Ok(())
    }

    fn visit_seq<A>(self, mut seq: A) -> Result<(), A::Error>
    where
        A: SeqAccess<'de>,
    {
        laux::lua_checkstack(self.state, 4, cstr!("json.decode.array"))
            .map_err(de::Error::custom)?;
        let table = LuaTable::new(self.state, seq.size_hint().unwrap_or(0), 0);
        let mut i = 0usize;
        // Each element is pushed onto the stack by its seed, then moved into the
        // table at the next array index.
        while seq
            .next_element_seed(LuaSeed {
                state: self.state,
                options: self.options,
                depth: self.depth + 1,
            })?
            .is_some()
        {
            i += 1;
            table.rawseti(i);
        }
        if self.options.has_metatfield {
            set_json_metatable(self.state, -1, JSON_ARRAY_META);
        }
        Ok(())
    }

    fn visit_map<A>(self, mut map: A) -> Result<(), A::Error>
    where
        A: MapAccess<'de>,
    {
        laux::lua_checkstack(self.state, 6, cstr!("json.decode.object"))
            .map_err(de::Error::custom)?;
        let table = LuaTable::new(self.state, 0, map.size_hint().unwrap_or(0));
        // `KeySeed` pushes the key onto the stack and returns whether it should be
        // kept (empty keys are skipped, matching the previous behaviour).
        while let Some(keep) = map.next_key_seed(KeySeed {
            state: self.state,
            options: self.options,
        })? {
            if keep {
                // Key is on the stack top; push the value above it, then `rawset`
                // pops both and stores them in the table.
                map.next_value_seed(LuaSeed {
                    state: self.state,
                    options: self.options,
                    depth: self.depth + 1,
                })?;
                table.insert_from_stack();
            } else {
                map.next_value::<IgnoredAny>()?;
            }
        }
        if self.options.has_metatfield {
            set_json_metatable(self.state, -1, JSON_OBJECT_META);
        }
        Ok(())
    }
}

/// Decodes a JSON object key and pushes it onto the Lua stack, reproducing the
/// `enable_number_key` behaviour. `type Value = bool`: `true` means the key was
/// pushed (keep it), `false` means it was an empty key (skip the entry).
struct KeySeed<'o> {
    state: LuaState,
    options: &'o JsonOptions,
}

impl<'de, 'o> DeserializeSeed<'de> for KeySeed<'o> {
    type Value = bool;

    fn deserialize<D>(self, deserializer: D) -> Result<bool, D::Error>
    where
        D: Deserializer<'de>,
    {
        deserializer.deserialize_str(self)
    }
}

impl<'de, 'o> Visitor<'de> for KeySeed<'o> {
    type Value = bool;

    fn expecting(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.write_str("a JSON object key")
    }

    fn visit_str<E>(self, k: &str) -> Result<bool, E> {
        if k.is_empty() {
            return Ok(false);
        }
        let c = k.as_bytes()[0];
        if (c.is_ascii_digit() || c == b'-') && self.options.enable_number_key {
            if let Ok(n) = k.parse::<ffi::lua_Integer>() {
                laux::lua_push(self.state, n);
            } else {
                laux::lua_push(self.state, k);
            }
        } else {
            laux::lua_push(self.state, k);
        }
        Ok(true)
    }

    fn visit_borrowed_str<E>(self, k: &'de str) -> Result<bool, E>
    where
        E: de::Error,
    {
        self.visit_str(k)
    }
}

/// Parse `data` as JSON and push the resulting Lua value.
///
/// Returns `Err` (never longjmps for parse errors) so callers can drop owned
/// resources before raising the Lua error. The parser borrows `data` and owns no
/// heap that would leak if a Lua push longjmps mid-decode.
/// # Safety
/// `data` must point to bytes whose backing storage remains rooted and stable
/// for the whole call. The decoder may append Lua values above the source
/// value, but must not pop, replace, reorder, or otherwise mutate the source
/// stack slot. This permits decoding directly from a Lua string without an
/// intermediate Rust allocation.
unsafe fn decode_bytes(state: LuaState, data: &[u8], options: &JsonOptions) -> Result<i32, String> {
    let mut de = serde_json::Deserializer::from_slice(data);
    LuaSeed {
        state,
        options,
        depth: 0,
    }
    .deserialize(&mut de)
    .map_err(|e| format!("json.decode: {}", e))?;
    // Reject trailing garbage after the top-level value (parity with from_slice).
    de.end().map_err(|e| format!("json.decode: {}", e))?;
    Ok(1)
}

/// Read the file at the given path and decode its contents as JSON.
///
/// File access is intentionally a *separate*, explicit API: `decode` only ever
/// parses in-memory bytes, so JSON arriving from a network/client source can
/// never be coerced into reading arbitrary files. Only call `decode_file` with
/// a trusted, caller-controlled path.
fn decode_file(lua: &mut laux::LuaStack<'_>) -> Result<c_int, String> {
    let state = lua.state();
    let options = fetch_options(lua)?;
    // The path is only borrowed while the synchronous file read runs. The
    // resulting buffer is owned and is released before Lua decoding starts.
    let contents: Vec<u8> = {
        let path = lua
            .value(1)
            .as_str()
            .ok_or_else(|| "json.decode_file: UTF-8 string expected".to_string())?;
        let mut file = File::open(path).map_err(|e| format!("json.decode: {}", e))?;
        let mut contents = Vec::new();
        file.read_to_end(&mut contents)
            .map_err(|e| format!("json.decode: {}", e))?;
        contents
    };

    if contents.is_empty() {
        laux::lua_pushnil(state);
        return Ok(1);
    }

    // Hand the bytes to Lua's GC heap, then drop the Rust-owned buffer. The
    // decoder below borrows from this rooted Lua string, so a longjmp cannot
    // leak a Rust allocation.
    let source_index = lua.top() + 1;
    laux::lua_push(state, contents.as_slice());
    drop(contents);

    let data = lua.value(source_index).as_bytes().unwrap_or_default();
    // SAFETY: `source_index` is an absolute stack slot rooted by this call;
    // decoding only appends values above it and does not replace or remove it.
    unsafe { decode_bytes(state, data, &options) }
}

fn concat(lua: &mut laux::LuaStack<'_>) -> Result<c_int, String> {
    let state = lua.state();
    let options = fetch_options(lua)?;

    if laux::lua_type(state, 1) == LuaType::String {
        let slc = lua.value(1).as_bytes().unwrap_or_default();
        let mut buf = Box::new(Buffer::with_capacity(slc.len() + BUFFER_HEAD_RESERVE));
        buf.write_slice(&[0; BUFFER_HEAD_RESERVE]);
        buf.write_slice(slc);
        buf.seek(BUFFER_HEAD_RESERVE as isize);
        laux::lua_pushlightuserdata(state, Box::into_raw(buf) as *mut c_void);
        return Ok(1);
    }

    laux::lua_checktype(state, 1, ffi::LUA_TTABLE).map_err(|err| format!("json.concat: {err}"))?;
    laux::lua_settop(state, 1);

    let mut writer = Box::new(Buffer::new());
    writer.write_slice(&[0; BUFFER_HEAD_RESERVE]);
    {
        let mut cursor = lua.array_cursor(1);
        while let Some(val) = cursor.next() {
            let index = val.index();
            match val.kind() {
                LuaType::String => writer.write_slice(val.as_bytes().unwrap_or_default()),
                LuaType::Number => writer
                    .write_chars(val.as_number().unwrap_or_default())
                    .map_err(|err| err.to_string())?,
                LuaType::Integer => writer
                    .write_chars(val.as_integer().unwrap_or_default())
                    .map_err(|err| err.to_string())?,
                LuaType::Boolean => {
                    let val = val.as_bool().unwrap_or(false);
                    if val {
                        writer.write_slice(JSON_TRUE);
                    } else {
                        writer.write_slice(JSON_FALSE);
                    }
                }
                LuaType::Table => {
                    unsafe {
                        encode_table(
                            writer.as_mut_vec(),
                            cursor.lua_mut(),
                            index,
                            0,
                            false,
                            &options,
                        )
                    }?;
                }
                _ => {
                    return Err(format!("json.concat: unsupport value type :{}", val.name()));
                }
            }
        }
    }

    writer.seek(BUFFER_HEAD_RESERVE as isize);
    laux::lua_pushlightuserdata(state, Box::into_raw(writer) as *mut c_void);

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
fn write_resp(writer: &mut Buffer, cmd: &[u8]) -> Result<(), String> {
    writer.write_slice(b"\r\n$");
    writer
        .write_chars(cmd.len())
        .map_err(|err| err.to_string())?;
    writer.write_slice(b"\r\n");
    writer.write_slice(cmd);
    Ok(())
}

fn write_resp_number(writer: &mut Buffer, value: impl fmt::Display) -> Result<(), String> {
    // Default Display for f64 needs up to 327 bytes, including subnormals.
    // Keep its existing formatting (including -0) without a temporary String.
    let mut number = std::io::Cursor::new([0u8; 384]);
    write!(number, "{value}")
        .map_err(|err| format!("numeric RESP argument formatting failed: {err}"))?;
    write_resp(writer, &number.get_ref()[..number.position() as usize])
}

fn concat_resp_one(
    writer: &mut Buffer,
    lua: &mut LuaStack<'_>,
    index: i32,
    options: &JsonOptions,
) -> Result<(), String> {
    let value = lua.value(index);
    match value.kind() {
        LuaType::Nil => {
            writer.write_slice(b"\r\n$-1");
        }
        LuaType::Number => write_resp_number(writer, value.as_number().unwrap_or_default())?,
        LuaType::Integer => write_resp_number(writer, value.as_integer().unwrap_or_default())?,
        LuaType::Boolean => write_resp(
            writer,
            if value.as_bool().unwrap_or(false) {
                JSON_TRUE
            } else {
                JSON_FALSE
            },
        )?,
        LuaType::String => {
            let val = value.as_bytes().unwrap_or_default();
            writer.write_slice(b"\r\n$");
            writer
                .write_chars(val.len())
                .map_err(|err| err.to_string())?;
            writer.write_slice(b"\r\n");
            writer.write_slice(val);
        }

        LuaType::Table => {
            if lua.has_metafield(index, cstr!("__redis")) {
                let mut cursor = lua.array_cursor(index);
                while let Some(value) = cursor.next() {
                    let value_index = value.index();
                    unsafe { concat_resp_one(writer, cursor.lua_mut(), value_index, options) }?;
                }
            } else {
                let mut w = Buffer::new();
                // SAFETY: the encoder only appends; the read position stays zero.
                encode_table(unsafe { w.as_mut_vec() }, lua, index, 0, false, options)?;
                write_resp(writer, w.as_slice())?;
            }
        }
        _ => {
            return Err(format!(
                "concat_resp_one: unsupport value type :{}",
                value.name()
            ));
        }
    }

    Ok(())
}

fn concat_resp(lua: &mut laux::LuaStack<'_>) -> Result<c_int, String> {
    let state = lua.state();
    let n = laux::lua_top(state);
    if n == 0 {
        return Ok(0);
    }

    let options = fetch_options(lua)?;

    let mut writer = Box::new(Buffer::new());
    writer.write_slice(&[0; BUFFER_HEAD_RESERVE]);
    let hash = {
        let key = lua.value(1).as_str();
        let hash_part = if n > 2 && key.is_some_and(|key| key.starts_with(['h', 'H'])) {
            lua.value(3).as_str()
        } else if n > 1 {
            lua.value(2).as_str()
        } else {
            None
        };
        if laux::lua_type(state, 2) != LuaType::Table && key.is_some_and(|key| !key.is_empty()) {
            hash_part.map(hash_string).unwrap_or(1)
        } else {
            1
        }
    };

    writer.write(b'*');
    writer.write_chars(n).map_err(|err| err.to_string())?;
    for i in 1..=n {
        concat_resp_one(&mut writer, lua, i, &options)?;
    }

    writer.write_slice(b"\r\n");

    writer.seek(BUFFER_HEAD_RESERVE as isize);
    laux::lua_pushlightuserdata(state, Box::into_raw(writer) as *mut c_void);
    laux::lua_push(state, (hash as ffi::lua_Integer).abs());

    Ok(2)
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

fn json_object(lua: &mut laux::LuaStack<'_>) -> c_int {
    let state = lua.state();
    if laux::lua_type(state, 1) == LuaType::Table {
        set_json_metatable(state, 1, JSON_OBJECT_META);
        unsafe { ffi::lua_settop(state.as_ptr(), 1) };
    } else {
        let nrec: i32 = lua.opt(1).unwrap_or(0);
        unsafe { ffi::lua_createtable(state.as_ptr(), 0, nrec) };
        set_json_metatable(state, -1, JSON_OBJECT_META);
    }
    1
}

fn json_array(lua: &mut laux::LuaStack<'_>) -> c_int {
    let state = lua.state();
    if laux::lua_type(state, 1) == LuaType::Table {
        set_json_metatable(state, 1, JSON_ARRAY_META);
        unsafe { ffi::lua_settop(state.as_ptr(), 1) };
    } else {
        let narr: i32 = lua.opt(1).unwrap_or(0);
        unsafe { ffi::lua_createtable(state.as_ptr(), narr, 0) };
        set_json_metatable(state, -1, JSON_ARRAY_META);
    }
    1
}

/// Alternative decoder backed by the yyjson C implementation
extern "C-unwind" fn yyjson_decode(state: LuaState) -> i32 {
    unsafe extern "C-unwind" {
        fn lua_json_decode(state: *mut ffi::lua_State) -> c_int;
    }
    unsafe { lua_json_decode(state.as_ptr()) }
}

pub extern "C-unwind" fn luaopen_json(state: LuaState) -> i32 {
    let l = [
        // lreg_raw!("decode", decode),
        lreg_raw!("decode", yyjson_decode),
        lreg_try!("decode_file", decode_file),
        lreg_try!("encode", encode),
        lreg_try!("concat", concat),
        lreg_try!("concat_resp", concat_resp),
        lreg_try!("options", set_options),
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

#[cfg(test)]
#[path = "../../tests/unit/modules/lua_json.rs"]
mod tests;
