use moon_base::{
    self, cstr,
    ffi::{self, luaL_Reg},
    laux::{self, LuaState, LuaTable, LuaType, LuaValue},
    lreg, lreg_null,
};
use serde::de::{self, DeserializeSeed, Deserializer, IgnoredAny, MapAccess, SeqAccess, Visitor};
use std::{
    ffi::{c_char, c_int, c_void},
    fmt,
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

const JSON_OBJECT_META: *const c_char = cstr!("__object");
const JSON_ARRAY_META: *const c_char = cstr!("__array");

const DEFAULT_CONCAT_BUFFER_SIZE: usize = 512;
const MIN_CONCAT_BUFFER_SIZE: usize = 16;

/// `#[repr(C)]` so the C JSON decoder (`lualib-src/lua_json_decode.c`) can read
/// the same options struct out of the shared `json_options` userdata upvalue.
/// The field order/types MUST stay in sync with the `json_options` struct there.
#[repr(C)]
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
        if i > 0 {
            writer.push(b',');
        }
        format_new_line(writer, fmt);
        format_space(writer, fmt, depth);

        if let LuaValue::Nil = val
            && !options.enable_sparse_array
        {
            writer.truncate(bsize);
            return encode_object(writer, table, depth, fmt, options, false);
        }
        encode_one(writer, val, depth, fmt, options)?;
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
        laux::lua_checkstack(self.state, 4, cstr!("json.decode.array"));
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
        laux::lua_checkstack(self.state, 6, cstr!("json.decode.object"));
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
fn decode_bytes(state: LuaState, data: &[u8], options: &JsonOptions) -> Result<i32, String> {
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

#[allow(unused)]
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

    // Read the file with the owned `File`/`contents` confined to this closure so
    // they are dropped before any longjmp — a longjmp would otherwise skip their
    // `Drop` and leak the file descriptor and buffer.
    let contents: Result<Vec<u8>, String> = (|| {
        let mut file = File::open(path).map_err(|e| format!("json.decode: {}", e))?;
        let mut contents = Vec::new();
        file.read_to_end(&mut contents)
            .map_err(|e| format!("json.decode: {}", e))?;
        Ok(contents)
    })();

    let contents = match contents {
        Ok(c) => c,
        Err(e) => {
            laux::lua_push(state, e);
            laux::throw_error(state)
        }
    };

    if contents.is_empty() {
        laux::lua_pushnil(state);
        return 1;
    }

    // Hand the bytes to Lua's GC heap, then drop the Rust-owned buffer. The
    // streaming decode below borrows from this Lua string, so no Rust heap is
    // held across the push phase: a longjmp mid-decode leaks nothing. The source
    // string sits below the decoded result and is discarded by the return.
    laux::lua_push(state, contents.as_slice());
    drop(contents);

    let data = unsafe { laux::lua_to_lstring(state, -1) };
    match decode_bytes(state, data, options) {
        Ok(n) => n,
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

/// Alternative decoder backed by the yyjson C implementation
extern "C-unwind" fn yyjson_decode(state: LuaState) -> i32 {
    unsafe extern "C-unwind" {
        fn lua_json_decode(state: *mut ffi::lua_State) -> c_int;
    }
    unsafe { lua_json_decode(state.as_ptr()) }
}

pub extern "C-unwind" fn luaopen_json(state: LuaState) -> i32 {
    let l = [
        // lreg!("decode", decode),
        lreg!("decode", yyjson_decode),
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
