//! Runtime schema validator for Lua tables.
//!
//! Rust port of the C++ `lua_schema.cpp` binding. A protobuf-like schema is
//! loaded once (or reloaded) via `schema.load`, then `schema.validate` checks
//! that a Lua table matches a named proto, raising a Lua error with a `trace`
//! path on the first mismatch.
//!
//! Design differences from the C++ original (all deliberate):
//!
//! - **Types are compiled at load time** into an enum ([`ValueType`]) instead of
//!   re-hashing the type string for every value during validation, and nested
//!   proto references are resolved to a `u32` index (`ValueType::Ref`) so
//!   undefined references fail fast at load.
//! - **Integer types are range/sign checked** (`int32` must fit `i32`, `uint32`
//!   must be `0..=u32::MAX`, `uint64` must be non-negative, ...) rather than only
//!   asserting "is an integer".
//! - **Proto kind is explicit (with a compatibility fallback)**: a wrapper proto
//!   (whole table validated as its single `data` field) is marked `wrapper =
//!   true` in the definition. For compatibility with Moon's generators, a proto
//!   whose name begins with `array_`/`map_` is also treated as a wrapper even
//!   without the explicit flag.
//! - **Errors propagate as `Result`** and surface through the shared registration
//!   wrapper at the FFI boundary (no exceptions across C frames).
//! - **The trace allocates lazily**: array indices are cheap [`Seg::Index`] and
//!   the path is only joined into a string when an error is actually produced.
//! - **The global schema uses an `AtomicPtr` swap** (mirroring `lua_protobuf`),
//!   so `load` may be called multiple times and readers on other actor threads
//!   keep a valid `&'static` view (the previous schema is intentionally leaked).

use moon_lua_sys::laux::{LuaStack, LuaStackValue, LuaState};
use moon_lua_sys::{cstr, ffi, laux, lreg_null, lreg_try, luaL_newlib};
use std::collections::HashMap;
use std::ffi::c_int;
use std::sync::atomic::{AtomicPtr, Ordering};

/// Max proto-nesting depth, guards against cyclic data blowing the stack.
const MAX_DEPTH: u32 = 64;

/// A scalar leaf type. `sint*`/`fixed*`/`sfixed*` collapse onto the matching
/// width, `bytes` onto `Str`, and `double` onto `Float`.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum Prim {
    Int32,
    Uint32,
    Int64,
    Uint64,
    Float,
    Bool,
    Str,
}

impl Prim {
    fn parse(name: &str) -> Option<Prim> {
        Some(match name {
            "int32" | "sint32" | "sfixed32" => Prim::Int32,
            "uint32" | "fixed32" => Prim::Uint32,
            "int64" | "sint64" | "sfixed64" => Prim::Int64,
            "uint64" | "fixed64" => Prim::Uint64,
            "float" | "double" => Prim::Float,
            "bool" => Prim::Bool,
            "string" | "bytes" => Prim::Str,
            _ => return None,
        })
    }

    /// Canonical name used in error messages.
    fn name(self) -> &'static str {
        match self {
            Prim::Int32 => "int32",
            Prim::Uint32 => "uint32",
            Prim::Int64 => "int64",
            Prim::Uint64 => "uint64",
            Prim::Float => "float",
            Prim::Bool => "bool",
            Prim::Str => "string",
        }
    }

    /// Whether a concrete Lua value satisfies this primitive, including the
    /// integer range/sign checks that the C++ original omits.
    fn accepts_stack(self, value: LuaStackValue<'_, '_>) -> bool {
        let kind = value.kind();
        match self {
            Prim::Int32 => value
                .as_integer()
                .is_some_and(|n| n >= i32::MIN as i64 && n <= i32::MAX as i64),
            Prim::Uint32 => value
                .as_integer()
                .is_some_and(|n| n >= 0 && n <= u32::MAX as i64),
            Prim::Int64 => kind == laux::LuaType::Integer,
            Prim::Uint64 => value.as_integer().is_some_and(|n| n >= 0),
            Prim::Float => matches!(kind, laux::LuaType::Number | laux::LuaType::Integer),
            Prim::Bool => kind == laux::LuaType::Boolean,
            Prim::Str => kind == laux::LuaType::String,
        }
    }
}

/// A field's value type: a scalar primitive or a reference to another proto.
#[derive(Clone, Copy)]
enum ValueType {
    Prim(Prim),
    Ref(u32),
}

/// How a field's values are laid out in the table.
#[derive(Clone, Copy)]
enum Container {
    /// A single value of `value_type`.
    Scalar,
    /// A Lua sequence whose elements are all `value_type`.
    Array,
    /// A map with `key` primitive keys and `value_type` values.
    Object { key: Prim },
}

struct Field {
    name: Box<str>,
    container: Container,
    value: ValueType,
}

struct Proto {
    name: Box<str>,
    /// When true, `validate` treats the passed table as the value of the single
    /// `data` field rather than iterating its keys as named fields.
    wrapper: bool,
    fields: HashMap<Box<str>, Field>,
}

struct Schema {
    protos: Vec<Proto>,
    by_name: HashMap<Box<str>, u32>,
}

// ---------------------------------------------------------------------------
// Global schema (AtomicPtr swap, à la lua_protobuf::GLOBAL_DESCRIPTOR).
// ---------------------------------------------------------------------------

static SCHEMA: AtomicPtr<Schema> = AtomicPtr::new(std::ptr::null_mut());

fn schema() -> Option<&'static Schema> {
    let ptr = SCHEMA.load(Ordering::Acquire);
    if ptr.is_null() {
        None
    } else {
        Some(unsafe { &*ptr })
    }
}

fn set_schema(s: Box<Schema>) {
    // Intentionally leak the previous schema: validators running on other actor
    // threads may still hold a `&'static Schema` borrowed from the old pointer,
    // so freeing it here would dangle. `load` is effectively a startup op, so
    // the leak is bounded. (Same rationale as `lua_protobuf::set_global_descriptor`.)
    let _leaked = SCHEMA.swap(Box::into_raw(s), Ordering::AcqRel);
}

// ---------------------------------------------------------------------------
// Loading / compilation
// ---------------------------------------------------------------------------

/// Reads an optional string field option from a field-definition table.
fn opt_str(lua: &mut LuaStack<'_>, index: i32, key: &str) -> Result<Option<String>, String> {
    let top = lua.top();
    lua.push(key);
    unsafe { ffi::lua_rawget(lua.as_ptr(), lua.abs_index(index)) };
    let value = lua.value(-1);
    let result = match value.kind() {
        laux::LuaType::Nil => Ok(None),
        laux::LuaType::String => Ok(value.as_string_lossy().map(|value| value.into_owned())),
        _ => Err(format!(
            "schema.load: option '{}' must be a string, got {}",
            key,
            value.name()
        )),
    };
    lua.set_top(top);
    result
}

fn opt_bool(lua: &mut LuaStack<'_>, index: i32, key: &str) -> Result<Option<bool>, String> {
    let top = lua.top();
    lua.push(key);
    unsafe { ffi::lua_rawget(lua.as_ptr(), lua.abs_index(index)) };
    let value = lua.value(-1);
    let result = match value.kind() {
        laux::LuaType::Nil => Ok(None),
        laux::LuaType::Boolean => Ok(value.as_bool()),
        _ => Err(format!(
            "schema.load: option '{}' must be a boolean, got {}",
            key,
            value.name()
        )),
    };
    lua.set_top(top);
    result
}

fn build_raw_proto(
    lua: &mut LuaStack<'_>,
    proto_name: String,
    def_index: i32,
) -> Result<RawProto, String> {
    let explicit_wrapper = opt_bool(lua, def_index, "wrapper")?.unwrap_or(false);
    let wrapper =
        explicit_wrapper || proto_name.starts_with("array_") || proto_name.starts_with("map_");

    let mut fields = Vec::new();
    for mut entry in lua.table_cursor(def_index) {
        let field_name = {
            let field_key = entry.key();
            match field_key.kind() {
                laux::LuaType::String => field_key
                    .as_string_lossy()
                    .map(|value| value.into_owned())
                    .unwrap_or_default(),
                _ => {
                    return Err(format!(
                        "schema.load: proto '{}' has a non-string field name",
                        proto_name
                    ));
                }
            }
        };
        if field_name == "wrapper" {
            continue;
        }
        let field_index = {
            let field_value = entry.value();
            if field_value.kind() != laux::LuaType::Table {
                return Err(format!(
                    "schema.load: field '{}.{}' must be a table, got {}",
                    proto_name,
                    field_name,
                    field_value.name()
                ));
            }
            field_value.index()
        };
        let (container, key_type, value_type) = unsafe {
            let lua = entry.lua_mut();
            let container = match opt_str(lua, field_index, "container")?.as_deref() {
                Some("array") => RawContainer::Array,
                Some("object") => RawContainer::Object,
                None | Some("") => RawContainer::Scalar,
                Some(other) => {
                    return Err(format!(
                        "schema.load: field '{}.{}' has unknown container '{}'",
                        proto_name, field_name, other
                    ));
                }
            };
            let key_type = opt_str(lua, field_index, "key_type")?;
            let value_type = opt_str(lua, field_index, "value_type")?;
            (container, key_type, value_type)
        };
        fields.push(RawField {
            name: field_name,
            container,
            key_type,
            value_type,
        });
    }

    Ok(RawProto {
        name: proto_name,
        wrapper,
        fields,
    })
}

/// Intermediate (pre-resolution) representation captured from Lua.
struct RawField {
    name: String,
    container: RawContainer,
    key_type: Option<String>,
    value_type: Option<String>,
}

enum RawContainer {
    Scalar,
    Array,
    Object,
}

struct RawProto {
    name: String,
    wrapper: bool,
    fields: Vec<RawField>,
}

/// Drains the Lua definition table into owned `RawProto`s, then resolves type
/// references into a compiled [`Schema`]. Any malformed definition or unknown
/// type reference is reported here (fail fast at load).
fn build_schema(lua: &mut LuaStack<'_>) -> Result<Schema, String> {
    let mut raws: Vec<RawProto> = Vec::new();

    for mut entry in lua.table_cursor(1) {
        let proto_name = {
            let key = entry.key();
            match key.kind() {
                laux::LuaType::String => key
                    .as_string_lossy()
                    .map(|value| value.into_owned())
                    .unwrap_or_default(),
                _ => return Err("schema.load: proto name must be a string".to_string()),
            }
        };
        let def_index = {
            let value = entry.value();
            if value.kind() != laux::LuaType::Table {
                return Err(format!(
                    "schema.load: proto '{}' definition must be a table, got {}",
                    proto_name,
                    value.name()
                ));
            }
            value.index()
        };
        raws.push(unsafe { build_raw_proto(entry.lua_mut(), proto_name, def_index) }?);
    }

    // Pass 2: index proto names, then resolve every field's type.
    let mut by_name: HashMap<Box<str>, u32> = HashMap::with_capacity(raws.len());
    for (i, rp) in raws.iter().enumerate() {
        if by_name
            .insert(rp.name.clone().into_boxed_str(), i as u32)
            .is_some()
        {
            return Err(format!("schema.load: duplicate proto '{}'", rp.name));
        }
    }

    let mut protos = Vec::with_capacity(raws.len());
    for rp in &raws {
        let mut fields: HashMap<Box<str>, Field> = HashMap::with_capacity(rp.fields.len());
        for rf in &rp.fields {
            let vt_name = rf.value_type.as_deref().unwrap_or("");
            let value = if let Some(p) = Prim::parse(vt_name) {
                ValueType::Prim(p)
            } else if let Some(&idx) = by_name.get(vt_name) {
                ValueType::Ref(idx)
            } else {
                return Err(format!(
                    "schema.load: field '{}.{}' value_type '{}' is not a known primitive or defined proto",
                    rp.name, rf.name, vt_name
                ));
            };

            let container = match rf.container {
                RawContainer::Scalar => Container::Scalar,
                RawContainer::Array => Container::Array,
                RawContainer::Object => {
                    let kt = rf.key_type.as_deref().unwrap_or("");
                    match Prim::parse(kt) {
                        Some(p) => Container::Object { key: p },
                        None => {
                            return Err(format!(
                                "schema.load: object field '{}.{}' requires a primitive key_type, got '{}'",
                                rp.name, rf.name, kt
                            ));
                        }
                    }
                }
            };

            fields.insert(
                rf.name.clone().into_boxed_str(),
                Field {
                    name: rf.name.clone().into_boxed_str(),
                    container,
                    value,
                },
            );
        }

        if rp.wrapper && !fields.contains_key("data") {
            return Err(format!(
                "schema.load: wrapper proto '{}' must define a 'data' field",
                rp.name
            ));
        }

        protos.push(Proto {
            name: rp.name.clone().into_boxed_str(),
            wrapper: rp.wrapper,
            fields,
        });
    }

    Ok(Schema { protos, by_name })
}

// ---------------------------------------------------------------------------
// Validation
// ---------------------------------------------------------------------------

/// One segment of the error trace path. Field names borrow the (`'static`)
/// compiled schema; only dynamic object keys own a `String`.
enum Seg {
    Field(&'static str),
    Index(usize),
    Key(String),
}

fn join(trace: &[Seg]) -> String {
    use std::fmt::Write;
    let mut s = String::new();
    for seg in trace {
        if !s.is_empty() {
            s.push('.');
        }
        match seg {
            Seg::Field(f) => s.push_str(f),
            Seg::Index(i) => {
                let _ = write!(s, "{}", i);
            }
            Seg::Key(k) => s.push_str(k),
        }
    }
    s
}

fn verify(
    lua: &mut LuaStack<'_>,
    schema: &'static Schema,
    proto_idx: u32,
    index: i32,
    trace: &mut Vec<Seg>,
    depth: u32,
) -> Result<(), String> {
    if depth > MAX_DEPTH {
        return Err(format!(
            "schema: nesting exceeds {} levels (cyclic data?). trace: {}",
            MAX_DEPTH,
            join(trace)
        ));
    }
    let state = lua.state();
    laux::lua_checkstack(state, 8, cstr!("schema.verify"))?;
    let index = lua.abs_index(index);
    let proto = &schema.protos[proto_idx as usize];

    let v = lua.value(index);
    if v.kind() != laux::LuaType::Table {
        let got = v.name();
        return Err(format!(
            "'{}' table expected, got {}. trace: {}",
            proto.name,
            got,
            join(trace)
        ));
    }

    if proto.wrapper {
        return verify_field(lua, schema, proto, index, "data", trace, depth);
    }

    for mut entry in lua.table_cursor(index) {
        let field = {
            let key = entry.key();
            if key.kind() != laux::LuaType::String {
                return Err(format!(
                    "'{}' has a non-string key. trace: {}",
                    proto.name,
                    join(trace)
                ));
            }
            find_field(proto, key.as_str().unwrap_or(""), trace)?
        };
        if let (Container::Scalar, ValueType::Prim(prim)) = (field.container, field.value) {
            trace.push(Seg::Field(&field.name));
            let result = check_primitive(entry.value(), prim, trace);
            trace.pop();
            result?;
            continue;
        }
        let value_index = entry.value().index();
        unsafe {
            verify_resolved_field(entry.lua_mut(), schema, value_index, field, trace, depth)
        }?;
    }
    Ok(())
}

fn find_field(
    proto: &'static Proto,
    field_name: &str,
    trace: &[Seg],
) -> Result<&'static Field, String> {
    proto.fields.get(field_name).ok_or_else(|| {
        format!(
            "attempt to index undefined field '{}.{}'. trace: {}",
            proto.name,
            field_name,
            join(trace)
        )
    })
}

fn verify_field(
    lua: &mut LuaStack<'_>,
    schema: &'static Schema,
    proto: &'static Proto,
    vindex: i32,
    field_name: &str,
    trace: &mut Vec<Seg>,
    depth: u32,
) -> Result<(), String> {
    let field = find_field(proto, field_name, trace)?;
    verify_resolved_field(lua, schema, vindex, field, trace, depth)
}

fn verify_resolved_field(
    lua: &mut LuaStack<'_>,
    schema: &'static Schema,
    vindex: i32,
    field: &'static Field,
    trace: &mut Vec<Seg>,
    depth: u32,
) -> Result<(), String> {
    trace.push(Seg::Field(&field.name));
    let r = match &field.container {
        Container::Scalar => check_value(lua, schema, vindex, field, trace, depth),
        Container::Array => verify_array(lua, schema, vindex, field, trace, depth),
        Container::Object { key } => verify_object(lua, schema, vindex, field, *key, trace, depth),
    };
    trace.pop();
    r
}

fn check_value(
    lua: &mut LuaStack<'_>,
    schema: &'static Schema,
    vindex: i32,
    field: &Field,
    trace: &mut Vec<Seg>,
    depth: u32,
) -> Result<(), String> {
    match &field.value {
        ValueType::Prim(p) => check_primitive(lua.value(vindex), *p, trace),
        ValueType::Ref(idx) => verify(lua, schema, *idx, vindex, trace, depth + 1),
    }
}

fn check_primitive(
    value: LuaStackValue<'_, '_>,
    primitive: Prim,
    trace: &[Seg],
) -> Result<(), String> {
    if primitive.accepts_stack(value) {
        Ok(())
    } else {
        Err(format!(
            "{} expected, got {}, value '{}'. trace: {}",
            primitive.name(),
            value.name(),
            value,
            join(trace)
        ))
    }
}

fn verify_array(
    lua: &mut LuaStack<'_>,
    schema: &'static Schema,
    vindex: i32,
    field: &Field,
    trace: &mut Vec<Seg>,
    depth: u32,
) -> Result<(), String> {
    let state = lua.state();
    let v = lua.value(vindex);
    if v.kind() != laux::LuaType::Table {
        let got = v.name();
        return Err(format!(
            "array (table) expected, got {}. trace: {}",
            got,
            join(trace)
        ));
    }

    let size = lua.array_len(vindex);
    if size == 0 {
        // `array_len` returns 0 for both an empty table and a non-sequence; only
        // the latter is an error, so disambiguate by checking for any key.
        let has_key = {
            let mut cursor = lua.table_cursor(vindex);
            Iterator::next(&mut cursor).is_some()
        };
        if has_key {
            return Err(format!(
                "not a valid array (sequence) table. trace: {}",
                join(trace)
            ));
        }
        return Ok(());
    }

    laux::lua_checkstack(state, 4, cstr!("schema.array"))?;
    let mut cursor = lua.array_cursor_len(vindex, size);
    let mut i = 0;
    while let Some(value) = cursor.next() {
        i += 1;
        trace.push(Seg::Index(i));
        let value_index = value.index();
        let r = match field.value {
            ValueType::Prim(primitive) => check_primitive(value, primitive, trace),
            ValueType::Ref(proto_idx) => unsafe {
                verify(
                    cursor.lua_mut(),
                    schema,
                    proto_idx,
                    value_index,
                    trace,
                    depth + 1,
                )
            },
        };
        trace.pop();
        r?;
    }
    Ok(())
}

fn verify_object(
    lua: &mut LuaStack<'_>,
    schema: &'static Schema,
    vindex: i32,
    field: &Field,
    key_prim: Prim,
    trace: &mut Vec<Seg>,
    depth: u32,
) -> Result<(), String> {
    let state = lua.state();
    let vindex = lua.abs_index(vindex);
    let v = lua.value(vindex);
    if v.kind() != laux::LuaType::Table {
        let got = v.name();
        return Err(format!(
            "object (table) expected, got {}. trace: {}",
            got,
            join(trace)
        ));
    }

    laux::lua_checkstack(state, 6, cstr!("schema.object"))?;
    for mut entry in lua.table_cursor(vindex) {
        // An object whose first key is integer 1 is ambiguous with an array, so
        // it must opt in via the `__object` metafield (mirrors the C++ rule).
        // The let-chain short-circuits: `getmetafield` only runs for the single
        // entry whose key is integer 1 (if any), not once per entry, so a map
        // without that key pays nothing.
        let (key_is_one, key_is_valid, key_name, key_string) = {
            let key = entry.key();
            (
                key.as_integer() == Some(1),
                key_prim.accepts_stack(key),
                key.name(),
                key.to_string(),
            )
        };
        let has_object_meta = !key_is_one
            || unsafe {
                let top = ffi::lua_gettop(state.as_ptr());
                let present = ffi::luaL_getmetafield(state.as_ptr(), vindex, cstr!("__object"))
                    != ffi::LUA_TNIL;
                ffi::lua_settop(state.as_ptr(), top);
                present
            };
        if !has_object_meta {
            return Err(format!(
                "object table uses integer key=1 but is missing metafield '__object'. trace: {}",
                join(trace)
            ));
        }

        trace.push(Seg::Key(key_string));
        if !key_is_valid {
            let msg = format!(
                "$key {} expected, got {}. trace: {}",
                key_prim.name(),
                key_name,
                join(trace)
            );
            trace.pop();
            return Err(msg);
        }
        let r = match field.value {
            ValueType::Prim(primitive) => check_primitive(entry.value(), primitive, trace),
            ValueType::Ref(proto_idx) => unsafe {
                let value_index = entry.value().index();
                verify(
                    entry.lua_mut(),
                    schema,
                    proto_idx,
                    value_index,
                    trace,
                    depth + 1,
                )
            },
        };
        trace.pop();
        r?;
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// FFI entry points
// ---------------------------------------------------------------------------

fn load(lua: &mut LuaStack<'_>) -> Result<c_int, String> {
    if lua.value(1).kind() != laux::LuaType::Table {
        return Err("schema.load: table expected".to_string());
    }
    match build_schema(lua) {
        Ok(s) => {
            set_schema(Box::new(s));
        }
        Err(e) => return Err(e),
    }
    Ok(0)
}

fn validate(lua: &mut LuaStack<'_>) -> Result<c_int, String> {
    let proto_name = match lua.value(1).as_str() {
        Some(name) => name,
        None => return Err("bad argument #1 (valid UTF-8 string expected)".to_string()),
    };
    if lua.value(2).kind() != laux::LuaType::Table {
        return Err("schema.validate: table expected".to_string());
    }

    let schema = match schema() {
        Some(s) => s,
        None => return Err("schema.validate: no schema has been loaded".to_string()),
    };
    let proto_idx = match schema.by_name.get(proto_name) {
        Some(&i) => i,
        None => {
            return Err(format!(
                "schema.validate: attempt to use undefined proto '{proto_name}'"
            ));
        }
    };

    let mut trace: Vec<Seg> = Vec::new();
    trace.push(Seg::Field(&schema.protos[proto_idx as usize].name));
    match verify(lua, schema, proto_idx, 2, &mut trace, 0) {
        Ok(()) => Ok(0),
        Err(msg) => Err(msg),
    }
}

pub extern "C-unwind" fn luaopen_schema(state: LuaState) -> c_int {
    let l = [
        lreg_try!("load", load),
        lreg_try!("validate", validate),
        lreg_null!(),
    ];
    luaL_newlib!(state, l);
    1
}

#[cfg(test)]
#[path = "../../tests/unit/modules/lua_schema.rs"]
mod tests;
