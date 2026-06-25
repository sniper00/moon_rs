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
//! - **Errors propagate as `Result`** and surface through a single
//!   `laux::lua_error` at the FFI boundary (no exceptions across C frames).
//! - **The trace allocates lazily**: array indices are cheap [`Seg::Index`] and
//!   the path is only joined into a string when an error is actually produced.
//! - **The global schema uses an `AtomicPtr` swap** (mirroring `lua_protobuf`),
//!   so `load` may be called multiple times and readers on other actor threads
//!   keep a valid `&'static` view (the previous schema is intentionally leaked).

use moon_base::laux::{LuaState, LuaTable, LuaValue};
use moon_base::{cstr, ffi, laux, lreg, lreg_null, luaL_newlib};
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
    fn accepts(self, v: &LuaValue) -> bool {
        match self {
            Prim::Int32 => {
                matches!(v, LuaValue::Integer(n) if *n >= i32::MIN as i64 && *n <= i32::MAX as i64)
            }
            Prim::Uint32 => matches!(v, LuaValue::Integer(n) if *n >= 0 && *n <= u32::MAX as i64),
            Prim::Int64 => matches!(v, LuaValue::Integer(_)),
            // A Lua integer is an `i64`, so the only meaningful uint64 check is
            // non-negativity; the upper bound is `i64::MAX` by construction.
            Prim::Uint64 => matches!(v, LuaValue::Integer(n) if *n >= 0),
            // Lua treats integers as numbers too, so accept both.
            Prim::Float => matches!(v, LuaValue::Number(_) | LuaValue::Integer(_)),
            Prim::Bool => matches!(v, LuaValue::Boolean(_)),
            Prim::Str => matches!(v, LuaValue::String(_)),
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
fn opt_str(def: &LuaTable, key: &str) -> Result<Option<String>, String> {
    let sv = def.rawget(key);
    match &sv.value {
        LuaValue::Nil => Ok(None),
        LuaValue::String(s) => Ok(Some(String::from_utf8_lossy(s).into_owned())),
        other => Err(format!(
            "schema.load: option '{}' must be a string, got {}",
            key,
            other.name()
        )),
    }
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
fn build_schema(state: LuaState) -> Result<Schema, String> {
    let outer = LuaTable::from_stack(state, 1);
    let mut raws: Vec<RawProto> = Vec::new();

    for (k, v) in outer.iter() {
        let proto_name = match &k {
            LuaValue::String(s) => String::from_utf8_lossy(s).into_owned(),
            _ => return Err("schema.load: proto name must be a string".to_string()),
        };
        let def = match v {
            LuaValue::Table(t) => t,
            other => {
                return Err(format!(
                    "schema.load: proto '{}' definition must be a table, got {}",
                    proto_name,
                    other.name()
                ));
            }
        };

        let explicit_wrapper = match def.rawget("wrapper").value {
            LuaValue::Boolean(b) => b,
            LuaValue::Nil => false,
            ref other => {
                return Err(format!(
                    "schema.load: proto '{}' field 'wrapper' must be a boolean, got {}",
                    proto_name,
                    other.name()
                ));
            }
        };
        // Backward compatibility with Moon's original schema convention: a proto
        // whose name begins with `array_`/`map_` is a wrapper (its single `data`
        // field holds the real container). Generators such as `proto.json` rely
        // on this prefix and never emit an explicit `wrapper = true`, so without
        // this fallback their `array_*`/`map_*` protos would be validated as
        // ordinary protos and reject their integer/sequence keys.
        let wrapper = explicit_wrapper
            || proto_name.starts_with("array_")
            || proto_name.starts_with("map_");

        let mut fields = Vec::new();
        for (fk, fv) in def.iter() {
            let field_name = match &fk {
                LuaValue::String(s) => String::from_utf8_lossy(s).into_owned(),
                _ => {
                    return Err(format!(
                        "schema.load: proto '{}' has a non-string field name",
                        proto_name
                    ));
                }
            };
            // `wrapper` is reserved configuration, not a field.
            if field_name == "wrapper" {
                continue;
            }
            let ftbl = match fv {
                LuaValue::Table(t) => t,
                other => {
                    return Err(format!(
                        "schema.load: field '{}.{}' must be a table, got {}",
                        proto_name,
                        field_name,
                        other.name()
                    ));
                }
            };

            let container = match opt_str(&ftbl, "container")?.as_deref() {
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
            let key_type = opt_str(&ftbl, "key_type")?;
            let value_type = opt_str(&ftbl, "value_type")?;

            fields.push(RawField {
                name: field_name,
                container,
                key_type,
                value_type,
            });
        }

        raws.push(RawProto {
            name: proto_name,
            wrapper,
            fields,
        });
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
    state: LuaState,
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
    laux::lua_checkstack(state, 8, cstr!("schema.verify"));
    let index = laux::lua_absindex(state, index);
    let proto = &schema.protos[proto_idx as usize];

    if laux::lua_type(state, index) != laux::LuaType::Table {
        let got = LuaValue::from_stack(state, index).name();
        return Err(format!(
            "'{}' table expected, got {}. trace: {}",
            proto.name,
            got,
            join(trace)
        ));
    }

    if proto.wrapper {
        return verify_field(state, schema, proto, index, "data", trace, depth);
    }

    let t = LuaTable::from_stack(state, index);
    for (k, _v) in t.iter() {
        let key = match &k {
            LuaValue::String(s) => std::str::from_utf8(s).unwrap_or(""),
            _ => {
                return Err(format!(
                    "'{}' has a non-string key. trace: {}",
                    proto.name,
                    join(trace)
                ));
            }
        };
        let vindex = laux::lua_absindex(state, -1);
        verify_field(state, schema, proto, vindex, key, trace, depth)?;
    }
    Ok(())
}

fn verify_field(
    state: LuaState,
    schema: &'static Schema,
    proto: &'static Proto,
    vindex: i32,
    field_name: &str,
    trace: &mut Vec<Seg>,
    depth: u32,
) -> Result<(), String> {
    let field = match proto.fields.get(field_name) {
        Some(f) => f,
        None => {
            return Err(format!(
                "attempt to index undefined field '{}.{}'. trace: {}",
                proto.name,
                field_name,
                join(trace)
            ));
        }
    };

    trace.push(Seg::Field(&field.name));
    let r = match &field.container {
        Container::Scalar => check_value(state, schema, vindex, field, trace, depth),
        Container::Array => verify_array(state, schema, vindex, field, trace, depth),
        Container::Object { key } => {
            verify_object(state, schema, vindex, field, *key, trace, depth)
        }
    };
    trace.pop();
    r
}

fn check_value(
    state: LuaState,
    schema: &'static Schema,
    vindex: i32,
    field: &Field,
    trace: &mut Vec<Seg>,
    depth: u32,
) -> Result<(), String> {
    match &field.value {
        ValueType::Prim(p) => {
            let v = LuaValue::from_stack(state, vindex);
            if p.accepts(&v) {
                Ok(())
            } else {
                Err(format!(
                    "{} expected, got {}, value '{}'. trace: {}",
                    p.name(),
                    v.name(),
                    v,
                    join(trace)
                ))
            }
        }
        ValueType::Ref(idx) => verify(state, schema, *idx, vindex, trace, depth + 1),
    }
}

fn verify_array(
    state: LuaState,
    schema: &'static Schema,
    vindex: i32,
    field: &Field,
    trace: &mut Vec<Seg>,
    depth: u32,
) -> Result<(), String> {
    if laux::lua_type(state, vindex) != laux::LuaType::Table {
        let got = LuaValue::from_stack(state, vindex).name();
        return Err(format!(
            "array (table) expected, got {}. trace: {}",
            got,
            join(trace)
        ));
    }

    let t = LuaTable::from_stack(state, vindex);
    let size = t.array_len();
    if size == 0 {
        // `array_len` returns 0 for both an empty table and a non-sequence; only
        // the latter is an error, so disambiguate by checking for any key.
        if t.iter().next().is_some() {
            return Err(format!(
                "not a valid array (sequence) table. trace: {}",
                join(trace)
            ));
        }
        return Ok(());
    }

    laux::lua_checkstack(state, 4, cstr!("schema.array"));
    for i in 1..=size {
        unsafe {
            ffi::lua_rawgeti(state.as_ptr(), vindex, i as ffi::lua_Integer);
        }
        trace.push(Seg::Index(i));
        let elem = laux::lua_absindex(state, -1);
        let r = check_value(state, schema, elem, field, trace, depth);
        trace.pop();
        laux::lua_pop(state, 1);
        r?;
    }
    Ok(())
}

fn verify_object(
    state: LuaState,
    schema: &'static Schema,
    vindex: i32,
    field: &Field,
    key_prim: Prim,
    trace: &mut Vec<Seg>,
    depth: u32,
) -> Result<(), String> {
    if laux::lua_type(state, vindex) != laux::LuaType::Table {
        let got = LuaValue::from_stack(state, vindex).name();
        return Err(format!(
            "object (table) expected, got {}. trace: {}",
            got,
            join(trace)
        ));
    }

    let t = LuaTable::from_stack(state, vindex);
    laux::lua_checkstack(state, 6, cstr!("schema.object"));
    for (k, _v) in t.iter() {
        // An object whose first key is integer 1 is ambiguous with an array, so
        // it must opt in via the `__object` metafield (mirrors the C++ rule).
        if let LuaValue::Integer(1) = k
            && t.getmetafield(cstr!("__object")).is_none()
        {
            return Err(format!(
                "object table uses integer key=1 but is missing metafield '__object'. trace: {}",
                join(trace)
            ));
        }

        trace.push(Seg::Key(k.to_string()));
        if !key_prim.accepts(&k) {
            return Err(format!(
                "$key {} expected, got {}. trace: {}",
                key_prim.name(),
                k.name(),
                join(trace)
            ));
        }
        let vidx = laux::lua_absindex(state, -1);
        let r = check_value(state, schema, vidx, field, trace, depth);
        trace.pop();
        r?;
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// FFI entry points
// ---------------------------------------------------------------------------

extern "C-unwind" fn load(state: LuaState) -> c_int {
    laux::lua_checktype(state, 1, ffi::LUA_TTABLE);
    match build_schema(state) {
        Ok(s) => {
            set_schema(Box::new(s));
            0
        }
        Err(e) => laux::lua_error(state, e),
    }
}

extern "C-unwind" fn validate(state: LuaState) -> c_int {
    let proto_name = laux::lua_get::<&str>(state, 1);
    laux::lua_checktype(state, 2, ffi::LUA_TTABLE);

    let schema = match schema() {
        Some(s) => s,
        None => laux::lua_error(state, "schema.validate: no schema has been loaded".to_string()),
    };
    let proto_idx = match schema.by_name.get(proto_name) {
        Some(&i) => i,
        None => laux::lua_error(
            state,
            format!("schema.validate: attempt to use undefined proto '{proto_name}'"),
        ),
    };

    let mut trace: Vec<Seg> = Vec::new();
    trace.push(Seg::Field(&schema.protos[proto_idx as usize].name));
    match verify(state, schema, proto_idx, 2, &mut trace, 0) {
        Ok(()) => 0,
        Err(msg) => laux::lua_error(state, msg),
    }
}

pub extern "C-unwind" fn luaopen_schema(state: LuaState) -> c_int {
    let l = [
        lreg!("load", load),
        lreg!("validate", validate),
        lreg_null!(),
    ];
    luaL_newlib!(state, l);
    1
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn prim_parse_aliases() {
        assert_eq!(Prim::parse("sint32"), Some(Prim::Int32));
        assert_eq!(Prim::parse("fixed32"), Some(Prim::Uint32));
        assert_eq!(Prim::parse("sfixed64"), Some(Prim::Int64));
        assert_eq!(Prim::parse("fixed64"), Some(Prim::Uint64));
        assert_eq!(Prim::parse("double"), Some(Prim::Float));
        assert_eq!(Prim::parse("bytes"), Some(Prim::Str));
        assert_eq!(Prim::parse("message"), None);
    }

    #[test]
    fn int_range_and_sign_checks() {
        assert!(Prim::Int32.accepts(&LuaValue::Integer(i32::MAX as i64)));
        assert!(!Prim::Int32.accepts(&LuaValue::Integer(i32::MAX as i64 + 1)));
        assert!(!Prim::Int32.accepts(&LuaValue::Integer(i32::MIN as i64 - 1)));

        assert!(Prim::Uint32.accepts(&LuaValue::Integer(u32::MAX as i64)));
        assert!(!Prim::Uint32.accepts(&LuaValue::Integer(-1)));
        assert!(!Prim::Uint32.accepts(&LuaValue::Integer(u32::MAX as i64 + 1)));

        assert!(Prim::Uint64.accepts(&LuaValue::Integer(i64::MAX)));
        assert!(!Prim::Uint64.accepts(&LuaValue::Integer(-1)));

        assert!(Prim::Int64.accepts(&LuaValue::Integer(-1)));
    }

    #[test]
    fn non_integer_types() {
        assert!(Prim::Bool.accepts(&LuaValue::Boolean(true)));
        assert!(!Prim::Bool.accepts(&LuaValue::Integer(1)));

        // float accepts both floats and integers (Lua numbers).
        assert!(Prim::Float.accepts(&LuaValue::Number(1.5)));
        assert!(Prim::Float.accepts(&LuaValue::Integer(3)));
        assert!(!Prim::Float.accepts(&LuaValue::String(b"x")));

        assert!(Prim::Str.accepts(&LuaValue::String(b"hi")));
        assert!(!Prim::Str.accepts(&LuaValue::Integer(1)));

        // integer types reject floats.
        assert!(!Prim::Int32.accepts(&LuaValue::Number(1.5)));
    }

    #[test]
    fn trace_join() {
        let trace = vec![
            Seg::Field("player"),
            Seg::Field("bag"),
            Seg::Index(3),
            Seg::Key("name".to_string()),
        ];
        assert_eq!(join(&trace), "player.bag.3.name");
    }
}
