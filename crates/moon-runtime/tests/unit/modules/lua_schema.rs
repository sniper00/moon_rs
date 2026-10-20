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
    let integer = laux::LuaType::Integer;
    assert!(Prim::Int32.accepts_kind(integer, Some(i32::MAX as i64)));
    assert!(!Prim::Int32.accepts_kind(integer, Some(i32::MAX as i64 + 1)));
    assert!(!Prim::Int32.accepts_kind(integer, Some(i32::MIN as i64 - 1)));

    assert!(Prim::Uint32.accepts_kind(integer, Some(u32::MAX as i64)));
    assert!(!Prim::Uint32.accepts_kind(integer, Some(-1)));
    assert!(!Prim::Uint32.accepts_kind(integer, Some(u32::MAX as i64 + 1)));

    assert!(Prim::Uint64.accepts_kind(integer, Some(i64::MAX)));
    assert!(!Prim::Uint64.accepts_kind(integer, Some(-1)));

    assert!(Prim::Int64.accepts_kind(integer, Some(-1)));
}

#[test]
fn non_integer_types() {
    assert!(Prim::Bool.accepts_kind(laux::LuaType::Boolean, None));
    assert!(!Prim::Bool.accepts_kind(laux::LuaType::Integer, Some(1)));

    // float accepts both floats and integers (Lua numbers).
    assert!(Prim::Float.accepts_kind(laux::LuaType::Number, None));
    assert!(Prim::Float.accepts_kind(laux::LuaType::Integer, Some(3)));
    assert!(!Prim::Float.accepts_kind(laux::LuaType::String, None));

    assert!(Prim::Str.accepts_kind(laux::LuaType::String, None));
    assert!(!Prim::Str.accepts_kind(laux::LuaType::Integer, Some(1)));

    // integer types reject floats.
    assert!(!Prim::Int32.accepts_kind(laux::LuaType::Number, None));
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

impl Prim {
    fn accepts_kind(self, kind: laux::LuaType, integer: Option<i64>) -> bool {
        match self {
            Prim::Int32 => integer.is_some_and(|n| n >= i32::MIN as i64 && n <= i32::MAX as i64),
            Prim::Uint32 => integer.is_some_and(|n| n >= 0 && n <= u32::MAX as i64),
            Prim::Int64 => kind == laux::LuaType::Integer,
            Prim::Uint64 => integer.is_some_and(|n| n >= 0),
            Prim::Float => matches!(kind, laux::LuaType::Number | laux::LuaType::Integer),
            Prim::Bool => kind == laux::LuaType::Boolean,
            Prim::Str => kind == laux::LuaType::String,
        }
    }
}
