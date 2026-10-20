use super::*;
use moon_base::laux::LuaGlobalState;
use serial_test::serial;
use std::ffi::CString;

// --- minimal FileDescriptorSet wire-format builder ---------------------

fn put_varint(out: &mut Vec<u8>, mut v: u64) {
    while v >= 0x80 {
        out.push(0x80 | (v & 0x7f) as u8);
        v >>= 7;
    }
    out.push(v as u8);
}

/// `LEN`-delimited field: tag + length + payload.
fn put_len(out: &mut Vec<u8>, field: u32, payload: &[u8]) {
    put_varint(out, pb_tag(field, 2) as u64);
    put_varint(out, payload.len() as u64);
    out.extend_from_slice(payload);
}

/// `VARINT` field.
fn put_var(out: &mut Vec<u8>, field: u32, v: u64) {
    put_varint(out, pb_tag(field, 0) as u64);
    put_varint(out, v);
}

/// FieldDescriptorProto: name(1), number(3), label(4), type(5), type_name(6).
fn field_proto(name: &str, number: u32, label: u32, type_: u32, type_name: &str) -> Vec<u8> {
    let mut f = Vec::new();
    put_len(&mut f, 1, name.as_bytes());
    put_var(&mut f, 3, number as u64);
    put_var(&mut f, 4, label as u64);
    put_var(&mut f, 5, type_ as u64);
    if !type_name.is_empty() {
        put_len(&mut f, 6, type_name.as_bytes());
    }
    f
}

/// Build a FileDescriptorSet describing:
///   syntax = "proto3"; package test;
///   message Foo { int32 a = 1; string b = 2; repeated int32 c = 3; }
///   message Bar { Foo foo = 1; map<string,int32> m = 2; }
fn build_descriptor_set() -> Vec<u8> {
    // Foo
    let mut foo = Vec::new();
    put_len(&mut foo, 1, b"Foo");
    put_len(&mut foo, 2, &field_proto("a", 1, 1, 5, "")); // int32
    put_len(&mut foo, 2, &field_proto("b", 2, 1, 9, "")); // string
    put_len(&mut foo, 2, &field_proto("c", 3, 3, 5, "")); // repeated int32

    // Bar.MEntry (synthetic map entry: map<string,int32>)
    let mut mentry = Vec::new();
    put_len(&mut mentry, 1, b"MEntry");
    put_len(&mut mentry, 2, &field_proto("key", 1, 1, 9, "")); // string
    put_len(&mut mentry, 2, &field_proto("value", 2, 1, 5, "")); // int32
    // MessageOptions: map_entry = true (field 7)
    let mut mopts = Vec::new();
    put_var(&mut mopts, 7, 1);
    put_len(&mut mentry, 7, &mopts);

    // Bar
    let mut bar = Vec::new();
    put_len(&mut bar, 1, b"Bar");
    put_len(&mut bar, 2, &field_proto("foo", 1, 1, 11, ".test.Foo")); // message
    put_len(&mut bar, 2, &field_proto("m", 2, 3, 11, ".test.Bar.MEntry")); // repeated map entry
    put_len(&mut bar, 3, &mentry); // nested_type

    // FileDescriptorProto
    let mut file = Vec::new();
    put_len(&mut file, 2, b"test"); // package
    put_len(&mut file, 4, &foo); // message_type
    put_len(&mut file, 4, &bar); // message_type
    put_len(&mut file, 12, b"proto3"); // syntax

    // FileDescriptorSet
    let mut set = Vec::new();
    put_len(&mut set, 1, &file);
    set
}

// --- general descriptor builder ---------------------------------------
//
// Field type numbers (descriptor.proto FieldDescriptorProto.Type).
const T_DOUBLE: u32 = 1;
const T_FLOAT: u32 = 2;
const T_INT64: u32 = 3;
const T_UINT64: u32 = 4;
const T_INT32: u32 = 5;
const T_FIXED64: u32 = 6;
const T_FIXED32: u32 = 7;
const T_BOOL: u32 = 8;
const T_STRING: u32 = 9;
const T_MESSAGE: u32 = 11;
const T_BYTES: u32 = 12;
const T_UINT32: u32 = 13;
const T_ENUM: u32 = 14;
const T_SFIXED32: u32 = 15;
const T_SFIXED64: u32 = 16;
const T_SINT32: u32 = 17;
const T_SINT64: u32 = 18;
// Labels.
const L_OPTIONAL: u32 = 1;
const L_REPEATED: u32 = 3;

/// FieldDescriptorProto with optional `packed` option and `oneof_index`.
#[allow(clippy::too_many_arguments)]
fn fld(
    name: &str,
    number: u32,
    label: u32,
    type_: u32,
    type_name: &str,
    packed: Option<bool>,
    oneof_index: Option<u32>,
) -> Vec<u8> {
    let mut f = Vec::new();
    put_len(&mut f, 1, name.as_bytes());
    put_var(&mut f, 3, number as u64);
    put_var(&mut f, 4, label as u64);
    put_var(&mut f, 5, type_ as u64);
    if !type_name.is_empty() {
        put_len(&mut f, 6, type_name.as_bytes());
    }
    if let Some(p) = packed {
        let mut opts = Vec::new();
        put_var(&mut opts, 2, p as u64);
        put_len(&mut f, 8, &opts);
    }
    if let Some(oi) = oneof_index {
        put_var(&mut f, 9, oi as u64);
    }
    f
}

/// DescriptorProto from fields + nested messages + oneof names, optionally a
/// `map_entry` message.
fn msg(
    name: &str,
    fields: &[Vec<u8>],
    nested: &[Vec<u8>],
    oneof_decls: &[&str],
    map_entry: bool,
) -> Vec<u8> {
    let mut m = Vec::new();
    put_len(&mut m, 1, name.as_bytes());
    for f in fields {
        put_len(&mut m, 2, f);
    }
    for n in nested {
        put_len(&mut m, 3, n);
    }
    for od in oneof_decls {
        let mut o = Vec::new();
        put_len(&mut o, 1, od.as_bytes());
        put_len(&mut m, 8, &o);
    }
    if map_entry {
        let mut opts = Vec::new();
        put_var(&mut opts, 7, 1);
        put_len(&mut m, 7, &opts);
    }
    m
}

/// Synthetic `<name>` map-entry message (key=1, value=2, map_entry=true).
fn map_entry(name: &str, key_type: u32, val_type: u32, val_type_name: &str) -> Vec<u8> {
    msg(
        name,
        &[
            fld("key", 1, L_OPTIONAL, key_type, "", None, None),
            fld("value", 2, L_OPTIONAL, val_type, val_type_name, None, None),
        ],
        &[],
        &[],
        true,
    )
}

/// EnumDescriptorProto with `(name, number)` values.
fn enum_proto(name: &str, values: &[(&str, i32)]) -> Vec<u8> {
    let mut e = Vec::new();
    put_len(&mut e, 1, name.as_bytes());
    for (vn, vv) in values {
        let mut v = Vec::new();
        put_len(&mut v, 1, vn.as_bytes());
        put_var(&mut v, 2, *vv as u32 as u64);
        put_len(&mut e, 2, &v);
    }
    e
}

/// FileDescriptorSet wrapping one file. `syntax = None` => proto2.
fn file_set(
    syntax: Option<&str>,
    package: &str,
    messages: &[Vec<u8>],
    enums: &[Vec<u8>],
) -> Vec<u8> {
    let mut file = Vec::new();
    if !package.is_empty() {
        put_len(&mut file, 2, package.as_bytes());
    }
    for m in messages {
        put_len(&mut file, 4, m);
    }
    for e in enums {
        put_len(&mut file, 5, e);
    }
    if let Some(s) = syntax {
        put_len(&mut file, 12, s.as_bytes());
    }
    let mut set = Vec::new();
    put_len(&mut set, 1, &file);
    set
}

fn new_vm() -> (LuaState, LuaGlobalState) {
    unsafe {
        let raw = ffi::luaL_newstate();
        assert!(!raw.is_null());
        let state = LuaState::new(raw).unwrap();
        let guard = LuaGlobalState::new(state);
        ffi::luaL_openlibs(raw);
        ffi::luaL_requiref(
            raw,
            cstr!("protobuf"),
            crate::not_null_wrapper!(luaopen_protobuf),
            1,
        );
        ffi::lua_pop(raw, 1);
        (state, guard)
    }
}

fn set_global_bytes(state: LuaState, name: &str, data: &[u8]) {
    unsafe {
        ffi::lua_pushlstring(state.as_ptr(), data.as_ptr() as _, data.len());
        let cname = CString::new(name).unwrap();
        ffi::lua_setglobal(state.as_ptr(), cname.as_ptr());
    }
}

fn run(state: LuaState, code: &str) -> Result<(), String> {
    unsafe {
        let c = CString::new(code).unwrap();
        if ffi::luaL_dostring(state.as_ptr(), c.as_ptr()) != ffi::LUA_OK {
            let err = ffi::lua_tostring(state.as_ptr(), -1);
            let msg = if err.is_null() {
                "unknown error".to_string()
            } else {
                std::ffi::CStr::from_ptr(err).to_string_lossy().into_owned()
            };
            ffi::lua_pop(state.as_ptr(), 1);
            Err(msg)
        } else {
            Ok(())
        }
    }
}

#[test]
#[serial]
fn protobuf_table_reads_ignore_metamethods() {
    let (state, _guard) = new_vm();
    for packed in [false, true] {
        let descriptor = file_set(
            Some("proto3"),
            "",
            &[msg(
                "Repeated",
                &[fld(
                    "values",
                    1,
                    L_REPEATED,
                    T_INT32,
                    "",
                    Some(packed),
                    None,
                )],
                &[],
                &[],
                false,
            )],
            &[],
        );
        set_global_bytes(state, "_desc", &descriptor);
        run(
            state,
            r#"
                local pb = require("protobuf")
                assert(pb.load(_desc))
                local values = {}
                for i = 1, 5 do values[i] = i end
                values[2] = nil
                assert(rawlen(values) == 5)
                local entered = 0
                local mt = {
                    __index = function() entered = entered + 1; return 99 end,
                    __len = function() error("__len must not run") end,
                    __pairs = function() error("__pairs must not run") end,
                }
                setmetatable(values, mt)
                local input = setmetatable({values = values}, mt)
                local decoded = pb.decode("Repeated", pb.encode("Repeated", input))
                assert(entered == 0, "encoding invoked __index")
                -- Raw nil follows the existing integer conversion (zero),
                -- rather than obtaining a replacement value from __index.
                assert(#decoded.values == 5 and decoded.values[2] == 0)
                for _, i in ipairs({1, 3, 4, 5}) do assert(decoded.values[i] == i) end

                local inherited = setmetatable({}, {__index = {values = {99}}})
                assert(#pb.decode("Repeated", pb.encode("Repeated", inherited)).values == 0)

                values[2] = 2147483648
                local ok, err = pcall(pb.encode, "Repeated", input)
                assert(not ok and err:find("out of i32 range", 1, true))
                assert(entered == 0)
                assert(pb.decode("Repeated", pb.encode("Repeated", {values = {7}})).values[1] == 7)
            "#,
        )
        .unwrap();
    }
}

#[test]
#[serial]
fn protobuf_scalar_and_repeated_roundtrip() {
    let (state, _guard) = new_vm();
    set_global_bytes(state, "_desc", &build_descriptor_set());
    let code = r#"
            local pb = require("protobuf")
            assert(pb.load(_desc), "load failed")

            local bytes = pb.encode("test.Foo", { a = 42, b = "hello", c = {1, 2, 3} })
            local t = pb.decode("test.Foo", bytes)
            assert(t.a == 42, "a mismatch: " .. tostring(t.a))
            assert(t.b == "hello", "b mismatch: " .. tostring(t.b))
            assert(#t.c == 3 and t.c[1] == 1 and t.c[3] == 3, "repeated mismatch")

            -- proto3 default (zero/empty) fields are set directly on the table
            local empty = pb.decode("test.Foo", pb.encode("test.Foo", {}))
            assert(empty.a == 0, "default int should be 0")
            assert(empty.b == "", "default string should be empty")
            assert(type(empty.c) == "table" and #empty.c == 0, "default repeated should be empty table")
        "#;
    run(state, code).expect("scalar/repeated roundtrip");
}

#[test]
#[serial]
fn protobuf_nested_and_map_roundtrip() {
    let (state, _guard) = new_vm();
    set_global_bytes(state, "_desc", &build_descriptor_set());
    let code = r#"
            local pb = require("protobuf")
            assert(pb.load(_desc), "load failed")

            local bytes = pb.encode("test.Bar", {
                foo = { a = 7, b = "nested" },
                m = { ["x"] = 10, ["y"] = 20 },
            })
            local t = pb.decode("test.Bar", bytes)
            assert(t.foo.a == 7, "nested a mismatch")
            assert(t.foo.b == "nested", "nested b mismatch")
            assert(t.m.x == 10 and t.m.y == 20, "map mismatch")
        "#;
    run(state, code).expect("nested/map roundtrip");
}

#[test]
#[serial]
fn protobuf_messages_and_fields_introspection() {
    let (state, _guard) = new_vm();
    set_global_bytes(state, "_desc", &build_descriptor_set());
    let code = r#"
            local pb = require("protobuf")
            assert(pb.load(_desc))
            local msgs = pb.messages()
            assert(msgs["test.Foo"] == "Foo", "messages should map full->simple name")
            assert(msgs["test.Bar"] == "Bar")
            local fields = pb.fields("test.Foo")
            assert(fields.a == 5, "a should be TYPE_INT32 (5)")
            assert(fields.b == 9, "b should be TYPE_STRING (9)")
        "#;
    run(state, code).expect("introspection");
}

// ======================================================================
// Ported from lua-protobuf test.lua (only features lua_protobuf.rs
// supports). Skipped there: the `protoc` text compiler, `pb.io/buffer/
// slice/conv/typefmt`, hooks, `pb.option` state flags, `pb.pack/unpack`,
// extensions, enum name<->value (`pb.enum`), `pb.defaults`, `pb.clear/
// type/state`, per-file mixed syntax, and tolerant decoding of malformed
// input. Those map to APIs/behaviors this module does not implement.
// ======================================================================

/// `test_type`: every scalar wire type roundtrips (proto2).
#[test]
#[serial]
fn ported_all_scalar_types_roundtrip() {
    let (state, _guard) = new_vm();
    let set = file_set(
        None,
        "",
        &[msg(
            "TestTypes",
            &[
                fld("dv", 1, L_OPTIONAL, T_DOUBLE, "", None, None),
                fld("fv", 2, L_OPTIONAL, T_FLOAT, "", None, None),
                fld("i64v", 3, L_OPTIONAL, T_INT64, "", None, None),
                fld("u64v", 4, L_OPTIONAL, T_UINT64, "", None, None),
                fld("i32v", 5, L_OPTIONAL, T_INT32, "", None, None),
                fld("u32v", 13, L_OPTIONAL, T_UINT32, "", None, None),
                fld("f64v", 6, L_OPTIONAL, T_FIXED64, "", None, None),
                fld("f32v", 7, L_OPTIONAL, T_FIXED32, "", None, None),
                fld("bv", 8, L_OPTIONAL, T_BOOL, "", None, None),
                fld("sv", 9, L_OPTIONAL, T_STRING, "", None, None),
                fld("btv", 12, L_OPTIONAL, T_BYTES, "", None, None),
                fld("sf32v", 15, L_OPTIONAL, T_SFIXED32, "", None, None),
                fld("sf64v", 16, L_OPTIONAL, T_SFIXED64, "", None, None),
                fld("s32v", 17, L_OPTIONAL, T_SINT32, "", None, None),
                fld("s64v", 18, L_OPTIONAL, T_SINT64, "", None, None),
            ],
            &[],
            &[],
            false,
        )],
        &[],
    );
    set_global_bytes(state, "_d", &set);
    let code = r#"
            local pb = require("protobuf")
            assert(pb.load(_d))
            local data = {
               dv = 0.125, fv = 0.5,
               i64v = -12345678901234567, u64v = 12345678901234567,
               i32v = -2101112222, u32v = 2101112222,
               f64v = 12345678901234567, f32v = 1231231234,
               bv = true, sv = "foo", btv = "bar",
               sf32v = -123, sf64v = -456, s32v = -1234, s64v = -4321,
            }
            local t = pb.decode("TestTypes", pb.encode("TestTypes", data))
            for k, v in pairs(data) do
               assert(t[k] == v, k .. " mismatch: " .. tostring(t[k]))
            end
        "#;
    run(state, code).expect("scalar types roundtrip");
}

/// `test_packed`: proto3 packs scalar repeateds by default, honors an
/// explicit `[packed=false]`, and proto2 honors `[packed=true]`.
#[test]
#[serial]
fn ported_packed_wire_bytes() {
    let (state, _guard) = new_vm();
    let d1 = file_set(
        Some("proto3"),
        "",
        &[msg(
            "MyMessage",
            &[
                fld("intList", 1, L_REPEATED, T_INT32, "", None, None),
                fld("nopacks", 2, L_REPEATED, T_INT32, "", Some(false), None),
            ],
            &[],
            &[],
            false,
        )],
        &[],
    );
    let d2 = file_set(
        None,
        "",
        &[
            msg("Empty", &[], &[], &[], false),
            msg(
                "TestPacked",
                &[fld("packs", 1, L_REPEATED, T_INT64, "", Some(true), None)],
                &[],
                &[],
                false,
            ),
        ],
        &[],
    );
    set_global_bytes(state, "_d1", &d1);
    set_global_bytes(state, "_d2", &d2);
    let code = r#"
            local pb = require("protobuf")
            assert(pb.load(_d1))
            -- proto3 repeated scalar => packed
            assert(pb.encode("MyMessage", { intList = {1,2,3} }) == "\10\3\1\2\3",
                   "intList should be packed")
            -- explicit [packed=false] => one tag per element
            assert(pb.encode("MyMessage", { nopacks = {1,2,3} }) == "\16\1\16\2\16\3",
                   "nopacks should be unpacked")

            assert(pb.load(_d2))
            local t = pb.decode("TestPacked", pb.encode("TestPacked", { packs = {1,2,3,4,-1,-2,3} }))
            assert(#t.packs == 7, "packs length")
            assert(t.packs[5] == -1 and t.packs[6] == -2, "packs negatives")
            local e = pb.decode("TestPacked", pb.encode("TestPacked", { packs = {} }))
            assert(type(e.packs) == "table" and #e.packs == 0, "empty packed")
        "#;
    run(state, code).expect("packed wire bytes");
}

/// `test_map`: string-keyed maps to scalar and to message values (proto3).
#[test]
#[serial]
fn ported_map_roundtrip() {
    let (state, _guard) = new_vm();
    let testmap = msg(
        "TestMap",
        &[
            fld(
                "map",
                1,
                L_REPEATED,
                T_MESSAGE,
                ".TestMap.MapEntry",
                None,
                None,
            ),
            fld(
                "packed_map",
                2,
                L_REPEATED,
                T_MESSAGE,
                ".TestMap.PackedMapEntry",
                Some(true),
                None,
            ),
            fld(
                "msg_map",
                3,
                L_REPEATED,
                T_MESSAGE,
                ".TestMap.MsgMapEntry",
                None,
                None,
            ),
        ],
        &[
            map_entry("MapEntry", T_STRING, T_INT32, ""),
            map_entry("PackedMapEntry", T_STRING, T_INT32, ""),
            map_entry("MsgMapEntry", T_STRING, T_MESSAGE, ".TestEmpty"),
        ],
        &[],
        false,
    );
    let set = file_set(
        Some("proto3"),
        "",
        &[msg("TestEmpty", &[], &[], &[], false), testmap],
        &[],
    );
    set_global_bytes(state, "_d", &set);
    let code = r#"
            local pb = require("protobuf")
            assert(pb.load(_d))

            -- empty maps roundtrip to empty tables
            local e = pb.decode("TestMap", pb.encode("TestMap",
                { map = {}, packed_map = {}, msg_map = {} }))
            assert(type(e.map) == "table" and next(e.map) == nil, "empty map")
            assert(type(e.msg_map) == "table" and next(e.msg_map) == nil, "empty msg_map")

            -- populated scalar maps; msg_map defaults to empty
            local t = pb.decode("TestMap", pb.encode("TestMap",
                { map = { one = 1, two = 2, three = 3 }, packed_map = { a = 10 } }))
            assert(t.map.one == 1 and t.map.two == 2 and t.map.three == 3, "scalar map")
            assert(t.packed_map.a == 10, "packed_map")
            assert(type(t.msg_map) == "table" and next(t.msg_map) == nil, "msg_map default")

            -- map<string, message> with an empty-message value
            local m = pb.decode("TestMap", pb.encode("TestMap", { msg_map = { [""] = {} } }))
            assert(type(m.msg_map[""]) == "table", "msg_map empty-message value")
        "#;
    run(state, code).expect("map roundtrip");
}

/// `test_oneof`: decode records the active field under the oneof name.
/// (Empty-message oneof members are dropped by `ignore_empty`, so only
/// non-empty members are exercised.)
#[test]
#[serial]
fn ported_oneof_discriminator() {
    let (state, _guard) = new_vm();
    let testoneof = msg(
        "TestOneof",
        &[
            fld("foo", 1, L_OPTIONAL, T_UINT32, "", None, Some(0)),
            fld("bar", 2, L_OPTIONAL, T_STRING, "", None, Some(0)),
        ],
        &[],
        &["body"],
        false,
    );
    let outter = msg(
        "Outter",
        &[fld(
            "msg",
            1,
            L_OPTIONAL,
            T_MESSAGE,
            ".TestOneof",
            None,
            None,
        )],
        &[],
        &[],
        false,
    );
    let set = file_set(Some("proto3"), "", &[testoneof, outter], &[]);
    set_global_bytes(state, "_d", &set);
    let code = r#"
            local pb = require("protobuf")
            assert(pb.load(_d))

            local a = pb.decode("TestOneof", pb.encode("TestOneof", { foo = 5 }))
            assert(a.foo == 5 and a.body == "foo", "oneof foo discriminator")

            local b = pb.decode("TestOneof", pb.encode("TestOneof", { bar = "hi" }))
            assert(b.bar == "hi" and b.body == "bar", "oneof bar discriminator")

            local o = pb.decode("Outter", pb.encode("Outter", { msg = { foo = 7 } }))
            assert(o.msg.foo == 7 and o.msg.body == "foo", "nested oneof")
        "#;
    run(state, code).expect("oneof discriminator");
}

/// `test_enum`: enums encode/decode as integers; `enums()` and `fields()`
/// expose them. (Enum name<->value conversion is not supported.)
#[test]
#[serial]
fn ported_enum_as_integer() {
    let (state, _guard) = new_vm();
    let set = file_set(
        None,
        "",
        &[msg(
            "TestEnum",
            &[fld("color", 1, L_OPTIONAL, T_ENUM, ".Color", None, None)],
            &[],
            &[],
            false,
        )],
        &[enum_proto(
            "Color",
            &[("Red", 0), ("Green", 1), ("Blue", 2)],
        )],
    );
    set_global_bytes(state, "_d", &set);
    let code = r#"
            local pb = require("protobuf")
            assert(pb.load(_d))

            local t = pb.decode("TestEnum", pb.encode("TestEnum", { color = 2 }))
            assert(t.color == 2, "enum integer roundtrip")

            local found = false
            for _, name in ipairs(pb.enums()) do
               if name == "Color" then found = true end
            end
            assert(found, "Color should be listed by enums()")

            assert(pb.fields("TestEnum").color == 14, "color should be TYPE_ENUM (14)")
        "#;
    run(state, code).expect("enum as integer");
}

/// `test_io`-style behavior: unknown fields are skipped across all wire
/// types, leaving known fields intact.
#[test]
#[serial]
fn ported_unknown_field_skipping() {
    let (state, _guard) = new_vm();
    let set = file_set(
        Some("proto3"),
        "",
        &[msg(
            "Msg",
            &[fld("a", 1, L_OPTIONAL, T_INT32, "", None, None)],
            &[],
            &[],
            false,
        )],
        &[],
    );
    set_global_bytes(state, "_d", &set);
    let code = r#"
            local pb = require("protobuf")
            assert(pb.load(_d))

            -- unknown varint field 5, then a=7
            assert(pb.decode("Msg", "\40\99\8\7").a == 7, "skip unknown varint")
            -- unknown fixed64 field 6 (8 bytes), then a=9
            assert(pb.decode("Msg", "\49\1\2\3\4\5\6\7\8\8\9").a == 9, "skip unknown fixed64")
            -- unknown fixed32 field 4 (4 bytes), then a=4
            assert(pb.decode("Msg", "\37\1\2\3\4\8\4").a == 4, "skip unknown fixed32")
            -- unknown length-delimited field 7, then a=3
            assert(pb.decode("Msg", "\58\3xyz\8\3").a == 3, "skip unknown bytes")
        "#;
    run(state, code).expect("unknown field skipping");
}

/// Error paths: wire-type mismatch, wrong Lua value type, unknown message.
#[test]
#[serial]
fn ported_error_paths() {
    let (state, _guard) = new_vm();
    let set = file_set(
        Some("proto3"),
        "",
        &[msg(
            "M",
            &[fld("s", 1, L_OPTIONAL, T_STRING, "", None, None)],
            &[],
            &[],
            false,
        )],
        &[],
    );
    set_global_bytes(state, "_d", &set);
    let code = r#"
            local pb = require("protobuf")
            assert(pb.load(_d))

            -- field 's' is a string (LEN) but bytes carry a varint => mismatch
            assert(not pcall(pb.decode, "M", "\8\1"), "wire type mismatch must error")
            -- string field given a number
            assert(not pcall(pb.encode, "M", { s = 123 }), "wrong value type must error")
            -- unknown message names
            assert(not pcall(pb.encode, "Nope", {}), "unknown message encode must error")
            assert(not pcall(pb.decode, "Nope", ""), "unknown message decode must error")

            local ok, err = pcall(pb.decode, "M", 42)
            assert(not ok, "non-string decode input must error")
            assert(tostring(err):find("argument #2 must be a string or buffer", 1, true),
                "decode input error should identify argument #2: " .. tostring(err))
        "#;
    run(state, code).expect("error paths");

    let location_error = run(
        state,
        r#"
                local pb = require("protobuf")
                pb.decode("M", 42)
            "#,
    )
    .expect_err("unprotected decode error should be raised");
    assert!(
        location_error.starts_with("[string"),
        "missing Lua source location: {location_error}"
    );
    assert!(
        location_error.contains("argument #2 must be a string or buffer"),
        "decode error should identify argument #2: {location_error}"
    );
}

/// Varint-heavy encode/decode throughput. Ignored by default; run with:
///   cargo test -p moon-runtime --release -- --ignored --nocapture protobuf_bench
#[test]
#[ignore]
#[serial]
fn protobuf_bench_varint_roundtrip() {
    use std::time::Instant;

    let (state, _guard) = new_vm();
    set_global_bytes(state, "_desc", &build_descriptor_set());

    // Warm up: load descriptor + build a varint-heavy payload (a large
    // packed repeated int32) once, then loop encode/decode in Lua so the
    // measured region is dominated by the Rust varint read/write paths.
    let iters = 200_000u64;
    let setup = r#"
            local pb = require("protobuf")
            assert(pb.load(_desc), "load failed")
            -- Foo.c is `repeated int32` (proto3 => packed): fill with mixed-size
            -- varints so the encoder/decoder exercise 1..5 byte varints.
            local c = {}
            for i = 1, 256 do c[i] = (i * 2654435761) % 2147483647 end
            _msg = { a = 123456, b = "benchmark-payload-string", c = c }
            _bytes = pb.encode("test.Foo", _msg)
        "#;
    run(state, setup).expect("bench setup");

    let bench = format!(
        r#"
            local pb = require("protobuf")
            local enc, dec = pb.encode, pb.decode
            local msg, bytes = _msg, _bytes
            local sink = 0
            for _ = 1, {iters} do
                local b = enc("test.Foo", msg)
                local t = dec("test.Foo", b)
                sink = sink + t.a
            end
            _sink = sink
        "#
    );

    let start = Instant::now();
    run(state, &bench).expect("bench loop");
    let elapsed = start.elapsed();

    let per = elapsed.as_secs_f64() / iters as f64;
    eprintln!(
        "protobuf_bench: {iters} encode+decode roundtrips in {:.3?} ({:.0} ns/op, {:.2} M ops/s)",
        elapsed,
        per * 1e9,
        (iters as f64 / elapsed.as_secs_f64()) / 1e6
    );
}

// --- baseline (pre-optimization) varint paths, for A/B comparison --------

fn baseline_write_varint(buf: &mut Buffer, mut val: u64) {
    while val >= 0x80 {
        buf.write(0x80 | (val & 0x7f) as u8);
        val >>= 7;
    }
    buf.write(val as u8);
}

/// Exercises the cached-list decode path: interleaved non-packed repeated
/// fields (cache must flush + recover the true length via `lua_rawlen`) and
/// contiguous repeated messages (cache reuse across recursive decodes).
#[test]
#[serial]
fn ported_repeated_interleaved_and_messages() {
    let (state, _guard) = new_vm();

    // proto2 (syntax = None) => repeated int32 defaults to unpacked, so each
    // element is its own varint-tagged occurrence in the wire stream.
    let inner = msg(
        "Inner",
        &[fld("v", 1, L_OPTIONAL, T_INT32, "", None, None)],
        &[],
        &[],
        false,
    );
    let m = msg(
        "M",
        &[
            fld("a", 1, L_REPEATED, T_INT32, "", None, None),
            fld("b", 2, L_REPEATED, T_INT32, "", None, None),
            fld("items", 3, L_REPEATED, T_MESSAGE, ".Inner", None, None),
        ],
        &[],
        &[],
        false,
    );
    let set = file_set(None, "", &[m, inner], &[]);
    set_global_bytes(state, "_d", &set);

    // Hand-crafted INTERLEAVED wire: a=10, b=20, a=11, b=21.
    // tag(a)=field1,varint=0x08 ; tag(b)=field2,varint=0x10.
    let wire = [0x08u8, 10, 0x10, 20, 0x08, 11, 0x10, 21];
    set_global_bytes(state, "_wire", &wire);

    let code = r#"
            local pb = require("protobuf")
            assert(pb.load(_d))

            -- interleaved repeated scalars: order within each field preserved
            local t = pb.decode("M", _wire)
            assert(#t.a == 2 and t.a[1] == 10 and t.a[2] == 11, "a interleaved mismatch")
            assert(#t.b == 2 and t.b[1] == 20 and t.b[2] == 21, "b interleaved mismatch")

            -- contiguous repeated message (recursion + cache reuse)
            local enc = pb.encode("M", { items = { {v=1}, {v=2}, {v=3} } })
            local d = pb.decode("M", enc)
            assert(#d.items == 3, "items length mismatch")
            assert(d.items[1].v == 1 and d.items[2].v == 2 and d.items[3].v == 3,
                "items values mismatch")

            -- mixed: scalars and repeated messages together
            local enc2 = pb.encode("M", { a = {1,2,3}, items = { {v=7} }, b = {9} })
            local d2 = pb.decode("M", enc2)
            assert(#d2.a == 3 and d2.a[3] == 3, "mixed a mismatch")
            assert(#d2.b == 1 and d2.b[1] == 9, "mixed b mismatch")
            assert(#d2.items == 1 and d2.items[1].v == 7, "mixed items mismatch")
        "#;
    run(state, code).expect("repeated interleaved/messages roundtrip");
}

/// Verifies the native `pb.encode` / `pb.decode` C functions are
/// stack-balanced: each leaves exactly one result and nothing else on the
/// Lua stack. (The in-function `debug_assert_eq!` stack checks additionally
/// guard every nested encode/decode during all the other tests.)
#[test]
#[serial]
fn native_calls_keep_lua_stack_balanced() {
    let (state, _guard) = new_vm();
    set_global_bytes(state, "_desc", &build_descriptor_set());
    run(
        state,
        r#"
            local pb = require("protobuf")
            assert(pb.load(_desc))
            _enc, _dec = pb.encode, pb.decode
            _tbl = { foo = { a = 7, b = "hi", c = {1, 2, 3} }, m = { alpha = 1, beta = 2 } }
            _bytes = pb.encode("test.Bar", _tbl)
        "#,
    )
    .expect("setup");

    let getglobal = |name: &str| {
        let c = CString::new(name).unwrap();
        unsafe { ffi::lua_getglobal(state.as_ptr(), c.as_ptr()) };
    };
    let pushstr = |s: &str| {
        let c = CString::new(s).unwrap();
        unsafe { ffi::lua_pushstring(state.as_ptr(), c.as_ptr()) };
    };

    unsafe {
        let l = state.as_ptr();

        // pb.decode("test.Bar", _bytes)
        let top0 = ffi::lua_gettop(l);
        getglobal("_dec");
        pushstr("test.Bar");
        getglobal("_bytes");
        assert_eq!(
            ffi::lua_pcall(l, 2, 1, 0),
            ffi::LUA_OK,
            "pb.decode call failed"
        );
        assert_eq!(
            ffi::lua_gettop(l),
            top0 + 1,
            "decode must push exactly one result"
        );
        ffi::lua_pop(l, 1);
        assert_eq!(ffi::lua_gettop(l), top0, "decode left the stack unbalanced");

        // pb.encode("test.Bar", _tbl)
        let top1 = ffi::lua_gettop(l);
        getglobal("_enc");
        pushstr("test.Bar");
        getglobal("_tbl");
        assert_eq!(
            ffi::lua_pcall(l, 2, 1, 0),
            ffi::LUA_OK,
            "pb.encode call failed"
        );
        assert_eq!(
            ffi::lua_gettop(l),
            top1 + 1,
            "encode must push exactly one result"
        );
        ffi::lua_pop(l, 1);
        assert_eq!(ffi::lua_gettop(l), top1, "encode left the stack unbalanced");
    }
}

/// Encode-side field-name lookup throughput: the std SipHash map (previous
/// implementation) vs the FxHash map now used for `fields_by_name`. This is
/// the per-key cost paid by `encode_message` for every entry in the user's
/// Lua table, so it directly reflects the encode-path change.
///   cargo test -p moon-runtime --release -- --ignored --nocapture protobuf_bench_name_lookup
#[test]
#[ignore]
fn protobuf_bench_name_lookup() {
    use std::collections::HashMap;
    use std::hint::black_box;
    use std::time::{Duration, Instant};

    // Representative protobuf field names for a medium-sized message.
    let names: Vec<String> = [
        "id",
        "name",
        "created_at",
        "updated_at",
        "owner_id",
        "status",
        "score",
        "tags",
        "description",
        "is_active",
        "parent_id",
        "metadata",
        "version",
        "checksum",
        "payload",
        "extra_options",
    ]
    .iter()
    .map(|s| s.to_string())
    .collect();

    let mut std_map: HashMap<String, usize> = HashMap::default();
    let mut fx_map: FxHashMap<String, usize> = FxHashMap::default();
    for (i, n) in names.iter().enumerate() {
        std_map.insert(n.clone(), i);
        fx_map.insert(n.clone(), i);
    }

    let rounds = 500_000u64;

    let t = Instant::now();
    let mut acc = 0usize;
    for _ in 0..rounds {
        for n in &names {
            acc += *std_map.get(black_box(n.as_str())).unwrap();
        }
    }
    black_box(acc);
    let std_d = t.elapsed();

    let t = Instant::now();
    let mut acc = 0usize;
    for _ in 0..rounds {
        for n in &names {
            acc += *fx_map.get(black_box(n.as_str())).unwrap();
        }
    }
    black_box(acc);
    let fx_d = t.elapsed();

    let total = rounds * names.len() as u64;
    let ns = |d: Duration| d.as_secs_f64() / total as f64 * 1e9;
    eprintln!("protobuf_bench_name_lookup ({total} lookups each):");
    eprintln!(
        "  field name lookup: SipHash {:.2} ns/op -> FxHash {:.2} ns/op  ({:+.1}%)",
        ns(std_d),
        ns(fx_d),
        (ns(fx_d) - ns(std_d)) / ns(std_d) * 100.0
    );
}

#[test]
#[serial]
fn protobuf_map_entry_duplicate_key_no_stack_leak() {
    let (state, _guard) = new_vm();
    set_global_bytes(state, "_desc", &build_descriptor_set());

    // test.Bar.m is map<string,int32> (Bar field #2). Craft two malformed
    // map entries by hand:
    //   _mal1: entry with key "a", then key "b", then value 5 (two keys).
    //   _mal2: entry with key "a", then key "b" and nothing else (truncated).
    // decode_map must treat the entry like a message (last key wins) and
    // must never leave a stray key on the Lua stack, which used to leak a
    // value into the caller and corrupt the whole decode result.
    let key = |s: &str| {
        let mut e = Vec::new();
        put_len(&mut e, 1, s.as_bytes()); // field #1 key (string)
        e
    };
    let mut mal1 = key("a");
    mal1.extend(key("b"));
    put_var(&mut mal1, 2, 5); // field #2 value (int32)

    let mut mal2 = key("a");
    mal2.extend(key("b"));

    let wrap_bar = |entry: &[u8]| {
        let mut bar = Vec::new();
        put_len(&mut bar, 2, entry); // Bar field #2 = m
        bar
    };
    set_global_bytes(state, "_mal1", &wrap_bar(&mal1));
    set_global_bytes(state, "_mal2", &wrap_bar(&mal2));

    let code = r#"
            local pb = require("protobuf")
            assert(pb.load(_desc), "load failed")
            local t = pb.decode("test.Bar", _mal1)
            assert(type(t) == "table", "decode returned " .. type(t))
            assert(t.m.b == 5, "duplicate key: last key should win")
            assert(t.m.a == nil, "duplicate key: dropped key must not appear")
            local t2 = pb.decode("test.Bar", _mal2)
            assert(type(t2) == "table", "truncated decode returned " .. type(t2))
            assert(next(t2.m) == nil, "key without value must not be inserted")
        "#;
    run(state, code).expect("duplicate map key must not corrupt decode");
}

#[test]
#[serial]
fn protobuf_oneof_empty_member_ignored() {
    let (state, _guard) = new_vm();

    // message One { oneof choice { string a = 1; string b = 2; } }
    let one = msg(
        "One",
        &[
            fld("a", 1, L_OPTIONAL, T_STRING, "", None, Some(0)),
            fld("b", 2, L_OPTIONAL, T_STRING, "", None, Some(0)),
        ],
        &[],
        &["choice"],
        false,
    );
    set_global_bytes(
        state,
        "_desc",
        &file_set(Some("proto3"), "test", &[one], &[]),
    );

    let code = r#"
            local pb = require("protobuf")
            assert(pb.load(_desc), "load failed")

            -- a="" is empty; ignore_empty treats it as unset, so this is a
            -- single-member oneof and must encode instead of erroring with
            -- "multiple oneof fields set".
            local bytes = pb.encode("test.One", { a = "", b = "hi" })
            local t = pb.decode("test.One", bytes)
            assert(type(t) == "table", "decode returned " .. type(t))
            assert(t.b == "hi", "b should roundtrip: " .. tostring(t.b))

            -- all members empty: nothing committed, no conflict
            assert(pcall(pb.encode, "test.One", { a = "", b = "" }))

            -- genuinely two committed members are still rejected
            local ok, err = pcall(pb.encode, "test.One", { a = "x", b = "y" })
            assert(not ok, "two committed oneof members must error")
            assert(
                type(err) == "string" and err:find("multiple oneof", 1, true),
                "error should name the conflict: " .. tostring(err)
            )
        "#;
    run(state, code).expect("oneof empty members must not count as set");
}

/// Isolated varint read/write throughput (no Lua). Compares the optimized
/// paths against the previous baseline so the speedup is visible without
/// the Lua interpreter / table overhead that dominates the roundtrip bench.
///   cargo test -p moon-runtime --release -- --ignored --nocapture protobuf_bench_varint_only
#[test]
#[ignore]
fn protobuf_bench_varint_only() {
    use std::hint::black_box;
    use std::time::Instant;

    // Mixed-size varints: 1..=10 bytes, weighted toward small values like
    // real protobuf payloads (field tags, small ints, lengths).
    let mut values: Vec<u64> = Vec::with_capacity(4096);
    let mut x: u64 = 0x9E37_79B9_7F4A_7C15;
    for i in 0..4096u64 {
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        // Spread across byte-lengths: mask to (i % 9 + 1) * 7 bits.
        let bits = ((i % 9) + 1) * 7;
        values.push(x & ((1u128 << bits) - 1) as u64);
    }
    let rounds = 20_000u64;

    // --- writes ---
    let mut buf = Buffer::new();
    let t = Instant::now();
    for _ in 0..rounds {
        buf.clear();
        for &v in &values {
            baseline_write_varint(&mut buf, black_box(v));
        }
        black_box(buf.data());
    }
    let base_w = t.elapsed();

    let mut buf = Buffer::new();
    let t = Instant::now();
    for _ in 0..rounds {
        buf.clear();
        for &v in &values {
            write_varint(&mut buf, black_box(v));
        }
        black_box(buf.data());
    }
    let opt_w = t.elapsed();

    // Encoded blob to decode (the optimized writer, padded so the fast path
    // engages until the very end).
    let mut blob = Buffer::new();
    for &v in &values {
        write_varint(&mut blob, v);
    }
    let bytes = blob.data().to_vec();
    let n = values.len() as u64;

    // --- reads (both go through StreamReader + Result so the only
    // difference measured is fast-path vs forced slow-path body) ---
    let t = Instant::now();
    for _ in 0..rounds {
        let mut sr = StreamReader::new(&bytes);
        for _ in 0..n {
            black_box(sr.read_varint_slow().unwrap());
        }
    }
    let base_r = t.elapsed();

    let t = Instant::now();
    for _ in 0..rounds {
        let mut sr = StreamReader::new(&bytes);
        for _ in 0..n {
            black_box(sr.read_varint().unwrap());
        }
    }
    let opt_r = t.elapsed();

    let total = rounds * n;
    let ns = |d: std::time::Duration| d.as_secs_f64() / total as f64 * 1e9;
    eprintln!("protobuf_bench_varint_only ({total} ops each):");
    eprintln!(
        "  write: baseline {:.2} ns/op -> optimized {:.2} ns/op  ({:+.1}%)",
        ns(base_w),
        ns(opt_w),
        (ns(opt_w) - ns(base_w)) / ns(base_w) * 100.0
    );
    eprintln!(
        "  read:  baseline {:.2} ns/op -> optimized {:.2} ns/op  ({:+.1}%)",
        ns(base_r),
        ns(opt_r),
        (ns(opt_r) - ns(base_r)) / ns(base_r) * 100.0
    );
}
