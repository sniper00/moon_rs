use luars::{Lua, LuaApi, LuaValue, SafeOption, Stdlib};

fn new_vm() -> Lua {
    let mut vm = Lua::new(SafeOption::default());
    vm.open_stdlib(Stdlib::All).unwrap();
    lualib::luaopen_custom_libs(&mut vm).unwrap();
    vm
}

fn eval_bool(vm: &mut Lua, code: &str) -> bool {
    vm.eval::<LuaValue>(code)
        .unwrap_or_else(|e| panic!("Lua error: {}", vm.get_error_message(e)))
        .as_boolean()
        .expect("expected boolean result")
}

// --- seri pack/unpack roundtrip tests ---

#[test]
fn seri_roundtrip_integer() {
    let mut vm = new_vm();
    let code = r#"
        local seri = require("seri")
        local buf = seri.packstring(42)
        local val = seri.unpack(buf)
        return val == 42
    "#;
    assert!(eval_bool(&mut vm, code));
}

#[test]
fn seri_roundtrip_negative_integer() {
    let mut vm = new_vm();
    let code = r#"
        local seri = require("seri")
        local buf = seri.packstring(-100)
        local val = seri.unpack(buf)
        return val == -100
    "#;
    assert!(eval_bool(&mut vm, code));
}

#[test]
fn seri_roundtrip_string() {
    let mut vm = new_vm();
    let code = r#"
        local seri = require("seri")
        local buf = seri.packstring("hello world")
        local val = seri.unpack(buf)
        return val == "hello world"
    "#;
    assert!(eval_bool(&mut vm, code));
}

#[test]
fn seri_roundtrip_boolean() {
    let mut vm = new_vm();
    let code = r#"
        local seri = require("seri")
        local buf = seri.packstring(true)
        local val = seri.unpack(buf)
        return val == true
    "#;
    assert!(eval_bool(&mut vm, code));
}

#[test]
fn seri_roundtrip_nil() {
    let mut vm = new_vm();
    let code = r#"
        local seri = require("seri")
        local buf = seri.packstring(nil)
        local val = seri.unpack(buf)
        return val == nil
    "#;
    assert!(eval_bool(&mut vm, code));
}

#[test]
fn seri_roundtrip_multiple_values() {
    let mut vm = new_vm();
    let code = r#"
        local seri = require("seri")
        local buf = seri.packstring(1, "two", true)
        local a, b, c = seri.unpack(buf)
        return a == 1 and b == "two" and c == true
    "#;
    assert!(eval_bool(&mut vm, code));
}

#[test]
fn seri_roundtrip_table() {
    let mut vm = new_vm();
    let code = r#"
        local seri = require("seri")
        local t = {name = "alice", age = 30}
        local buf = seri.packstring(t)
        local result = seri.unpack(buf)
        return result.name == "alice" and result.age == 30
    "#;
    assert!(eval_bool(&mut vm, code));
}

#[test]
fn seri_roundtrip_array() {
    let mut vm = new_vm();
    let code = r#"
        local seri = require("seri")
        local t = {10, 20, 30}
        local buf = seri.packstring(t)
        local result = seri.unpack(buf)
        return result[1] == 10 and result[2] == 20 and result[3] == 30
    "#;
    assert!(eval_bool(&mut vm, code));
}

#[test]
fn seri_roundtrip_nested_table() {
    let mut vm = new_vm();
    let code = r#"
        local seri = require("seri")
        local t = {inner = {x = 1, y = 2}}
        local buf = seri.packstring(t)
        local result = seri.unpack(buf)
        return result.inner.x == 1 and result.inner.y == 2
    "#;
    assert!(eval_bool(&mut vm, code));
}

#[test]
fn seri_roundtrip_large_integer() {
    let mut vm = new_vm();
    let code = r#"
        local seri = require("seri")
        local big = 2^53
        local buf = seri.packstring(big)
        local val = seri.unpack(buf)
        return val == big
    "#;
    assert!(eval_bool(&mut vm, code));
}

#[test]
fn seri_roundtrip_empty_string() {
    let mut vm = new_vm();
    let code = r#"
        local seri = require("seri")
        local buf = seri.packstring("")
        local val = seri.unpack(buf)
        return val == ""
    "#;
    assert!(eval_bool(&mut vm, code));
}

#[test]
fn seri_packstring_roundtrip() {
    let mut vm = new_vm();
    let code = r#"
        local seri = require("seri")
        local buf = seri.packstring("hello")
        local val = seri.unpack(buf)
        return val == "hello"
    "#;
    assert!(eval_bool(&mut vm, code));
}

// --- JSON encode/decode roundtrip tests ---

#[test]
fn json_encode_decode_object() {
    let mut vm = new_vm();
    let code = r#"
        local json = require("json")
        local obj = {name = "bob", age = 25}
        local str = json.encode(obj)
        local result = json.decode(str)
        return result.name == "bob" and result.age == 25
    "#;
    assert!(eval_bool(&mut vm, code));
}

#[test]
fn json_encode_decode_array() {
    let mut vm = new_vm();
    let code = r#"
        local json = require("json")
        local arr = {1, 2, 3}
        local str = json.encode(arr)
        local result = json.decode(str)
        return result[1] == 1 and result[2] == 2 and result[3] == 3
    "#;
    assert!(eval_bool(&mut vm, code));
}

#[test]
fn json_encode_decode_string() {
    let mut vm = new_vm();
    let code = r#"
        local json = require("json")
        local str = json.encode("hello")
        local result = json.decode(str)
        return result == "hello"
    "#;
    assert!(eval_bool(&mut vm, code));
}

#[test]
fn json_encode_decode_number() {
    let mut vm = new_vm();
    let code = r#"
        local json = require("json")
        local str = json.encode(42)
        local result = json.decode(str)
        return result == 42
    "#;
    assert!(eval_bool(&mut vm, code));
}

#[test]
fn json_encode_decode_boolean() {
    let mut vm = new_vm();
    let code = r#"
        local json = require("json")
        local str = json.encode(true)
        return json.decode(str) == true
    "#;
    assert!(eval_bool(&mut vm, code));
}

#[test]
fn json_encode_decode_nested() {
    let mut vm = new_vm();
    let code = r#"
        local json = require("json")
        local obj = {users = {{name = "a"}, {name = "b"}}}
        local str = json.encode(obj)
        local result = json.decode(str)
        return result.users[1].name == "a" and result.users[2].name == "b"
    "#;
    assert!(eval_bool(&mut vm, code));
}

#[test]
fn json_decode_invalid_raises_lua_error() {
    let mut vm = new_vm();
    let code = r#"
        local json = require("json")
        local ok, err = pcall(json.decode, "{invalid json")
        return ok == false and type(err) == "string"
    "#;
    assert!(eval_bool(&mut vm, code));
}

#[test]
fn json_decode_empty_input_returns_nil() {
    let mut vm = new_vm();
    let code = r#"
        local json = require("json")
        return json.decode("") == nil
    "#;
    assert!(eval_bool(&mut vm, code));
}

#[test]
fn json_encode_decode_empty_object() {
    let mut vm = new_vm();
    let code = r#"
        local json = require("json")
        local str = json.encode({})
        return str == "[]" or str == "{}"
    "#;
    assert!(eval_bool(&mut vm, code));
}

#[test]
fn json_object_forces_array_like_table_to_object() {
    let mut vm = new_vm();
    let code = r#"
        local json = require("json")
        local obj = json.object({ "a", "b" })
        local str = json.encode(obj)
        return str:sub(1, 1) == "{"
            and str:find('"1":"a"', 1, true) ~= nil
            and str:find('"2":"b"', 1, true) ~= nil
    "#;
    assert!(eval_bool(&mut vm, code));
}

#[test]
fn json_array_forces_empty_table_to_array() {
    let mut vm = new_vm();
    let code = r#"
        local json = require("json")
        local arr = json.array()
        return json.encode(arr) == "[]"
    "#;
    assert!(eval_bool(&mut vm, code));
}

#[test]
fn json_decode_preserves_object_type_for_number_keys() {
    let mut vm = new_vm();
    let code = r#"
        local json = require("json")
        local obj = json.decode('{"1":"a"}')
        local str = json.encode(obj)
        return str:sub(1, 1) == "{"
            and str:find('"1":"a"', 1, true) ~= nil
    "#;
    assert!(eval_bool(&mut vm, code));
}

#[test]
fn json_decode_preserves_empty_array_type() {
    let mut vm = new_vm();
    let code = r#"
        local json = require("json")
        local arr = json.decode("[]")
        return json.encode(arr) == "[]"
    "#;
    assert!(eval_bool(&mut vm, code));
}

#[test]
fn json_encode_decode_special_chars() {
    let mut vm = new_vm();
    let code = r#"
        local json = require("json")
        local obj = {msg = 'hello "world"\nnewline'}
        local str = json.encode(obj)
        local result = json.decode(str)
        return result.msg == obj.msg
    "#;
    assert!(eval_bool(&mut vm, code));
}

#[test]
fn json_null_value() {
    let mut vm = new_vm();
    let code = r#"
        local json = require("json")
        local result = json.decode('{"a": null}')
        return result.a == json.null
    "#;
    assert!(eval_bool(&mut vm, code));
}

#[test]
fn json_encode_decode_unicode() {
    let mut vm = new_vm();
    let code = r#"
        local json = require("json")
        local obj = {text = "你好世界"}
        local str = json.encode(obj)
        local result = json.decode(str)
        return result.text == "你好世界"
    "#;
    assert!(eval_bool(&mut vm, code));
}
