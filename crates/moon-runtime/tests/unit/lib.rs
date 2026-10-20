use super::*;
use moon_base::ffi;
use moon_base::laux::LuaGlobalState;
use moon_runtime::buffer::Buffer;
use moon_runtime::context::{self, Message, MessageBody};
use std::ffi::CString;

fn new_lua_vm() -> (LuaState, LuaGlobalState) {
    unsafe {
        let raw = ffi::luaL_newstate();
        assert!(!raw.is_null(), "failed to create Lua state");
        let state = LuaState::new(raw).unwrap();
        let guard = LuaGlobalState::new(state);
        ffi::luaL_openlibs(raw);
        lua_require!(state, "json", lua_json::luaopen_json);
        lua_require!(state, "buffer", lua_buffer::luaopen_buffer);
        lua_require!(state, "seri", lua_seri::luaopen_seri);
        lua_require!(state, "schema", lua_schema::luaopen_schema);
        lua_require!(state, "utils", lua_utils::luaopen_utils);
        (state, guard)
    }
}

fn run_lua(state: LuaState, code: &str) -> Result<(), String> {
    unsafe {
        let c_code = CString::new(code).unwrap();
        let status = ffi::luaL_dostring(state.as_ptr(), c_code.as_ptr());
        if status != ffi::LUA_OK {
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

fn run_lua_expr(state: LuaState, expr: &str) -> String {
    let code = format!("_test_result = tostring({})", expr);
    run_lua(state, &code).expect("lua expression failed");
    unsafe {
        let c_field = CString::new("_test_result").unwrap();
        ffi::lua_getglobal(state.as_ptr(), c_field.as_ptr());
        let s = ffi::lua_tostring(state.as_ptr(), -1);
        let result = std::ffi::CStr::from_ptr(s).to_string_lossy().into_owned();
        ffi::lua_pop(state.as_ptr(), 1);
        result
    }
}

// ========================= JSON tests =========================

#[test]
fn json_encode_table() {
    let (state, _guard) = new_lua_vm();
    let result = run_lua_expr(state, r#"require("json").encode({a=1, b="hello"})"#);
    let v: serde_json::Value = serde_json::from_str(&result).unwrap();
    assert_eq!(v["a"], 1);
    assert_eq!(v["b"], "hello");
}

#[test]
fn schema_cursor_validation_handles_nested_arrays_and_objects() {
    let (state, _guard) = new_lua_vm();
    let code = r#"
            local schema = require("schema")
            schema.load {
                Item = {
                    id = { value_type = "int32" },
                    tags = { container = "array", value_type = "string" },
                },
                Root = {
                    child = { value_type = "Item" },
                    scores = { container = "array", value_type = "int64" },
                    items = { container = "object", key_type = "int32", value_type = "Item" },
                },
            }
            schema.validate("Root", {
                child = { id = 1, tags = { "a", "b" } },
                scores = { 10, 20, 30 },
                items = { [2] = { id = 3, tags = {} } },
            })
            local ok, err = pcall(function()
                schema.validate("Root", {
                    child = { id = 1.5, tags = {} },
                    scores = { 10 },
                    items = {},
                })
            end)
            assert(not ok)
            assert(string.find(err, "trace: Root.child.id", 1, true))
        "#;
    run_lua(state, code).expect("schema cursor validation failed");
}

#[test]
fn json_encode_array() {
    let (state, _guard) = new_lua_vm();
    let result = run_lua_expr(state, r#"require("json").encode({1, 2, 3})"#);
    assert_eq!(result, "[1,2,3]");
}

#[test]
fn json_encode_empty_table_as_array() {
    let (state, _guard) = new_lua_vm();
    let result = run_lua_expr(state, r#"require("json").encode({})"#);
    assert_eq!(result, "[]");
}

#[test]
fn json_decode_object() {
    let (state, _guard) = new_lua_vm();
    let result = run_lua_expr(state, r#"require("json").decode('{"x":42}').x"#);
    assert_eq!(result, "42");
}

#[test]
fn json_decode_array() {
    let (state, _guard) = new_lua_vm();
    let result = run_lua_expr(state, r#"require("json").decode('[10,20,30]')[2]"#);
    assert_eq!(result, "20");
}

#[test]
fn json_roundtrip() {
    let (state, _guard) = new_lua_vm();
    let code = r#"
            local json = require("json")
            local t = {name="test", values={1,2,3}, nested={ok=true}}
            local s = json.encode(t)
            local t2 = json.decode(s)
            assert(t2.name == "test")
            assert(t2.values[1] == 1)
            assert(t2.values[3] == 3)
            assert(t2.nested.ok == true)
        "#;
    run_lua(state, code).expect("json roundtrip failed");
}

#[test]
fn json_decode_null() {
    let (state, _guard) = new_lua_vm();
    let code = r#"
            local json = require("json")
            local t = json.decode('{"a":null}')
            assert(type(t.a) == "userdata", "null should be lightuserdata")
            assert(t.a == json.null)
        "#;
    run_lua(state, code).expect("json null test failed");
}

#[test]
fn json_encode_nested_types() {
    let (state, _guard) = new_lua_vm();
    let code = r#"
            local json = require("json")
            local s = json.encode({n=1, f=3.14, b=true, s="hello"})
            local t = json.decode(s)
            assert(t.n == 1)
            assert(math.abs(t.f - 3.14) < 0.001)
            assert(t.b == true)
            assert(t.s == "hello")
        "#;
    run_lua(state, code).expect("json nested types failed");
}

#[test]
fn json_decode_depth_limit() {
    let (state, _guard) = new_lua_vm();
    let mut deep = String::new();
    for _ in 0..70 {
        deep.push_str(r#"{"a":"#);
    }
    deep.push('1');
    for _ in 0..70 {
        deep.push('}');
    }
    let code = format!(
        r#"local ok, err = pcall(require("json").decode, '{}') return ok"#,
        deep
    );
    let result = run_lua_expr(state, &format!(r#"(function() {} end)()"#, code));
    assert_eq!(result, "false", "deeply nested JSON should fail");
}

#[test]
fn json_pointer_input_validates_length_before_accessing_memory() {
    let (state, _guard) = new_lua_vm();
    unsafe {
        // Zero-length input must not dereference even a non-null dangling pointer.
        ffi::lua_pushlightuserdata(state.as_ptr(), std::ptr::dangling_mut::<u8>().cast());
        ffi::lua_setglobal(state.as_ptr(), cstr!("empty_pointer"));
    }
    run_lua(
        state,
        r#"
        local json = require('json')
        assert(select('#', json.decode(empty_pointer, 0)) == 0)
        assert(select('#', json.decode(json.null, 0)) == 0)
        assert(select('#', json.decode('')) == 0)
        for _, length in ipairs({-1, math.mininteger}) do
            local ok, err = pcall(json.decode, empty_pointer, length)
            assert(not ok and err:find('length must not be negative', 1, true), tostring(err))
        end
        assert(not pcall(json.decode, empty_pointer, 1.5))
        assert(json.decode('{"ok":true}').ok)
    "#,
    )
    .expect("JSON pointer lengths must be checked before reading");
}

#[test]
fn json_decode_rethrows_depth_errors_and_recovers_for_the_next_call() {
    let (state, _guard) = new_lua_vm();
    run_lua(
        state,
        r#"
        local json = require('json')
        local deep = string.rep('[', 70) .. '1' .. string.rep(']', 70)
        for _ = 1, 100 do
            local ok, err = pcall(json.decode, deep)
            assert(not ok and err:find('json.decode: too deep', 1, true), tostring(err))
            assert(json.decode('{"value":[1,true,"hello"]}').value[3] == 'hello')
        end
        assert(not pcall(json.decode, '{'))
        assert(json.decode('42') == 42)
    "#,
    )
    .expect("protected JSON decoding must preserve errors and subsequent calls");
}

#[test]
fn json_number_key_option() {
    let (state, _guard) = new_lua_vm();
    let code = r#"
            local json = require("json")
            local t = json.decode('{"1":"a","2":"b"}')
            assert(t[1] == "a", "number key should be converted")
            assert(t[2] == "b")
        "#;
    run_lua(state, code).expect("json number key test failed");
}

#[test]
fn json_boolean_options_preserve_lua_truthiness() {
    let (state, _guard) = new_lua_vm();
    let code = r#"
            local json = require("json")
            local old = json.options("enable_number_key", false)
            assert(old == true)

            old = json.options("enable_number_key", 0)
            assert(old == false)

            old = json.options("enable_number_key", nil)
            assert(old == true)
        "#;
    run_lua(state, code).expect("json option truthiness changed");
}

#[test]
fn json_object_empty_encodes_as_object() {
    let (state, _guard) = new_lua_vm();
    let result = run_lua_expr(state, r#"require("json").encode(require("json").object())"#);
    assert_eq!(result, "{}");
}

#[test]
fn json_array_empty_encodes_as_array() {
    let (state, _guard) = new_lua_vm();
    let result = run_lua_expr(state, r#"require("json").encode(require("json").array())"#);
    assert_eq!(result, "[]");
}

#[test]
fn json_object_with_data() {
    let (state, _guard) = new_lua_vm();
    let code = r#"
            local json = require("json")
            local t = json.object()
            t.name = "test"
            local s = json.encode(t)
            local t2 = json.decode(s)
            assert(t2.name == "test")
        "#;
    run_lua(state, code).expect("json object with data failed");
}

#[test]
fn json_array_with_data() {
    let (state, _guard) = new_lua_vm();
    let code = r#"
            local json = require("json")
            local t = json.array()
            t[1] = "a"
            t[2] = "b"
            local s = json.encode(t)
            assert(s == '["a","b"]', "expected array encoding, got: " .. s)
        "#;
    run_lua(state, code).expect("json array with data failed");
}

#[test]
fn json_concat_and_resp_preserve_encoded_payloads() {
    let (state, _guard) = new_lua_vm();
    let code = r#"
            local json = require("json")
            local buffer = require("buffer")

            local ptr = json.concat({"a", 2, true, {x = 1}})
            local data = buffer.read(ptr, buffer.size(ptr))
            assert(data == 'a2true{"x":1}', "json.concat payload mismatch: " .. data)
            buffer.drop(ptr)

            local resp_ptr, hash = json.concat_resp("SET", "key", "value")
            assert(type(hash) == "number", "RESP hash must be numeric")
            local resp = buffer.read(resp_ptr, buffer.size(resp_ptr))
            assert(resp == "*3\r\n$3\r\nSET\r\n$3\r\nkey\r\n$5\r\nvalue\r\n",
                "json.concat_resp payload mismatch: " .. resp)
            buffer.drop(resp_ptr)
        "#;
    run_lua(state, code).expect("JSON concatenation payload regression");
}

#[test]
fn json_concat_error_cleans_the_cursor_before_the_next_call() {
    let (state, _guard) = new_lua_vm();
    let code = r#"
            local json = require("json")
            local buffer = require("buffer")

            local ok, err = pcall(json.concat, {"prefix", function() end})
            assert(not ok, "unsupported nested value must fail")
            assert(err:find("json.concat: unsupport value type", 1, true), tostring(err))

            local binary = string.char(0, 255, 1)
            local ptr = json.concat({binary, {value = 7}})
            local data = buffer.read(ptr, buffer.size(ptr))
            assert(data == binary .. '{"value":7}', "call after cursor error was corrupted")
            buffer.drop(ptr)
        "#;
    run_lua(state, code).expect("JSON cursor error cleanup regression");
}

#[test]
fn json_object_marks_existing_table() {
    let (state, _guard) = new_lua_vm();
    let result = run_lua_expr(
        state,
        r#"require("json").encode(require("json").object({}))"#,
    );
    assert_eq!(result, "{}");
}

#[test]
fn json_array_marks_existing_table() {
    let (state, _guard) = new_lua_vm();
    let result = run_lua_expr(
        state,
        r#"require("json").encode(require("json").array({}))"#,
    );
    assert_eq!(result, "[]");
}

// --- streaming-decoder regression coverage (no intermediate Value DOM) ---

#[test]
fn json_decode_escaped_strings() {
    let (state, _guard) = new_lua_vm();
    // Each `\\x` in this Lua literal is the two raw bytes `\x` handed to the
    // JSON parser as an escape sequence, so the decoded value is the unescaped form.
    let code = r#"
            local json = require("json")
            local t = json.decode('{"nl":"a\\nb","tab":"a\\tb","q":"a\\"b","bs":"a\\\\b","u":"caf\\u00e9"}')
            assert(t.nl == "a\nb", "newline escape mismatch: " .. tostring(t.nl))
            assert(t.tab == "a\tb", "tab escape mismatch")
            assert(t.q == 'a"b', "quote escape mismatch")
            assert(t.bs == "a\\b", "backslash escape mismatch")
            assert(t.u == "caf\xc3\xa9", "unicode escape mismatch: " .. tostring(t.u))
        "#;
    run_lua(state, code).expect("json escaped strings failed");
}

#[test]
fn json_decode_rejects_trailing_garbage() {
    let (state, _guard) = new_lua_vm();
    let result = run_lua_expr(
        state,
        r#"select(1, pcall(require("json").decode, '{"a":1} trailing'))"#,
    );
    assert_eq!(result, "false", "trailing content after JSON should error");
}

#[test]
fn json_decode_big_u64_as_float() {
    let (state, _guard) = new_lua_vm();
    // 2^63 = 9223372036854775808 is > i64::MAX, so it must decode as a float.
    let code = r#"
            local json = require("json")
            local t = json.decode('{"big":9223372036854775808}')
            assert(math.type(t.big) == "float", "expected float, got " .. tostring(math.type(t.big)))
            assert(t.big == 9223372036854775808.0)
        "#;
    run_lua(state, code).expect("json big u64 test failed");
}

#[test]
fn json_decode_skips_empty_key() {
    let (state, _guard) = new_lua_vm();
    // Empty object keys are intentionally dropped (parity with the old decoder).
    let code = r#"
            local json = require("json")
            local t = json.decode('{"":"skipped","keep":"yes"}')
            assert(t[""] == nil, "empty key should be skipped")
            assert(t.keep == "yes")
        "#;
    run_lua(state, code).expect("json empty key test failed");
}

#[test]
fn json_decode_deep_nested_arrays() {
    let (state, _guard) = new_lua_vm();
    let code = r#"
            local json = require("json")
            local t = json.decode('[[[[1,2],[3]],[[4]]],[]]')
            assert(t[1][1][1][1] == 1)
            assert(t[1][1][1][2] == 2)
            assert(t[1][1][2][1] == 3)
            assert(t[1][2][1][1] == 4)
            assert(#t[2] == 0)
        "#;
    run_lua(state, code).expect("json deep nested arrays failed");
}

#[cfg(feature = "httpc")]
#[test]
fn httpc_parsers_borrow_lua_input() {
    let (state, _guard) = new_lua_vm();
    lua_require!(state, "httpc.core", lua_httpc::luaopen_httpc);
    let code = r#"
            local httpc = require("httpc.core")
            local response = httpc.parse_response(
                "HTTP/1.1 201 Created\r\nX-Test: value\r\n\r\n")
            assert(response.version == "1.1")
            assert(response.status_code == 201)
            assert(response.headers["x-test"] == "value")

            local request = httpc.parse_request(
                "GET /items?q=1 HTTP/1.1\r\nHost: example\r\n\r\n")
            assert(request.method == "GET")
            assert(request.path == "/items")
            assert(request.query_string == "q=1")
            assert(request.headers.host == "example")
        "#;
    run_lua(state, code).expect("httpc parser borrow test failed");
}

#[cfg(feature = "httpc")]
#[test]
fn httpc_parser_failures_remain_return_values() {
    let (state, _guard) = new_lua_vm();
    lua_require!(state, "httpc.core", lua_httpc::luaopen_httpc);
    let code = r#"
            local httpc = require("httpc.core")

            local ok, value, err = pcall(httpc.parse_response, "")
            assert(ok and value == false, tostring(err))
            assert(err:find("Invalid HTTP version", 1, true), err)

            ok, value, err = pcall(httpc.parse_request, "GET")
            assert(ok and value == false, tostring(err))
            assert(err:find("Incomplete request", 1, true), err)
        "#;
    run_lua(state, code).expect("httpc parser error return contract changed");
}

#[test]
fn socket_sync_failures_remain_return_values() {
    let (state, _guard) = new_lua_vm();
    lua_require!(state, "net.core", lua_socket::luaopen_socket);
    let code = r#"
            local socket = require("net.core")
            local missing_fd = 0x7fffffffffffffff

            local ok, value, err = pcall(socket.read, missing_fd, 1)
            assert(ok and value == false, tostring(err))
            assert(err:find("not found", 1, true), err)

            ok, value, err = pcall(socket.read, missing_fd, "\n")
            assert(ok and value == false and err:find("not found", 1, true))
            ok, value, err = pcall(socket.read_frame, missing_fd)
            assert(ok and value == false, tostring(err))
            assert(err == "read_frame: fd " .. missing_fd .. " not found", err)

            ok, value, err = pcall(socket.read, missing_fd, "")
            assert(ok and value == false, tostring(err))
            assert(err:find("delim is empty", 1, true), err)

            for _, name in ipairs({"write", "write_frame"}) do
                ok, value, err = pcall(socket[name], missing_fd, "hello")
                assert(ok and value == false, tostring(err))
                assert(err == name .. ": fd " .. missing_fd .. " not found", err)

                ok, err = pcall(socket[name], missing_fd, {})
                assert(not ok and err:find("bad argument #2", 1, true), tostring(err))
            end

            -- Empty frames are successful no-ops, including when close=true.
            ok, value = pcall(socket.write_frame, missing_fd, "", nil, true)
            assert(ok and value == true)
            ok, value, err = pcall(socket.write, missing_fd, "")
            assert(ok and value == false and err:find("not found", 1, true))
        "#;
    run_lua(state, code).expect("socket error return contract changed");
}

#[cfg(feature = "grpc")]
#[test]
fn grpc_connect_validation_remains_a_return_value() {
    let (state, _guard) = new_lua_vm();
    lua_require!(state, "grpc.core", lua_grpc::luaopen_grpc);
    let code = r#"
            local grpc = require("grpc.core")
            local ok, value, err = pcall(grpc.connect, {})
            assert(ok and value == false, tostring(err))
            assert(err:find("endpoint", 1, true), err)
        "#;
    run_lua(state, code).expect("grpc error return contract changed");
}

#[cfg(feature = "websocket")]
#[test]
fn websocket_listen_failure_remains_a_return_value() {
    let (state, _guard) = new_lua_vm();
    lua_require!(state, "ws.core", lua_websocket::luaopen_websocket);
    let code = r#"
            local ws = require("ws.core")
            local ok, value, err = pcall(ws.listen, "not-a-socket-address")
            assert(ok and value == false, tostring(err))
            assert(err:find("ws listen", 1, true), err)
        "#;
    run_lua(state, code).expect("websocket error return contract changed");
}

#[cfg(feature = "excel")]
#[test]
fn excel_unsupported_type_remains_a_return_value() {
    let (state, _guard) = new_lua_vm();
    lua_require!(state, "excel", lua_excel::luaopen_excel);
    let code = r#"
            local excel = require("excel")
            local ok, value, err = pcall(excel.read, "data.unsupported")
            assert(ok and value == false, tostring(err))
            assert(err:find("unsupport file type", 1, true), err)
        "#;
    run_lua(state, code).expect("excel error return contract changed");
}

// ========================= PG protocol (json.pq_*) tests =========================

// ========================= Seri tests =========================

#[test]
fn seri_pack_unpack_basic() {
    let (state, _guard) = new_lua_vm();
    let code = r#"
            local seri = require("seri")
            local packed = seri.packstring("hello", 42, true, 3.14)
            local a, b, c, d = seri.unpack(packed)
            assert(a == "hello", "string mismatch")
            assert(b == 42, "int mismatch")
            assert(c == true, "bool mismatch")
            assert(math.abs(d - 3.14) < 0.001, "float mismatch")
        "#;
    run_lua(state, code).expect("seri pack/unpack basic failed");
}

#[test]
fn seri_pack_unpack_table() {
    let (state, _guard) = new_lua_vm();
    let code = r#"
            local seri = require("seri")
            local packed = seri.packstring({a=1, b="two", c={3, 4}})
            local t = seri.unpack(packed)
            assert(t.a == 1)
            assert(t.b == "two")
            assert(t.c[1] == 3)
            assert(t.c[2] == 4)
        "#;
    run_lua(state, code).expect("seri pack/unpack table failed");
}

#[test]
fn seri_pack_unpack_nil() {
    let (state, _guard) = new_lua_vm();
    let code = r#"
            local seri = require("seri")
            local packed = seri.packstring(nil)
            local v = seri.unpack(packed)
            assert(v == nil)
        "#;
    run_lua(state, code).expect("seri pack/unpack nil failed");
}

#[test]
fn seri_unpack_malformed_stream_returns_error() {
    let (state, _guard) = new_lua_vm();
    let code = r#"
            local seri = require("seri")
            local ok, err = pcall(function()
                return seri.unpack(string.char(6))
            end)
            assert(not ok, "malformed stream should fail")
            assert(string.find(err, "Invalid serialize stream", 1, true),
                "missing malformed stream error: " .. tostring(err))
        "#;
    run_lua(state, code).expect("seri malformed stream error test failed");
}

#[test]
fn seri_pack_empty_roundtrip() {
    let (state, _guard) = new_lua_vm();
    let code = r#"
            local seri = require("seri")
            local buffer = require("buffer")
            local ptr = seri.pack()
            assert(type(ptr) == "userdata", "empty pack must return a buffer")
            local data = buffer.read(ptr, buffer.size(ptr))
            buffer.drop(ptr)
            assert(data == "")
            assert(select('#', seri.unpack(data)) == 0)
            assert(seri.packstring() == "")
            assert(select('#', seri.unpack(seri.packstring(nil))) == 1)
        "#;
    run_lua(state, code).expect("seri empty pack failed");
}

#[test]
fn seri_unpack_temporary_strings_under_gc_preserves_nil_counts() {
    let (state, _guard) = new_lua_vm();
    run_lua(
        state,
        r#"
        local seri = require('seri')
        for _, mode in ipairs({'incremental', 'generational'}) do
            collectgarbage(mode)
            for i = 1, 100 do
                local input = {label = string.rep('payload' .. i, 100), array = {i, true, {i+1}}}
                local result = table.pack(seri.unpack(seri.packstring(nil, input, nil), 'ignored'))
                assert(result.n == 3 and result[1] == nil and result[3] == nil)
                assert(result[2].label == input.label)
                assert(result[2].array[1] == i and result[2].array[3][1] == i+1)
                collectgarbage('step', 0)
            end
        end
        assert(select('#', seri.unpack('')) == 0)
        assert(select('#', seri.unpack(nil)) == 0)
        assert(not pcall(seri.unpack, string.char(6)))
        assert(seri.unpack(seri.packstring(42)) == 42)
    "#,
    )
    .expect("temporary serialized strings must survive decoding and GC");
}

#[test]
fn seri_pack_metapairs_roundtrip() {
    let (state, _guard) = new_lua_vm();
    let code = r#"
            local seri = require("seri")
            local data = { x = 1, y = "two" }
            local t = setmetatable({}, {
                __pairs = function() return next, data, nil end
            })
            local out = seri.unpack(seri.packstring(t))
            assert(out.x == 1, "x mismatch")
            assert(out.y == "two", "y mismatch")
        "#;
    run_lua(state, code).expect("seri __pairs roundtrip failed");
}

#[test]
fn seri_pack_metapairs_error_propagates() {
    // A __pairs metamethod that raises must abort serialization with a Lua
    // error (regression test: it used to silently produce a truncated stream).
    let (state, _guard) = new_lua_vm();
    let code = r#"
            local seri = require("seri")
            local t = setmetatable({}, {
                __pairs = function() error("boom in pairs") end
            })
            local ok, err = pcall(seri.packstring, t)
            assert(not ok, "pack should fail when __pairs errors")
            assert(string.find(err, "serialize", 1, true), "missing serialize prefix: " .. tostring(err))
            assert(string.find(err, "boom in pairs", 1, true), "inner error not propagated: " .. tostring(err))
        "#;
    run_lua(state, code).expect("seri __pairs error test failed");
}

#[test]
fn seri_pack_metapairs_iterator_error_propagates() {
    // The per-step iterator pcall failure path must also surface as an error.
    let (state, _guard) = new_lua_vm();
    let code = r#"
            local seri = require("seri")
            local t = setmetatable({}, {
                __pairs = function(tbl)
                    return function() error("boom in iterator") end, tbl, nil
                end
            })
            local ok, err = pcall(seri.packstring, t)
            assert(not ok, "pack should fail when iterator errors")
            assert(string.find(err, "serialize", 1, true), "missing serialize prefix: " .. tostring(err))
            assert(string.find(err, "boom in iterator", 1, true), "inner error not propagated: " .. tostring(err))
        "#;
    run_lua(state, code).expect("seri __pairs iterator error test failed");
}

// ========================= Utils tests =========================

#[test]
fn utils_base64_roundtrip() {
    let (state, _guard) = new_lua_vm();
    let code = r#"
            local utils = require("utils")
            local encoded = utils.base64_encode("hello world")
            assert(encoded == "aGVsbG8gd29ybGQ=", "base64 encode mismatch: " .. encoded)
            local decoded = utils.base64_decode(encoded)
            assert(decoded == "hello world", "base64 decode mismatch")
        "#;
    run_lua(state, code).expect("base64 roundtrip failed");
}

#[test]
fn utils_base64_empty() {
    let (state, _guard) = new_lua_vm();
    let code = r#"
            local utils = require("utils")
            local encoded = utils.base64_encode("")
            assert(encoded == "", "empty base64 encode")
            local decoded = utils.base64_decode("")
            assert(decoded == "", "empty base64 decode")
        "#;
    run_lua(state, code).expect("base64 empty failed");
}

#[test]
fn utils_hash_md5() {
    let (state, _guard) = new_lua_vm();
    let result = run_lua_expr(state, r#"require("utils").hash("md5", "hello")"#);
    assert_eq!(result, "5d41402abc4b2a76b9719d911017c592");
}

#[test]
fn utils_hash_sha256() {
    let (state, _guard) = new_lua_vm();
    let result = run_lua_expr(state, r#"require("utils").hash("sha256", "hello")"#);
    assert_eq!(
        result,
        "2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824"
    );
}

// ========================= Buffer (Lua API) tests =========================

#[test]
fn buffer_write_and_read() {
    let (state, _guard) = new_lua_vm();
    let code = r#"
            local buffer = require("buffer")
            local buf = buffer.new()
            buffer.write(buf, "hello world")
            local data = buffer.read(buf, 11)
            assert(data == "hello world", "buffer read mismatch: " .. tostring(data))
            buffer.drop(buf)
        "#;
    run_lua(state, code).expect("buffer write/read failed");
}

#[test]
fn buffer_size() {
    let (state, _guard) = new_lua_vm();
    let code = r#"
            local buffer = require("buffer")
            local buf = buffer.new()
            assert(buffer.size(buf) == 0, "initial size should be 0")
            buffer.write(buf, "12345")
            assert(buffer.size(buf) == 5, "size should be 5 after write")
            buffer.read(buf, 3)
            assert(buffer.size(buf) == 2, "size should be 2 after read")
            buffer.drop(buf)
        "#;
    run_lua(state, code).expect("buffer size test failed");
}

#[test]
fn buffer_clear() {
    let (state, _guard) = new_lua_vm();
    let code = r#"
            local buffer = require("buffer")
            local buf = buffer.new()
            buffer.write(buf, "hello")
            buffer.clear(buf)
            assert(buffer.size(buf) == 0, "size should be 0 after clear")
            buffer.drop(buf)
        "#;
    run_lua(state, code).expect("buffer clear test failed");
}

#[test]
fn buffer_write_front() {
    let (state, _guard) = new_lua_vm();
    let code = r#"
            local buffer = require("buffer")
            local buf = buffer.new()
            buffer.write(buf, "world")
            buffer.read(buf, 1)
            buffer.write_front(buf, "w")
            local data = buffer.read(buf, 5)
            assert(data == "world", "write_front mismatch: " .. tostring(data))
            buffer.drop(buf)
        "#;
    run_lua(state, code).expect("buffer write_front test failed");
}

#[test]
fn buffer_concat_uses_lifetime_array_cursor() {
    let (state, _guard) = new_lua_vm();
    let code = r#"
            local buffer = require("buffer")
            local ptr = buffer.concat({"a", {"b", 3}, true})
            local value = buffer.unpack(ptr, "Z")
            assert(value == "ab3true", "nested concat mismatch: " .. tostring(value))
            buffer.drop(ptr)
        "#;
    run_lua(state, code).expect("buffer nested concat failed");
}

#[test]
fn buffer_seek() {
    let (state, _guard) = new_lua_vm();
    let code = r#"
            local buffer = require("buffer")
            local buf = buffer.new()
            buffer.write(buf, "hello world")
            buffer.seek(buf, 6)
            local data = buffer.read(buf, 5)
            assert(data == "world", "seek+read mismatch: " .. tostring(data))
            buffer.drop(buf)
        "#;
    run_lua(state, code).expect("buffer seek test failed");
}

#[test]
fn buffer_result_callbacks_report_lua_errors() {
    let (state, _guard) = new_lua_vm();
    let code = r#"
            local buffer = require("buffer")

            local function invalid_concat()
                return buffer.concat(function() end)
            end
            local ok, err = pcall(invalid_concat)
            assert(not ok and err:find("unsupport type", 1, true), tostring(err))
            assert(err:match(":%d+: buffer%.concat"), "missing Lua source location: " .. tostring(err))

            ok, err = pcall(buffer.write, nil, "data")
            assert(not ok and err:find("invalid `Buffer` pointer", 1, true), tostring(err))

            ok, err = pcall(buffer.drop, nil)
            assert(not ok and err:find("invalid `Buffer` pointer", 1, true), tostring(err))

            ok, err = pcall(buffer.write, {}, "data")
            assert(not ok and err:find("invalid `Buffer` pointer", 1, true), tostring(err))
        "#;
    run_lua(state, code).expect("buffer Result callback error propagation failed");
}

// ========================= Message decoder tests =========================
//
// These exercise the dispatch table built by `build_decoders` plus each
// generic decoder in `message_decode` (and the `seri` decoder), by feeding a
// hand-built `Message` through `DECODERS[ptype]` exactly like the runtime's
// `lua_decode_message_payload` does, then inspecting the resulting Lua stack.

fn buffer_msg(ptype: u8, data: &[u8]) -> Message {
    Message {
        from: 0,
        to: 0,
        session: 0,
        data: MessageBody::Buffer(ptype, Box::new(Buffer::from_slice(data))),
    }
}

fn isize_msg(ptype: u8, v: isize) -> Message {
    Message {
        from: 0,
        to: 0,
        session: 0,
        data: MessageBody::ISize(ptype, v),
    }
}

/// Dispatch a message through the real `DECODERS` table (covering the
/// `build_decoders` wiring) and return the number of values pushed.
unsafe fn decode_via_table(state: LuaState, mut msg: Message) -> i32 {
    let decoder = DECODERS[msg.ptype() as usize];
    unsafe { decoder(state, &mut msg as *mut Message) }
}

unsafe fn stack_bytes(state: LuaState, idx: i32) -> Vec<u8> {
    unsafe {
        let mut len = 0usize;
        let ptr = ffi::lua_tolstring(state.as_ptr(), idx, &mut len);
        assert!(!ptr.is_null(), "value at {} is not a string", idx);
        std::slice::from_raw_parts(ptr as *const u8, len).to_vec()
    }
}

#[test]
fn decode_error_message_pushes_false_and_text() {
    let (state, _guard) = new_lua_vm();
    unsafe {
        let n = decode_via_table(state, buffer_msg(context::PTYPE_ERROR, b"boom"));
        assert_eq!(n, 2, "PTYPE_ERROR should push (false, message)");
        assert_eq!(
            ffi::lua_toboolean(state.as_ptr(), 1),
            0,
            "first value must be false"
        );
        assert_eq!(stack_bytes(state, 2), b"boom");
    }
}

#[test]
fn decode_error_message_invalid_utf8_is_lossy() {
    let (state, _guard) = new_lua_vm();
    unsafe {
        let n = decode_via_table(state, buffer_msg(context::PTYPE_ERROR, &[0xff, 0xfe, b'!']));
        assert_eq!(n, 2);
        assert_eq!(ffi::lua_toboolean(state.as_ptr(), 1), 0);
        // Invalid bytes are replaced (U+FFFD), the trailing '!' is preserved.
        let bytes = stack_bytes(state, 2);
        assert!(
            bytes.ends_with(b"!"),
            "tail should survive lossy decode: {:?}",
            bytes
        );
    }
}

#[test]
fn decode_integer_message_for_integer_and_timer() {
    let (state, _guard) = new_lua_vm();
    unsafe {
        // PTYPE_INTEGER and PTYPE_TIMER both map to decode_integer_message.
        let n = decode_via_table(state, isize_msg(context::PTYPE_INTEGER, 12345));
        assert_eq!(n, 1);
        assert_eq!(ffi::lua_tointeger(state.as_ptr(), 1), 12345);
        ffi::lua_settop(state.as_ptr(), 0);

        let n = decode_via_table(state, isize_msg(context::PTYPE_TIMER, -7));
        assert_eq!(n, 1);
        assert_eq!(ffi::lua_tointeger(state.as_ptr(), 1), -7);
    }
}

#[test]
fn decode_buffer_as_string_for_text_and_socket_tcp() {
    let (state, _guard) = new_lua_vm();
    unsafe {
        let n = decode_via_table(state, buffer_msg(context::PTYPE_TEXT, b"hello"));
        assert_eq!(n, 1);
        assert_eq!(stack_bytes(state, 1), b"hello");
        ffi::lua_settop(state.as_ptr(), 0);

        // Binary payloads (including embedded NULs) must be preserved verbatim.
        let raw = [0u8, 1, 2, 255, b'a'];
        let n = decode_via_table(state, buffer_msg(context::PTYPE_SOCKET_TCP, &raw));
        assert_eq!(n, 1);
        assert_eq!(stack_bytes(state, 1), raw);
    }
}

#[test]
fn decode_lua_message_seri_roundtrip() {
    let (state, _guard) = new_lua_vm();
    unsafe {
        // Produce a real seri stream, read its raw bytes back out.
        run_lua(
            state,
            r#"_packed = require("seri").packstring(42, "hi", true)"#,
        )
        .expect("seri pack failed");
        let name = CString::new("_packed").unwrap();
        ffi::lua_getglobal(state.as_ptr(), name.as_ptr());
        let packed = stack_bytes(state, -1);
        ffi::lua_settop(state.as_ptr(), 0);

        // PTYPE_LUA (and PTYPE_DEBUG) decode via lua_seri::decode_buffer_message.
        let n = decode_via_table(state, buffer_msg(context::PTYPE_LUA, &packed));
        assert_eq!(n, 3, "expected 3 decoded values");
        assert_eq!(ffi::lua_tointeger(state.as_ptr(), 1), 42);
        assert_eq!(stack_bytes(state, 2), b"hi");
        assert_eq!(ffi::lua_toboolean(state.as_ptr(), 3), 1);
    }
}

#[test]
fn decode_lua_message_empty_buffer_yields_nothing() {
    let (state, _guard) = new_lua_vm();
    unsafe {
        let n = decode_via_table(state, buffer_msg(context::PTYPE_DEBUG, b""));
        assert_eq!(n, 0, "empty seri stream decodes to no values");
    }
}

#[test]
fn decode_default_for_unmapped_ptype() {
    let (state, _guard) = new_lua_vm();
    unsafe {
        // PTYPE_SYSTEM has no registered decoder -> default_decode (0 results).
        let n = decode_via_table(state, buffer_msg(context::PTYPE_SYSTEM, b"x"));
        assert_eq!(n, 0);
        assert_eq!(ffi::lua_gettop(state.as_ptr()), 0);
    }
}

#[test]
fn decode_integer_rejects_wrong_body() {
    let (state, _guard) = new_lua_vm();
    unsafe {
        // A Buffer body for an integer ptype must fail gracefully as (false, err).
        let n = decode_via_table(state, buffer_msg(context::PTYPE_INTEGER, b"x"));
        assert_eq!(n, 2);
        assert_eq!(ffi::lua_toboolean(state.as_ptr(), 1), 0);
        assert!(
            !stack_bytes(state, 2).is_empty(),
            "should carry an error message"
        );
    }
}

#[test]
fn decode_error_rejects_wrong_body() {
    let (state, _guard) = new_lua_vm();
    unsafe {
        // An ISize body for PTYPE_ERROR cannot be borrowed as a Buffer.
        let n = decode_via_table(state, isize_msg(context::PTYPE_ERROR, 1));
        assert_eq!(n, 2);
        assert_eq!(ffi::lua_toboolean(state.as_ptr(), 1), 0);
    }
}

#[test]
fn decode_socket_event_rejects_wrong_body() {
    let (state, _guard) = new_lua_vm();
    unsafe {
        // Neither Boxed(SocketEvent) nor Buffer -> the error arm: (false, err).
        let n = decode_via_table(state, isize_msg(context::PTYPE_SOCKET_EVENT, 1));
        assert_eq!(n, 2);
        assert_eq!(ffi::lua_toboolean(state.as_ptr(), 1), 0);
    }
}

#[test]
fn database_lua_iterator_closes_on_break_error_and_explicit_close() {
    let (state, _guard) = new_lua_vm();
    let source = include_str!("../../../../lualib/moon/db/cursor.lua");
    let test = format!(
        r#"
        local responses, calls
        package.loaded.moon = {{ wait = function(session)
            calls = calls + 1
            local result = assert(responses[session])
            return table.unpack(result)
        end }}
        local iterator = (function() {source} end)()
        local function reset()
            local cursor = {{ closed=0, advanced=0 }}
            function cursor:close() self.closed = self.closed + 1 end
            function cursor:next() self.advanced = self.advanced + 1; return 2 end
            calls = 0
            responses = {{ [1] = {{ {{1,2}}, cursor }}, [2] = {{ {{3}} }} }}
            return cursor
        end
        local cursor = reset()
        local rows = {{}}
        for row in iterator(1) do rows[#rows+1] = row end
        assert(table.concat(rows, ',') == '1,2,3' and calls == 2)
        assert(cursor.advanced == 1 and cursor.closed == 0)

        cursor = reset()
        for row in iterator(1) do assert(row == 1); break end
        assert(cursor.closed == 1 and cursor.advanced == 0 and calls == 1)

        cursor = reset()
        assert(not pcall(function()
            for _ in iterator(1) do error('stop') end
        end))
        assert(cursor.closed == 1 and calls == 1)

        cursor = reset()
        local iter, _, _, guard = iterator(1)
        assert(iter() == 1)
        getmetatable(guard).__close()
        getmetatable(guard).__close()
        assert(cursor.closed == 1 and iter() == nil)

        cursor = reset()
        responses[1] = {{ {{}}, cursor }}
        for _ in iterator(1) do error('empty batch') end
        assert(cursor.closed == 1)

        reset()
        responses[1] = {{ false, 'query failed' }}
        for _ in iterator(1) do error('failed batch') end
        assert(calls == 1)

        cursor = reset()
        function cursor:next() return false, 'handler is gone' end
        local ok, err = pcall(function() for _ in iterator(1) do end end)
        assert(not ok and err == 'handler is gone' and calls == 1)
    "#
    );
    run_lua(state, &test).expect("shared database iterator regression");
}
