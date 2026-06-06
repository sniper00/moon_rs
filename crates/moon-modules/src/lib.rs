#![allow(clippy::collapsible_if)]

use std::ffi::c_int;
use std::sync::atomic::{AtomicI64, Ordering};
use moon_lua::{cstr, ffi, laux::{self, LuaState}};

/// Stack-allocated byte buffer. `data[0]` stores the length, `data[1..]` stores
/// the content (string or binary). Max capacity is N-1 bytes. No heap allocation.
#[derive(Debug, Clone, Copy)]
pub struct ShortBytes<const N: usize> {
    data: [u8; N],
}

impl<const N: usize> ShortBytes<N> {
    pub fn new(src: &[u8]) -> Option<Self> {
        if src.is_empty() || src.len() >= N {
            return None;
        }
        let mut data = [0u8; N];
        data[0] = src.len() as u8;
        data[1..1 + src.len()].copy_from_slice(src);
        Some(Self { data })
    }

    #[inline]
    pub fn as_slice(&self) -> &[u8] {
        &self.data[1..1 + self.data[0] as usize]
    }

    #[inline]
    pub fn len(&self) -> usize {
        self.data[0] as usize
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.data[0] == 0
    }
}

mod lua_buffer;
#[cfg(feature = "excel")]
mod lua_excel;
mod lua_fs;
#[cfg(feature = "httpc")]
mod lua_httpc;
mod lua_seri;
mod lua_socket;
mod lua_utils;
#[cfg(feature = "sqlx")]
mod lua_sqlx;
#[cfg(feature = "mongodb")]
mod lua_mongodb;
#[cfg(feature = "pg")]
mod lua_pg;
#[cfg(feature = "redis")]
mod lua_redis;
#[cfg(feature = "websocket")]
mod lua_websocket;
#[cfg(feature = "httpd")]
mod lua_httpd;
#[cfg(feature = "cluster")]
mod lua_cluster;

pub mod lua_json;
pub mod lua_actor;

static NET_UUID: AtomicI64 = AtomicI64::new(1);

pub fn next_net_fd() -> i64 {
    let fd = NET_UUID.fetch_add(1, Ordering::AcqRel);
    if fd == i64::MAX {
        panic!("net fd overflow");
    }
    fd
}

/// Unified Lua error return: pushes `(false, errmsg)` and returns 2.
pub fn lua_push_error(state: LuaState, msg: &str) -> c_int {
    laux::lua_push(state, false);
    laux::lua_push(state, msg);
    2
}


#[macro_export]
macro_rules! not_null_wrapper {
    ($fn:expr) => {
        {
            unsafe extern "C-unwind" fn func_wrapper(state: *mut ffi::lua_State) -> i32 {
                #[allow(unused_unsafe)]
                #[allow(clippy::macro_metavars_in_unsafe)]
                unsafe { $fn(LuaState::new(state).unwrap()) }
            }
            func_wrapper
        }
    };
}

#[macro_export]
macro_rules! lua_require {
    ($state:expr, $name:expr, $fn:expr) => {
        #[allow(unused_unsafe)]
        #[allow(clippy::macro_metavars_in_unsafe)]
        unsafe {
            ffi::luaL_requiref($state.as_ptr(), cstr!($name), not_null_wrapper!($fn), 0);
            ffi::lua_pop($state.as_ptr(), 1);
        }
    };
}

pub fn luaopen_custom_libs(state: LuaState) {
    #[cfg(feature = "httpc")]
    lua_require!(state, "httpc.core", lua_httpc::luaopen_httpc);
    #[cfg(feature = "httpd")]
    lua_require!(state, "httpd.core", lua_httpd::luaopen_httpd);
    lua_require!(state, "net.core", lua_socket::luaopen_socket);
    #[cfg(feature = "excel")]
    lua_require!(state, "excel", lua_excel::luaopen_excel);
    lua_require!(state, "fs", lua_fs::luaopen_fs);
    lua_require!(state, "json", lua_json::luaopen_json);
    lua_require!(state, "buffer", lua_buffer::luaopen_buffer);
    lua_require!(state, "seri", lua_seri::luaopen_seri);
    #[cfg(feature = "sqlx")]
    lua_require!(state, "sqlx.core", lua_sqlx::luaopen_sqlx);
    #[cfg(feature = "mongodb")]
    lua_require!(state, "mongodb.core", lua_mongodb::luaopen_mongodb);
    #[cfg(feature = "pg")]
    lua_require!(state, "pg.core", lua_pg::luaopen_pg);
    #[cfg(feature = "redis")]
    lua_require!(state, "redis.core", lua_redis::luaopen_redis);
    #[cfg(feature = "websocket")]
    lua_require!(state, "ws.core", lua_websocket::luaopen_websocket);
    #[cfg(feature = "cluster")]
    lua_require!(state, "cluster.core", lua_cluster::luaopen_cluster);
    lua_require!(state, "utils", lua_utils::luaopen_utils);
}

#[cfg(test)]
mod tests {
    use super::*;
    use moon_lua::ffi;
    use moon_lua::laux::LuaGlobalState;
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
        let result = run_lua_expr(state, &format!(
            r#"(function() {} end)()"#,
            code
        ));
        assert_eq!(result, "false", "deeply nested JSON should fail");
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
    fn json_object_marks_existing_table() {
        let (state, _guard) = new_lua_vm();
        let result = run_lua_expr(state, r#"require("json").encode(require("json").object({}))"#);
        assert_eq!(result, "{}");
    }

    #[test]
    fn json_array_marks_existing_table() {
        let (state, _guard) = new_lua_vm();
        let result = run_lua_expr(state, r#"require("json").encode(require("json").array({}))"#);
        assert_eq!(result, "[]");
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
    fn seri_pack_empty_returns_nothing() {
        let (state, _guard) = new_lua_vm();
        let code = r#"
            local seri = require("seri")
            local n = select('#', seri.pack())
            assert(n == 0, "empty pack should return nothing")
        "#;
        run_lua(state, code).expect("seri empty pack failed");
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
        assert_eq!(result, "2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824");
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
}
