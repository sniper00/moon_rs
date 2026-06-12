#![allow(clippy::collapsible_if)]

// This crate was merged from the former `moon-runtime` + `moon-modules` crates.
// The self-alias lets the native-module sources keep referring to runtime items
// through their original `moon_runtime::...` paths.
extern crate self as moon_runtime;

use moon_base::{
    cstr, ffi,
    laux::{self, LuaState, LuaValue},
};
use std::ffi::c_int;
use std::sync::Arc;
use std::sync::LazyLock;
use std::sync::atomic::{AtomicI64, Ordering};

// ---- Actor server runtime (formerly the `moon-runtime` crate) ----
pub mod actor;
// `Buffer` lives in the shared `moon-base` crate; re-export it so the
// long-standing `moon_runtime::buffer` path keeps working.
pub use moon_base::buffer;
use buffer::Buffer;
pub mod context;
pub mod error;
pub mod log;

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

#[inline]
#[allow(clippy::not_unsafe_ptr_arg_deref)]
pub fn check_buffer(state: LuaState, index: i32) -> Box<Buffer> {
    match LuaValue::from_stack(state, index) {
        LuaValue::String(s) => Box::new(Buffer::from(s)),
        LuaValue::LightUserData(ptr) => unsafe {
            if ptr.is_null() {
                laux::lua_error(
                    state,
                    format!(
                        "bad argument #{} (buffer expected, got null pointer)",
                        index
                    ),
                );
            }
            Box::from_raw(ptr as *mut Buffer)
        },
        _ => {
            laux::lua_error(
                state,
                format!(
                    "bad argument #{} (buffer expected, got {})",
                    index,
                    laux::type_name(state, index)
                ),
            );
        }
    }
}

#[inline]
#[allow(clippy::not_unsafe_ptr_arg_deref)]
pub fn check_arc_buffer(state: LuaState, index: i32) -> Arc<Buffer> {
    match LuaValue::from_stack(state, index) {
        LuaValue::String(s) => Arc::new(Buffer::from(s)),
        LuaValue::LightUserData(ptr) => unsafe {
            if ptr.is_null() {
                laux::lua_error(
                    state,
                    format!(
                        "bad argument #{} (buffer expected, got null pointer)",
                        index
                    ),
                );
            }
            Arc::from(Box::from_raw(ptr as *mut Buffer))
        },
        LuaValue::UserData(ptr) => unsafe {
            if ptr.is_null() {
                laux::lua_error(
                    state,
                    format!(
                        "bad argument #{} (buffer expected, got null pointer)",
                        index
                    ),
                );
            }
            let arc = &*(ptr as *const Arc<Buffer>);
            arc.clone()
        },
        _ => {
            laux::lua_error(
                state,
                format!(
                    "bad argument #{} (buffer expected, got {})",
                    index,
                    laux::type_name(state, index)
                ),
            );
        }
    }
}

pub fn escape_print(input: &[u8]) -> String {
    const HEX: &[u8] = b"0123456789abcdef";
    let mut result = String::with_capacity(input.len());

    for &byte in input {
        match byte {
            b'\\' => result.push_str("\\\\"),
            b'"' => result.push_str("\\\""),
            b if b.is_ascii_graphic() || b.is_ascii_whitespace() => result.push(b as char),
            _ => {
                result.push('\\');
                result.push('x');
                result.push(HEX[(byte >> 4) as usize] as char);
                result.push(HEX[(byte & 0xf) as usize] as char);
            }
        }
    }

    result
}

// The native Lua module sources live under `src/modules/`. They are kept as
// crate-root modules (via `#[path]`) so existing `crate::lua_xxx::...` paths and
// the public `moon_runtime::lua_actor` / `lua_json` / `lua_coroutine` keep working.
#[path = "modules/lua_buffer.rs"]
mod lua_buffer;
#[cfg(feature = "cluster")]
#[path = "modules/lua_cluster.rs"]
mod lua_cluster;
#[path = "modules/lua_coroutine.rs"]
pub mod lua_coroutine;
#[cfg(feature = "excel")]
#[path = "modules/lua_excel.rs"]
mod lua_excel;
#[path = "modules/lua_fs.rs"]
mod lua_fs;
#[cfg(feature = "httpc")]
#[path = "modules/lua_httpc.rs"]
mod lua_httpc;
#[cfg(feature = "httpd")]
#[path = "modules/lua_httpd.rs"]
mod lua_httpd;
#[cfg(feature = "mongodb")]
#[path = "modules/lua_mongodb.rs"]
mod lua_mongodb;
#[cfg(feature = "pg")]
#[path = "modules/lua_pg.rs"]
mod lua_pg;
#[cfg(feature = "protobuf")]
#[path = "modules/lua_protobuf.rs"]
mod lua_protobuf;
#[path = "modules/lua_random.rs"]
mod lua_random;
#[cfg(feature = "redis")]
#[path = "modules/lua_redis.rs"]
mod lua_redis;
#[path = "modules/lua_seri.rs"]
mod lua_seri;
#[path = "modules/lua_socket.rs"]
mod lua_socket;
#[cfg(feature = "sqlx")]
#[path = "modules/lua_sqlx.rs"]
mod lua_sqlx;
#[path = "modules/lua_utils.rs"]
mod lua_utils;
#[cfg(feature = "websocket")]
#[path = "modules/lua_websocket.rs"]
mod lua_websocket;
mod message_decode;
mod request_pool;

#[path = "modules/lua_actor.rs"]
pub mod lua_actor;
#[path = "modules/lua_json.rs"]
pub mod lua_json;

/// Shared hardening limits used by native modules.
///
/// Keeping these defaults in one place makes it visible when HTTP, WebSocket,
/// socket, cluster, and database modules share the same resource ceilings.
///
/// This is a superset config catalog: many fields are read only by
/// feature-gated modules (DB, HTTP, cluster, websocket), so individual fields
/// are legitimately unused in builds that disable those features. Allow dead
/// code struct-wide rather than annotating each field.
#[allow(dead_code)]
#[derive(Debug, Clone, Copy)]
pub(crate) struct Limits {
    /// Hard ceiling on bytes a single network-facing operation may read or
    /// accumulate in memory. Used by sockets and HTTP client/server paths to
    /// bound attacker-controlled `Content-Length`, frame, or read sizes.
    pub network_read_bytes: usize,
    /// Default maximum number of simultaneously accepted inbound connections
    /// per listener. Shared by TCP socket, HTTP server, and WebSocket listeners;
    /// callers may still override it through `max_connections`.
    pub listener_connections: usize,
    /// Maximum rows/documents a single non-streaming DB query may materialize.
    /// Large result sets should use streaming APIs so one request cannot buffer
    /// unbounded SQL rows or MongoDB documents.
    pub db_query_rows: usize,
    /// Default batch size for streaming DB cursors. Kept lower than
    /// `db_query_rows` so each cursor response has a predictable per-message
    /// memory footprint while still allowing callers to tune it per request.
    pub db_stream_batch_rows: i64,
    /// Default bounded request queue capacity for async DB/worker pools. This is
    /// the backpressure limit for Redis, PG, sqlx, and MongoDB handlers before
    /// enqueue starts failing instead of growing memory without bound.
    pub request_queue_capacity: usize,
    /// Default maximum bytes accumulated by socket `read_until` when the caller
    /// does not provide an explicit limit. This protects delimiter reads from
    /// scanning forever or buffering unbounded input.
    pub socket_read_until_bytes: usize,
    /// Maximum total bytes accepted by one socket write batch. Lua can pass many
    /// buffers at once, so this bounds the transient aggregation before the IO
    /// task writes to the network.
    pub socket_write_batch_bytes: usize,
    /// Capacity of outbound network writer queues. Shared by socket and cluster
    /// writers to bound pending writes when a peer or remote node is slow.
    pub network_write_queue_capacity: usize,
    /// Maximum HTTP client timeout accepted from Lua, in milliseconds. Prevents
    /// accidental or malicious requests from pinning client tasks for excessive
    /// durations.
    pub http_client_timeout_ms: u64,
    /// Maximum payload size for one cluster frame. Cluster peers are trusted more
    /// than public clients, but frame length still comes from the wire and must
    /// have a hard allocation cap.
    pub cluster_frame_bytes: usize,
    /// Default maximum request/response body size for the HTTP server helpers.
    /// This is separate from `network_read_bytes` so HTTP server policy can stay
    /// conservative while the global network ceiling remains a hard stop.
    pub http_body_bytes: usize,
    /// Static files larger than this threshold are streamed instead of read into
    /// memory at once. Small files are served directly for simplicity; large
    /// files avoid a single allocation proportional to file size.
    pub http_static_stream_threshold_bytes: u64,
    /// Time-to-live for cached static-file metadata. The cache avoids repeated
    /// filesystem metadata lookups while keeping changes visible quickly.
    pub http_static_cache_ttl_secs: u64,
    /// Maximum entries in the HTTP static-file metadata cache. Prevents a wide
    /// set of requested paths from growing the cache indefinitely.
    pub http_static_cache_entries: usize,
    /// Maximum wire message size accepted from DB protocols. Used by Redis RESP
    /// and PostgreSQL protocol decoders to cap allocation from server-provided
    /// message lengths before parsing rows, arrays, or protocol errors.
    pub db_wire_message_bytes: usize,
    /// Maximum RESP array item count accepted from Redis. Protects nested array
    /// decoding from allocating a huge vector based on a wire-provided count.
    pub redis_array_items: usize,
    /// Default DB pool size for modules that expose `max_connections`. Redis has
    /// its own historical default, but PG/sqlx use this shared baseline.
    pub db_pool_size: u32,
    /// Default read/query timeout for DB protocols, in milliseconds. Used where
    /// the module has a protocol-level read timeout separate from connect time.
    pub db_read_timeout_ms: u64,
}

impl Limits {
    pub const fn new() -> Self {
        Self {
            network_read_bytes: 512 * 1024 * 1024,
            listener_connections: 100_000,
            db_query_rows: 100_000,
            db_stream_batch_rows: 100,
            request_queue_capacity: 1024,
            socket_read_until_bytes: 16 * 1024 * 1024,
            socket_write_batch_bytes: 256 * 1024,
            network_write_queue_capacity: 64 * 1024,
            http_client_timeout_ms: 100_000,
            cluster_frame_bytes: 512 * 1024 * 1024,
            http_body_bytes: 10 * 1024 * 1024,
            http_static_stream_threshold_bytes: 1024 * 1024,
            http_static_cache_ttl_secs: 5,
            http_static_cache_entries: 10_000,
            db_wire_message_bytes: 64 * 1024 * 1024,
            redis_array_items: 1024 * 1024,
            db_pool_size: 5,
            db_read_timeout_ms: 10_000,
        }
    }
}

pub(crate) const LIMITS: Limits = Limits::new();

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
    ($fn:expr) => {{
        unsafe extern "C-unwind" fn func_wrapper(state: *mut ffi::lua_State) -> i32 {
            #[allow(unused_unsafe)]
            #[allow(clippy::macro_metavars_in_unsafe)]
            unsafe {
                $fn(LuaState::new(state).unwrap())
            }
        }
        func_wrapper
    }};
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
    unsafe extern "C-unwind" {
        fn luaopen_sharetable_core(L: *mut ffi::lua_State) -> c_int;
    }

    #[cfg(feature = "httpc")]
    lua_require!(state, "httpc.core", lua_httpc::luaopen_httpc);
    #[cfg(feature = "httpd")]
    lua_require!(state, "httpd.core", lua_httpd::luaopen_httpd);
    lua_require!(state, "net.core", lua_socket::luaopen_socket);
    #[cfg(feature = "excel")]
    lua_require!(state, "excel", lua_excel::luaopen_excel);
    lua_require!(state, "fs", lua_fs::luaopen_fs);
    lua_require!(state, "json", lua_json::luaopen_json);
    lua_require!(state, "random", lua_random::luaopen_random);
    lua_require!(state, "buffer", lua_buffer::luaopen_buffer);
    lua_require!(state, "seri", lua_seri::luaopen_seri);
    lua_require!(state, "utils", lua_utils::luaopen_utils);
    #[cfg(feature = "protobuf")]
    lua_require!(state, "protobuf", lua_protobuf::luaopen_protobuf);
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
    unsafe {
        ffi::luaL_requiref(
            state.as_ptr(),
            cstr!("sharetable.core"),
            luaopen_sharetable_core,
            0,
        );
        ffi::lua_pop(state.as_ptr(), 1);
    }
}

/// Eagerly build the process-wide message-decoder table.
///
/// Call this once at startup (before any actor is spawned) so the one-time
/// initialization happens on the main path instead of lazily on the first
/// decode. Idempotent: subsequent calls are no-ops.
pub fn init_message_decoders() {
    LazyLock::force(&DECODERS);
}

/// Process-wide message-decoder dispatch table, keyed by `ptype`.
///
/// `luaopen_custom_libs` runs once per actor, so the registration must NOT be a
/// per-actor write into a shared `static mut` (that races readers in `handle()`
/// and is UB under the aliasing model / a Rust 2024 hazard). Instead the table
/// is built exactly once via `LazyLock`; every actor and the dispatch path in
/// `lua_actor::lua_decode_message_payload` only ever *read* it.
pub(crate) static DECODERS: LazyLock<[message_decode::MessageDecodeFn; 256]> =
    LazyLock::new(build_decoders);

fn build_decoders() -> [message_decode::MessageDecodeFn; 256] {
    use moon_runtime::context::{
        PTYPE_DEBUG, PTYPE_ERROR, PTYPE_INTEGER, PTYPE_LUA, PTYPE_SOCKET_EVENT, PTYPE_SOCKET_TCP,
        PTYPE_TEXT, PTYPE_TIMER,
    };
    #[cfg(feature = "httpc")]
    use moon_runtime::context::PTYPE_HTTPC;
    #[cfg(feature = "httpd")]
    use moon_runtime::context::PTYPE_HTTPD;
    #[cfg(feature = "mongodb")]
    use moon_runtime::context::PTYPE_MONGODB;
    #[cfg(feature = "pg")]
    use moon_runtime::context::PTYPE_PG;
    #[cfg(feature = "redis")]
    use moon_runtime::context::PTYPE_REDIS;
    #[cfg(feature = "sqlx")]
    use moon_runtime::context::PTYPE_SQLX;
    #[cfg(feature = "websocket")]
    use moon_runtime::context::PTYPE_WEBSOCKET;

    let mut decoders: [message_decode::MessageDecodeFn; 256] =
        [message_decode::default_decode as message_decode::MessageDecodeFn; 256];

    decoders[PTYPE_ERROR as usize] = message_decode::decode_error_message;
    decoders[PTYPE_INTEGER as usize] = message_decode::decode_integer_message;
    decoders[PTYPE_TIMER as usize] = message_decode::decode_integer_message;
    decoders[PTYPE_TEXT as usize] = message_decode::decode_buffer_as_string_message;
    decoders[PTYPE_SOCKET_TCP as usize] = message_decode::decode_buffer_as_string_message;
    decoders[PTYPE_LUA as usize] = lua_seri::decode_buffer_message;
    decoders[PTYPE_DEBUG as usize] = lua_seri::decode_buffer_message;
    decoders[PTYPE_SOCKET_EVENT as usize] = lua_socket::decode_socket_event_message;
    #[cfg(feature = "httpc")]
    {
        decoders[PTYPE_HTTPC as usize] = lua_httpc::decode_httpc_message;
    }
    #[cfg(feature = "httpd")]
    {
        decoders[PTYPE_HTTPD as usize] = lua_httpd::decode_httpd_message;
    }
    #[cfg(feature = "sqlx")]
    {
        decoders[PTYPE_SQLX as usize] = lua_sqlx::decode_sqlx_message;
    }
    #[cfg(feature = "mongodb")]
    {
        decoders[PTYPE_MONGODB as usize] = lua_mongodb::decode_mongodb_message;
    }
    #[cfg(feature = "websocket")]
    {
        decoders[PTYPE_WEBSOCKET as usize] = lua_websocket::decode_websocket_message;
    }
    #[cfg(feature = "pg")]
    {
        decoders[PTYPE_PG as usize] = lua_pg::decode_pg_message;
    }
    #[cfg(feature = "redis")]
    {
        decoders[PTYPE_REDIS as usize] = lua_redis::decode_redis_message;
    }

    decoders
}

#[cfg(test)]
mod tests {
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
        let result = run_lua_expr(state, &format!(r#"(function() {} end)()"#, code));
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
}
