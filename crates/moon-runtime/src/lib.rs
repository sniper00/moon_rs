#![allow(clippy::collapsible_if)]

// This crate was merged from the former `moon-runtime` + `moon-modules` crates.
// The self-alias lets the native-module sources keep referring to runtime items
// through their original `moon_runtime::...` paths.
extern crate self as moon_runtime;

use moon_lua_sys::{
    cstr, ffi,
    laux::{self, LuaStack, LuaState, LuaType},
};
use std::ffi::c_int;
use std::sync::Arc;
use std::sync::LazyLock;
use std::sync::atomic::{AtomicI64, Ordering};

// ---- Actor server runtime (formerly the `moon-runtime` crate) ----
pub mod actor;
// `Buffer` lives in the shared `moon-lua-sys` crate; re-export it so the
// long-standing `moon_runtime::buffer` path keeps working.
use buffer::Buffer;
pub use moon_lua_sys::buffer;
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
pub fn check_buffer(lua: &LuaStack<'_>, index: i32) -> Result<Box<Buffer>, String> {
    let value = lua.value(index);
    match value.kind() {
        LuaType::String => Ok(Box::new(Buffer::from(value.as_bytes().unwrap_or_default()))),
        LuaType::LightUserData => unsafe {
            let Some(ptr) = value.as_light_userdata() else {
                return Err(format!(
                    "bad argument #{} (buffer expected, got null pointer)",
                    index
                ));
            };
            if ptr.is_null() {
                return Err(format!(
                    "bad argument #{} (buffer expected, got null pointer)",
                    index
                ));
            }
            Ok(Box::from_raw(ptr.cast::<Buffer>()))
        },
        _ => Err(format!(
            "bad argument #{} (buffer expected, got {})",
            index,
            value.name()
        )),
    }
}

#[inline]
#[allow(clippy::not_unsafe_ptr_arg_deref)]
pub fn check_arc_buffer(lua: &LuaStack<'_>, index: i32) -> Result<Arc<Buffer>, String> {
    let value = lua.value(index);
    match value.kind() {
        LuaType::String => Ok(Arc::new(Buffer::from(value.as_bytes().unwrap_or_default()))),
        LuaType::LightUserData => unsafe {
            let Some(ptr) = value.as_light_userdata() else {
                return Err(format!(
                    "bad argument #{} (buffer expected, got null pointer)",
                    index
                ));
            };
            if ptr.is_null() {
                return Err(format!(
                    "bad argument #{} (buffer expected, got null pointer)",
                    index
                ));
            }
            Ok(Arc::from(Box::from_raw(ptr.cast::<Buffer>())))
        },
        LuaType::UserData => unsafe {
            let ptr = value.as_userdata::<Arc<Buffer>>();
            let Some(ptr) = ptr else {
                return Err(format!(
                    "bad argument #{} (buffer expected, got null pointer)",
                    index
                ));
            };
            Ok(ptr.as_ref().clone())
        },
        _ => Err(format!(
            "bad argument #{} (buffer expected, got {})",
            index,
            value.name()
        )),
    }
}

/// Borrows a valid UTF-8 string from a Lua stack slot, or returns an argument-indexed error.
pub(crate) fn checked_str<'ctx>(lua: &'ctx LuaStack<'_>, index: i32) -> Result<&'ctx str, String> {
    let value = lua.value(index);
    value
        .as_str()
        .ok_or_else(|| format!("bad argument #{index} (valid UTF-8 string expected)"))
}

/// Borrows raw bytes from a Lua stack slot, or returns an argument-indexed error.
pub(crate) fn checked_bytes<'ctx>(
    lua: &'ctx LuaStack<'_>,
    index: i32,
) -> Result<&'ctx [u8], String> {
    let value = lua.value(index);
    value
        .as_bytes()
        .ok_or_else(|| format!("bad argument #{index} (string expected)"))
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

#[cfg(any(feature = "sqlx", feature = "mongodb"))]
mod db_cursor;
#[cfg(feature = "grpc")]
#[path = "modules/lua_grpc.rs"]
mod lua_grpc;
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
#[path = "modules/lua_schema.rs"]
mod lua_schema;
#[path = "modules/lua_seri.rs"]
mod lua_seri;
#[path = "modules/lua_socket.rs"]
mod lua_socket;
#[cfg(feature = "sqlx")]
#[path = "modules/lua_sqlx.rs"]
mod lua_sqlx;
#[path = "modules/lua_utils.rs"]
mod lua_utils;
#[path = "modules/lua_uuid.rs"]
mod lua_uuid;
#[cfg(feature = "websocket")]
#[path = "modules/lua_websocket.rs"]
mod lua_websocket;
#[path = "modules/lua_zset.rs"]
mod lua_zset;
mod message_decode;
mod net_io;
mod request_pool;

#[path = "modules/lua_actor.rs"]
pub mod lua_actor;
#[path = "modules/lua_json.rs"]
pub mod lua_json;

/// Fast, non-cryptographic hashing for internal maps keyed by trusted data.
///
/// std's default `HashMap` uses SipHash (DoS-resistant but slow). Several native
/// modules key hot-path lookup tables by trusted, non-attacker-controlled data —
/// protobuf descriptor names/numbers loaded from a build artifact, zset player
/// ids — where collision-DoS resistance is unneeded. They share this single
/// FxHash implementation (the same fast hash rustc uses internally) instead of
/// each rolling their own.
pub(crate) mod hash {
    use std::collections::{HashMap, HashSet};
    use std::hash::{BuildHasherDefault, Hasher};

    /// FxHash: `hash = (hash.rotate_left(5) ^ word) * SEED`, processing 8 bytes
    /// at a time. Handles both string and integer keys.
    #[derive(Default)]
    pub(crate) struct FxHasher {
        hash: u64,
    }

    impl FxHasher {
        const SEED: u64 = 0x51_7c_c1_b7_27_22_0a_95;

        #[inline]
        fn add(&mut self, word: u64) {
            self.hash = (self.hash.rotate_left(5) ^ word).wrapping_mul(Self::SEED);
        }
    }

    impl Hasher for FxHasher {
        #[inline]
        fn write(&mut self, bytes: &[u8]) {
            let mut chunks = bytes.chunks_exact(8);
            for c in &mut chunks {
                self.add(u64::from_le_bytes(c.try_into().unwrap()));
            }
            let rem = chunks.remainder();
            if !rem.is_empty() {
                let mut buf = [0u8; 8];
                buf[..rem.len()].copy_from_slice(rem);
                self.add(u64::from_le_bytes(buf));
            }
        }

        #[inline]
        fn write_u8(&mut self, i: u8) {
            self.add(i as u64);
        }

        #[inline]
        fn write_u32(&mut self, i: u32) {
            self.add(i as u64);
        }

        #[inline]
        fn write_u64(&mut self, i: u64) {
            self.add(i);
        }

        #[inline]
        fn write_usize(&mut self, i: usize) {
            self.add(i as u64);
        }

        #[inline]
        fn finish(&self) -> u64 {
            self.hash
        }
    }

    pub(crate) type FxBuildHasher = BuildHasherDefault<FxHasher>;
    pub(crate) type FxHashMap<K, V> = HashMap<K, V, FxBuildHasher>;
    #[allow(dead_code)]
    pub(crate) type FxHashSet<T> = HashSet<T, FxBuildHasher>;
}

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
    /// accumulate in memory. Used by sockets, cluster and HTTP client/server paths to
    /// bound attacker-controlled `Content-Length`, frame, or read sizes.
    pub max_network_read_bytes: usize,
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
    /// Maximum total bytes accepted by one socket write batch. Lua can pass many
    /// buffers at once, so this bounds the transient aggregation before the IO
    /// task writes to the network.
    pub socket_write_batch_bytes: usize,
    /// Maximum HTTP client timeout accepted from Lua, in milliseconds. Prevents
    /// accidental or malicious requests from pinning client tasks for excessive
    /// durations.
    pub http_client_timeout_ms: u64,
    /// Default maximum request/response body size for the HTTP server helpers.
    /// This is separate from `max_network_read_bytes` so HTTP server policy can stay
    /// conservative while the global network ceiling remains a hard stop.
    pub max_http_body_bytes: usize,
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
    /// Maximum number of elements a single `zset:range()` call may return.
    /// Bounds the per-call allocation (8 bytes/element) for an in-memory ordered
    /// set. Intentionally larger than `db_query_rows` because a zset is an
    /// in-process structure with no I/O or driver buffering, but kept in this
    /// central catalog so the ceiling is discoverable alongside the DB caps.
    pub zset_range_len: usize,
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
            max_network_read_bytes: 512 * 1024 * 1024,
            listener_connections: 100_000,
            db_query_rows: 100_000,
            db_stream_batch_rows: 100,
            request_queue_capacity: 1024,
            socket_write_batch_bytes: 256 * 1024,
            http_client_timeout_ms: 10_000,
            max_http_body_bytes: 10 * 1024 * 1024,
            http_static_stream_threshold_bytes: 1024 * 1024,
            http_static_cache_ttl_secs: 5,
            http_static_cache_entries: 10_000,
            db_wire_message_bytes: 64 * 1024 * 1024,
            redis_array_items: 1024 * 1024,
            zset_range_len: 1_000_000,
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
pub fn lua_push_error_tuple(state: LuaState, msg: &str) -> c_int {
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
        // C module amalgamated into the lua55 static lib (lua/clonefunc.c via onelua.c).
        fn luaopen_clonefunc(L: *mut ffi::lua_State) -> c_int;
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
    lua_require!(state, "zset", lua_zset::luaopen_zset);
    lua_require!(state, "schema", lua_schema::luaopen_schema);
    lua_require!(state, "buffer", lua_buffer::luaopen_buffer);
    lua_require!(state, "seri", lua_seri::luaopen_seri);
    lua_require!(state, "utils", lua_utils::luaopen_utils);
    lua_require!(state, "uuid", lua_uuid::luaopen_uuid);
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
    #[cfg(feature = "grpc")]
    lua_require!(state, "grpc.core", lua_grpc::luaopen_grpc);
    unsafe {
        ffi::luaL_requiref(
            state.as_ptr(),
            cstr!("sharetable.core"),
            luaopen_sharetable_core,
            0,
        );
        ffi::lua_pop(state.as_ptr(), 1);

        ffi::luaL_requiref(state.as_ptr(), cstr!("clonefunc"), luaopen_clonefunc, 0);
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
    #[cfg(feature = "grpc")]
    use moon_runtime::context::PTYPE_GRPC;
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
    use moon_runtime::context::{
        PTYPE_DEBUG, PTYPE_ERROR, PTYPE_INTEGER, PTYPE_LUA, PTYPE_SOCKET_EVENT, PTYPE_SOCKET_TCP,
        PTYPE_TEXT, PTYPE_TIMER,
    };

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
    #[cfg(feature = "grpc")]
    {
        decoders[PTYPE_GRPC as usize] = lua_grpc::decode_grpc_message;
    }

    decoders
}

#[cfg(test)]
#[path = "../tests/unit/lib.rs"]
mod tests;
