//! Native Redis driver exposed to Lua as `redis.core`.
//!
//! Architecture mirrors `lua_pg.rs`:
//!  - A global registry of named connection pools (`REDIS_CONNECTIONS`).
//!  - `connect` validates a connection, then spawns `pool_size` worker tasks,
//!    each owning one `TcpStream` and reconnecting on socket errors.
//!  - Requests carry a pre-built RESP buffer (encoded on the Lua/actor thread)
//!    which is moved to a worker; the worker writes it and reads the reply.
//!  - Responses are raw RESP bytes; `decode` parses them into Lua values on
//!    the actor thread.
//!  - Async delivery via `PTYPE_REDIS` + `moon.wait(session)`.

use crate::request_pool::{
    PendingCounter, QueuedRequest, WorkerHandle, WorkerSet, drain_queued_requests,
};
use dashmap::DashMap;
use lazy_static::lazy_static;
use moon_base::laux::LuaState;
use moon_base::{
    cstr, ffi, laux,
    laux::{LuaTable, LuaValue},
    lreg, lreg_null, luaL_newlib, push_lua_table,
};
use moon_runtime::actor::LuaActor;
use moon_runtime::context::{self, ActorId, CONTEXT};
use std::{ffi::c_int, sync::Arc, time::Duration};
use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::net::TcpStream;
use tokio::sync::mpsc;
use tokio::time::timeout;

lazy_static! {
    static ref REDIS_CONNECTIONS: DashMap<String, RedisPool> = DashMap::new();
}

// ---------------------------------------------------------------------------
// Connection parameters (from Lua table: host, port, auth, db, timeout)
// ---------------------------------------------------------------------------

#[derive(Clone)]
struct ConnParams {
    host: String,
    port: u16,
    username: String,
    password: String,
    db: u16,
    read_timeout_ms: u64,
}

// ---------------------------------------------------------------------------
// Pool
// ---------------------------------------------------------------------------

/// A message delivered to a worker: either a command request or a graceful
/// shutdown signal (sent by `close()` so the worker exits and drops its
/// connection even while the Lua-side pool handle keeps the pool `Arc` alive).
enum RedisMessage {
    Request(RedisRequest),
    Shutdown,
}

#[derive(Clone)]
struct RedisPool {
    inner: Arc<WorkerSet<RedisMessage>>,
}

impl RedisPool {
    fn dispatch(
        &self,
        owner: ActorId,
        session: i64,
        data: Vec<u8>,
        reply_count: u32,
    ) -> Result<(), String> {
        self.inner.dispatch(RedisMessage::Request(RedisRequest {
            owner,
            session,
            data,
            reply_count,
        }))
    }
}

struct RedisRequest {
    owner: ActorId,
    session: i64,
    data: Vec<u8>,
    reply_count: u32,
}

impl QueuedRequest for RedisMessage {
    fn owner_session(&self) -> Option<(ActorId, i64)> {
        match self {
            RedisMessage::Request(req) => Some((req.owner, req.session)),
            RedisMessage::Shutdown => None,
        }
    }
}

// ---------------------------------------------------------------------------
// RESP reply
// ---------------------------------------------------------------------------

#[allow(dead_code)]
enum RedisReply {
    Status(String),
    Error(String),
    Integer(i64),
    Bulk(Option<Vec<u8>>),
    Array(Option<Vec<RedisReply>>),
}

enum RedisResponse {
    Connect,
    Error(String),
    /// Single command: raw RESP bytes (parsed on actor thread).
    Raw(Vec<u8>),
    /// Pipeline: raw RESP bytes for N replies.
    RawPipeline(Vec<u8>, u32),
    /// Pub/sub watch handle ready.
    Watch(RedisWatch),
    /// Pub/sub message delivery (`message` / `pmessage`).
    WatchMessage(RedisReply),
}

// ---------------------------------------------------------------------------
// Pub/Sub watch (dedicated connection, not pooled)
// ---------------------------------------------------------------------------

#[derive(Clone)]
struct RedisWatch {
    tx: mpsc::UnboundedSender<WatchOp>,
}

enum WatchOp {
    Subscribe(Vec<String>),
    PSubscribe(Vec<String>),
    Unsubscribe(Vec<String>),
    PUnsubscribe(Vec<String>),
    WaitMessage { owner: ActorId, session: i64 },
    Close,
}

impl RedisWatch {
    fn send_op(&self, op: WatchOp) -> Result<(), String> {
        self.tx.send(op).map_err(|e| e.to_string())
    }
}

fn is_pubsub_delivery(reply: &RedisReply) -> bool {
    if let RedisReply::Array(Some(items)) = reply {
        if let Some(RedisReply::Bulk(Some(b))) = items.first() {
            let t = std::str::from_utf8(b).unwrap_or("");
            return t == "message" || t == "pmessage";
        }
    }
    false
}

struct WatchMessageWait {
    owner: ActorId,
    session: i64,
}

async fn watch_loop(mut conn: RedisConn, mut rx: mpsc::UnboundedReceiver<WatchOp>) {
    let mut pending_wait: Option<WatchMessageWait> = None;

    loop {
        tokio::select! {
            op = rx.recv() => {
                match op {
                    Some(WatchOp::Subscribe(channels)) => {
                        for ch in channels {
                            if conn.send_command(&["SUBSCRIBE", &ch]).await.is_err() {
                                break;
                            }
                        }
                    }
                    Some(WatchOp::PSubscribe(patterns)) => {
                        for pat in patterns {
                            if conn.send_command(&["PSUBSCRIBE", &pat]).await.is_err() {
                                break;
                            }
                        }
                    }
                    Some(WatchOp::Unsubscribe(channels)) => {
                        for ch in channels {
                            if conn.send_command(&["UNSUBSCRIBE", &ch]).await.is_err() {
                                break;
                            }
                        }
                    }
                    Some(WatchOp::PUnsubscribe(patterns)) => {
                        for pat in patterns {
                            if conn.send_command(&["PUNSUBSCRIBE", &pat]).await.is_err() {
                                break;
                            }
                        }
                    }
                    Some(WatchOp::WaitMessage { owner, session }) => {
                        // Only one waiter is supported per watch connection. A
                        // second concurrent wait must not silently replace the
                        // first (that would hang the first coroutine forever):
                        // reject the new request and keep the existing waiter.
                        if pending_wait.is_some() {
                            let _ = CONTEXT.send_value(
                                context::PTYPE_REDIS,
                                owner,
                                session,
                                RedisResponse::Error(
                                    "watch: a message wait is already pending".to_string(),
                                ),
                            );
                        } else {
                            pending_wait = Some(WatchMessageWait { owner, session });
                        }
                    }
                    Some(WatchOp::Close) | None => break,
                }
            }
            reply = conn.read_reply() => {
                match reply {
                    Ok(r) if is_pubsub_delivery(&r) => {
                        if let Some(wait) = pending_wait.take() {
                            let _ = CONTEXT.send_value(
                                context::PTYPE_REDIS,
                                wait.owner,
                                wait.session,
                                RedisResponse::WatchMessage(r),
                            );
                        }
                    }
                    Ok(_) => {}
                    Err(e) => {
                        if let Some(wait) = pending_wait.take() {
                            let _ = CONTEXT.send_value(
                                context::PTYPE_REDIS,
                                wait.owner,
                                wait.session,
                                RedisResponse::Error(e),
                            );
                        }
                        break;
                    }
                }
            }
        }
    }

    // The loop is exiting (explicit Close, all senders dropped, or a read
    // error already handled above). If a waiter is still pending, wake it with
    // an error instead of leaving the Lua coroutine blocked on `moon.wait`
    // forever.
    if let Some(wait) = pending_wait.take() {
        let _ = CONTEXT.send_value(
            context::PTYPE_REDIS,
            wait.owner,
            wait.session,
            RedisResponse::Error("watch closed".to_string()),
        );
    }
}

// ---------------------------------------------------------------------------
// Connection
// ---------------------------------------------------------------------------

const MAX_MESSAGE_LEN: usize = crate::LIMITS.db_wire_message_bytes;
const MAX_ARRAY_COUNT: usize = crate::LIMITS.redis_array_items;

struct RedisConn {
    stream: BufReader<TcpStream>,
    read_timeout: Duration,
    line_buf: Vec<u8>,
}

impl RedisConn {
    async fn connect(params: &ConnParams, timeout_ms: u64) -> Result<Self, String> {
        let fut = Self::connect_inner(params);
        match timeout(Duration::from_millis(timeout_ms), fut).await {
            Ok(res) => res,
            Err(_) => Err(format!(
                "connect timeout after {}ms to {}:{}",
                timeout_ms, params.host, params.port
            )),
        }
    }

    async fn connect_inner(params: &ConnParams) -> Result<Self, String> {
        let addr = format!("{}:{}", params.host, params.port);
        let tcp = TcpStream::connect(&addr)
            .await
            .map_err(|e| format!("connect {}: {}", addr, e))?;

        let sock_ref = socket2::SockRef::from(&tcp);
        let ka = socket2::TcpKeepalive::new()
            .with_time(Duration::from_secs(60))
            .with_interval(Duration::from_secs(15));
        let _ = sock_ref.set_tcp_keepalive(&ka);
        tcp.set_nodelay(true).ok();

        let mut conn = RedisConn {
            stream: BufReader::new(tcp),
            read_timeout: Duration::from_millis(params.read_timeout_ms),
            line_buf: Vec::with_capacity(128),
        };

        // AUTH (Redis 6+ ACL: AUTH username password)
        if !params.password.is_empty() {
            if params.username.is_empty() {
                conn.send_command(&["AUTH", &params.password]).await?;
            } else {
                conn.send_command(&["AUTH", &params.username, &params.password])
                    .await?;
            }
            let reply = conn.read_reply().await?;
            match &reply {
                RedisReply::Status(_) => {}
                RedisReply::Error(e) => return Err(format!("AUTH failed: {}", e)),
                _ => return Err("AUTH: unexpected reply".to_string()),
            }
        }

        // SELECT db
        if params.db > 0 {
            let mut tmp = [0u8; lexical_core::BUFFER_SIZE];
            // `lexical_core::write` always emits ASCII digits, so this is valid UTF-8.
            let Ok(db_str) = std::str::from_utf8(lexical_core::write(params.db, &mut tmp)) else {
                return Err("SELECT: db number is not valid utf-8".to_string());
            };
            conn.send_command(&["SELECT", db_str]).await?;
            let reply = conn.read_reply().await?;
            match &reply {
                RedisReply::Status(_) => {}
                RedisReply::Error(e) => return Err(format!("SELECT failed: {}", e)),
                _ => return Err("SELECT: unexpected reply".to_string()),
            }
        }

        Ok(conn)
    }

    async fn send_command(&mut self, args: &[&str]) -> Result<(), String> {
        let mut buf = Vec::with_capacity(64);
        encode_resp_strings(&mut buf, args);
        self.stream
            .get_mut()
            .write_all(&buf)
            .await
            .map_err(|e| format!("write: {}", e))
    }

    async fn send_raw(&mut self, data: &[u8]) -> Result<(), String> {
        self.stream
            .get_mut()
            .write_all(data)
            .await
            .map_err(|e| format!("write: {}", e))
    }

    async fn read_reply(&mut self) -> Result<RedisReply, String> {
        match timeout(self.read_timeout, self.read_reply_inner()).await {
            Ok(r) => r,
            Err(_) => Err("read timeout".to_string()),
        }
    }

    fn read_reply_inner(
        &mut self,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<RedisReply, String>> + Send + '_>>
    {
        Box::pin(async move {
            self.line_buf.clear();
            self.stream
                .read_until(b'\n', &mut self.line_buf)
                .await
                .map_err(|e| format!("read: {}", e))?;
            if self.line_buf.len() < 3 {
                return Err(format!("invalid RESP line (len={})", self.line_buf.len()));
            }
            let data = &self.line_buf[1..self.line_buf.len() - 2];
            match self.line_buf[0] {
                b'+' => Ok(RedisReply::Status(
                    String::from_utf8_lossy(data).into_owned(),
                )),
                b'-' => Ok(RedisReply::Error(
                    String::from_utf8_lossy(data).into_owned(),
                )),
                b':' => {
                    let v = lexical_core::parse::<i64>(data)
                        .map_err(|e| format!("invalid integer: {}", e))?;
                    Ok(RedisReply::Integer(v))
                }
                b'$' => {
                    let len = lexical_core::parse::<i64>(data)
                        .map_err(|e| format!("invalid bulk length: {}", e))?;
                    if len < 0 {
                        return Ok(RedisReply::Bulk(None));
                    }
                    let len = len as usize;
                    if len > MAX_MESSAGE_LEN {
                        return Err(format!("bulk string too large: {} bytes", len));
                    }
                    let mut buf = vec![0u8; len + 2];
                    self.stream
                        .read_exact(&mut buf)
                        .await
                        .map_err(|e| format!("read bulk: {}", e))?;
                    buf.truncate(len);
                    Ok(RedisReply::Bulk(Some(buf)))
                }
                b'*' => {
                    let count = lexical_core::parse::<i64>(data)
                        .map_err(|e| format!("invalid array count: {}", e))?;
                    if count < 0 {
                        return Ok(RedisReply::Array(None));
                    }
                    let count = count as usize;
                    if count > MAX_ARRAY_COUNT {
                        return Err(format!(
                            "array too large: {} elements (max {})",
                            count, MAX_ARRAY_COUNT
                        ));
                    }
                    let mut items = Vec::with_capacity(count);
                    for _ in 0..count {
                        items.push(self.read_reply_inner().await?);
                    }
                    Ok(RedisReply::Array(Some(items)))
                }
                c => Err(format!("unknown RESP type: {}", c as char)),
            }
        })
    }

    async fn execute(&mut self, data: &[u8]) -> Result<RedisReply, String> {
        self.send_raw(data).await?;
        self.read_reply().await
    }

    async fn execute_pipeline(
        &mut self,
        data: &[u8],
        count: usize,
    ) -> Result<Vec<RedisReply>, String> {
        self.send_raw(data).await?;
        let mut replies = Vec::with_capacity(count);
        for _ in 0..count {
            replies.push(self.read_reply().await?);
        }
        Ok(replies)
    }

    // ---- Raw-bytes path (hot path for await mode) ----

    fn read_raw_reply_into<'a>(
        &'a mut self,
        out: &'a mut Vec<u8>,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<(), String>> + Send + 'a>> {
        Box::pin(async move {
            self.line_buf.clear();
            self.stream
                .read_until(b'\n', &mut self.line_buf)
                .await
                .map_err(|e| format!("read: {}", e))?;
            if self.line_buf.len() < 3 {
                return Err(format!("invalid RESP line (len={})", self.line_buf.len()));
            }
            let type_byte = self.line_buf[0];
            let num = if matches!(type_byte, b'$' | b'*') {
                lexical_core::parse::<i64>(&self.line_buf[1..self.line_buf.len() - 2])
                    .map_err(|e| format!("invalid RESP length: {}", e))?
            } else {
                0
            };
            out.extend_from_slice(&self.line_buf);

            match type_byte {
                b'+' | b'-' | b':' => Ok(()),
                b'$' => {
                    if num < 0 {
                        return Ok(());
                    }
                    let len = num as usize;
                    if len > MAX_MESSAGE_LEN {
                        return Err(format!("bulk string too large: {} bytes", len));
                    }
                    let start = out.len();
                    out.resize(start + len + 2, 0);
                    self.stream
                        .read_exact(&mut out[start..])
                        .await
                        .map_err(|e| format!("read bulk: {}", e))?;
                    Ok(())
                }
                b'*' => {
                    if num < 0 {
                        return Ok(());
                    }
                    let count = num as usize;
                    if count > MAX_ARRAY_COUNT {
                        return Err(format!(
                            "array too large: {} elements (max {})",
                            count, MAX_ARRAY_COUNT
                        ));
                    }
                    for _ in 0..count {
                        self.read_raw_reply_into(out).await?;
                    }
                    Ok(())
                }
                c => Err(format!("unknown RESP type: {}", c as char)),
            }
        })
    }

    async fn read_raw_reply(&mut self, out: &mut Vec<u8>) -> Result<(), String> {
        match timeout(self.read_timeout, self.read_raw_reply_into(out)).await {
            Ok(r) => r,
            Err(_) => Err("read timeout".to_string()),
        }
    }

    #[allow(dead_code)]
    async fn execute_raw(&mut self, data: &[u8], out: &mut Vec<u8>) -> Result<(), String> {
        self.send_raw(data).await?;
        self.read_raw_reply(out).await
    }

    #[allow(dead_code)]
    async fn execute_pipeline_raw(
        &mut self,
        data: &[u8],
        count: usize,
        out: &mut Vec<u8>,
    ) -> Result<(), String> {
        self.send_raw(data).await?;
        for _ in 0..count {
            self.read_raw_reply(out).await?;
        }
        Ok(())
    }
}

/// Encode a command from string slices into RESP format.
fn encode_resp_strings(buf: &mut Vec<u8>, args: &[&str]) {
    write_resp_array_header(buf, args.len());
    for arg in args {
        write_bulk_bytes(buf, arg.as_bytes());
    }
}

/// Write `*N\r\n` RESP array header.
#[inline]
fn write_resp_array_header(buf: &mut Vec<u8>, count: usize) {
    buf.push(b'*');
    let mut tmp = [0u8; lexical_core::BUFFER_SIZE];
    let n = lexical_core::write(count, &mut tmp);
    buf.extend_from_slice(n);
    buf.extend_from_slice(b"\r\n");
}

// ---------------------------------------------------------------------------
// Worker task
// ---------------------------------------------------------------------------

async fn worker_loop(
    name: String,
    params: ConnParams,
    timeout_ms: u64,
    mut rx: mpsc::Receiver<RedisMessage>,
    counter: PendingCounter,
    initial_conn: Option<RedisConn>,
) {
    let mut conn: Option<RedisConn> = initial_conn;

    while let Some(msg) = rx.recv().await {
        let req = match msg {
            RedisMessage::Request(req) => req,
            RedisMessage::Shutdown => {
                drain_queued_requests(&mut rx, &counter, |owner, session| {
                    let _ = CONTEXT.send_value(
                        context::PTYPE_REDIS,
                        owner,
                        session,
                        RedisResponse::Error("redis connection closed".to_string()),
                    );
                });
                break;
            }
        };
        let mut failed_times = 0;
        loop {
            if conn.is_none() {
                match RedisConn::connect(&params, timeout_ms).await {
                    Ok(c) => conn = Some(c),
                    Err(e) => {
                        if req.session != 0 {
                            let _ = CONTEXT.send_value(
                                context::PTYPE_REDIS,
                                req.owner,
                                req.session,
                                RedisResponse::Error(e),
                            );
                            counter.dec();
                            break;
                        } else {
                            if failed_times == 0 {
                                log::error!("redis '{}' reconnect failed: {}. retrying.", name, e,);
                            }
                            failed_times += 1;
                            tokio::time::sleep(Duration::from_secs(1)).await;
                            continue;
                        }
                    }
                }
            }

            let c = conn.as_mut().unwrap();
            let count = req.reply_count as usize;

            if req.session != 0 {
                let write_result = c.send_raw(&req.data).await;

                let result: Result<RedisResponse, String> = match write_result {
                    Err(e) => Err(format!("write: {}", e)),
                    Ok(()) => {
                        let mut raw = Vec::with_capacity(if count == 1 { 64 } else { count * 32 });
                        let mut read_err = None;
                        for _ in 0..count {
                            if let Err(e) = c.read_raw_reply(&mut raw).await {
                                read_err = Some(e);
                                break;
                            }
                        }

                        match read_err {
                            Some(e) => Err(e),
                            None => {
                                let response = if count == 1 {
                                    RedisResponse::Raw(raw)
                                } else {
                                    RedisResponse::RawPipeline(raw, req.reply_count)
                                };
                                let _ = CONTEXT.send_value(
                                    context::PTYPE_REDIS,
                                    req.owner,
                                    req.session,
                                    response,
                                );
                                counter.dec();
                                break;
                            }
                        }
                    }
                };
                if let Err(e) = result {
                    conn = None;
                    let _ = CONTEXT.send_value(
                        context::PTYPE_REDIS,
                        req.owner,
                        req.session,
                        RedisResponse::Error(e),
                    );
                    counter.dec();
                    break;
                }
            } else {
                let result: Result<(), String> = if count == 1 {
                    match c.execute(&req.data).await {
                        Ok(RedisReply::Error(e)) => {
                            log::error!("redis '{}' command error: {}", name, e);
                            Ok(())
                        }
                        Ok(_) => Ok(()),
                        Err(e) => Err(e),
                    }
                } else {
                    match c.execute_pipeline(&req.data, count).await {
                        Ok(replies) => {
                            for r in &replies {
                                if let RedisReply::Error(e) = r {
                                    log::error!("redis '{}' pipeline error: {}", name, e);
                                }
                            }
                            Ok(())
                        }
                        Err(e) => Err(e),
                    }
                };
                match result {
                    Ok(()) => {
                        counter.dec();
                        break;
                    }
                    Err(e) => {
                        conn = None;
                        if failed_times == 0 {
                            log::error!("redis '{}' socket error: {}. retrying.", name, e);
                        }
                        failed_times += 1;
                        tokio::time::sleep(Duration::from_secs(1)).await;
                        continue;
                    }
                }
            }
        }
    }
}

// ---------------------------------------------------------------------------
// RESP encoding (runs on the Lua/actor thread)
// ---------------------------------------------------------------------------

/// Encode one Lua value as a RESP bulk string into `buf`.
/// Tables are automatically JSON-encoded. Returns an error for
/// unsupported types or JSON encoding failures.
fn write_bulk_arg(buf: &mut Vec<u8>, state: LuaState, idx: i32) -> Result<(), String> {
    let val = LuaValue::from_stack(state, idx);
    match val {
        LuaValue::Nil | LuaValue::None => {
            buf.extend_from_slice(b"$-1\r\n");
        }
        LuaValue::Integer(v) => {
            let mut tmp = [0u8; lexical_core::BUFFER_SIZE];
            let written = lexical_core::write(v, &mut tmp);
            write_bulk_bytes(buf, written);
        }
        LuaValue::Number(v) => {
            let mut tmp = [0u8; lexical_core::BUFFER_SIZE];
            let written = lexical_core::write(v, &mut tmp);
            write_bulk_bytes(buf, written);
        }
        LuaValue::Boolean(v) => {
            write_bulk_bytes(buf, if v { b"1" } else { b"0" });
        }
        LuaValue::String(s) => {
            write_bulk_bytes(buf, s);
        }
        LuaValue::Table(tbl) => {
            let options = crate::lua_json::JsonOptions::default();
            let mut json = Vec::with_capacity(64);
            crate::lua_json::encode_table(&mut json, &tbl, 0, false, &options)
                .map_err(|e| format!("JSON encode failed: {}", e))?;
            write_bulk_bytes(buf, &json);
        }
        _ => {
            let type_name = unsafe {
                let tp = ffi::lua_type(state.as_ptr(), idx);
                std::ffi::CStr::from_ptr(ffi::lua_typename(state.as_ptr(), tp))
                    .to_str()
                    .unwrap_or("unknown")
            };
            return Err(format!("unsupported type: {}", type_name));
        }
    }
    Ok(())
}

fn write_bulk_bytes(buf: &mut Vec<u8>, data: &[u8]) {
    buf.push(b'$');
    let mut tmp = [0u8; lexical_core::BUFFER_SIZE];
    let len_bytes = lexical_core::write(data.len(), &mut tmp);
    buf.extend_from_slice(len_bytes);
    buf.extend_from_slice(b"\r\n");
    buf.extend_from_slice(data);
    buf.extend_from_slice(b"\r\n");
}

// ---------------------------------------------------------------------------
// Lua-facing functions
// ---------------------------------------------------------------------------

const REDIS_POOL_META: *const std::ffi::c_char = cstr!("redis_pool_metatable");
const REDIS_WATCH_META: *const std::ffi::c_char = cstr!("redis_watch_metatable");

fn parse_conn_params(state: LuaState, idx: i32) -> ConnParams {
    laux::lua_checktype(state, idx, ffi::LUA_TTABLE);

    unsafe { ffi::lua_getfield(state.as_ptr(), idx, cstr!("host")) };
    let host = if laux::lua_type(state, -1) == laux::LuaType::String {
        unsafe { laux::lua_check_str(state, -1) }.to_string()
    } else {
        "127.0.0.1".to_string()
    };
    laux::lua_pop(state, 1);

    unsafe { ffi::lua_getfield(state.as_ptr(), idx, cstr!("port")) };
    let port: u16 = if laux::lua_type(state, -1) == laux::LuaType::Number {
        laux::lua_opt(state, -1).unwrap_or(6379)
    } else {
        6379
    };
    laux::lua_pop(state, 1);

    unsafe { ffi::lua_getfield(state.as_ptr(), idx, cstr!("username")) };
    let username = if laux::lua_type(state, -1) == laux::LuaType::String {
        unsafe { laux::lua_check_str(state, -1) }.to_string()
    } else {
        String::new()
    };
    laux::lua_pop(state, 1);

    unsafe { ffi::lua_getfield(state.as_ptr(), idx, cstr!("auth")) };
    let password = if laux::lua_type(state, -1) == laux::LuaType::String {
        unsafe { laux::lua_check_str(state, -1) }.to_string()
    } else {
        String::new()
    };
    laux::lua_pop(state, 1);

    unsafe { ffi::lua_getfield(state.as_ptr(), idx, cstr!("db")) };
    let db: u16 = if laux::lua_type(state, -1) == laux::LuaType::Number {
        laux::lua_opt(state, -1).unwrap_or(0)
    } else {
        0
    };
    laux::lua_pop(state, 1);

    unsafe { ffi::lua_getfield(state.as_ptr(), idx, cstr!("read_timeout")) };
    let read_timeout_ms: u64 = if laux::lua_type(state, -1) == laux::LuaType::Number {
        laux::lua_opt(state, -1).unwrap_or(crate::LIMITS.db_read_timeout_ms)
    } else {
        crate::LIMITS.db_read_timeout_ms
    };
    laux::lua_pop(state, 1);

    ConnParams {
        host,
        port,
        username,
        password,
        db,
        read_timeout_ms,
    }
}

fn parse_connect_timeout(state: LuaState, idx: i32, default: u64) -> u64 {
    unsafe { ffi::lua_getfield(state.as_ptr(), idx, cstr!("timeout")) };
    let v = if laux::lua_type(state, -1) == laux::LuaType::Number {
        laux::lua_opt(state, -1).unwrap_or(default)
    } else {
        default
    };
    laux::lua_pop(state, 1);
    v
}

fn collect_string_args(state: LuaState, start: i32) -> Vec<String> {
    let top = laux::lua_top(state);
    let mut out = Vec::with_capacity((top - start + 1) as usize);
    for i in start..=top {
        out.push(unsafe { laux::lua_check_str(state, i) }.to_string());
    }
    out
}

/// `redis.connect(opts_table, name, timeout_ms, pool_size)`
///
/// `opts_table`: `{ host = "127.0.0.1", port = 6379, auth = "password", db = 0 }`
extern "C-unwind" fn connect(state: LuaState) -> c_int {
    let params = parse_conn_params(state, 1);

    let name: String = laux::lua_opt(state, 2).unwrap_or_else(|| "default".to_string());
    let timeout_ms: u64 = laux::lua_opt(state, 3).unwrap_or(5000);
    let pool_size: usize = laux::lua_opt(state, 4).unwrap_or(1);
    let pool_size = pool_size.max(1);
    let queue_capacity: usize =
        laux::lua_opt(state, 5).unwrap_or(crate::LIMITS.request_queue_capacity);
    let queue_capacity = queue_capacity.max(1);

    let actor = LuaActor::from_lua_state(state);
    let owner = unsafe { (*actor).id };
    let session = unsafe { (*actor).next_session() };

    CONTEXT.io_runtime().spawn(async move {
        let first_conn = match RedisConn::connect(&params, timeout_ms).await {
            Ok(c) => c,
            Err(e) => {
                let _ = CONTEXT.send_value(
                    context::PTYPE_REDIS,
                    owner,
                    session,
                    RedisResponse::Error(e),
                );
                return;
            }
        };

        let mut workers = Vec::with_capacity(pool_size);
        let mut seed_conn = Some(first_conn);
        for _ in 0..pool_size {
            let (tx, rx) = mpsc::channel(queue_capacity);
            let counter = PendingCounter::new();
            CONTEXT.io_runtime().spawn(worker_loop(
                name.clone(),
                params.clone(),
                timeout_ms,
                rx,
                counter.clone(),
                seed_conn.take(),
            ));
            workers.push(WorkerHandle::new(tx, counter));
        }

        let pool = RedisPool {
            inner: Arc::new(WorkerSet::new(name.clone(), workers)),
        };
        // Replacing an existing pool of the same name: shut down the previous
        // pool's workers so their tasks/connections don't leak (the old workers
        // drain their queued requests, then exit on `Shutdown`).
        if let Some(old) = REDIS_CONNECTIONS.insert(name, pool) {
            log::warn!(
                "redis '{}' reconnected with the same name; shutting down the previous pool",
                old.inner.name()
            );
            for w in old.inner.workers() {
                let _ = w.tx().send(RedisMessage::Shutdown).await;
            }
        }
        let _ = CONTEXT.send_value(context::PTYPE_REDIS, owner, session, RedisResponse::Connect);
    });

    laux::lua_push(state, session);
    1
}

extern "C-unwind" fn find_connection(state: LuaState) -> c_int {
    let name = unsafe { laux::lua_check_str(state, 1) };
    match REDIS_CONNECTIONS.get(name) {
        Some(pair) => {
            let methods = [
                lreg!("command", command),
                lreg!("pipeline", pipeline),
                lreg!("exec_command", exec_command),
                lreg!("exec_pipeline", exec_pipeline),
                lreg!("len", pool_len),
                lreg!("close", close),
                lreg_null!(),
            ];
            if laux::lua_newuserdata(
                state,
                pair.value().clone(),
                REDIS_POOL_META,
                methods.as_ref(),
            )
            .is_none()
            {
                laux::lua_pushnil(state);
            }
        }
        None => laux::lua_pushnil(state),
    }
    1
}

fn dispatch_async(state: LuaState, pool: &RedisPool, data: Vec<u8>, reply_count: u32) -> c_int {
    let actor = LuaActor::from_lua_state(state);
    let owner = unsafe { (*actor).id };
    let session = unsafe { (*actor).next_session() };
    match pool.dispatch(owner, session, data, reply_count) {
        Ok(_) => {
            laux::lua_push(state, session);
            1
        }
        Err(err) => {
            push_lua_table!(state, "code" => "SOCKET", "message" => err);
            1
        }
    }
}

fn dispatch_forget(state: LuaState, pool: &RedisPool, data: Vec<u8>, reply_count: u32) -> c_int {
    let owner = unsafe { (*LuaActor::from_lua_state(state)).id };
    match pool.dispatch(owner, 0, data, reply_count) {
        Ok(_) => {
            laux::lua_push(state, true);
            1
        }
        Err(err) => {
            push_lua_table!(state, "code" => "SOCKET", "message" => err);
            1
        }
    }
}

/// `handle:command(cmd, arg1, arg2, ...)` — single Redis command.
extern "C-unwind" fn command(state: LuaState) -> c_int {
    command_impl(state, false)
}
extern "C-unwind" fn exec_command(state: LuaState) -> c_int {
    command_impl(state, true)
}

fn command_impl(state: LuaState, forget: bool) -> c_int {
    let pool = laux::lua_touserdata::<RedisPool>(state, 1).expect("invalid redis pool pointer");
    let top = laux::lua_top(state);
    let nargs = (top - 1) as usize;

    let mut data = Vec::with_capacity(64);
    write_resp_array_header(&mut data, nargs);

    for i in 2..=top {
        if let Err(e) = write_bulk_arg(&mut data, state, i) {
            push_lua_table!(state, "code" => "ENCODE", "message" => format!("arg {}: {}", i - 1, e));
            return 1;
        }
    }

    if forget {
        dispatch_forget(state, pool, data, 1)
    } else {
        dispatch_async(state, pool, data, 1)
    }
}

/// `handle:pipeline(ops, resp_flag)` — pipelined commands.
///
/// `ops` is `{ {"SET", "k", "v"}, {"GET", "k"}, ... }`.
extern "C-unwind" fn pipeline(state: LuaState) -> c_int {
    pipeline_impl(state, false)
}
extern "C-unwind" fn exec_pipeline(state: LuaState) -> c_int {
    pipeline_impl(state, true)
}

fn pipeline_impl(state: LuaState, forget: bool) -> c_int {
    let pool = laux::lua_touserdata::<RedisPool>(state, 1).expect("invalid redis pool pointer");
    let ops_idx = laux::lua_absindex(state, 2);
    laux::lua_checktype(state, ops_idx, ffi::LUA_TTABLE);

    let n = unsafe { ffi::lua_rawlen(state.as_ptr(), ops_idx) } as usize;
    if n == 0 {
        push_lua_table!(state, "code" => "ENCODE", "message" => "pipeline: empty ops");
        return 1;
    }
    if n > u32::MAX as usize {
        push_lua_table!(state, "code" => "ENCODE", "message" => format!("pipeline: too many commands ({})", n));
        return 1;
    }

    let mut data = Vec::with_capacity(128);

    for i in 1..=n {
        unsafe { ffi::lua_rawgeti(state.as_ptr(), ops_idx, i as ffi::lua_Integer) };
        let cmd_idx = laux::lua_top(state);
        if laux::lua_type(state, cmd_idx) != laux::LuaType::Table {
            laux::lua_pop(state, 1);
            push_lua_table!(state, "code" => "ENCODE", "message" => format!("pipeline[{}]: expected table", i));
            return 1;
        }

        let cmd_len = unsafe { ffi::lua_rawlen(state.as_ptr(), cmd_idx) } as usize;

        write_resp_array_header(&mut data, cmd_len);

        for j in 1..=cmd_len {
            unsafe { ffi::lua_rawgeti(state.as_ptr(), cmd_idx, j as ffi::lua_Integer) };
            let result = write_bulk_arg(&mut data, state, laux::lua_top(state));
            laux::lua_pop(state, 1);
            if let Err(e) = result {
                laux::lua_pop(state, 1); // pop the command table
                push_lua_table!(state, "code" => "ENCODE", "message" => format!("pipeline[{}][{}]: {}", i, j, e));
                return 1;
            }
        }

        laux::lua_pop(state, 1);
    }

    if forget {
        dispatch_forget(state, pool, data, n as u32)
    } else {
        dispatch_async(state, pool, data, n as u32)
    }
}

extern "C-unwind" fn pool_len(state: LuaState) -> c_int {
    let pool = laux::lua_touserdata::<RedisPool>(state, 1).expect("invalid redis pool pointer");
    let table = LuaTable::new(state, pool.inner.workers().len(), 0);
    for w in pool.inner.workers() {
        table.push(w.counter().load());
    }
    1
}

extern "C-unwind" fn close(state: LuaState) -> c_int {
    let pool = laux::lua_touserdata::<RedisPool>(state, 1).expect("invalid redis pool pointer");
    // Only remove our own entry: if a `connect()` with the same name has already
    // replaced this pool, closing through this (now stale) handle must not evict
    // the newer pool. Identify ourselves by the `inner` Arc.
    REDIS_CONNECTIONS.remove_if(pool.inner.name(), |_, v| Arc::ptr_eq(&v.inner, &pool.inner));
    // Signal every worker to finish any queued requests and then exit, so its
    // task ends and the TCP connection is dropped. Removing the registry entry
    // alone is not enough because the Lua handle still holds a pool `Arc`.
    for worker in pool.inner.workers() {
        let tx = worker.tx().clone();
        CONTEXT.io_runtime().spawn(async move {
            let _ = tx.send(RedisMessage::Shutdown).await;
        });
    }
    laux::lua_push(state, true);
    1
}

extern "C-unwind" fn stats(state: LuaState) -> c_int {
    let table = LuaTable::new(state, 0, REDIS_CONNECTIONS.len());
    REDIS_CONNECTIONS.iter().for_each(|pair| {
        let pool = &pair.value().inner;
        table.rawset_x(pair.key().as_str(), || {
            crate::request_pool::push_pool_stats(
                state,
                pool.pending(),
                pool.total(),
                pool.peak(),
                pool.worker_count() as i64,
            );
        });
    });
    1
}

/// `redis.watch(opts_table)` — dedicated pub/sub connection.
extern "C-unwind" fn watch_connect(state: LuaState) -> c_int {
    let params = parse_conn_params(state, 1);
    let timeout_ms = parse_connect_timeout(state, 1, 5000);

    let actor = LuaActor::from_lua_state(state);
    let owner = unsafe { (*actor).id };
    let session = unsafe { (*actor).next_session() };

    CONTEXT.io_runtime().spawn(async move {
        match RedisConn::connect(&params, timeout_ms).await {
            Ok(conn) => {
                let (tx, rx) = mpsc::unbounded_channel();
                CONTEXT.io_runtime().spawn(watch_loop(conn, rx));
                let watch = RedisWatch { tx };
                let _ = CONTEXT.send_value(
                    context::PTYPE_REDIS,
                    owner,
                    session,
                    RedisResponse::Watch(watch),
                );
            }
            Err(e) => {
                let _ = CONTEXT.send_value(
                    context::PTYPE_REDIS,
                    owner,
                    session,
                    RedisResponse::Error(e),
                );
            }
        }
    });

    laux::lua_push(state, session);
    1
}

extern "C-unwind" fn watch_subscribe(state: LuaState) -> c_int {
    let watch = laux::lua_touserdata::<RedisWatch>(state, 1).expect("invalid redis watch pointer");
    let channels = collect_string_args(state, 2);
    match watch.send_op(WatchOp::Subscribe(channels)) {
        Ok(()) => laux::lua_push(state, true),
        Err(err) => push_lua_table!(state, "code" => "SOCKET", "message" => err.as_str()),
    }
    1
}

extern "C-unwind" fn watch_psubscribe(state: LuaState) -> c_int {
    let watch = laux::lua_touserdata::<RedisWatch>(state, 1).expect("invalid redis watch pointer");
    let patterns = collect_string_args(state, 2);
    match watch.send_op(WatchOp::PSubscribe(patterns)) {
        Ok(()) => laux::lua_push(state, true),
        Err(err) => push_lua_table!(state, "code" => "SOCKET", "message" => err.as_str()),
    }
    1
}

extern "C-unwind" fn watch_unsubscribe(state: LuaState) -> c_int {
    let watch = laux::lua_touserdata::<RedisWatch>(state, 1).expect("invalid redis watch pointer");
    let channels = collect_string_args(state, 2);
    match watch.send_op(WatchOp::Unsubscribe(channels)) {
        Ok(()) => laux::lua_push(state, true),
        Err(err) => push_lua_table!(state, "code" => "SOCKET", "message" => err.as_str()),
    }
    1
}

extern "C-unwind" fn watch_punsubscribe(state: LuaState) -> c_int {
    let watch = laux::lua_touserdata::<RedisWatch>(state, 1).expect("invalid redis watch pointer");
    let patterns = collect_string_args(state, 2);
    match watch.send_op(WatchOp::PUnsubscribe(patterns)) {
        Ok(()) => laux::lua_push(state, true),
        Err(err) => push_lua_table!(state, "code" => "SOCKET", "message" => err.as_str()),
    }
    1
}

extern "C-unwind" fn watch_message(state: LuaState) -> c_int {
    let watch = laux::lua_touserdata::<RedisWatch>(state, 1).expect("invalid redis watch pointer");
    let actor = LuaActor::from_lua_state(state);
    let owner = unsafe { (*actor).id };
    let session = unsafe { (*actor).next_session() };
    match watch.send_op(WatchOp::WaitMessage { owner, session }) {
        Ok(()) => laux::lua_push(state, session),
        Err(err) => push_lua_table!(state, "code" => "SOCKET", "message" => err.as_str()),
    }
    1
}

extern "C-unwind" fn watch_close(state: LuaState) -> c_int {
    let watch = laux::lua_touserdata::<RedisWatch>(state, 1).expect("invalid redis watch pointer");
    let _ = watch.send_op(WatchOp::Close);
    laux::lua_push(state, true);
    1
}

fn push_watch_userdata(state: LuaState, watch: RedisWatch) {
    let methods = [
        lreg!("subscribe", watch_subscribe),
        lreg!("psubscribe", watch_psubscribe),
        lreg!("unsubscribe", watch_unsubscribe),
        lreg!("punsubscribe", watch_punsubscribe),
        lreg!("message", watch_message),
        lreg!("close", watch_close),
        lreg_null!(),
    ];
    if laux::lua_newuserdata(state, watch, REDIS_WATCH_META, methods.as_ref()).is_none() {
        laux::lua_pushnil(state);
    }
}

fn push_reply_to_lua(state: LuaState, reply: &RedisReply) -> Result<(), String> {
    match reply {
        RedisReply::Status(s) => {
            laux::lua_push(state, s.as_str());
        }
        RedisReply::Error(e) => {
            push_lua_table!(state, "code" => "REDIS", "message" => e.as_str());
        }
        RedisReply::Integer(v) => {
            laux::lua_push(state, *v);
        }
        RedisReply::Bulk(None) => {
            laux::lua_pushnil(state);
        }
        RedisReply::Bulk(Some(b)) => {
            laux::lua_push(state, b.as_slice());
        }
        RedisReply::Array(None) => {
            laux::lua_pushnil(state);
        }
        RedisReply::Array(Some(items)) => {
            let table = LuaTable::new(state, items.len(), 0);
            for (i, item) in items.iter().enumerate() {
                push_reply_to_lua(state, item)?;
                table.rawseti(i + 1);
            }
        }
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// Response decoding (runs on the actor thread)
// ---------------------------------------------------------------------------

/// Find the position of `\r\n` starting from `start`.
#[inline]
fn find_crlf(raw: &[u8], start: usize) -> Result<usize, String> {
    let slice = &raw[start..];
    if let Some(pos) = memchr::memchr(b'\r', slice) {
        let abs = start + pos;
        if abs + 1 < raw.len() && raw[abs + 1] == b'\n' {
            return Ok(abs);
        }
    }
    Err("missing CRLF in RESP data".to_string())
}

/// Parse one RESP value from raw bytes at `pos`, push it onto the Lua stack,
/// and return the new position past the consumed bytes.
fn parse_raw_push(state: LuaState, raw: &[u8], pos: usize) -> Result<usize, String> {
    if pos >= raw.len() {
        return Err("unexpected end of RESP data".to_string());
    }
    let end = find_crlf(raw, pos + 1)?;
    let data = &raw[pos + 1..end];
    let next = end + 2;
    match raw[pos] {
        b'+' => {
            laux::lua_push(state, data);
            Ok(next)
        }
        b'-' => {
            let msg = std::str::from_utf8(data).unwrap_or("unknown error");
            push_lua_table!(state, "code" => "REDIS", "message" => msg);
            Ok(next)
        }
        b':' => {
            let v =
                lexical_core::parse::<i64>(data).map_err(|e| format!("invalid integer: {}", e))?;
            laux::lua_push(state, v);
            Ok(next)
        }
        b'$' => {
            let len = lexical_core::parse::<i64>(data)
                .map_err(|e| format!("invalid bulk length: {}", e))?;
            if len < 0 {
                laux::lua_pushnil(state);
                return Ok(next);
            }
            let len = len as usize;
            let data_start = next;
            let data_end = data_start + len;
            if data_end + 2 > raw.len() {
                return Err("truncated bulk string in RESP data".to_string());
            }
            laux::lua_push(state, &raw[data_start..data_end]);
            Ok(data_end + 2)
        }
        b'*' => {
            let count = lexical_core::parse::<i64>(data)
                .map_err(|e| format!("invalid array count: {}", e))?;
            if count < 0 {
                laux::lua_pushnil(state);
                return Ok(next);
            }
            let count = count as usize;
            if count > MAX_ARRAY_COUNT {
                return Err(format!(
                    "array too large: {} elements (max {})",
                    count, MAX_ARRAY_COUNT
                ));
            }
            laux::lua_checkstack(state, 4, std::ptr::null());
            let table = LuaTable::new(state, count, 0);
            let mut cur = next;
            for i in 0..count {
                cur = parse_raw_push(state, raw, cur)?;
                table.rawseti(i + 1);
            }
            Ok(cur)
        }
        c => Err(format!("unknown RESP type: {}", c as char)),
    }
}

fn push_redis_response(state: LuaState, response: RedisResponse) -> c_int {
    match response {
        RedisResponse::Connect => {
            LuaTable::new(state, 0, 0);
            1
        }
        RedisResponse::Error(msg) => {
            push_lua_table!(state, "code" => "SOCKET", "message" => msg.as_str());
            1
        }
        RedisResponse::Raw(raw) => {
            if let Err(e) = parse_raw_push(state, &raw, 0) {
                push_lua_table!(state, "code" => "DECODE", "message" => e.as_str());
            }
            1
        }
        RedisResponse::RawPipeline(raw, count) => {
            let count = count as usize;
            let table = LuaTable::new(state, count, 0);
            let mut pos = 0;
            for i in 0..count {
                match parse_raw_push(state, &raw, pos) {
                    Ok(next) => {
                        pos = next;
                        table.rawseti(i + 1);
                    }
                    Err(e) => {
                        push_lua_table!(state, "code" => "DECODE", "message" => e.as_str());
                        table.rawseti(i + 1);
                        break;
                    }
                }
            }
            1
        }
        RedisResponse::Watch(watch) => {
            push_watch_userdata(state, watch);
            1
        }
        RedisResponse::WatchMessage(reply) => {
            if let Err(e) = push_reply_to_lua(state, &reply) {
                push_lua_table!(state, "code" => "DECODE", "message" => e.as_str());
            }
            1
        }
    }
}

pub unsafe extern "C-unwind" fn decode_redis_message(
    state: LuaState,
    m: *mut moon_runtime::context::Message,
) -> c_int {
    match unsafe { crate::message_decode::take_boxed::<RedisResponse>(m) } {
        Ok(response) => push_redis_response(state, response),
        Err(e) => crate::lua_push_error(state, &e),
    }
}

pub extern "C-unwind" fn luaopen_redis(state: LuaState) -> c_int {
    let l = [
        lreg!("connect", connect),
        lreg!("find_connection", find_connection),
        lreg!("watch", watch_connect),
        lreg!("stats", stats),
        lreg_null!(),
    ];
    luaL_newlib!(state, l);
    1
}

#[cfg(test)]
mod tests {
    use super::*;

    // -- RESP encoding -------------------------------------------------------

    #[test]
    fn array_header_encoding() {
        let mut buf = Vec::new();
        write_resp_array_header(&mut buf, 3);
        assert_eq!(buf, b"*3\r\n");

        buf.clear();
        write_resp_array_header(&mut buf, 0);
        assert_eq!(buf, b"*0\r\n");

        buf.clear();
        write_resp_array_header(&mut buf, 100);
        assert_eq!(buf, b"*100\r\n");
    }

    #[test]
    fn bulk_bytes_encoding() {
        let mut buf = Vec::new();
        write_bulk_bytes(&mut buf, b"SET");
        assert_eq!(buf, b"$3\r\nSET\r\n");

        buf.clear();
        write_bulk_bytes(&mut buf, b"");
        assert_eq!(buf, b"$0\r\n\r\n");

        buf.clear();
        write_bulk_bytes(&mut buf, b"hello world");
        assert_eq!(buf, b"$11\r\nhello world\r\n");
    }

    #[test]
    fn bulk_bytes_binary_data() {
        let mut buf = Vec::new();
        write_bulk_bytes(&mut buf, &[0x00, 0xff, 0x0d, 0x0a]);
        assert_eq!(buf, b"$4\r\n\x00\xff\x0d\x0a\r\n");
    }

    #[test]
    fn encode_resp_strings_set() {
        let mut buf = Vec::new();
        encode_resp_strings(&mut buf, &["SET", "mykey", "myval"]);
        assert_eq!(buf, b"*3\r\n$3\r\nSET\r\n$5\r\nmykey\r\n$5\r\nmyval\r\n");
    }

    #[test]
    fn encode_resp_strings_get() {
        let mut buf = Vec::new();
        encode_resp_strings(&mut buf, &["GET", "key"]);
        assert_eq!(buf, b"*2\r\n$3\r\nGET\r\n$3\r\nkey\r\n");
    }

    #[test]
    fn encode_resp_strings_no_args() {
        let mut buf = Vec::new();
        encode_resp_strings(&mut buf, &["PING"]);
        assert_eq!(buf, b"*1\r\n$4\r\nPING\r\n");
    }

    // -- find_crlf -----------------------------------------------------------

    #[test]
    fn find_crlf_basic() {
        assert_eq!(find_crlf(b"+OK\r\n", 1), Ok(3));
        assert_eq!(find_crlf(b"$3\r\nSET\r\n", 1), Ok(2));
    }

    #[test]
    fn find_crlf_from_offset() {
        let data = b"*2\r\n$3\r\nGET\r\n";
        assert_eq!(find_crlf(data, 1), Ok(2));
        assert_eq!(find_crlf(data, 5), Ok(6));
    }

    #[test]
    fn find_crlf_missing() {
        assert!(find_crlf(b"+OK", 1).is_err());
        assert!(find_crlf(b"+OK\r", 1).is_err()); // \r at end, no \n
    }

    #[test]
    fn find_crlf_lone_cr_not_matched() {
        // lone \r without \n is not a valid CRLF — memchr finds the first \r
        // but the next byte is not \n, so it fails
        assert!(find_crlf(b"+O\rK", 1).is_err());
    }

    // -- ConnParams ----------------------------------------------------------

    #[test]
    fn conn_params_defaults() {
        let p = ConnParams {
            host: "localhost".into(),
            port: 6379,
            username: String::new(),
            password: String::new(),
            db: 0,
            read_timeout_ms: crate::LIMITS.db_read_timeout_ms,
        };
        assert_eq!(p.host, "localhost");
        assert_eq!(p.port, 6379);
        assert!(p.username.is_empty());
        assert!(p.password.is_empty());
        assert_eq!(p.db, 0);
        assert_eq!(p.read_timeout_ms, crate::LIMITS.db_read_timeout_ms);
    }

    // -- Limits --------------------------------------------------------------

    #[test]
    fn max_constants_are_reasonable() {
        assert_eq!(MAX_MESSAGE_LEN, crate::LIMITS.db_wire_message_bytes);
        assert_eq!(MAX_ARRAY_COUNT, crate::LIMITS.redis_array_items);
    }

    // -- RESP round-trip via raw bytes ---------------------------------------

    #[test]
    fn resp_status_raw_format() {
        let raw = b"+OK\r\n";
        let end = find_crlf(raw, 1).unwrap();
        assert_eq!(&raw[1..end], b"OK");
    }

    #[test]
    fn resp_error_raw_format() {
        let raw = b"-ERR unknown command\r\n";
        let end = find_crlf(raw, 1).unwrap();
        assert_eq!(&raw[1..end], b"ERR unknown command");
    }

    #[test]
    fn resp_integer_raw_format() {
        let raw = b":42\r\n";
        let end = find_crlf(raw, 1).unwrap();
        let val = lexical_core::parse::<i64>(&raw[1..end]).unwrap();
        assert_eq!(val, 42);
    }

    #[test]
    fn resp_negative_integer() {
        let raw = b":-1\r\n";
        let end = find_crlf(raw, 1).unwrap();
        let val = lexical_core::parse::<i64>(&raw[1..end]).unwrap();
        assert_eq!(val, -1);
    }

    #[test]
    fn resp_bulk_string_raw_format() {
        let raw = b"$5\r\nhello\r\n";
        let end = find_crlf(raw, 1).unwrap();
        let len = lexical_core::parse::<i64>(&raw[1..end]).unwrap() as usize;
        let data_start = end + 2;
        assert_eq!(&raw[data_start..data_start + len], b"hello");
    }

    #[test]
    fn resp_null_bulk_string() {
        let raw = b"$-1\r\n";
        let end = find_crlf(raw, 1).unwrap();
        let len = lexical_core::parse::<i64>(&raw[1..end]).unwrap();
        assert_eq!(len, -1);
    }

    #[test]
    fn resp_nested_array_raw_format() {
        // *2\r\n*1\r\n:1\r\n:2\r\n
        let raw = b"*2\r\n*1\r\n:1\r\n:2\r\n";
        let end = find_crlf(raw, 1).unwrap();
        let count = lexical_core::parse::<i64>(&raw[1..end]).unwrap();
        assert_eq!(count, 2);
    }

    // -- Encode then verify --------------------------------------------------

    #[test]
    fn encode_decode_round_trip() {
        let mut buf = Vec::new();
        encode_resp_strings(&mut buf, &["HSET", "myhash", "field1", "value1"]);
        assert_eq!(&buf[..4], b"*4\r\n");
        assert!(buf.ends_with(b"$6\r\nvalue1\r\n"));
    }

    #[test]
    fn pipeline_encoding() {
        let mut buf = Vec::new();
        encode_resp_strings(&mut buf, &["SET", "a", "1"]);
        encode_resp_strings(&mut buf, &["SET", "b", "2"]);
        let expected = b"*3\r\n$3\r\nSET\r\n$1\r\na\r\n$1\r\n1\r\n\
                         *3\r\n$3\r\nSET\r\n$1\r\nb\r\n$1\r\n2\r\n";
        assert_eq!(buf, expected.to_vec());
    }

    // -- is_pubsub_delivery ---------------------------------------------------

    #[test]
    fn pubsub_delivery_message() {
        let reply = RedisReply::Array(Some(vec![
            RedisReply::Bulk(Some(b"message".to_vec())),
            RedisReply::Bulk(Some(b"chan".to_vec())),
            RedisReply::Bulk(Some(b"payload".to_vec())),
        ]));
        assert!(is_pubsub_delivery(&reply));
    }

    #[test]
    fn pubsub_delivery_pmessage() {
        let reply = RedisReply::Array(Some(vec![
            RedisReply::Bulk(Some(b"pmessage".to_vec())),
            RedisReply::Bulk(Some(b"pattern*".to_vec())),
            RedisReply::Bulk(Some(b"chan".to_vec())),
            RedisReply::Bulk(Some(b"data".to_vec())),
        ]));
        assert!(is_pubsub_delivery(&reply));
    }

    #[test]
    fn pubsub_delivery_subscribe_is_not_delivery() {
        let reply = RedisReply::Array(Some(vec![
            RedisReply::Bulk(Some(b"subscribe".to_vec())),
            RedisReply::Bulk(Some(b"chan".to_vec())),
            RedisReply::Integer(1),
        ]));
        assert!(!is_pubsub_delivery(&reply));
    }

    #[test]
    fn pubsub_delivery_non_array() {
        assert!(!is_pubsub_delivery(&RedisReply::Status("OK".into())));
        assert!(!is_pubsub_delivery(&RedisReply::Integer(42)));
        assert!(!is_pubsub_delivery(&RedisReply::Bulk(None)));
    }

    #[test]
    fn pubsub_delivery_empty_array() {
        assert!(!is_pubsub_delivery(&RedisReply::Array(Some(vec![]))));
        assert!(!is_pubsub_delivery(&RedisReply::Array(None)));
    }

    #[test]
    fn pubsub_delivery_first_element_integer_not_delivery() {
        let reply = RedisReply::Array(Some(vec![
            RedisReply::Integer(1),
            RedisReply::Bulk(Some(b"message".to_vec())),
        ]));
        assert!(!is_pubsub_delivery(&reply));
    }

    // -- Pool dispatch round-robin --------------------------------------------

    #[test]
    fn pool_dispatch_round_robin() {
        let mut workers = Vec::new();
        let mut _receivers = Vec::new();
        for _ in 0..3 {
            let (tx, rx) = mpsc::channel::<RedisMessage>(8);
            _receivers.push(rx);
            workers.push(WorkerHandle::new(tx, PendingCounter::new()));
        }
        let pool = RedisPool {
            inner: Arc::new(WorkerSet::new("test".into(), workers)),
        };

        for i in 0..9 {
            pool.dispatch(1, i as i64, vec![0], 1).unwrap();
        }

        // Each worker should have received 3 requests
        for w in pool.inner.workers() {
            assert_eq!(w.counter().load(), 3);
        }
    }

    #[test]
    fn pool_dispatch_closed_worker_returns_error() {
        let (tx, rx) = mpsc::channel::<RedisMessage>(1);
        drop(rx); // close the receiver
        let workers = vec![WorkerHandle::new(tx, PendingCounter::new())];
        let pool = RedisPool {
            inner: Arc::new(WorkerSet::new("dead".into(), workers)),
        };

        let result = pool.dispatch(1, 1, vec![0], 1);
        assert!(result.is_err());
    }

    #[test]
    fn pool_pending_sums_all_workers() {
        let mut workers = Vec::new();
        for i in 0..4 {
            let (tx, _rx) = mpsc::channel::<RedisMessage>(1);
            workers.push(WorkerHandle::new(tx, PendingCounter::with_value(i * 10)));
        }
        let pool = RedisPool {
            inner: Arc::new(WorkerSet::new("test".into(), workers)),
        };
        // 0 + 10 + 20 + 30 = 60
        assert_eq!(pool.inner.pending(), 60);
    }

    // -- find_crlf edge cases -------------------------------------------------

    #[test]
    fn find_crlf_at_very_start() {
        assert_eq!(find_crlf(b"\r\n", 0), Ok(0));
    }

    #[test]
    fn find_crlf_first_cr_not_followed_by_lf() {
        // First \r at pos 1, next byte is 'x', so not CRLF.
        // memchr only finds the first \r, so even though \r\n exists later, it fails.
        assert!(find_crlf(b"+\rxOK\r\n", 1).is_err());
    }

    #[test]
    fn find_crlf_at_boundary() {
        // \r is at end of buffer with no following \n
        assert!(find_crlf(b"+OK\r", 1).is_err());
    }

    // -- RESP encoding edge cases ---------------------------------------------

    #[test]
    fn encode_resp_strings_empty_args() {
        let mut buf = Vec::new();
        encode_resp_strings(&mut buf, &[]);
        assert_eq!(buf, b"*0\r\n");
    }

    #[test]
    fn encode_resp_strings_large_arg_count() {
        let args: Vec<&str> = (0..256).map(|_| "x").collect();
        let mut buf = Vec::new();
        encode_resp_strings(&mut buf, &args);
        assert!(buf.starts_with(b"*256\r\n"));
    }

    #[test]
    fn bulk_bytes_with_crlf_in_data() {
        let mut buf = Vec::new();
        write_bulk_bytes(&mut buf, b"a\r\nb");
        // Binary-safe: length=4, data contains \r\n
        assert_eq!(buf, b"$4\r\na\r\nb\r\n");
    }

    // -- RESP raw parsing edge cases ------------------------------------------

    #[test]
    fn resp_empty_bulk_string() {
        let raw = b"$0\r\n\r\n";
        let end = find_crlf(raw, 1).unwrap();
        let len = lexical_core::parse::<i64>(&raw[1..end]).unwrap() as usize;
        let data_start = end + 2;
        assert_eq!(len, 0);
        assert_eq!(&raw[data_start..data_start + len], b"");
    }

    #[test]
    fn resp_null_array() {
        let raw = b"*-1\r\n";
        let end = find_crlf(raw, 1).unwrap();
        let count = lexical_core::parse::<i64>(&raw[1..end]).unwrap();
        assert_eq!(count, -1);
    }

    #[test]
    fn resp_empty_array() {
        let raw = b"*0\r\n";
        let end = find_crlf(raw, 1).unwrap();
        let count = lexical_core::parse::<i64>(&raw[1..end]).unwrap();
        assert_eq!(count, 0);
    }

    #[test]
    fn resp_large_integer() {
        let raw = b":9223372036854775807\r\n"; // i64::MAX
        let end = find_crlf(raw, 1).unwrap();
        let val = lexical_core::parse::<i64>(&raw[1..end]).unwrap();
        assert_eq!(val, i64::MAX);
    }

    #[test]
    fn resp_large_bulk_string_length() {
        // Build a bulk string with 1024 bytes
        let payload = vec![b'x'; 1024];
        let mut raw = format!("${}\r\n", payload.len()).into_bytes();
        raw.extend_from_slice(&payload);
        raw.extend_from_slice(b"\r\n");
        let end = find_crlf(&raw, 1).unwrap();
        let len = lexical_core::parse::<i64>(&raw[1..end]).unwrap() as usize;
        assert_eq!(len, 1024);
        let data_start = end + 2;
        assert_eq!(&raw[data_start..data_start + len], payload.as_slice());
    }

    // -- ConnParams field coverage --------------------------------------------

    #[test]
    fn conn_params_with_auth_and_db() {
        let p = ConnParams {
            host: "redis.example.com".into(),
            port: 6380,
            username: "user".into(),
            password: "pass123".into(),
            db: 5,
            read_timeout_ms: 5_000,
        };
        assert_eq!(p.host, "redis.example.com");
        assert_eq!(p.port, 6380);
        assert_eq!(p.username, "user");
        assert_eq!(p.password, "pass123");
        assert_eq!(p.db, 5);
        assert_eq!(p.read_timeout_ms, 5_000);
    }
}
