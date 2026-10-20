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
use bytes::Bytes;
use dashmap::DashMap;
use lazy_static::lazy_static;
use moon_base::laux::{LuaStack, LuaState};
use moon_base::{
    cstr, ffi, laux,
    laux::{LuaTable, LuaType},
    lreg, lreg_null, lreg_try, luaL_newlib, push_lua_table,
};
use moon_runtime::actor::LuaActor;
use moon_runtime::context::{self, ActorId, CONTEXT};
use std::{collections::VecDeque, ffi::c_int, pin::Pin, sync::Arc, time::Duration};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::TcpStream;
use tokio::sync::mpsc;
use tokio::time::timeout;

lazy_static! {
    static ref REDIS_CONNECTIONS: DashMap<String, RedisPool> = DashMap::new();
}

// ---------------------------------------------------------------------------
// Connection parameters (parsed from a `redis://` URL, incl. pool query params)
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

/// Everything `connect` needs, parsed from a single connection URL. The
/// wire-protocol params come from the URL authority/path; the pool settings
/// come from the `?key=value` query string.
struct ConnectConfig {
    params: ConnParams,
    /// Pool name used for registry lookup (`find_connection`).
    name: String,
    connect_timeout_ms: u64,
    pool_size: usize,
    queue_capacity: usize,
}

impl ConnectConfig {
    /// Parse `redis://username:password@host:port/db?param=value&...`.
    ///
    /// The `/db` segment is optional and defaults to `0` when absent.
    ///
    /// Query params (all optional):
    ///   * `name` — pool name for `find_connection` (default "default")
    ///   * `connect_timeout` — connect timeout in ms (default 5000)
    ///   * `pool_size`/`max_connections` — pool size (default 1)
    ///   * `read_timeout` — read timeout in ms (default 10000)
    ///   * `queue_capacity` — per-worker request queue capacity (default 1024)
    fn parse(url_str: &str) -> Result<Self, String> {
        let url = url::Url::parse(url_str).map_err(|e| format!("invalid connection url: {}", e))?;
        match url.scheme() {
            "redis" => {}
            other => return Err(format!("unsupported scheme '{}', expected redis", other)),
        }

        let host = url.host_str().unwrap_or("127.0.0.1").to_string();
        let port = url.port().unwrap_or(6379);
        let username = percent_decode(url.username());
        let password = url.password().map(percent_decode).unwrap_or_default();

        let db_path = url.path().trim_start_matches('/');
        let db: u16 = if db_path.is_empty() {
            0
        } else {
            db_path
                .parse()
                .map_err(|_| format!("invalid db number: {:?}", db_path))?
        };

        let mut name = "default".to_string();
        let mut connect_timeout_ms: u64 = 5000;
        let mut pool_size: usize = 1;
        let mut read_timeout_ms: u64 = crate::LIMITS.db_read_timeout_ms;
        let mut queue_capacity: usize = crate::LIMITS.request_queue_capacity;

        for (k, v) in url.query_pairs() {
            match k.as_ref() {
                "name" => name = v.into_owned(),
                "connect_timeout" => connect_timeout_ms = parse_num("connect_timeout", &v)?,
                "pool_size" | "max_connections" => pool_size = parse_num("pool_size", &v)?,
                "read_timeout" => read_timeout_ms = parse_num("read_timeout", &v)?,
                "queue_capacity" => queue_capacity = parse_num("queue_capacity", &v)?,
                other => return Err(format!("unknown connection parameter: '{}'", other)),
            }
        }
        if name.is_empty() {
            name = "default".to_string();
        }

        Ok(ConnectConfig {
            params: ConnParams {
                host,
                port,
                username,
                password,
                db,
                read_timeout_ms,
            },
            name,
            connect_timeout_ms,
            pool_size: pool_size.max(1),
            queue_capacity: queue_capacity.max(1),
        })
    }
}

fn parse_num<T>(key: &str, value: &str) -> Result<T, String>
where
    T: std::str::FromStr,
{
    value
        .parse::<T>()
        .map_err(|_| format!("invalid value for '{}': {:?}", key, value))
}

/// Minimal percent-decoding (the `url` crate keeps userinfo percent-encoded).
fn percent_decode(s: &str) -> String {
    let bytes = s.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'%' && i + 2 < bytes.len() {
            let hi = (bytes[i + 1] as char).to_digit(16);
            let lo = (bytes[i + 2] as char).to_digit(16);
            if let (Some(hi), Some(lo)) = (hi, lo) {
                out.push((hi * 16 + lo) as u8);
                i += 3;
                continue;
            }
        }
        out.push(bytes[i]);
        i += 1;
    }
    String::from_utf8_lossy(&out).into_owned()
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
    Bulk(Option<Bytes>),
    Array(Option<Vec<RedisReply>>),
}

enum RedisResponse {
    /// Successful connect; carries the registered pool name for `find_connection`.
    Connect(String),
    /// Connection-string parse failure.
    Config(String),
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

/// Cap on pub/sub messages buffered while no coroutine is waiting on this
/// watch. Messages are never dropped merely because a waiter was not pending
/// at the instant they arrived (see `watch_loop`), but a subscriber that stops
/// calling `message()` must not grow memory without bound either.
const MAX_BUFFERED_WATCH_MESSAGES: usize = 1024;

async fn watch_loop(mut conn: RedisConn, mut rx: mpsc::UnboundedReceiver<WatchOp>) {
    let mut pending_wait: Option<WatchMessageWait> = None;
    // Deliveries that arrived while no waiter was pending, kept in arrival
    // order for the next `WaitMessage` op.
    let mut buffered: VecDeque<RedisReply> = VecDeque::with_capacity(64);

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
                        // A delivery may already be buffered (one that arrived
                        // between subscribe and this wait): serve it immediately
                        // rather than leaving the new waiter blocked while a
                        // message sits queued.
                        if let Some(reply) = buffered.pop_front() {
                            let _ = CONTEXT.send_value(
                                context::PTYPE_REDIS,
                                owner,
                                session,
                                RedisResponse::WatchMessage(reply),
                            );
                        } else if pending_wait.is_some() {
                            // Only one waiter is supported per watch connection. A
                            // second concurrent wait must not silently replace the
                            // first (that would hang the first coroutine forever):
                            // reject the new request and keep the existing waiter.
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
                        } else {
                            // No waiter right now — buffer instead of dropping.
                            // (Bounded: a subscriber that never reads again is
                            // failing to keep up; the oldest delivery is evicted
                            // rather than growing memory forever.)
                            if buffered.len() == MAX_BUFFERED_WATCH_MESSAGES {
                                buffered.pop_front();
                            }
                            buffered.push_back(r);
                        }
                    }
                    // A bare error reply on this connection can only be a
                    // rejected SUBSCRIBE/PSUBSCRIBE/UNSUBSCRIBE (e.g. NOAUTH or a
                    // channel ACL denial). `send_command` above only detects
                    // transport errors, so without this arm the rejection was
                    // silently discarded: subscribe() had returned `true` and the
                    // next message() wait hung forever. Surface it to any pending
                    // waiter; if nobody is waiting, the failure is unreportable
                    // through the fire-and-forget subscribe() API, so shut the
                    // connection down so the next operation fails loudly
                    // ("watch closed") instead of the Lua side believing it is
                    // subscribed when it is not.
                    Ok(RedisReply::Error(e)) | Err(e) => {
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
                    Ok(_) => {}
                }
            }
        }
    }

    // The loop is exiting (explicit Close, all senders dropped, a rejected
    // subscribe, or a read error already handled above). If a waiter is still
    // pending, wake it with an error instead of leaving the Lua coroutine
    // blocked on `moon.wait` forever.
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
const MAX_RESP_DEPTH: usize = 128;
const MAX_RAW_REPLY_LEN: usize = crate::LIMITS.max_network_read_bytes;
// Amortize socket reads across pipeline replies and large Stream responses.
const READ_BUFFER_CAPACITY: usize = 64 * 1024;

#[derive(Clone, Copy)]
enum RawReadState {
    Header,
    Bulk(usize),
    BulkCrlf(usize),
}

/// Locate one RESP2 reply without constructing a value tree or allocating an
/// async future per node. Progress survives buffer refills, including split
/// headers and bulk terminators. Only fragmented headers need a scratch copy.
struct RawReplyScanner {
    state: RawReadState,
    remaining: usize,
    parents: Vec<usize>,
    line: Vec<u8>,
    bytes: usize,
}

enum RawHeader {
    Scalar,
    Bulk(usize),
    Array(usize),
}

impl RawReplyScanner {
    fn new() -> Self {
        Self {
            state: RawReadState::Header,
            remaining: 1,
            parents: Vec::new(),
            line: Vec::new(),
            bytes: 0,
        }
    }

    fn reset(&mut self) {
        self.state = RawReadState::Header;
        self.remaining = 1;
        self.parents.clear();
        self.line.clear();
        self.bytes = 0;
    }

    fn complete(&self) -> bool {
        self.remaining == 0
    }

    fn account(&mut self, count: usize) -> Result<(), String> {
        if count > MAX_RAW_REPLY_LEN - self.bytes {
            return Err("RESP reply too large".to_string());
        }
        self.bytes += count;
        Ok(())
    }

    fn finish_value(&mut self) {
        self.remaining -= 1;
        while self.remaining == 0 {
            match self.parents.pop() {
                Some(remaining) => self.remaining = remaining,
                None => break,
            }
        }
        self.state = RawReadState::Header;
    }

    fn header(line: &[u8]) -> Result<RawHeader, String> {
        if line.len() < 3 || !line.ends_with(b"\r\n") {
            return Err("invalid RESP line terminator".to_string());
        }
        match line[0] {
            b'+' | b'-' | b':' => Ok(RawHeader::Scalar),
            b'$' | b'*' => {
                let num = lexical_core::parse::<i64>(&line[1..line.len() - 2])
                    .map_err(|e| format!("invalid RESP length: {}", e))?;
                if num == -1 {
                    return Ok(RawHeader::Scalar);
                }
                let count =
                    usize::try_from(num).map_err(|_| format!("invalid RESP length: {}", num))?;
                if line[0] == b'$' {
                    if count > MAX_MESSAGE_LEN {
                        return Err(format!("bulk string too large: {} bytes", count));
                    }
                    Ok(RawHeader::Bulk(count))
                } else {
                    if count > MAX_ARRAY_COUNT {
                        return Err(format!(
                            "array too large: {} elements (max {})",
                            count, MAX_ARRAY_COUNT
                        ));
                    }
                    Ok(RawHeader::Array(count))
                }
            }
            c => Err(format!("unknown RESP type: {}", c as char)),
        }
    }

    /// SET/GET/INCR/XADD normally return a complete scalar in one socket buffer.
    /// Avoid walking the incremental state machine (especially the bulk body
    /// and its two terminator bytes) for that common case.
    fn buffered_scalar_len(input: &[u8]) -> Result<Option<usize>, String> {
        if !matches!(input.first(), Some(b'+' | b'-' | b':' | b'$')) {
            return Ok(None);
        }
        let Some(end) = memchr::memchr(b'\n', input) else {
            return Ok(None);
        };
        let header_len = end + 1;
        if header_len > MAX_MESSAGE_LEN {
            return Err("RESP header too large".to_string());
        }
        match Self::header(&input[..header_len])? {
            RawHeader::Scalar => Ok(Some(header_len)),
            RawHeader::Bulk(len) => {
                let data_end = header_len + len;
                let reply_end = data_end + 2;
                if input.len() < reply_end {
                    return Ok(None);
                }
                if &input[data_end..reply_end] != b"\r\n" {
                    return Err("invalid RESP bulk terminator".to_string());
                }
                Ok(Some(reply_end))
            }
            RawHeader::Array(_) => unreachable!(),
        }
    }

    /// Return bytes consumed from this slice, stopping exactly at the end of
    /// the reply so a following pipeline response stays in the socket buffer.
    fn consume(&mut self, input: &[u8]) -> Result<usize, String> {
        if self.bytes == 0
            && let Some(len) = Self::buffered_scalar_len(input)?
        {
            self.account(len)?;
            self.remaining = 0;
            return Ok(len);
        }
        let mut pos = 0;
        while pos < input.len() && !self.complete() {
            match self.state {
                RawReadState::Header => {
                    let rest = &input[pos..];
                    let newline = memchr::memchr(b'\n', rest);
                    let len = newline.map_or(rest.len(), |end| end + 1);
                    self.account(len)?;
                    if self.line.len() + len > MAX_MESSAGE_LEN {
                        return Err("RESP header too large".to_string());
                    }
                    pos += len;
                    if newline.is_none() {
                        self.line.extend_from_slice(&rest[..len]);
                        break;
                    }
                    let header = if self.line.is_empty() {
                        Self::header(&rest[..len])?
                    } else {
                        self.line.extend_from_slice(&rest[..len]);
                        let header = Self::header(&self.line)?;
                        self.line.clear();
                        header
                    };
                    match header {
                        RawHeader::Scalar => self.finish_value(),
                        RawHeader::Bulk(0) => self.state = RawReadState::BulkCrlf(0),
                        RawHeader::Bulk(len) => self.state = RawReadState::Bulk(len),
                        RawHeader::Array(count) => {
                            if self.parents.len() == MAX_RESP_DEPTH {
                                return Err("RESP arrays nested too deeply".to_string());
                            }
                            if count == 0 {
                                self.finish_value();
                            } else {
                                self.parents.push(self.remaining - 1);
                                self.remaining = count;
                            }
                        }
                    }
                }
                RawReadState::Bulk(remaining) => {
                    let len = remaining.min(input.len() - pos);
                    self.account(len)?;
                    pos += len;
                    self.state = if len == remaining {
                        RawReadState::BulkCrlf(0)
                    } else {
                        RawReadState::Bulk(remaining - len)
                    };
                }
                RawReadState::BulkCrlf(seen) => {
                    if input[pos] != b"\r\n"[seen] {
                        return Err("invalid RESP bulk terminator".to_string());
                    }
                    self.account(1)?;
                    pos += 1;
                    if seen == 1 {
                        self.finish_value();
                    } else {
                        self.state = RawReadState::BulkCrlf(1);
                    }
                }
            }
        }
        Ok(pos)
    }
}

struct RedisConn {
    stream: BufReader<TcpStream>,
    read_timeout: Duration,
    read_timer: Pin<Box<tokio::time::Sleep>>,
    raw_scanner: RawReplyScanner,
    // Pub/sub selects between reading and control messages. Keep both the
    // partial frame and its deadline on the connection when a read is canceled.
    pending_reply: Vec<u8>,
    typed_read_active: bool,
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
            stream: BufReader::with_capacity(READ_BUFFER_CAPACITY, tcp),
            read_timeout: Duration::from_millis(params.read_timeout_ms),
            read_timer: Box::pin(tokio::time::sleep(Duration::from_millis(
                params.read_timeout_ms,
            ))),
            raw_scanner: RawReplyScanner::new(),
            pending_reply: Vec::new(),
            typed_read_active: false,
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
        self.send_raw(&buf).await
    }

    async fn send_raw(&mut self, data: &[u8]) -> Result<(), String> {
        self.stream
            .get_mut()
            .write_all(data)
            .await
            .map_err(|e| format!("write: {}", e))
    }

    async fn read_reply(&mut self) -> Result<RedisReply, String> {
        if !self.typed_read_active {
            self.raw_scanner.reset();
            self.pending_reply.clear();
            self.read_timer
                .as_mut()
                .reset(tokio::time::Instant::now() + self.read_timeout);
            self.typed_read_active = true;
        }
        loop {
            if Self::read_raw_buffer(
                &mut self.stream,
                &mut self.raw_scanner,
                &mut self.pending_reply,
            )? {
                // Simple replies contain no bulk slices, so keep this Vec for
                // the next read. Bulk replies move it into Bytes for sharing.
                let reply = if matches!(self.pending_reply.first(), Some(b'+' | b'-' | b':')) {
                    let parsed = find_crlf(&self.pending_reply, 1).and_then(|end| {
                        parse_simple_reply(self.pending_reply[0], &self.pending_reply[1..end])
                    });
                    self.pending_reply.clear();
                    parsed
                } else {
                    let raw = Bytes::from(std::mem::take(&mut self.pending_reply));
                    parse_owned_reply(&raw, &mut 0)
                };
                self.typed_read_active = false;
                return reply;
            }
            self.refill_raw_buffer().await?;
        }
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

    /// Scan and copy only the bytes belonging to this reply. The BufReader
    /// retains any subsequent replies, and the scanner retains partial-frame
    /// progress instead of reparsing the response on every socket read.
    fn read_raw_buffer(
        stream: &mut BufReader<TcpStream>,
        scanner: &mut RawReplyScanner,
        out: &mut Vec<u8>,
    ) -> Result<bool, String> {
        let buffered = stream.buffer();
        let used = scanner.consume(buffered)?;
        out.extend_from_slice(&buffered[..used]);
        stream.consume(used);
        Ok(scanner.complete())
    }

    async fn refill_raw_buffer(&mut self) -> Result<(), String> {
        tokio::select! {
            biased;
            _ = self.read_timer.as_mut() => Err("read timeout".to_string()),
            result = self.stream.fill_buf() => {
                if result.map_err(|e| format!("read: {}", e))?.is_empty() {
                    Err("unexpected EOF in RESP reply".to_string())
                } else {
                    Ok(())
                }
            }
        }
    }

    async fn read_raw_reply(&mut self, out: &mut Vec<u8>) -> Result<(), String> {
        self.raw_scanner.reset();
        // Pipeline replies already buffered need neither an async read nor a
        // timer reset. This is also the fast path for small nested RESP arrays.
        if Self::read_raw_buffer(&mut self.stream, &mut self.raw_scanner, out)? {
            return Ok(());
        }

        self.read_timer
            .as_mut()
            .reset(tokio::time::Instant::now() + self.read_timeout);
        loop {
            // One deadline for the entire reply, not a new timeout for each
            // fragment. Each subsequent pipeline reply gets its own deadline.
            self.refill_raw_buffer().await?;
            if Self::read_raw_buffer(&mut self.stream, &mut self.raw_scanner, out)? {
                return Ok(());
            }
        }
    }

    async fn read_raw_replies(&mut self, out: &mut Vec<u8>, count: usize) -> Result<(), String> {
        if count == 1 {
            return self.read_raw_reply(out).await;
        }
        self.raw_scanner.reset();
        let mut remaining = count;
        let mut deadline_active = false;
        while remaining != 0 {
            let buffered = self.stream.buffer();
            let mut used = 0;
            while used < buffered.len() && remaining != 0 {
                used += self.raw_scanner.consume(&buffered[used..])?;
                if !self.raw_scanner.complete() {
                    break;
                }
                remaining -= 1;
                self.raw_scanner.reset();
                deadline_active = false;
            }
            // Copy an entire run of replies once, rather than extending the
            // output and advancing BufReader separately for every small reply.
            out.extend_from_slice(&buffered[..used]);
            self.stream.consume(used);
            if remaining == 0 {
                break;
            }
            if !deadline_active {
                self.read_timer
                    .as_mut()
                    .reset(tokio::time::Instant::now() + self.read_timeout);
                deadline_active = true;
            }
            self.refill_raw_buffer().await?;
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
                })
                .await;
                break;
            }
        };
        let _pending = counter.finish_on_drop();
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
                let result: Result<RedisResponse, String> = async {
                    c.send_raw(&req.data).await?;
                    let mut raw = Vec::with_capacity(if count == 1 { 64 } else { count * 32 });
                    c.read_raw_replies(&mut raw, count).await?;
                    Ok(if count == 1 {
                        RedisResponse::Raw(raw)
                    } else {
                        RedisResponse::RawPipeline(raw, req.reply_count)
                    })
                }
                .await;
                let response = match result {
                    Ok(response) => response,
                    Err(e) => {
                        conn = None;
                        RedisResponse::Error(e)
                    }
                };
                let _ = CONTEXT.send_value(context::PTYPE_REDIS, req.owner, req.session, response);
                break;
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
fn write_bulk_arg(buf: &mut Vec<u8>, lua: &mut LuaStack<'_>, idx: i32) -> Result<(), String> {
    let value = lua.value(idx);
    match value.kind() {
        LuaType::Nil | LuaType::None => {
            buf.extend_from_slice(b"$-1\r\n");
        }
        LuaType::Integer => {
            let v = value.as_integer().unwrap_or_default();
            let mut tmp = [0u8; lexical_core::BUFFER_SIZE];
            let written = lexical_core::write(v, &mut tmp);
            write_bulk_bytes(buf, written);
        }
        LuaType::Number => {
            let v = value.as_number().unwrap_or_default();
            let mut tmp = [0u8; lexical_core::BUFFER_SIZE];
            let written = lexical_core::write(v, &mut tmp);
            write_bulk_bytes(buf, written);
        }
        LuaType::Boolean => {
            let v = value.as_bool().unwrap_or(false);
            write_bulk_bytes(buf, if v { b"1" } else { b"0" });
        }
        LuaType::String => {
            write_bulk_bytes(buf, value.as_bytes().unwrap_or_default());
        }
        LuaType::Table => {
            let options = crate::lua_json::JsonOptions::default();
            let mut json = Vec::with_capacity(64);
            crate::lua_json::encode_table(&mut json, lua, idx, 0, false, &options)
                .map_err(|e| format!("JSON encode failed: {}", e))?;
            write_bulk_bytes(buf, &json);
        }
        _ => {
            return Err(format!("unsupported type: {}", value.name()));
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

fn collect_string_args(lua: &LuaStack<'_>, start: i32) -> Result<Vec<String>, String> {
    let top = lua.top();
    let mut out = Vec::with_capacity((top - start + 1) as usize);
    for i in start..=top {
        out.push(
            lua.get::<String>(i)
                .map_err(|err| format!("redis.watch: {err}"))?,
        );
    }
    Ok(out)
}

/// `redis.connect(url)`
///
/// `url`: `redis://username:password@host:port/db?name=...&pool_size=...`
fn connect(lua: &mut LuaStack<'_>) -> Result<c_int, String> {
    let state = lua.state();
    let url = lua
        .get::<String>(1)
        .map_err(|err| format!("redis.connect: {err}"))?;

    let actor = LuaActor::from_lua_state(state);
    let owner = unsafe { (*actor).id };
    let session = unsafe { (*actor).next_session() };

    CONTEXT.io_runtime().spawn(async move {
        let ConnectConfig {
            params,
            name,
            connect_timeout_ms: timeout_ms,
            pool_size,
            queue_capacity,
        } = match ConnectConfig::parse(&url) {
            Ok(c) => c,
            Err(e) => {
                let _ = CONTEXT.send_value(
                    context::PTYPE_REDIS,
                    owner,
                    session,
                    RedisResponse::Config(e),
                );
                return;
            }
        };

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
        if let Some(old) = REDIS_CONNECTIONS.insert(name.clone(), pool) {
            log::warn!(
                "redis '{}' reconnected with the same name; shutting down the previous pool",
                old.inner.name()
            );
            old.inner.notify_shutdown(|| RedisMessage::Shutdown);
        }
        let _ = CONTEXT.send_value(
            context::PTYPE_REDIS,
            owner,
            session,
            RedisResponse::Connect(name),
        );
    });

    laux::lua_push(state, session);
    Ok(1)
}

fn find_connection(lua: &mut LuaStack<'_>) -> Result<c_int, String> {
    let state = lua.state();
    let pool = {
        let name = lua
            .value(1)
            .as_str()
            .ok_or_else(|| "redis.find_connection: UTF-8 string expected".to_string())?;
        REDIS_CONNECTIONS.get(name).map(|pair| pair.value().clone())
    };
    match pool {
        Some(pool) => {
            let methods = [
                lreg!("command", command),
                lreg_try!("pipeline", pipeline),
                lreg!("exec_command", exec_command),
                lreg_try!("exec_pipeline", exec_pipeline),
                lreg!("len", pool_len),
                lreg!("close", close),
                lreg_null!(),
            ];
            if laux::lua_newuserdata(state, pool, REDIS_POOL_META, methods.as_ref()).is_none() {
                laux::lua_pushnil(state);
            }
        }
        None => laux::lua_pushnil(state),
    }
    Ok(1)
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
fn command(lua: &mut LuaStack<'_>) -> c_int {
    command_impl(lua, false)
}
fn exec_command(lua: &mut LuaStack<'_>) -> c_int {
    command_impl(lua, true)
}

fn command_impl(lua: &mut LuaStack<'_>, forget: bool) -> c_int {
    let state = lua.state();
    let pool_ptr = lua
        .value(1)
        .as_userdata::<RedisPool>()
        .expect("invalid redis pool pointer");
    let pool = unsafe { pool_ptr.as_ref() };
    let top = laux::lua_top(state);
    let nargs = (top - 1) as usize;

    let mut data = Vec::with_capacity(64);
    write_resp_array_header(&mut data, nargs);

    for i in 2..=top {
        if let Err(e) = write_bulk_arg(&mut data, lua, i) {
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
fn pipeline(lua: &mut LuaStack<'_>) -> Result<c_int, String> {
    pipeline_impl(lua, false)
}
fn exec_pipeline(lua: &mut LuaStack<'_>) -> Result<c_int, String> {
    pipeline_impl(lua, true)
}

fn pipeline_impl(lua: &mut LuaStack<'_>, forget: bool) -> Result<c_int, String> {
    let state = lua.state();
    let pool_ptr = lua
        .value(1)
        .as_userdata::<RedisPool>()
        .expect("invalid redis pool pointer");
    let pool = unsafe { pool_ptr.as_ref() };
    let ops_idx = laux::lua_absindex(state, 2);
    laux::lua_checktype(state, ops_idx, ffi::LUA_TTABLE)
        .map_err(|err| format!("redis.pipeline: argument #2 {err}"))?;

    let n = unsafe { ffi::lua_rawlen(state.as_ptr(), ops_idx) } as usize;
    if n == 0 {
        push_lua_table!(state, "code" => "ENCODE", "message" => "pipeline: empty ops");
        return Ok(1);
    }
    if n > u32::MAX as usize {
        push_lua_table!(state, "code" => "ENCODE", "message" => format!("pipeline: too many commands ({})", n));
        return Ok(1);
    }

    let mut data = Vec::with_capacity(128);

    for i in 1..=n {
        unsafe { ffi::lua_rawgeti(state.as_ptr(), ops_idx, i as ffi::lua_Integer) };
        let cmd_idx = laux::lua_top(state);
        if laux::lua_type(state, cmd_idx) != laux::LuaType::Table {
            laux::lua_pop(state, 1);
            push_lua_table!(state, "code" => "ENCODE", "message" => format!("pipeline[{}]: expected table", i));
            return Ok(1);
        }

        let cmd_len = unsafe { ffi::lua_rawlen(state.as_ptr(), cmd_idx) } as usize;

        write_resp_array_header(&mut data, cmd_len);

        for j in 1..=cmd_len {
            unsafe { ffi::lua_rawgeti(state.as_ptr(), cmd_idx, j as ffi::lua_Integer) };
            let result = write_bulk_arg(&mut data, lua, cmd_idx + 1);
            laux::lua_pop(state, 1);
            if let Err(e) = result {
                laux::lua_pop(state, 1); // pop the command table
                push_lua_table!(state, "code" => "ENCODE", "message" => format!("pipeline[{}][{}]: {}", i, j, e));
                return Ok(1);
            }
        }

        laux::lua_pop(state, 1);
    }

    Ok(if forget {
        dispatch_forget(state, pool, data, n as u32)
    } else {
        dispatch_async(state, pool, data, n as u32)
    })
}

fn pool_len(lua: &mut LuaStack<'_>) -> c_int {
    let state = lua.state();
    let pool_ptr = lua
        .value(1)
        .as_userdata::<RedisPool>()
        .expect("invalid redis pool pointer");
    let pool = unsafe { pool_ptr.as_ref() };
    let table = LuaTable::new(state, pool.inner.workers().len(), 0);
    for w in pool.inner.workers() {
        table.push(w.counter().load());
    }
    1
}

fn close(lua: &mut LuaStack<'_>) -> c_int {
    let state = lua.state();
    let pool_ptr = lua
        .value(1)
        .as_userdata::<RedisPool>()
        .expect("invalid redis pool pointer");
    let pool = unsafe { pool_ptr.as_ref() };
    // Only remove our own entry: if a `connect()` with the same name has already
    // replaced this pool, closing through this (now stale) handle must not evict
    // the newer pool. Identify ourselves by the `inner` Arc.
    REDIS_CONNECTIONS.remove_if(pool.inner.name(), |_, v| Arc::ptr_eq(&v.inner, &pool.inner));
    // Signal every worker to finish any queued requests and then exit, so its
    // task ends and the TCP connection is dropped. Removing the registry entry
    // alone is not enough because the Lua handle still holds a pool `Arc`.
    pool.inner.notify_shutdown(|| RedisMessage::Shutdown);
    laux::lua_push(state, true);
    1
}

fn stats(lua: &mut LuaStack<'_>) -> c_int {
    let state = lua.state();
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

/// `redis.watch(url)` — dedicated pub/sub connection. Accepts the same
/// `redis://...` URL as `connect` (the pool-only params like `name`/`pool_size`
/// are ignored here).
fn watch_connect(lua: &mut LuaStack<'_>) -> Result<c_int, String> {
    let state = lua.state();
    let url = lua
        .get::<String>(1)
        .map_err(|err| format!("redis.watch: {err}"))?;

    let actor = LuaActor::from_lua_state(state);
    let owner = unsafe { (*actor).id };
    let session = unsafe { (*actor).next_session() };

    CONTEXT.io_runtime().spawn(async move {
        let (params, timeout_ms) = match ConnectConfig::parse(&url) {
            Ok(c) => (c.params, c.connect_timeout_ms),
            Err(e) => {
                let _ = CONTEXT.send_value(
                    context::PTYPE_REDIS,
                    owner,
                    session,
                    RedisResponse::Config(e),
                );
                return;
            }
        };

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
    Ok(1)
}

fn watch_subscription(
    lua: &mut LuaStack<'_>,
    make_op: fn(Vec<String>) -> WatchOp,
) -> Result<c_int, String> {
    let state = lua.state();
    let watch_ptr = lua
        .value(1)
        .as_userdata::<RedisWatch>()
        .expect("invalid redis watch pointer");
    let watch = unsafe { watch_ptr.as_ref() };
    let args = collect_string_args(lua, 2)?;
    match watch.send_op(make_op(args)) {
        Ok(()) => laux::lua_push(state, true),
        Err(err) => push_lua_table!(state, "code" => "SOCKET", "message" => err.as_str()),
    }
    Ok(1)
}

fn watch_subscribe(lua: &mut LuaStack<'_>) -> Result<c_int, String> {
    watch_subscription(lua, WatchOp::Subscribe)
}

fn watch_psubscribe(lua: &mut LuaStack<'_>) -> Result<c_int, String> {
    watch_subscription(lua, WatchOp::PSubscribe)
}

fn watch_unsubscribe(lua: &mut LuaStack<'_>) -> Result<c_int, String> {
    watch_subscription(lua, WatchOp::Unsubscribe)
}

fn watch_punsubscribe(lua: &mut LuaStack<'_>) -> Result<c_int, String> {
    watch_subscription(lua, WatchOp::PUnsubscribe)
}

fn watch_message(lua: &mut LuaStack<'_>) -> c_int {
    let state = lua.state();
    let watch_ptr = lua
        .value(1)
        .as_userdata::<RedisWatch>()
        .expect("invalid redis watch pointer");
    let watch = unsafe { watch_ptr.as_ref() };
    let actor = LuaActor::from_lua_state(state);
    let owner = unsafe { (*actor).id };
    let session = unsafe { (*actor).next_session() };
    match watch.send_op(WatchOp::WaitMessage { owner, session }) {
        Ok(()) => laux::lua_push(state, session),
        Err(err) => push_lua_table!(state, "code" => "SOCKET", "message" => err.as_str()),
    }
    1
}

fn watch_close(lua: &mut LuaStack<'_>) -> c_int {
    let state = lua.state();
    let watch_ptr = lua
        .value(1)
        .as_userdata::<RedisWatch>()
        .expect("invalid redis watch pointer");
    let watch = unsafe { watch_ptr.as_ref() };
    let _ = watch.send_op(WatchOp::Close);
    laux::lua_push(state, true);
    1
}

fn push_watch_userdata(state: LuaState, watch: RedisWatch) {
    let methods = [
        lreg_try!("subscribe", watch_subscribe),
        lreg_try!("psubscribe", watch_psubscribe),
        lreg_try!("unsubscribe", watch_unsubscribe),
        lreg_try!("punsubscribe", watch_punsubscribe),
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
            laux::lua_push(state, b.as_ref());
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
// Response decoding (owned values for workers, Lua values on the actor thread)
// ---------------------------------------------------------------------------

// Only called on a complete frame validated by RawReplyScanner, including its
// length and nesting bounds. Pub/sub and fire-and-forget need owned values;
// bulk strings share the complete frame allocation until delivery. Awaited
// commands decode the same wire representation directly to Lua.
fn parse_simple_reply(tag: u8, data: &[u8]) -> Result<RedisReply, String> {
    match tag {
        b'+' => Ok(RedisReply::Status(
            String::from_utf8_lossy(data).into_owned(),
        )),
        b'-' => Ok(RedisReply::Error(
            String::from_utf8_lossy(data).into_owned(),
        )),
        b':' => Ok(RedisReply::Integer(
            lexical_core::parse::<i64>(data).map_err(|e| format!("invalid integer: {}", e))?,
        )),
        c => Err(format!("unknown RESP type: {}", c as char)),
    }
}

fn parse_owned_reply(raw: &Bytes, pos: &mut usize) -> Result<RedisReply, String> {
    let tag = *raw.get(*pos).ok_or("unexpected end of RESP data")?;
    let end = find_crlf(raw, *pos + 1)?;
    let data = &raw[*pos + 1..end];
    *pos = end + 2;
    match tag {
        b'+' | b'-' | b':' => parse_simple_reply(tag, data),
        b'$' => {
            let len = lexical_core::parse::<i64>(data)
                .map_err(|e| format!("invalid bulk length: {}", e))?;
            if len < 0 {
                return Ok(RedisReply::Bulk(None));
            }
            let end = *pos + len as usize;
            raw.get(*pos..end).ok_or("truncated bulk string")?;
            let value = raw.slice(*pos..end);
            *pos = end + 2;
            Ok(RedisReply::Bulk(Some(value)))
        }
        b'*' => {
            let count = lexical_core::parse::<i64>(data)
                .map_err(|e| format!("invalid array count: {}", e))?;
            if count < 0 {
                return Ok(RedisReply::Array(None));
            }
            let mut items = Vec::with_capacity(count as usize);
            for _ in 0..count {
                items.push(parse_owned_reply(raw, pos)?);
            }
            Ok(RedisReply::Array(Some(items)))
        }
        c => Err(format!("unknown RESP type: {}", c as char)),
    }
}

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
            laux::lua_checkstack(state, 4, std::ptr::null())?;
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
        RedisResponse::Connect(name) => {
            // No `.code` => success; `.name` lets redis.lua look the pool up.
            push_lua_table!(state, "name" => name.as_str());
            1
        }
        RedisResponse::Config(msg) => {
            push_lua_table!(state, "code" => "CONFIG", "message" => msg.as_str());
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
        Err(e) => crate::lua_push_error_tuple(state, &e),
    }
}

pub extern "C-unwind" fn luaopen_redis(state: LuaState) -> c_int {
    let l = [
        lreg_try!("connect", connect),
        lreg_try!("find_connection", find_connection),
        lreg_try!("watch", watch_connect),
        lreg!("stats", stats),
        lreg_null!(),
    ];
    luaL_newlib!(state, l);
    1
}

#[cfg(test)]
#[path = "../../tests/unit/modules/lua_redis.rs"]
mod tests;
