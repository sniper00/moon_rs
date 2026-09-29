//! Native PostgreSQL driver exposed to Lua as `pg.core`.
//!
//! This is a hand-written port of the PostgreSQL v3 wire protocol (previously
//! implemented in pure Lua at `lualib/moon/db/pg.lua` on top of `moon.socket`).
//! It deliberately depends on **no** existing Postgres client crate: the goal is
//! full control over the bytes on the wire and minimal data copies.
//!
//! Design (mirrors `lua_sqlx.rs`):
//!  - A global registry of named connection pools (`PG_CONNECTIONS`).
//!  - `connect` validates the URL + one connection on the io runtime, then
//!    spawns `max_connections` worker tasks, each owning one `TcpStream` and
//!    reconnecting on socket errors.
//!  - Requests carry a **pre-built wire buffer** (encoded directly from Lua
//!    values on the calling thread, like the old `json.pq_query`) which is
//!    *moved* to a worker; the worker just writes it and reads the reply.
//!  - Responses keep **raw message bytes**; `decode` parses them straight into
//!    Lua tables on the actor thread (single copy: wire bytes -> Lua values).
//!  - Async delivery via `PTYPE_PG` + `moon.wait(session)`.

use crate::lua_json::{JsonOptions, encode_table};
use crate::request_pool::{
    PendingCounter, QueuedRequest, WorkerHandle, WorkerSet, drain_queued_requests,
};
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
use std::{borrow::Cow, ffi::c_int, ops::Range, pin::Pin, sync::Arc, time::Duration};
use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::net::TcpStream;
use tokio::sync::mpsc;
use tokio::time::timeout;

lazy_static! {
    static ref PG_CONNECTIONS: DashMap<String, PgPool> = DashMap::new();
}

// ---------------------------------------------------------------------------
// Connection parameters (parsed from a sqlx-style URL, incl. pool query params)
// ---------------------------------------------------------------------------

#[derive(Clone)]
struct ConnParams {
    host: String,
    port: u16,
    user: String,
    password: String,
    database: String,
    application_name: String,
}

/// Everything `connect` needs, parsed from a single connection URL. The
/// wire-protocol params come from the URL authority/path; the pool settings
/// come from the `?key=value` query string.
struct ConnectConfig {
    params: ConnParams,
    /// Pool name used for registry lookup (`find_connection`).
    name: String,
    connect_timeout_ms: u64,
    max_connections: usize,
    read_timeout_ms: u64,
    queue_capacity: usize,
}

impl ConnectConfig {
    /// Parse `postgresql://user:password@host:port/database?param=value&...`.
    ///
    /// Query params (all optional except `name`):
    ///   * `name` (**required**) — pool name for `find_connection`
    ///   * `application_name` (default "moon")
    ///   * `connect_timeout` — connect timeout in ms (default 5000)
    ///   * `max_connections`/`pool_size` — pool size (default 5)
    ///   * `read_timeout` — read timeout in ms (default 10000)
    ///   * `queue_capacity` — per-worker request queue capacity (default 1024)
    fn parse(database_url: &str) -> Result<Self, String> {
        let url =
            url::Url::parse(database_url).map_err(|e| format!("invalid connection url: {}", e))?;
        match url.scheme() {
            "postgres" | "postgresql" => {}
            other => return Err(format!("unsupported scheme '{}', expected postgres", other)),
        }

        let host = url.host_str().unwrap_or("localhost").to_string();
        let port = url.port().unwrap_or(5432);

        let user = percent_decode(url.username());
        if user.is_empty() {
            return Err("missing user in connection url".to_string());
        }
        let password = url.password().map(percent_decode).unwrap_or_default();

        let database = url.path().trim_start_matches('/').to_string();
        if database.is_empty() {
            return Err("missing database in connection url".to_string());
        }

        let mut application_name: Option<String> = None;
        let mut name: Option<String> = None;
        let mut connect_timeout_ms: u64 = 5000;
        let mut max_connections: usize = crate::LIMITS.db_pool_size as usize;
        let mut read_timeout_ms: u64 = crate::LIMITS.db_read_timeout_ms;
        let mut queue_capacity: usize = crate::LIMITS.request_queue_capacity;

        for (k, v) in url.query_pairs() {
            match k.as_ref() {
                "application_name" => application_name = Some(v.into_owned()),
                "name" => name = Some(v.into_owned()),
                "connect_timeout" => connect_timeout_ms = parse_num("connect_timeout", &v)?,
                "max_connections" | "pool_size" => {
                    max_connections = parse_num("max_connections", &v)?
                }
                "read_timeout" => read_timeout_ms = parse_num("read_timeout", &v)?,
                "queue_capacity" => queue_capacity = parse_num("queue_capacity", &v)?,
                other => return Err(format!("unknown connection parameter: '{}'", other)),
            }
        }

        let name = name
            .filter(|s| !s.is_empty())
            .ok_or("missing 'name' query parameter in connection url")?;

        Ok(ConnectConfig {
            params: ConnParams {
                host,
                port,
                user,
                password,
                database,
                application_name: application_name.unwrap_or_else(|| "moon".to_string()),
            },
            name,
            connect_timeout_ms,
            max_connections: max_connections.max(1),
            read_timeout_ms,
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

/// A message delivered to a worker: either a query request or a graceful
/// shutdown signal (sent by `close()` so the worker exits and drops its
/// connection even while the Lua-side pool handle keeps the pool `Arc` alive).
enum PgMessage {
    Request(PgRequest),
    Shutdown,
}

#[derive(Clone)]
struct PgPool {
    inner: Arc<WorkerSet<PgMessage>>,
}

impl PgPool {
    fn dispatch(&self, owner: ActorId, session: i64, data: Vec<u8>) -> Result<(), String> {
        self.inner.dispatch(PgMessage::Request(PgRequest {
            owner,
            session,
            data,
        }))
    }
}

struct PgRequest {
    owner: ActorId,
    session: i64,
    data: Vec<u8>,
}

impl QueuedRequest for PgMessage {
    fn owner_session(&self) -> Option<(ActorId, i64)> {
        match self {
            PgMessage::Request(req) => Some((req.owner, req.session)),
            PgMessage::Shutdown => None,
        }
    }
}

// ---------------------------------------------------------------------------
// Responses
// ---------------------------------------------------------------------------

struct Notification {
    pid: i32,
    channel: String,
    payload: String,
}

struct DbError {
    /// (field type byte, value), only mapped fields are kept.
    severity: Option<String>,
    code: Option<String>,
    message: Option<String>,
    position: Option<String>,
    detail: Option<String>,
    schema: Option<String>,
    table: Option<String>,
    constraint: Option<String>,
}

/// One statement's raw result, accumulated until its CommandComplete.
#[derive(Default)]
struct Statement {
    row_desc: Option<Vec<u8>>,
    data_rows: DataRows,
    command_tag: Option<Vec<u8>>,
}

/// Length-prefixed row bodies in one allocation, moved to the actor with the
/// result. Fragmented rows are appended directly, without a temporary body.
#[derive(Default)]
struct DataRows {
    bytes: Vec<u8>,
    count: usize,
}

impl DataRows {
    fn begin(&mut self, len: usize) -> &mut Vec<u8> {
        self.bytes.reserve(4 + len);
        self.bytes.extend_from_slice(&(len as u32).to_be_bytes());
        self.count += 1;
        &mut self.bytes
    }

    fn iter(&self) -> impl Iterator<Item = &[u8]> {
        let mut rest = self.bytes.as_slice();
        std::iter::from_fn(move || {
            if rest.is_empty() {
                return None;
            }
            // These lengths are written by begin(), after validating the wire
            // header. A failed/incomplete read never publishes this result.
            let len = u32::from_be_bytes(rest[..4].try_into().unwrap()) as usize;
            let row = &rest[4..4 + len];
            rest = &rest[4 + len..];
            Some(row)
        })
    }
}

struct QueryResult {
    statements: Vec<Statement>,
    notifications: Vec<Notification>,
    // Boxed: DbError is ~200 bytes of Option<String> fields and is almost
    // always None, so boxing keeps QueryResult / PgResponse small.
    error: Option<Box<DbError>>,
    /// ReadyForQuery transaction status: b'I' idle, b'T' in-txn, b'E' failed-txn.
    txn_status: u8,
}

struct QueryCollector {
    result: QueryResult,
    current: Statement,
    capture: bool,
    total_rows: usize,
    row_cap: usize,
    rows_exceeded: bool,
}

impl QueryCollector {
    fn new(capture: bool, row_cap: usize) -> Self {
        Self {
            result: QueryResult {
                statements: Vec::new(),
                notifications: Vec::new(),
                error: None,
                txn_status: b'I',
            },
            current: Statement::default(),
            capture,
            total_rows: 0,
            row_cap,
            rows_exceeded: false,
        }
    }

    fn row_destination(&mut self, len: usize) -> Option<&mut Vec<u8>> {
        if self.rows_exceeded {
            return None;
        }
        self.total_rows += 1;
        if self.total_rows > self.row_cap {
            self.rows_exceeded = true;
            self.current.data_rows = DataRows::default();
            return None;
        }
        self.capture.then(|| self.current.data_rows.begin(len))
    }

    /// Whether a non-row body needs to be retained long enough to inspect it.
    fn needs_body(&self, kind: u8) -> bool {
        matches!(kind, b'E' | b'Z') || (self.capture && matches!(kind, b'T' | b'C' | b'A'))
    }

    /// Returns true only at the query cycle boundary, including error replies.
    fn accept(&mut self, kind: u8, body: &[u8]) -> Result<bool, String> {
        match kind {
            b'D' => {
                if let Some(out) = self.row_destination(body.len()) {
                    out.extend_from_slice(body);
                }
            }
            b'T' if self.capture => self.current.row_desc = Some(body.to_vec()),
            b'C' if self.capture => {
                self.current.command_tag = Some(body.to_vec());
                self.result
                    .statements
                    .push(std::mem::take(&mut self.current));
            }
            b'A' if self.capture => {
                if let Some(n) = parse_notification(body) {
                    self.result.notifications.push(n);
                }
            }
            b'E' => self.result.error = Some(Box::new(parse_error(body))),
            b'Z' => {
                if !matches!(body, [b'I' | b'T' | b'E']) {
                    return Err("invalid ReadyForQuery message".to_string());
                }
                self.result.txn_status = body[0];
                return Ok(true);
            }
            _ => {}
        }
        Ok(false)
    }

    fn finish(mut self) -> QueryResult {
        if self.rows_exceeded && self.result.error.is_none() {
            // Keep this a query error: session-zero transport failures are
            // retried by the worker. Always drain to ReadyForQuery first.
            self.result.error = Some(Box::new(DbError {
                severity: None,
                code: Some("54000".to_string()),
                message: Some(format!(
                    "query returned more than {} rows; use a streaming/paginated query for large result sets",
                    self.row_cap
                )),
                position: None,
                detail: None,
                schema: None,
                table: None,
                constraint: None,
            }));
        }
        self.result
    }
}

enum PgResponse {
    /// Successful connect; carries the registered pool name for `find_connection`.
    Connect(String),
    /// Configuration / connection-string parse failure.
    Config(String),
    /// Socket / IO failure.
    Socket(String),
    Result(QueryResult),
}

// ---------------------------------------------------------------------------
// Wire protocol: connection + auth
// ---------------------------------------------------------------------------

struct PgConn {
    stream: BufReader<TcpStream>,
    read_timeout: Duration,
    read_timer: Pin<Box<tokio::time::Sleep>>,
}

const AUTH_OK: i32 = 0;
const AUTH_CLEARTEXT: i32 = 3;
const AUTH_MD5: i32 = 5;
const AUTH_SASL: i32 = 10;
const AUTH_SASL_CONTINUE: i32 = 11;
const AUTH_SASL_FINAL: i32 = 12;

impl PgConn {
    async fn connect(
        params: &ConnParams,
        timeout_ms: u64,
        read_timeout_ms: u64,
    ) -> Result<Self, String> {
        let fut = Self::connect_inner(params, read_timeout_ms);
        match timeout(Duration::from_millis(timeout_ms), fut).await {
            Ok(res) => res,
            Err(_) => Err(format!(
                "connect timeout after {}ms to {}:{}",
                timeout_ms, params.host, params.port
            )),
        }
    }

    async fn connect_inner(params: &ConnParams, read_timeout_ms: u64) -> Result<Self, String> {
        let stream = TcpStream::connect((params.host.as_str(), params.port))
            .await
            .map_err(|e| format!("tcp connect failed: {}", e))?;
        let _ = stream.set_nodelay(true);
        let sock = socket2::SockRef::from(&stream);
        let ka = socket2::TcpKeepalive::new()
            .with_time(Duration::from_secs(60))
            .with_interval(Duration::from_secs(15));
        let _ = sock.set_tcp_keepalive(&ka);
        let read_timeout = Duration::from_millis(read_timeout_ms);
        let mut conn = PgConn {
            stream: BufReader::with_capacity(16 * 1024, stream),
            read_timeout,
            read_timer: Box::pin(tokio::time::sleep(read_timeout)),
        };
        conn.startup(params).await?;
        conn.authenticate(params).await?;
        conn.wait_until_ready().await?;
        Ok(conn)
    }

    async fn startup(&mut self, params: &ConnParams) -> Result<(), String> {
        let mut body: Vec<u8> = Vec::with_capacity(128);
        body.extend_from_slice(&196608i32.to_be_bytes()); // protocol version 3.0
        write_cstr(&mut body, b"user");
        write_cstr(&mut body, params.user.as_bytes());
        write_cstr(&mut body, b"database");
        write_cstr(&mut body, params.database.as_bytes());
        write_cstr(&mut body, b"application_name");
        write_cstr(&mut body, params.application_name.as_bytes());
        body.push(0); // terminator

        let mut msg = Vec::with_capacity(body.len() + 4);
        msg.extend_from_slice(&((body.len() + 4) as u32).to_be_bytes());
        msg.extend_from_slice(&body);
        self.write_all(&msg).await
    }

    async fn authenticate(&mut self, params: &ConnParams) -> Result<(), String> {
        loop {
            let (t, body) = self.read_message().await?;
            match t {
                b'R' => {
                    if body.len() < 4 {
                        return Err("truncated authentication message".to_string());
                    }
                    let code = i32::from_be_bytes([body[0], body[1], body[2], body[3]]);
                    match code {
                        AUTH_OK => return Ok(()),
                        AUTH_CLEARTEXT => self.cleartext_auth(params).await?,
                        AUTH_MD5 => self.md5_auth(params, &body).await?,
                        AUTH_SASL => return self.scram_auth(params, &body).await,
                        other => {
                            return Err(format!("unsupported authentication method: {}", other));
                        }
                    }
                }
                b'E' => return Err(parse_error_string(&body)),
                other => {
                    return Err(format!("unexpected message during auth: {}", other as char));
                }
            }
        }
    }

    async fn cleartext_auth(&mut self, params: &ConnParams) -> Result<(), String> {
        let mut body = Vec::with_capacity(params.password.len() + 1);
        write_cstr(&mut body, params.password.as_bytes());
        self.send_message(b'p', &body).await
    }

    async fn md5_auth(&mut self, params: &ConnParams, body: &[u8]) -> Result<(), String> {
        if body.len() < 8 {
            return Err("truncated MD5 authentication salt".to_string());
        }
        let salt = &body[4..8];
        // concat("md5", md5(md5(password + user) + salt))
        let inner = md5_hex(
            [params.password.as_bytes(), params.user.as_bytes()]
                .concat()
                .as_slice(),
        );
        let outer = md5_hex([inner.as_bytes(), salt].concat().as_slice());
        let mut out = Vec::with_capacity(4 + outer.len());
        out.extend_from_slice(b"md5");
        out.extend_from_slice(outer.as_bytes());
        let mut payload = Vec::with_capacity(out.len() + 1);
        write_cstr(&mut payload, &out);
        self.send_message(b'p', &payload).await
    }

    async fn scram_auth(&mut self, params: &ConnParams, body: &[u8]) -> Result<(), String> {
        let mechs: Vec<&str> = body[4..]
            .split(|&b| b == 0)
            .filter_map(|s| std::str::from_utf8(s).ok())
            .filter(|s| !s.is_empty())
            .collect();
        if !mechs.contains(&"SCRAM-SHA-256") {
            return Err(format!("unsupported SCRAM mechanisms: {:?}", mechs));
        }

        let mut client =
            scram::ScramSha256Client::new(params.user.clone(), params.password.clone());
        let client_first = format!("n,,{}", client.prepare_first_message()?);

        // SASLInitialResponse: mechanism name + i32 length + client-first-message
        let mut init = Vec::with_capacity(client_first.len() + 32);
        write_cstr(&mut init, b"SCRAM-SHA-256");
        init.extend_from_slice(&(client_first.len() as i32).to_be_bytes());
        init.extend_from_slice(client_first.as_bytes());
        self.send_message(b'p', &init).await?;

        // AuthenticationSASLContinue
        let (t, body) = self.read_message().await?;
        if t == b'E' {
            return Err(parse_error_string(&body));
        }
        if t != b'R' || body.len() < 4 || read_i32(&body, 0) != AUTH_SASL_CONTINUE {
            return Err("unexpected message during SCRAM continue".to_string());
        }
        client.process_server_first(&String::from_utf8_lossy(&body[4..]))?;

        // SASLResponse: client-final-message
        let client_final = client.prepare_final_message()?;
        self.send_message(b'p', client_final.as_bytes()).await?;

        // AuthenticationSASLFinal
        let (t, body) = self.read_message().await?;
        if t == b'E' {
            return Err(parse_error_string(&body));
        }
        if t != b'R' || body.len() < 4 || read_i32(&body, 0) != AUTH_SASL_FINAL {
            return Err("unexpected message during SCRAM final".to_string());
        }
        client.process_server_final(&String::from_utf8_lossy(&body[4..]))?;
        if !client.is_authenticated() {
            return Err("SCRAM-SHA-256 authentication failed".to_string());
        }

        // Expect AuthenticationOk next.
        let (t, body) = self.read_message().await?;
        if t == b'E' {
            return Err(parse_error_string(&body));
        }
        if t != b'R' || body.len() < 4 || read_i32(&body, 0) != AUTH_OK {
            return Err("expected AuthenticationOk after SCRAM".to_string());
        }
        Ok(())
    }

    /// Consume ParameterStatus / BackendKeyData until ReadyForQuery.
    async fn wait_until_ready(&mut self) -> Result<(), String> {
        loop {
            let (t, body) = self.read_message().await?;
            match t {
                b'Z' => return Ok(()),
                b'E' => return Err(parse_error_string(&body)),
                _ => {}
            }
        }
    }

    // --- low-level IO --------------------------------------------------------

    async fn write_all(&mut self, data: &[u8]) -> Result<(), String> {
        self.stream
            .write_all(data)
            .await
            .map_err(|e| format!("socket write failed: {}", e))
    }

    async fn send_message(&mut self, msg_type: u8, body: &[u8]) -> Result<(), String> {
        let mut msg = Vec::with_capacity(body.len() + 5);
        msg.push(msg_type);
        msg.extend_from_slice(&((body.len() + 4) as u32).to_be_bytes());
        msg.extend_from_slice(body);
        self.write_all(&msg).await
    }

    fn message_header(header: &[u8]) -> Result<(u8, usize), String> {
        let len = read_i32(header, 1);
        if len < 4 {
            return Err(format!("invalid message length: {}", len));
        }
        let body_len = (len - 4) as usize;
        if body_len > MAX_MESSAGE_LEN {
            return Err(format!(
                "message too large: {} bytes (max {})",
                body_len, MAX_MESSAGE_LEN
            ));
        }
        Ok((header[0], body_len))
    }

    async fn read_header(&mut self) -> Result<(u8, usize), String> {
        if self.stream.buffer().len() >= 5 {
            let header = Self::message_header(self.stream.buffer())?;
            self.stream.consume(5);
            return Ok(header);
        }
        let mut header = [0u8; 5];
        self.read_timer
            .as_mut()
            .reset(tokio::time::Instant::now() + self.read_timeout);
        tokio::select! {
            result = self.stream.read_exact(&mut header) => result,
            _ = self.read_timer.as_mut() => {
                return Err("socket read timed out".to_string());
            }
        }
        .map_err(|e| format!("socket read failed: {}", e))?;
        Self::message_header(&header)
    }

    /// Append directly into the final row storage, or drain without allocating.
    /// Preserve the existing separate header/body timeout budgets; fragments
    /// of one body share a deadline. Buffered bytes need no timer or async read.
    async fn read_body(
        &mut self,
        mut remaining: usize,
        mut out: Option<&mut Vec<u8>>,
    ) -> Result<(), String> {
        let mut deadline_active = false;
        while remaining != 0 {
            let buffered = self.stream.buffer();
            if !buffered.is_empty() {
                let used = remaining.min(buffered.len());
                if let Some(out) = out.as_mut() {
                    out.extend_from_slice(&buffered[..used]);
                }
                self.stream.consume(used);
                remaining -= used;
                continue;
            }
            if !deadline_active {
                self.read_timer
                    .as_mut()
                    .reset(tokio::time::Instant::now() + self.read_timeout);
                deadline_active = true;
            }
            tokio::select! {
                biased;
                _ = self.read_timer.as_mut() => {
                    return Err("socket read timed out".to_string());
                }
                result = self.stream.fill_buf() => {
                    if result.map_err(|e| format!("socket read failed: {}", e))?.is_empty() {
                        return Err("socket read failed: unexpected EOF".to_string());
                    }
                }
            }
        }
        Ok(())
    }

    /// Owned-message path for authentication. Queries use the buffered path.
    async fn read_message(&mut self) -> Result<(u8, Vec<u8>), String> {
        let (kind, len) = self.read_header().await?;
        let mut body = Vec::new();
        self.read_body(len, Some(&mut body)).await?;
        Ok((kind, body))
    }

    /// A pooled request must leave the connection idle: transactions cannot
    /// span requests because the next request may run on another worker.
    async fn finish_request(&mut self, result: &mut QueryResult) -> Result<(), String> {
        if result.txn_status == b'I' {
            return Ok(());
        }
        if result.error.is_none() {
            result.error = Some(Box::new(parse_error(
                b"SERROR\0C25000\0Mopen transactions cannot span pooled requests\0\0",
            )));
        }
        self.rollback().await
    }

    async fn rollback(&mut self) -> Result<(), String> {
        let result = self.execute(b"Q\0\0\0\x0dROLLBACK\0", false).await?;
        if result.error.is_some() || result.txn_status != b'I' {
            return Err("ROLLBACK failed to restore an idle connection".to_string());
        }
        Ok(())
    }

    /// Write once, then scan buffered frames synchronously until ReadyForQuery.
    /// Only split frames need asynchronous reads. No result rows are retained
    /// for fire-and-forget requests, but errors and transaction status survive.
    async fn execute(&mut self, data: &[u8], capture: bool) -> Result<QueryResult, String> {
        self.write_all(data).await?;
        let mut collector = QueryCollector::new(capture, crate::LIMITS.db_query_rows);
        let mut scratch = Vec::new();
        loop {
            let buffered = self.stream.buffer();
            if buffered.len() >= 5 {
                let (kind, len) = Self::message_header(buffered)?;
                if buffered.len() >= 5 + len {
                    let done = collector.accept(kind, &buffered[5..5 + len])?;
                    self.stream.consume(5 + len);
                    if done {
                        break;
                    }
                    continue;
                }
            }

            let (kind, len) = self.read_header().await?;
            if kind == b'D' {
                self.read_body(len, collector.row_destination(len)).await?;
            } else if collector.needs_body(kind) {
                scratch.clear();
                self.read_body(len, Some(&mut scratch)).await?;
                if collector.accept(kind, &scratch)? {
                    break;
                }
            } else {
                self.read_body(len, None).await?;
            }
        }
        Ok(collector.finish())
    }
}

// ---------------------------------------------------------------------------
// Backend message parsing helpers
// ---------------------------------------------------------------------------

#[inline]
fn read_i32(buf: &[u8], pos: usize) -> i32 {
    i32::from_be_bytes([buf[pos], buf[pos + 1], buf[pos + 2], buf[pos + 3]])
}

#[inline]
fn read_u16(buf: &[u8], pos: usize) -> u16 {
    u16::from_be_bytes([buf[pos], buf[pos + 1]])
}

fn write_cstr(buf: &mut Vec<u8>, s: &[u8]) {
    buf.extend_from_slice(s);
    buf.push(0);
}

fn md5_hex(data: &[u8]) -> String {
    use md5::{Digest, Md5};
    let mut hasher = Md5::new();
    hasher.update(data);
    let digest = hasher.finalize();
    let mut s = String::with_capacity(32);
    for b in digest.iter() {
        s.push_str(&format!("{:02x}", b));
    }
    s
}

/// Parse an ErrorResponse / NoticeResponse body into mapped fields.
fn parse_error(body: &[u8]) -> DbError {
    let mut err = DbError {
        severity: None,
        code: None,
        message: None,
        position: None,
        detail: None,
        schema: None,
        table: None,
        constraint: None,
    };
    let mut i = 0;
    while i < body.len() {
        let field = body[i];
        if field == 0 {
            break;
        }
        i += 1;
        let start = i;
        while i < body.len() && body[i] != 0 {
            i += 1;
        }
        let value = String::from_utf8_lossy(&body[start..i]).into_owned();
        i += 1; // skip NUL
        match field {
            b'S' => err.severity = Some(value),
            b'C' => err.code = Some(value),
            b'M' => err.message = Some(value),
            b'P' => err.position = Some(value),
            b'D' => err.detail = Some(value),
            b's' => err.schema = Some(value),
            b't' => err.table = Some(value),
            b'n' => err.constraint = Some(value),
            _ => {}
        }
    }
    err
}

fn parse_error_string(body: &[u8]) -> String {
    let err = parse_error(body);
    err.message
        .unwrap_or_else(|| "unknown database error".to_string())
}

fn parse_notification(body: &[u8]) -> Option<Notification> {
    if body.len() < 6 {
        return None;
    }
    let pid = read_i32(body, 0);
    let rest = &body[4..];
    let channel_end = memchr::memchr(0, rest)?;
    let payload = &rest[channel_end + 1..];
    let payload_end = memchr::memchr(0, payload)?;
    let channel = String::from_utf8_lossy(&rest[..channel_end]).into_owned();
    let payload = String::from_utf8_lossy(&payload[..payload_end]).into_owned();
    Some(Notification {
        pid,
        channel,
        payload,
    })
}

// ---------------------------------------------------------------------------
// Worker task
// ---------------------------------------------------------------------------

async fn worker_loop(
    name: String,
    params: ConnParams,
    timeout_ms: u64,
    read_timeout_ms: u64,
    mut rx: mpsc::Receiver<PgMessage>,
    counter: PendingCounter,
    initial_conn: Option<PgConn>,
) {
    let mut conn: Option<PgConn> = initial_conn;
    while let Some(msg) = rx.recv().await {
        let req = match msg {
            PgMessage::Request(req) => req,
            PgMessage::Shutdown => {
                drain_queued_requests(&mut rx, &counter, |owner, session| {
                    let _ = CONTEXT.send_value(
                        context::PTYPE_PG,
                        owner,
                        session,
                        PgResponse::Socket("pg connection closed".to_string()),
                    );
                });
                break;
            }
        };
        let mut failed_times = 0;
        loop {
            // Ensure a live connection.
            if conn.is_none() {
                match PgConn::connect(&params, timeout_ms, read_timeout_ms).await {
                    Ok(c) => conn = Some(c),
                    Err(e) => {
                        if req.session != 0 {
                            let _ = CONTEXT.send_value(
                                context::PTYPE_PG,
                                req.owner,
                                req.session,
                                PgResponse::Socket(e),
                            );
                            counter.dec();
                            break;
                        } else {
                            if failed_times == 0 {
                                log::error!("pg '{}' reconnect failed: {}. retrying.", name, e);
                            }
                            failed_times += 1;
                            tokio::time::sleep(Duration::from_secs(1)).await;
                            continue;
                        }
                    }
                }
            }

            let c = conn.as_mut().unwrap();
            match c.execute(&req.data, req.session != 0).await {
                Ok(mut result) => {
                    if c.finish_request(&mut result).await.is_err() {
                        conn = None;
                    }
                    if req.session != 0 {
                        let _ = CONTEXT.send_value(
                            context::PTYPE_PG,
                            req.owner,
                            req.session,
                            PgResponse::Result(result),
                        );
                    } else if let Some(err) = &result.error {
                        log::error!(
                            "pg '{}' execute error: {}",
                            name,
                            err.message.as_deref().unwrap_or("unknown")
                        );
                    }
                    counter.dec();
                    break;
                }
                Err(e) => {
                    // Socket-level failure: drop the connection and reconnect.
                    conn = None;
                    if req.session != 0 {
                        let _ = CONTEXT.send_value(
                            context::PTYPE_PG,
                            req.owner,
                            req.session,
                            PgResponse::Socket(e),
                        );
                        counter.dec();
                        break;
                    } else {
                        if failed_times == 0 {
                            log::error!("pg '{}' socket error: {}. retrying.", name, e);
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
// Request encoding (runs on the Lua/actor thread, low-copy)
// ---------------------------------------------------------------------------

const PQ_PARSE: u8 = b'P';
const PQ_BIND: u8 = b'B';
const PQ_DESCRIBE: u8 = b'D';
const PQ_EXECUTE: u8 = b'E';
const PQ_SYNC: u8 = b'S';

fn start_message(buf: &mut Vec<u8>, msg_type: u8) -> usize {
    buf.push(msg_type);
    let stub = buf.len();
    buf.extend_from_slice(&[0u8; 4]);
    stub
}

fn end_message(buf: &mut [u8], stub: usize) {
    let len = (buf.len() - stub) as u32;
    buf[stub..stub + 4].copy_from_slice(&len.to_be_bytes());
}

/// Write a single Bind parameter value (text format) read from `idx`.
fn write_param(
    buf: &mut Vec<u8>,
    lua: &mut LuaStack<'_>,
    idx: i32,
    options: &JsonOptions,
) -> Result<(), String> {
    let value = lua.value(idx);
    let kind = value.kind();
    let is_null = matches!(kind, LuaType::Nil | LuaType::None)
        || (kind == LuaType::LightUserData
            && value.as_light_userdata().is_some_and(|ptr| ptr.is_null()));
    if is_null {
        buf.extend_from_slice(&(-1i32).to_be_bytes());
        return Ok(());
    }

    let stub = buf.len();
    buf.extend_from_slice(&[0u8; 4]); // length placeholder
    match kind {
        LuaType::Integer => {
            let mut tmp = [0u8; lexical_core::BUFFER_SIZE];
            buf.extend_from_slice(lexical_core::write(
                value.as_integer().unwrap_or_default(),
                &mut tmp,
            ));
        }
        LuaType::Number => {
            // Keep Rust's existing float spelling (including -0, NaN and inf)
            // while formatting directly into the wire buffer, without String.
            std::io::Write::write_fmt(
                buf,
                format_args!("{}", value.as_number().unwrap_or_default()),
            )
            .expect("writing to Vec cannot fail");
        }
        LuaType::Boolean => buf.extend_from_slice(if value.as_bool().unwrap_or(false) {
            b"true"
        } else {
            b"false"
        }),
        LuaType::String => buf.extend_from_slice(value.as_bytes().unwrap_or_default()),
        LuaType::Table => {
            encode_table(buf, lua, idx, 0, false, options)?;
        }
        _ => {
            return Err(format!("unsupported parameter type: {}", value.name()));
        }
    }
    let size = (buf.len() - stub - 4) as u32;
    buf[stub..stub + 4].copy_from_slice(&size.to_be_bytes());
    Ok(())
}

/// Append Parse/Bind/Describe/Execute for one statement.
///
/// `param_indices` are the Lua stack indices of the bound parameters (text
/// format); empty for the implicit BEGIN/COMMIT statements.
fn append_statement(
    buf: &mut Vec<u8>,
    lua: &mut LuaStack<'_>,
    sql: &[u8],
    param_indices: Range<i32>,
    options: &JsonOptions,
) -> Result<(), String> {
    append_parse_unnamed(buf, sql);

    // Bind
    // The PG v3 Bind message encodes the parameter count as an i16, so a
    // statement can carry at most 65535 parameters. Reject overflow instead of
    // silently truncating the count (which would desync the wire protocol).
    if param_indices.len() > u16::MAX as usize {
        return Err(format!(
            "too many bind parameters: {} (max {})",
            param_indices.len(),
            u16::MAX
        ));
    }
    let stub = start_message(buf, PQ_BIND);
    write_cstr(buf, b""); // portal
    write_cstr(buf, b""); // statement
    buf.extend_from_slice(&0u16.to_be_bytes()); // parameter format codes (0 => text)
    buf.extend_from_slice(&(param_indices.len() as u16).to_be_bytes());
    for i in param_indices {
        write_param(buf, lua, i, options)?;
    }
    buf.extend_from_slice(&1u16.to_be_bytes()); // one result format code
    buf.extend_from_slice(&0u16.to_be_bytes()); // text
    end_message(buf, stub);

    append_describe_execute(buf);

    Ok(())
}

/// Request metadata and all rows from the unnamed portal. Bulk writes need
/// Describe too, because their conflict clause can include RETURNING.
fn append_describe_execute(buf: &mut Vec<u8>) {
    // Describe (portal)
    let stub = start_message(buf, PQ_DESCRIBE);
    buf.push(b'P');
    write_cstr(buf, b"");
    end_message(buf, stub);

    // Execute
    let stub = start_message(buf, PQ_EXECUTE);
    write_cstr(buf, b""); // portal
    buf.extend_from_slice(&0u32.to_be_bytes()); // unlimited rows
    end_message(buf, stub);
}

fn append_sync(buf: &mut Vec<u8>) {
    let stub = start_message(buf, PQ_SYNC);
    end_message(buf, stub);
}

/// Parse `sql` into the unnamed statement (no parameter type OIDs). Used by
/// parameterized queries and bulk writes, which can bind it repeatedly.
fn append_parse_unnamed(buf: &mut Vec<u8>, sql: &[u8]) {
    let stub = start_message(buf, PQ_PARSE);
    write_cstr(buf, b""); // unnamed statement
    write_cstr(buf, sql);
    buf.extend_from_slice(&0u16.to_be_bytes()); // no parameter type OIDs
    end_message(buf, stub);
}

/// PostgreSQL caps the bound-parameter count of one message at 65535 (u16).
const MAX_BIND_PARAMS: usize = u16::MAX as usize;
const MAX_MESSAGE_LEN: usize = crate::LIMITS.db_wire_message_bytes;

/// Encode a set-based bulk write: one statement Parsed once per distinct tuple
/// count, then Bound/Executed for chunks of `rows`. `cols_per_tuple` parameters
/// are read (in order) from each row table. When the rows span more than one
/// chunk the whole thing is wrapped in `BEGIN`/`COMMIT` for atomicity.
///
/// `build_sql(tuple_count)` returns the SQL whose placeholders are `$1..$N`
/// (N = tuple_count * cols_per_tuple), numbered row-major.
fn encode_many(
    lua: &mut LuaStack<'_>,
    buf: &mut Vec<u8>,
    rows_idx: i32,
    nrows: usize,
    cols_per_tuple: usize,
    options: &JsonOptions,
    build_sql: &dyn Fn(usize) -> String,
) -> Result<(), String> {
    let state = lua.state();
    if cols_per_tuple > MAX_BIND_PARAMS {
        return Err(format!(
            "too many columns per row ({cols_per_tuple}); max is {MAX_BIND_PARAMS}"
        ));
    }
    let max_per_chunk = (MAX_BIND_PARAMS / cols_per_tuple).max(1);
    let total_chunks = nrows.div_ceil(max_per_chunk);
    let multi = total_chunks > 1;

    if multi {
        append_statement(buf, lua, b"BEGIN", 0..0, options)?;
    }

    let mut parsed_len = 0usize; // tuple count currently held by the unnamed stmt
    let mut start = 0usize;
    while start < nrows {
        let len = std::cmp::min(max_per_chunk, nrows - start);
        if len != parsed_len {
            let sql = build_sql(len);
            append_parse_unnamed(buf, sql.as_bytes());
            parsed_len = len;
        }

        // Bind
        let stub = start_message(buf, PQ_BIND);
        write_cstr(buf, b""); // portal
        write_cstr(buf, b""); // unnamed statement (already parsed)
        buf.extend_from_slice(&0u16.to_be_bytes()); // parameter format codes (0 => text)
        buf.extend_from_slice(&((len * cols_per_tuple) as u16).to_be_bytes());
        for r in 0..len {
            let row_i = (start + r + 1) as ffi::lua_Integer;
            unsafe { ffi::lua_rawgeti(state.as_ptr(), rows_idx, row_i) };
            let row_top = laux::lua_top(state);
            if laux::lua_type(state, row_top) != laux::LuaType::Table {
                laux::lua_pop(state, 1);
                return Err(format!("row {} is not a table", start + r + 1));
            }
            let row_len = unsafe { ffi::lua_rawlen(state.as_ptr(), row_top) } as usize;
            if row_len != cols_per_tuple {
                laux::lua_pop(state, 1);
                return Err(format!(
                    "row {} has {} values, expected {}",
                    start + r + 1,
                    row_len,
                    cols_per_tuple
                ));
            }
            for c in 1..=cols_per_tuple {
                unsafe { ffi::lua_rawgeti(state.as_ptr(), row_top, c as ffi::lua_Integer) };
                let vtop = laux::lua_top(state);
                let res = write_param(buf, lua, vtop, options);
                laux::lua_pop(state, 1); // value
                if let Err(e) = res {
                    laux::lua_pop(state, 1); // row table
                    return Err(e);
                }
            }
            laux::lua_pop(state, 1); // row table
        }
        buf.extend_from_slice(&0u16.to_be_bytes()); // 0 result format codes => all text
        end_message(buf, stub);

        append_describe_execute(buf);

        start += len;
    }

    if multi {
        append_statement(buf, lua, b"COMMIT", 0..0, options)?;
    }
    append_sync(buf);
    Ok(())
}

/// Double-quote a SQL identifier, escaping embedded quotes per the SQL standard.
fn quote_ident(id: &str) -> String {
    let mut s = String::with_capacity(id.len() + 2);
    s.push('"');
    for ch in id.chars() {
        if ch == '"' {
            s.push('"');
        }
        s.push(ch);
    }
    s.push('"');
    s
}

/// Validate a PG type name: only `[a-zA-Z0-9_ \[\]]` allowed (covers
/// `bigint`, `character varying`, `integer[]`, etc.)
fn validate_type_name(t: &str) -> Result<(), String> {
    if t.is_empty() {
        return Err("empty type name".to_string());
    }
    if t.bytes()
        .all(|b| b.is_ascii_alphanumeric() || b == b'_' || b == b' ' || b == b'[' || b == b']')
    {
        Ok(())
    } else {
        Err(format!("invalid type name: {:?}", t))
    }
}

/// Validate an optional `ON CONFLICT ...` clause that `insert_many` appends to
/// the generated SQL verbatim (it cannot be parameterized). This is a
/// defense-in-depth check, not a full parser: it enforces the expected
/// `ON CONFLICT` prefix and rejects statement chaining / comment-out tokens
/// (`;`, `--`, `/*`). The clause is still caller-controlled SQL — do not build
/// it from untrusted input.
fn validate_conflict_clause(clause: &str) -> Result<(), String> {
    let trimmed = clause.trim();
    let has_prefix = trimmed
        .get(..11)
        .map(|p| p.eq_ignore_ascii_case("ON CONFLICT"))
        .unwrap_or(false);
    if !has_prefix {
        return Err("conflict clause must start with 'ON CONFLICT'".to_string());
    }
    if trimmed.contains(';') || trimmed.contains("--") || trimmed.contains("/*") {
        return Err("conflict clause contains a disallowed token (';', '--', or '/*')".to_string());
    }
    Ok(())
}

/// Build an `ON CONFLICT ...` clause from a structured Lua table, quoting every
/// identifier via [`quote_ident`]. Because no caller text is ever interpolated
/// (only validated/quoted identifiers), this form is safe to build from
/// untrusted input — unlike the raw string form. Accepted fields:
/// * `columns` — array of conflict-target column names (`ON CONFLICT (c1,c2)`), or
/// * `constraint` — a constraint name (`ON CONFLICT ON CONSTRAINT name`)
///   (mutually exclusive with `columns`);
/// * `update` — array of columns for `DO UPDATE SET c = EXCLUDED.c, ...`;
///   omit (or leave empty) for `DO NOTHING`.
fn build_conflict_from_table(state: LuaState, idx: i32) -> Result<String, String> {
    let mut clause = String::from("ON CONFLICT");

    // --- conflict target: `constraint` name or `columns` list (not both) ---
    unsafe { ffi::lua_getfield(state.as_ptr(), idx, cstr!("constraint")) };
    let constraint = if laux::lua_type(state, -1) == laux::LuaType::String {
        Some(stack_string(state, -1, "conflict.constraint")?)
    } else {
        None
    };
    laux::lua_pop(state, 1);

    unsafe { ffi::lua_getfield(state.as_ptr(), idx, cstr!("columns")) };
    let target_cols = if laux::lua_type(state, -1) == laux::LuaType::Table {
        let r = read_string_array(state, laux::lua_top(state), "conflict.columns");
        laux::lua_pop(state, 1);
        Some(r?)
    } else {
        laux::lua_pop(state, 1);
        None
    };

    match (constraint, target_cols) {
        (Some(_), Some(_)) => {
            return Err("conflict: specify either `columns` or `constraint`, not both".to_string());
        }
        (Some(name), None) => {
            clause.push_str(" ON CONSTRAINT ");
            clause.push_str(&quote_ident(&name));
        }
        (None, Some(cols)) => {
            clause.push_str(" (");
            for (i, c) in cols.iter().enumerate() {
                if i > 0 {
                    clause.push(',');
                }
                clause.push_str(&quote_ident(c));
            }
            clause.push(')');
        }
        // No explicit target — only meaningful with `DO NOTHING`.
        (None, None) => {}
    }

    // --- action: `update` columns → DO UPDATE SET, otherwise DO NOTHING ---
    unsafe { ffi::lua_getfield(state.as_ptr(), idx, cstr!("update")) };
    let update_cols = if laux::lua_type(state, -1) == laux::LuaType::Table {
        let r = read_string_array(state, laux::lua_top(state), "conflict.update");
        laux::lua_pop(state, 1);
        Some(r?)
    } else {
        laux::lua_pop(state, 1);
        None
    };

    match update_cols {
        Some(cols) => {
            clause.push_str(" DO UPDATE SET ");
            for (i, c) in cols.iter().enumerate() {
                if i > 0 {
                    clause.push(',');
                }
                let q = quote_ident(c);
                clause.push_str(&q);
                clause.push_str("=EXCLUDED.");
                clause.push_str(&q);
            }
        }
        None => clause.push_str(" DO NOTHING"),
    }

    Ok(clause)
}

/// Parse the optional `conflict` argument of `insert_many` at stack `idx`.
///
/// * A **table** builds the clause from validated/quoted identifiers and is
///   safe to construct from untrusted input (see [`build_conflict_from_table`]).
/// * A **string** is treated as trusted, caller-authored SQL appended verbatim
///   (only a defense-in-depth [`validate_conflict_clause`] check is applied) —
///   do **not** build the string form from untrusted input.
/// * `nil`/absent yields `None`; any other type is an error.
fn parse_conflict(state: LuaState, idx: i32) -> Result<Option<String>, String> {
    match laux::lua_type(state, idx) {
        laux::LuaType::None | laux::LuaType::Nil => Ok(None),
        laux::LuaType::Table => Ok(Some(build_conflict_from_table(state, idx)?)),
        laux::LuaType::String => {
            let cf = stack_string(state, idx, "conflict")?;
            validate_conflict_clause(&cf)?;
            Ok(Some(cf))
        }
        _ => Err("conflict must be a table (recommended) or a string".to_string()),
    }
}

/// Build `INSERT INTO tbl (c1,..) VALUES ($1,..),($..),.. [conflict]` for
/// `tuple_count` rows. Placeholders are numbered row-major from `$1`.
fn build_insert_sql(
    table: &str,
    columns: &[String],
    tuple_count: usize,
    conflict: Option<&str>,
) -> String {
    let ncols = columns.len();
    let mut s = String::with_capacity(64 + table.len() + tuple_count * ncols * 6);
    s.push_str("INSERT INTO ");
    s.push_str(&quote_ident(table));
    s.push_str(" (");
    for (i, c) in columns.iter().enumerate() {
        if i > 0 {
            s.push(',');
        }
        s.push_str(&quote_ident(c));
    }
    s.push_str(") VALUES ");
    let mut param_no = 1usize;
    for t in 0..tuple_count {
        if t > 0 {
            s.push(',');
        }
        s.push('(');
        for c in 0..ncols {
            if c > 0 {
                s.push(',');
            }
            s.push('$');
            s.push_str(&param_no.to_string());
            param_no += 1;
            let _ = c;
        }
        s.push(')');
    }
    if let Some(cf) = conflict {
        s.push(' ');
        s.push_str(cf);
    }
    s
}

/// Build a set-based bulk UPDATE:
/// `UPDATE tbl AS _t SET c1=_d.c1,.. FROM (VALUES (..),..) AS _d(_k,c1,..)
///  WHERE _t.key <cmp> _d._k`.
///
/// Bound params inside `VALUES` are untyped and default to `text`, so the join
/// key needs an explicit cast. With `key_type` (e.g. "bigint") we cast the
/// param (`_d._k::bigint`), keeping the table's index usable. Without it we
/// cast the table column to text (`_t.key::text = _d._k`), which works for any
/// type but cannot use an index on the key. The SET assignments rely on the
/// normal assignment cast (text -> column type), so they need no annotation.
fn build_update_sql(
    table: &str,
    key: &str,
    set_cols: &[String],
    tuple_count: usize,
    key_type: Option<&str>,
) -> String {
    let cols_per_tuple = 1 + set_cols.len();
    let qt = quote_ident(table);
    let qk = quote_ident(key);
    let mut s = String::with_capacity(64 + qt.len() + tuple_count * cols_per_tuple * 6);
    s.push_str("UPDATE ");
    s.push_str(&qt);
    s.push_str(" AS _t SET ");
    for (i, c) in set_cols.iter().enumerate() {
        if i > 0 {
            s.push_str(", ");
        }
        let qc = quote_ident(c);
        s.push_str(&qc);
        s.push_str(" = _d.");
        s.push_str(&qc);
    }
    s.push_str(" FROM (VALUES ");
    let mut param_no = 1usize;
    for t in 0..tuple_count {
        if t > 0 {
            s.push(',');
        }
        s.push('(');
        for c in 0..cols_per_tuple {
            if c > 0 {
                s.push(',');
            }
            s.push('$');
            s.push_str(&param_no.to_string());
            param_no += 1;
        }
        s.push(')');
    }
    s.push_str(") AS _d(_k");
    for c in set_cols {
        s.push_str(", ");
        s.push_str(&quote_ident(c));
    }
    s.push_str(") WHERE _t.");
    s.push_str(&qk);
    match key_type {
        Some(kt) => {
            s.push_str(" = _d._k::");
            s.push_str(kt);
        }
        None => {
            s.push_str("::text = _d._k");
        }
    }
    s
}

/// Read an array of column-name strings from the table at `idx`.
fn read_string_array(state: LuaState, idx: i32, what: &str) -> Result<Vec<String>, String> {
    if laux::lua_type(state, idx) != laux::LuaType::Table {
        return Err(format!("{} must be an array of column names", what));
    }
    let n = unsafe { ffi::lua_rawlen(state.as_ptr(), idx) };
    if n == 0 {
        return Err(format!("{} is empty", what));
    }
    let mut out = Vec::with_capacity(n);
    for i in 1..=n {
        unsafe { ffi::lua_rawgeti(state.as_ptr(), idx, i as ffi::lua_Integer) };
        let top = laux::lua_top(state);
        if laux::lua_type(state, top) != laux::LuaType::String {
            laux::lua_pop(state, 1);
            return Err(format!("{}[{}] must be a string", what, i));
        }
        let s = stack_string(state, top, what)?;
        laux::lua_pop(state, 1);
        out.push(s);
    }
    Ok(out)
}

fn stack_string(state: LuaState, index: i32, what: &str) -> Result<String, String> {
    if laux::lua_type(state, index) != LuaType::String {
        return Err(format!("{} must be a string", what));
    }
    let mut len = 0;
    let ptr = unsafe { ffi::lua_tolstring(state.as_ptr(), index, &mut len) };
    if ptr.is_null() {
        return Err(format!("{} must be a string", what));
    }
    let bytes = unsafe { std::slice::from_raw_parts(ptr as *const u8, len) };
    Ok(String::from_utf8_lossy(bytes).into_owned())
}

// ---------------------------------------------------------------------------
// Lua-facing functions
// ---------------------------------------------------------------------------

const PG_POOL_META: *const std::ffi::c_char = cstr!("pg_pool_metatable");

fn connect(lua: &mut LuaStack<'_>) -> Result<c_int, String> {
    let state = lua.state();
    let database_url = lua
        .get::<String>(1)
        .map_err(|err| format!("pg.connect: {err}"))?;

    let actor = LuaActor::from_lua_state(state);
    let owner = unsafe { (*actor).id };
    let session = unsafe { (*actor).next_session() };

    CONTEXT.io_runtime().spawn(async move {
        let ConnectConfig {
            params,
            name,
            connect_timeout_ms: timeout_ms,
            max_connections,
            read_timeout_ms,
            queue_capacity,
        } = match ConnectConfig::parse(&database_url) {
            Ok(c) => c,
            Err(e) => {
                let _ =
                    CONTEXT.send_value(context::PTYPE_PG, owner, session, PgResponse::Config(e));
                return;
            }
        };

        // Validate one connection up-front so connect errors surface to Lua.
        // Hand it to the first worker instead of dropping it.
        let first_conn = match PgConn::connect(&params, timeout_ms, read_timeout_ms).await {
            Ok(c) => c,
            Err(e) => {
                let _ =
                    CONTEXT.send_value(context::PTYPE_PG, owner, session, PgResponse::Socket(e));
                return;
            }
        };

        let mut workers = Vec::with_capacity(max_connections);
        let mut seed_conn = Some(first_conn);
        for _ in 0..max_connections {
            let (tx, rx) = mpsc::channel(queue_capacity);
            let counter = PendingCounter::new();
            CONTEXT.io_runtime().spawn(worker_loop(
                name.clone(),
                params.clone(),
                timeout_ms,
                read_timeout_ms,
                rx,
                counter.clone(),
                seed_conn.take(),
            ));
            workers.push(WorkerHandle::new(tx, counter));
        }

        let pool = PgPool {
            inner: Arc::new(WorkerSet::new(name.clone(), workers)),
        };
        // Replacing an existing pool of the same name: shut down the previous
        // pool's workers so their tasks/connections don't leak (the old workers
        // drain their queued requests, then exit on `Shutdown`).
        if let Some(old) = PG_CONNECTIONS.insert(name.clone(), pool) {
            log::warn!(
                "pg '{}' reconnected with the same name; shutting down the previous pool",
                old.inner.name()
            );
            for w in old.inner.workers() {
                let _ = w.tx().send(PgMessage::Shutdown).await;
            }
        }
        let _ = CONTEXT.send_value(context::PTYPE_PG, owner, session, PgResponse::Connect(name));
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
            .ok_or_else(|| "pg.find_connection: UTF-8 string expected".to_string())?;
        PG_CONNECTIONS.get(name).map(|pair| pair.value().clone())
    };
    match pool {
        Some(pool) => {
            let methods = [
                lreg!("query", query),
                lreg!("query_params", query_params),
                lreg_try!("pipe", pipe),
                lreg_try!("insert_many", insert_many),
                lreg_try!("update_many", update_many),
                lreg!("exec_query", exec_query),
                lreg!("exec_query_params", exec_query_params),
                lreg_try!("exec_pipe", exec_pipe),
                lreg_try!("exec_insert_many", exec_insert_many),
                lreg_try!("exec_update_many", exec_update_many),
                lreg!("len", pool_len),
                lreg!("close", close),
                lreg_null!(),
            ];
            if laux::lua_newuserdata(state, pool, PG_POOL_META, methods.as_ref()).is_none() {
                laux::lua_pushnil(state);
            }
        }
        None => laux::lua_pushnil(state),
    }
    Ok(1)
}

fn dispatch_async(state: LuaState, pool: &PgPool, data: Vec<u8>) -> c_int {
    let actor = LuaActor::from_lua_state(state);
    let owner = unsafe { (*actor).id };
    let session = unsafe { (*actor).next_session() };
    match pool.dispatch(owner, session, data) {
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

fn dispatch_forget(state: LuaState, pool: &PgPool, data: Vec<u8>) -> c_int {
    let owner = unsafe { (*LuaActor::from_lua_state(state)).id };
    match pool.dispatch(owner, 0, data) {
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

/// `handle:query(sql)` — simple query protocol (multi-statement).
///
/// **Trust requirement:** `sql` is sent on the wire verbatim with no parameter
/// binding (the simple-query protocol has no placeholders), so it is fully
/// caller-controlled SQL. Never build it from untrusted input — use the
/// extended-protocol helpers (`query_params`, `insert_many`, ...) with bound
/// parameters for anything that includes user data.
fn query(lua: &mut LuaStack<'_>) -> c_int {
    query_impl(lua, false)
}
fn exec_query(lua: &mut LuaStack<'_>) -> c_int {
    query_impl(lua, true)
}

fn query_impl(lua: &mut LuaStack<'_>, forget: bool) -> c_int {
    let state = lua.state();
    let pool_ptr = lua
        .value(1)
        .as_userdata::<PgPool>()
        .expect("invalid pg pool pointer");
    let pool = unsafe { pool_ptr.as_ref() };
    let sql = lua.value(2).as_bytes().unwrap_or_default();

    let mut data = Vec::with_capacity(sql.len() + 6);
    data.push(b'Q');
    data.extend_from_slice(&((sql.len() + 5) as u32).to_be_bytes());
    data.extend_from_slice(sql);
    data.push(0);

    if forget {
        dispatch_forget(state, pool, data)
    } else {
        dispatch_async(state, pool, data)
    }
}

/// `handle:query_params(sql, ...)` — extended protocol with binds.
fn query_params(lua: &mut LuaStack<'_>) -> c_int {
    query_params_impl(lua, false)
}
fn exec_query_params(lua: &mut LuaStack<'_>) -> c_int {
    query_params_impl(lua, true)
}

fn query_params_impl(lua: &mut LuaStack<'_>, forget: bool) -> c_int {
    let state = lua.state();
    let pool_ptr = lua
        .value(1)
        .as_userdata::<PgPool>()
        .expect("invalid pg pool pointer");
    let pool = unsafe { pool_ptr.as_ref() };
    let sql_idx = 2;
    // SAFETY: the SQL argument is rooted at an absolute stack slot for this
    // call. `append_statement` only appends temporary values above it and does
    // not replace, remove, or reorder the source slot.
    let (sql_ptr, sql_len) = {
        let sql = unsafe { lua.value_bytes_append_only(sql_idx).unwrap_or_default() };
        (sql.as_ptr(), sql.len())
    };
    // The source slot remains rooted and append-only throughout this helper;
    // use the stable Lua string pointer instead of allocating a Rust copy.
    let sql = unsafe { std::slice::from_raw_parts(sql_ptr, sql_len) };

    let top = laux::lua_top(state);
    let param_indices = (sql_idx + 1)..(top + 1);

    let options = JsonOptions::default();
    let mut data = Vec::with_capacity(64 + sql.len());
    if let Err(err) = append_statement(&mut data, lua, sql, param_indices, &options) {
        push_lua_table!(state, "code" => "ENCODE", "message" => err);
        return 1;
    }
    append_sync(&mut data);

    if forget {
        dispatch_forget(state, pool, data)
    } else {
        dispatch_async(state, pool, data)
    }
}

/// `handle:pipe({ {sql, p1, ...}, ... })` — pipelined transaction.
fn pipe(lua: &mut LuaStack<'_>) -> Result<c_int, String> {
    pipe_impl(lua, false)
}
fn exec_pipe(lua: &mut LuaStack<'_>) -> Result<c_int, String> {
    pipe_impl(lua, true)
}

fn pipe_impl(lua: &mut LuaStack<'_>, forget: bool) -> Result<c_int, String> {
    let state = lua.state();
    let pool_ptr = lua
        .value(1)
        .as_userdata::<PgPool>()
        .expect("invalid pg pool pointer");
    let pool = unsafe { pool_ptr.as_ref() };
    let queries_idx = laux::lua_absindex(state, 2);
    laux::lua_checktype(state, queries_idx, ffi::LUA_TTABLE)
        .map_err(|err| format!("pg.pipe: argument #2 {err}"))?;

    let options = JsonOptions::default();
    let mut data = Vec::with_capacity(256);

    if let Err(err) = append_statement(&mut data, lua, b"BEGIN", 0..0, &options) {
        push_lua_table!(state, "code" => "ENCODE", "message" => err);
        return Ok(1);
    }

    let n = unsafe { ffi::lua_rawlen(state.as_ptr(), queries_idx) };
    for i in 1..=n {
        unsafe { ffi::lua_rawgeti(state.as_ptr(), queries_idx, i as ffi::lua_Integer) };
        let stmt_idx = laux::lua_top(state);
        if laux::lua_type(state, stmt_idx) != laux::LuaType::Table {
            laux::lua_pop(state, 1);
            push_lua_table!(state, "code" => "ENCODE", "message" => format!("pipe: expected table at index {}", i));
            return Ok(1);
        }

        // sql = stmt[1]; params = stmt[2..]
        unsafe { ffi::lua_rawgeti(state.as_ptr(), stmt_idx, 1) };
        let sql_top = laux::lua_top(state);
        // SAFETY: `sql_top` remains rooted until `append_statement` returns;
        // that helper only appends temporary values above the source.
        let (sql_ptr, sql_len) = {
            let sql = unsafe { lua.value_bytes_append_only(sql_top).unwrap_or_default() };
            (sql.as_ptr(), sql.len())
        };
        let sql = unsafe { std::slice::from_raw_parts(sql_ptr, sql_len) };

        let stmt_len = unsafe { ffi::lua_rawlen(state.as_ptr(), stmt_idx) };
        let param_count = stmt_len.saturating_sub(1);
        if param_count > MAX_BIND_PARAMS {
            laux::lua_pop(state, 2);
            push_lua_table!(state, "code" => "ENCODE", "message" => format!(
                "too many bind parameters: {} (max {})", param_count, MAX_BIND_PARAMS
            ));
            return Ok(1);
        }
        laux::lua_checkstack(state, param_count as i32 + 4, std::ptr::null())?;
        for p in 2..=stmt_len {
            unsafe { ffi::lua_rawgeti(state.as_ptr(), stmt_idx, p as ffi::lua_Integer) };
        }

        let res = append_statement(
            &mut data,
            lua,
            sql,
            (sql_top + 1)..(sql_top + 1 + param_count as i32),
            &options,
        );
        laux::lua_pop(state, param_count as i32 + 2);
        if let Err(err) = res {
            push_lua_table!(state, "code" => "ENCODE", "message" => err);
            return Ok(1);
        }
    }

    if let Err(err) = append_statement(&mut data, lua, b"COMMIT", 0..0, &options) {
        push_lua_table!(state, "code" => "ENCODE", "message" => err);
        return Ok(1);
    }
    append_sync(&mut data);

    if forget {
        Ok(dispatch_forget(state, pool, data))
    } else {
        Ok(dispatch_async(state, pool, data))
    }
}

/// `handle:insert_many(session, table, columns, rows, conflict?)` — bulk
/// INSERT/UPSERT. Rows are packed into one multi-row `VALUES` statement (one
/// Parse, one Bind, one Execute, one plan), auto-chunked under the 65535
/// parameter limit and wrapped in a transaction when more than one chunk.
///
/// `columns` is an array of column names; `rows` is an array of value arrays
/// (each with one value per column). `conflict` is optional and may be either:
///   * a **table** (recommended, injection-safe), e.g.
///     `{ columns = {"uid","key"}, update = {"value"} }` →
///     `ON CONFLICT ("uid","key") DO UPDATE SET "value"=EXCLUDED."value"`, or
///     `{ constraint = "pk", }` → `ON CONFLICT ON CONSTRAINT "pk" DO NOTHING`;
///   * a **string** (legacy, trusted SQL) appended verbatim, e.g.
///     "ON CONFLICT (uid,key) DO UPDATE SET value = EXCLUDED.value" — it must
///     begin with `ON CONFLICT` and may not contain `;`, `--`, or `/*`
///     (`validate_conflict_clause`). Do not build the string form from
///     untrusted input; use the table form instead.
///
/// Note: a single multi-row UPSERT cannot touch the same conflict key twice —
/// de-duplicate keys (keep the latest) before calling.
fn insert_many(lua: &mut LuaStack<'_>) -> Result<c_int, String> {
    insert_many_impl(lua, false)
}
fn exec_insert_many(lua: &mut LuaStack<'_>) -> Result<c_int, String> {
    insert_many_impl(lua, true)
}

fn insert_many_impl(lua: &mut LuaStack<'_>, forget: bool) -> Result<c_int, String> {
    let state = lua.state();
    let pool_ptr = lua
        .value(1)
        .as_userdata::<PgPool>()
        .expect("invalid pg pool pointer");
    let pool = unsafe { pool_ptr.as_ref() };
    let table = lua
        .get::<String>(2)
        .map_err(|err| format!("pg.insert_many: {err}"))?;

    let columns = match read_string_array(state, 3, "insert_many: columns") {
        Ok(c) => c,
        Err(e) => {
            push_lua_table!(state, "code" => "ENCODE", "message" => e);
            return Ok(1);
        }
    };
    let rows_idx = 4;
    laux::lua_checktype(state, rows_idx, ffi::LUA_TTABLE)
        .map_err(|err| format!("pg.insert_many: rows {err}"))?;
    let nrows = unsafe { ffi::lua_rawlen(state.as_ptr(), rows_idx) };
    if nrows == 0 {
        push_lua_table!(state, "code" => "ENCODE", "message" => "insert_many: rows is empty");
        return Ok(1);
    }
    let conflict: Option<String> = match parse_conflict(state, 5) {
        Ok(c) => c,
        Err(e) => {
            push_lua_table!(state, "code" => "ENCODE", "message" => format!("insert_many: {}", e));
            return Ok(1);
        }
    };

    laux::lua_checkstack(state, 4, std::ptr::null())?;
    let options = JsonOptions::default();
    let mut data = Vec::with_capacity(128 + table.len() + nrows * columns.len() * 8);
    let build =
        |tuple_count: usize| build_insert_sql(&table, &columns, tuple_count, conflict.as_deref());
    if let Err(err) = encode_many(
        lua,
        &mut data,
        rows_idx,
        nrows,
        columns.len(),
        &options,
        &build,
    ) {
        push_lua_table!(state, "code" => "ENCODE", "message" => err);
        return Ok(1);
    }

    if forget {
        Ok(dispatch_forget(state, pool, data))
    } else {
        Ok(dispatch_async(state, pool, data))
    }
}

/// `handle:update_many(session, table, key_column, set_columns, rows, key_type?)`
/// — bulk UPDATE via `UPDATE ... FROM (VALUES ...)`. Each row is `{ key, set1,
/// set2, ... }` (key first, then one value per `set_columns` entry). Auto-chunked
/// and wrapped in a transaction across chunks, like `insert_many`.
///
/// `key_type` (e.g. "bigint") casts the join key param so the table's index on
/// `key_column` stays usable; omit it and the key column is compared as text
/// (works for any type, but no index).
fn update_many(lua: &mut LuaStack<'_>) -> Result<c_int, String> {
    update_many_impl(lua, false)
}
fn exec_update_many(lua: &mut LuaStack<'_>) -> Result<c_int, String> {
    update_many_impl(lua, true)
}

fn update_many_impl(lua: &mut LuaStack<'_>, forget: bool) -> Result<c_int, String> {
    let state = lua.state();
    let pool_ptr = lua
        .value(1)
        .as_userdata::<PgPool>()
        .expect("invalid pg pool pointer");
    let pool = unsafe { pool_ptr.as_ref() };
    let table = lua
        .get::<String>(2)
        .map_err(|err| format!("pg.update_many: {err}"))?;
    let key = lua
        .get::<String>(3)
        .map_err(|err| format!("pg.update_many: {err}"))?;

    let set_cols = match read_string_array(state, 4, "update_many: set_columns") {
        Ok(c) => c,
        Err(e) => {
            push_lua_table!(state, "code" => "ENCODE", "message" => e);
            return Ok(1);
        }
    };
    let rows_idx = 5;
    laux::lua_checktype(state, rows_idx, ffi::LUA_TTABLE)
        .map_err(|err| format!("pg.update_many: rows {err}"))?;
    let nrows = unsafe { ffi::lua_rawlen(state.as_ptr(), rows_idx) };
    if nrows == 0 {
        push_lua_table!(state, "code" => "ENCODE", "message" => "update_many: rows is empty");
        return Ok(1);
    }
    let key_type: Option<String> = if laux::lua_type(state, 6) == laux::LuaType::String {
        let kt = lua
            .get::<String>(6)
            .map_err(|err| format!("pg.update_many: {err}"))?;
        if let Err(e) = validate_type_name(&kt) {
            push_lua_table!(state, "code" => "ENCODE", "message" => format!("update_many: {}", e));
            return Ok(1);
        }
        Some(kt)
    } else {
        None
    };

    laux::lua_checkstack(state, 4, std::ptr::null())?;
    let options = JsonOptions::default();
    let cols_per_tuple = 1 + set_cols.len();
    let mut data = Vec::with_capacity(128 + table.len() + nrows * cols_per_tuple * 8);
    let build = |tuple_count: usize| {
        build_update_sql(&table, &key, &set_cols, tuple_count, key_type.as_deref())
    };
    if let Err(err) = encode_many(
        lua,
        &mut data,
        rows_idx,
        nrows,
        cols_per_tuple,
        &options,
        &build,
    ) {
        push_lua_table!(state, "code" => "ENCODE", "message" => err);
        return Ok(1);
    }

    if forget {
        Ok(dispatch_forget(state, pool, data))
    } else {
        Ok(dispatch_async(state, pool, data))
    }
}

fn pool_len(lua: &mut LuaStack<'_>) -> c_int {
    let state = lua.state();
    let pool_ptr = lua
        .value(1)
        .as_userdata::<PgPool>()
        .expect("invalid pg pool pointer");
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
        .as_userdata::<PgPool>()
        .expect("invalid pg pool pointer");
    let pool = unsafe { pool_ptr.as_ref() };
    // Only remove our own entry: if a `connect()` with the same name has already
    // replaced this pool, closing through this (now stale) handle must not evict
    // the newer pool. Identify ourselves by the `inner` Arc.
    PG_CONNECTIONS.remove_if(pool.inner.name(), |_, v| Arc::ptr_eq(&v.inner, &pool.inner));
    // Signal every worker to finish any queued requests and then exit, so its
    // task ends and the TCP connection is dropped. Removing the registry entry
    // alone is not enough because the Lua handle still holds a pool `Arc`.
    for worker in pool.inner.workers() {
        let tx = worker.tx().clone();
        CONTEXT.io_runtime().spawn(async move {
            let _ = tx.send(PgMessage::Shutdown).await;
        });
    }
    laux::lua_push(state, true);
    1
}

fn stats(lua: &mut LuaStack<'_>) -> c_int {
    let state = lua.state();
    let table = LuaTable::new(state, 0, PG_CONNECTIONS.len());
    PG_CONNECTIONS.iter().for_each(|pair| {
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

// ---------------------------------------------------------------------------
// Response decoding (runs on the actor thread; raw bytes -> Lua tables)
// ---------------------------------------------------------------------------

/// PG type OIDs that map to a Lua number/boolean; everything else stays text.
fn convert_value(state: LuaState, oid: i32, value: &[u8]) {
    let s = || std::str::from_utf8(value).unwrap_or_default();
    match oid {
        16 => laux::lua_push(state, value == b"t"), // bool
        20 | 21 | 23 => match s().parse::<i64>() {
            Ok(v) => laux::lua_push(state, v),
            Err(_) => laux::lua_push(state, value),
        },
        700 | 701 | 1700 => match s().parse::<f64>() {
            Ok(v) => laux::lua_push(state, v),
            Err(_) => laux::lua_push(state, value),
        },
        _ => laux::lua_push(state, value),
    }
}

/// Parse a RowDescription body into `(name, type_oid)` fields.
fn parse_row_desc(body: &[u8]) -> Result<Vec<(Cow<'_, str>, i32)>, String> {
    if body.len() < 2 {
        return Err("truncated RowDescription".to_string());
    }
    let num = read_u16(body, 0) as usize;
    let mut fields = Vec::with_capacity(num.min(body.len() / 19));
    let mut rest = &body[2..];
    for _ in 0..num {
        let end = memchr::memchr(0, rest).ok_or("unterminated RowDescription field name")?;
        let name = String::from_utf8_lossy(&rest[..end]);
        rest = &rest[end + 1..];
        // table_oid(4) col_attr(2) type_oid(4) type_size(2) type_mod(4) format(2)
        if rest.len() < 18 {
            return Err("truncated RowDescription field metadata".to_string());
        }
        if read_u16(rest, 16) != 0 {
            return Err("unsupported binary RowDescription field".to_string());
        }
        fields.push((name, read_i32(rest, 6)));
        rest = &rest[18..];
    }
    if !rest.is_empty() {
        return Err("trailing bytes in RowDescription".to_string());
    }
    Ok(fields)
}

/// Consume one text column, rejecting malformed lengths rather than returning
/// silently truncated values. NULL is the only valid negative length.
fn take_column<'a>(rest: &mut &'a [u8]) -> Result<Option<&'a [u8]>, String> {
    if rest.len() < 4 {
        return Err("truncated DataRow column length".to_string());
    }
    let len = read_i32(rest, 0);
    *rest = &rest[4..];
    if len == -1 {
        return Ok(None);
    }
    let len = usize::try_from(len).map_err(|_| "invalid DataRow column length")?;
    if len > rest.len() {
        return Err("truncated DataRow column value".to_string());
    }
    let value = &rest[..len];
    *rest = &rest[len..];
    Ok(Some(value))
}

/// Parse `command_tag` -> (command, affected_rows).
fn parse_command_tag(tag: &[u8]) -> (String, Option<i64>) {
    let end = tag.iter().position(|&b| b == 0).unwrap_or(tag.len());
    let s = String::from_utf8_lossy(&tag[..end]);
    let mut parts = s.split_whitespace();
    let command = parts.next().unwrap_or("").to_string();
    let affected = parts.last().and_then(|t| t.parse::<i64>().ok());
    (command, affected)
}

/// Push one statement's result as a Lua value (rows table, {affected_rows}, or true).
fn push_statement_result(state: LuaState, stmt: &Statement) -> Result<(), String> {
    let (command, affected_rows) = stmt
        .command_tag
        .as_ref()
        .map(|t| parse_command_tag(t))
        .unwrap_or((String::new(), None));

    if let Some(row_desc) = &stmt.row_desc {
        let fields = parse_row_desc(row_desc)?;
        // Root each Lua key once per result set. Repeated rows can push the
        // existing key with rawgeti instead of rehashing its bytes each time.
        let keys = if stmt.data_rows.count > 1 {
            let keys = LuaTable::new(state, fields.len(), 0);
            for (i, (name, _)) in fields.iter().enumerate() {
                laux::lua_push(state, name.as_ref());
                keys.rawseti(i + 1);
            }
            Some(keys)
        } else {
            None
        };
        let table = LuaTable::new(state, stmt.data_rows.count, 0);
        for (ri, row) in stmt.data_rows.iter().enumerate() {
            if row.len() < 2 || read_u16(row, 0) as usize != fields.len() {
                return Err("DataRow column count does not match RowDescription".to_string());
            }
            let row_table = LuaTable::new(state, 0, fields.len());
            let mut rest = &row[2..];
            for (ci, (name, oid)) in fields.iter().enumerate() {
                if let Some(value) = take_column(&mut rest)? {
                    if let Some(keys) = &keys {
                        unsafe {
                            ffi::lua_rawgeti(
                                state.as_ptr(),
                                keys.index(),
                                (ci + 1) as ffi::lua_Integer,
                            )
                        };
                    } else {
                        laux::lua_push(state, name.as_ref());
                    }
                    convert_value(state, *oid, value);
                    unsafe { ffi::lua_rawset(state.as_ptr(), row_table.index()) };
                }
            }
            if !rest.is_empty() {
                return Err("trailing bytes in DataRow".to_string());
            }
            table.rawseti(ri + 1);
        }
        if let Some(n) = affected_rows {
            if command != "SELECT" {
                table.insert("affected_rows", n);
            }
        }
        if let Some(keys) = keys {
            unsafe { ffi::lua_remove(state.as_ptr(), keys.index()) };
        }
        return Ok(());
    }

    if let Some(n) = affected_rows {
        push_lua_table!(state, "affected_rows" => n);
    } else {
        laux::lua_push(state, true);
    }
    Ok(())
}

/// Push the aggregated `data` field across statements (mirrors pg.lua).
fn push_data(state: LuaState, statements: &[Statement]) -> Result<(), String> {
    match statements.len() {
        0 => laux::lua_pushnil(state),
        1 => push_statement_result(state, &statements[0])?,
        n => {
            let table = LuaTable::new(state, n, 0);
            for (i, stmt) in statements.iter().enumerate() {
                push_statement_result(state, stmt)?;
                table.rawseti(i + 1);
            }
        }
    }
    Ok(())
}

fn push_notifications(state: LuaState, notifications: &[Notification]) {
    let table = LuaTable::new(state, notifications.len(), 0);
    for (i, n) in notifications.iter().enumerate() {
        let one = LuaTable::new(state, 0, 4);
        one.insert("operation", "notification");
        one.insert("pid", n.pid as i64);
        one.insert("channel", n.channel.as_str());
        one.insert("payload", n.payload.as_str());
        table.rawseti(i + 1);
    }
}

fn push_db_error(state: LuaState, err: &DbError) {
    let table = LuaTable::new(state, 0, 8);
    if let Some(v) = &err.severity {
        table.insert("severity", v.as_str());
    }
    if let Some(v) = &err.code {
        table.insert("code", v.as_str());
    } else {
        table.insert("code", "DB");
    }
    if let Some(v) = &err.message {
        table.insert("message", v.as_str());
    }
    if let Some(v) = &err.position {
        table.insert("position", v.as_str());
    }
    if let Some(v) = &err.detail {
        table.insert("detail", v.as_str());
    }
    if let Some(v) = &err.schema {
        table.insert("schema", v.as_str());
    }
    if let Some(v) = &err.table {
        table.insert("table", v.as_str());
    }
    if let Some(v) = &err.constraint {
        table.insert("constraint", v.as_str());
    }
}

fn push_pg_response(state: LuaState, response: PgResponse) -> c_int {
    let top = laux::lua_top(state);
    match try_push_pg_response(state, response) {
        Ok(count) => count,
        Err(message) => {
            // Remove partially built tables and rooted keys before returning
            // a decoding error. The worker has already drained the response.
            laux::lua_settop(state, top);
            push_lua_table!(state, "code" => "PROTOCOL", "message" => message);
            1
        }
    }
}

fn try_push_pg_response(state: LuaState, response: PgResponse) -> Result<c_int, String> {
    match response {
        PgResponse::Connect(name) => {
            // No `.code` => success; `.name` lets pg.lua look the pool up.
            push_lua_table!(state, "name" => name);
            Ok(1)
        }
        PgResponse::Config(msg) => {
            push_lua_table!(state, "code" => "CONFIG", "message" => msg);
            Ok(1)
        }
        PgResponse::Socket(msg) => {
            push_lua_table!(state, "code" => "SOCKET", "message" => msg);
            Ok(1)
        }
        PgResponse::Result(result) => {
            let num_queries = result.statements.len() as i64;
            if let Some(err) = &result.error {
                push_db_error(state, err);
            } else {
                LuaTable::new(state, 0, 3);
            }
            // Successful and failed queries carry the same result metadata.
            let idx = laux::lua_top(state);
            laux::lua_push(state, "num_queries");
            laux::lua_push(state, num_queries);
            unsafe { ffi::lua_rawset(state.as_ptr(), idx) };

            laux::lua_push(state, "data");
            push_data(state, &result.statements)?;
            unsafe { ffi::lua_rawset(state.as_ptr(), idx) };

            if !result.notifications.is_empty() {
                laux::lua_push(state, "notifications");
                push_notifications(state, &result.notifications);
                unsafe { ffi::lua_rawset(state.as_ptr(), idx) };
            }
            Ok(1)
        }
    }
}

pub unsafe extern "C-unwind" fn decode_pg_message(
    state: LuaState,
    m: *mut moon_runtime::context::Message,
) -> c_int {
    match unsafe { crate::message_decode::take_boxed::<PgResponse>(m) } {
        Ok(response) => push_pg_response(state, response),
        Err(e) => crate::lua_push_error_tuple(state, &e),
    }
}

pub extern "C-unwind" fn luaopen_pg(state: LuaState) -> c_int {
    let l = [
        lreg_try!("connect", connect),
        lreg_try!("find_connection", find_connection),
        lreg!("stats", stats),
        lreg_null!(),
    ];
    luaL_newlib!(state, l);
    1
}

mod scram {
    //! SCRAM-SHA-256 client (RFC 5802 / RFC 7677).
    //!
    //! HMAC-SHA256 and PBKDF2 are implemented inline on top of `sha2::Sha256` to
    //! avoid pulling extra crates whose `digest` major versions might diverge.

    use base64::{Engine, engine::general_purpose::STANDARD as BASE64};
    use sha2::{Digest, Sha256};
    use std::collections::HashMap;

    const SHA256_SIZE: usize = 32;
    const SHA256_BLOCK: usize = 64;

    #[derive(PartialEq, Eq, Clone, Copy)]
    enum State {
        Initial,
        FirstSent,
        FinalSent,
        Authenticated,
        Error,
    }

    fn hmac_sha256(key: &[u8], msg: &[u8]) -> [u8; SHA256_SIZE] {
        let mut block = [0u8; SHA256_BLOCK];
        if key.len() > SHA256_BLOCK {
            let hashed = Sha256::digest(key);
            block[..SHA256_SIZE].copy_from_slice(&hashed);
        } else {
            block[..key.len()].copy_from_slice(key);
        }

        let mut ipad = [0x36u8; SHA256_BLOCK];
        let mut opad = [0x5cu8; SHA256_BLOCK];
        for i in 0..SHA256_BLOCK {
            ipad[i] ^= block[i];
            opad[i] ^= block[i];
        }

        let mut inner = Sha256::new();
        inner.update(ipad);
        inner.update(msg);
        let inner_hash = inner.finalize();

        let mut outer = Sha256::new();
        outer.update(opad);
        outer.update(inner_hash);

        let mut out = [0u8; SHA256_SIZE];
        out.copy_from_slice(&outer.finalize());
        out
    }

    fn pbkdf2_hmac_sha256_one_block(
        password: &[u8],
        salt: &[u8],
        iterations: u32,
    ) -> [u8; SHA256_SIZE] {
        let mut salt_with_index = Vec::with_capacity(salt.len() + 4);
        salt_with_index.extend_from_slice(salt);
        salt_with_index.extend_from_slice(&1u32.to_be_bytes());

        let mut u = hmac_sha256(password, &salt_with_index);
        let mut result = u;
        for _ in 1..iterations {
            u = hmac_sha256(password, &u);
            for i in 0..SHA256_SIZE {
                result[i] ^= u[i];
            }
        }
        result
    }

    fn generate_nonce(length: usize) -> String {
        const CHARS: &[u8] = b"abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";
        use rand::RngExt;
        let mut rng = rand::rng();
        (0..length)
            .map(|_| CHARS[rng.random_range(0..CHARS.len())] as char)
            .collect()
    }

    fn parse_scram_attributes(message: &str) -> HashMap<char, String> {
        let mut attributes = HashMap::new();
        for segment in message.split(',') {
            let bytes = segment.as_bytes();
            if bytes.len() >= 3 && bytes[1] == b'=' {
                attributes.insert(bytes[0] as char, segment[2..].to_string());
            } else if bytes.len() == 1 {
                attributes.insert(bytes[0] as char, String::new());
            }
        }
        attributes
    }

    pub(super) struct ScramSha256Client {
        username: String,
        password: String,
        client_nonce: String,
        server_nonce: String,
        salt: Vec<u8>,
        iterations: u32,
        client_first_message_bare: String,
        auth_message: String,
        client_final_message_without_proof: String,
        salted_password: [u8; SHA256_SIZE],
        client_key: [u8; SHA256_SIZE],
        stored_key: [u8; SHA256_SIZE],
        state: State,
    }

    impl ScramSha256Client {
        pub(super) fn new(username: String, password: String) -> Self {
            Self {
                username,
                password,
                client_nonce: String::new(),
                server_nonce: String::new(),
                salt: Vec::new(),
                iterations: 0,
                client_first_message_bare: String::new(),
                auth_message: String::new(),
                client_final_message_without_proof: String::new(),
                salted_password: [0u8; SHA256_SIZE],
                client_key: [0u8; SHA256_SIZE],
                stored_key: [0u8; SHA256_SIZE],
                state: State::Initial,
            }
        }

        pub(super) fn prepare_first_message(&mut self) -> Result<String, String> {
            if self.state != State::Initial {
                return Err("Invalid state for preparing first message.".into());
            }
            self.client_nonce = generate_nonce(24);
            self.client_first_message_bare = format!("n={},r={}", self.username, self.client_nonce);
            self.state = State::FirstSent;
            Ok(self.client_first_message_bare.clone())
        }

        pub(super) fn process_server_first(
            &mut self,
            server_first_message: &str,
        ) -> Result<(), String> {
            if self.state != State::FirstSent {
                return Err("Invalid state for processing server first message.".into());
            }

            let attributes = parse_scram_attributes(server_first_message);
            let r = attributes.get(&'r');
            let s = attributes.get(&'s');
            let i = attributes.get(&'i');
            let (r, s, i) = match (r, s, i) {
                (Some(r), Some(s), Some(i)) => (r, s, i),
                _ => {
                    self.state = State::Error;
                    return Err(
                        "Server first message missing required attributes (r, s, i).".into(),
                    );
                }
            };

            self.server_nonce = r.clone();
            if !self.server_nonce.starts_with(&self.client_nonce) {
                self.state = State::Error;
                return Err("Server nonce does not match client nonce prefix.".into());
            }

            self.iterations = match i.parse::<u32>() {
                Ok(n) => n,
                Err(_) => {
                    self.state = State::Error;
                    return Err("Server iterations count is not a valid integer.".into());
                }
            };
            if self.iterations == 0 {
                self.state = State::Error;
                return Err("Server iterations count cannot be zero.".into());
            }

            self.salt = match BASE64.decode(s) {
                Ok(salt) => salt,
                Err(_) => {
                    self.state = State::Error;
                    return Err("Failed to decode salt from base64.".into());
                }
            };

            self.salted_password =
                pbkdf2_hmac_sha256_one_block(self.password.as_bytes(), &self.salt, self.iterations);
            self.client_key = hmac_sha256(&self.salted_password, b"Client Key");
            self.stored_key
                .copy_from_slice(&Sha256::digest(self.client_key));

            self.auth_message = format!(
                "{},{}",
                self.client_first_message_bare, server_first_message
            );
            Ok(())
        }

        pub(super) fn prepare_final_message(&mut self) -> Result<String, String> {
            if self.state != State::FirstSent || self.auth_message.is_empty() {
                return Err("Invalid state or missing data for preparing final message.".into());
            }

            let channel_binding = BASE64.encode(b"n,,");
            self.client_final_message_without_proof =
                format!("c={},r={}", channel_binding, self.server_nonce);

            let full_auth_message = format!(
                "{},{}",
                self.auth_message, self.client_final_message_without_proof
            );

            let client_signature = hmac_sha256(&self.stored_key, full_auth_message.as_bytes());
            let mut client_proof = [0u8; SHA256_SIZE];
            for i in 0..SHA256_SIZE {
                client_proof[i] = self.client_key[i] ^ client_signature[i];
            }

            let proof = BASE64.encode(client_proof);
            self.state = State::FinalSent;
            Ok(format!(
                "{},p={}",
                self.client_final_message_without_proof, proof
            ))
        }

        pub(super) fn process_server_final(
            &mut self,
            server_final_message: &str,
        ) -> Result<(), String> {
            if self.state != State::FinalSent {
                return Err("Invalid state for processing server final message.".into());
            }

            let attributes = parse_scram_attributes(server_final_message);
            let v = match attributes.get(&'v') {
                Some(v) => v,
                None => {
                    self.state = State::Error;
                    return Err("Server final message missing required attribute 'v'.".into());
                }
            };

            let server_signature = match BASE64.decode(v) {
                Ok(sig) => sig,
                Err(_) => {
                    self.state = State::Error;
                    return Err("Failed to decode server signature from base64.".into());
                }
            };

            let server_key = hmac_sha256(&self.salted_password, b"Server Key");
            let full_auth_message = format!(
                "{},{}",
                self.auth_message, self.client_final_message_without_proof
            );
            let expected_server_signature = hmac_sha256(&server_key, full_auth_message.as_bytes());

            if server_signature.as_slice() == expected_server_signature.as_slice() {
                self.state = State::Authenticated;
                Ok(())
            } else {
                self.state = State::Error;
                Err("Server signature verification failed.".into())
            }
        }

        pub(super) fn is_authenticated(&self) -> bool {
            self.state == State::Authenticated
        }
    }

    #[cfg(test)]
    mod tests {
        use super::*;

        fn hex(bytes: &[u8]) -> String {
            bytes.iter().map(|b| format!("{:02x}", b)).collect()
        }

        #[test]
        fn hmac_sha256_rfc4231_case2() {
            let mac = hmac_sha256(b"Jefe", b"what do ya want for nothing?");
            assert_eq!(
                hex(&mac),
                "5bdcc146bf60754e6a042426089575c75a003f089d2739839dec58b964ec3843"
            );
        }

        #[test]
        fn pbkdf2_hmac_sha256_known_vectors() {
            let dk = pbkdf2_hmac_sha256_one_block(b"password", b"salt", 1);
            assert_eq!(
                hex(&dk),
                "120fb6cffcf8b32c43e7225256c4f837a86548c92ccc35480805987cb70be17b"
            );
            let dk = pbkdf2_hmac_sha256_one_block(b"password", b"salt", 4096);
            assert_eq!(
                hex(&dk),
                "c5e478d59288c841aa530db6845c4c8d962893a001ce4e11a4963873aa98134a"
            );
        }

        #[test]
        fn scram_rfc7677_exchange() {
            let mut c = ScramSha256Client::new("user".to_string(), "pencil".to_string());
            c.client_nonce = "rOprNGfwEbeRWgbNEkqO".to_string();
            c.client_first_message_bare = format!("n={},r={}", c.username, c.client_nonce);
            c.state = State::FirstSent;

            let server_first = "r=rOprNGfwEbeRWgbNEkqO%hvYDpWUa2RaTCAfuxFIlj)hNlF$k0,\
                                s=W22ZaJ0SNY7soEsUEjb6gQ==,i=4096";
            c.process_server_first(server_first).unwrap();

            let final_msg = c.prepare_final_message().unwrap();
            assert_eq!(
                final_msg,
                "c=biws,r=rOprNGfwEbeRWgbNEkqO%hvYDpWUa2RaTCAfuxFIlj)hNlF$k0,\
                 p=dHzbZapWIk4jUhN+Ute9ytag9zjfMHgsqmmiz7AndVQ="
            );

            let server_final = "v=6rriTRBi23WpRR/wtup+mMhUZUn/dB5nLTJRsjl95G4=";
            c.process_server_final(server_final).unwrap();
            assert!(c.is_authenticated());
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn wire_frame(kind: u8, body: &[u8]) -> Vec<u8> {
        let mut wire = vec![kind];
        wire.extend_from_slice(&((body.len() + 4) as i32).to_be_bytes());
        wire.extend_from_slice(body);
        wire
    }

    fn description(fields: &[(&str, i32)]) -> Vec<u8> {
        let mut body = (fields.len() as u16).to_be_bytes().to_vec();
        for (name, oid) in fields {
            write_cstr(&mut body, name.as_bytes());
            body.extend_from_slice(&[0; 6]);
            body.extend_from_slice(&oid.to_be_bytes());
            body.extend_from_slice(&[0; 8]);
        }
        body
    }

    fn data_row(values: &[Option<&[u8]>]) -> Vec<u8> {
        let mut row = (values.len() as u16).to_be_bytes().to_vec();
        for value in values {
            match value {
                Some(value) => {
                    row.extend_from_slice(&(value.len() as i32).to_be_bytes());
                    row.extend_from_slice(value);
                }
                None => row.extend_from_slice(&(-1i32).to_be_bytes()),
            }
        }
        row
    }

    async fn test_connection(capacity: usize, millis: u64) -> (PgConn, TcpStream) {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let (client, server) = tokio::join!(
            TcpStream::connect(listener.local_addr().unwrap()),
            listener.accept()
        );
        let read_timeout = Duration::from_millis(millis);
        (
            PgConn {
                stream: BufReader::with_capacity(capacity, client.unwrap()),
                read_timeout,
                read_timer: Box::pin(tokio::time::sleep(read_timeout)),
            },
            server.unwrap().0,
        )
    }

    #[tokio::test]
    async fn buffered_and_fragmented_queries_keep_statement_and_cycle_boundaries() {
        let desc = description(&[("value", 25)]);
        let small = data_row(&[Some(b"small")]);
        let payload = b"\0\xff\r\n".repeat(10_000);
        let large = data_row(&[Some(&payload)]);
        let notify = [42i32.to_be_bytes().as_slice(), b"chan\0payload\0"].concat();
        let mut wire = Vec::new();
        for (kind, body) in [
            (b'1', b"".as_slice()),
            (b'2', b""),
            (b'T', desc.as_slice()),
            (b'D', small.as_slice()),
            (b'D', large.as_slice()),
            (b'N', b"SNOTICE\0Mtest\0\0"),
            (b'A', notify.as_slice()),
            (b'C', b"SELECT 2\0"),
            (b'C', b"UPDATE 3\0"),
            (b'Z', b"I"),
            (b'C', b"DELETE 4\0"),
            (b'Z', b"T"),
        ] {
            wire.extend(wire_frame(kind, body));
        }
        for capacity in [1, 4, 5, 17, 16 * 1024, wire.len()] {
            let (mut conn, mut server) = test_connection(capacity, 5000).await;
            let bytes = wire.clone();
            let writer = tokio::spawn(async move { server.write_all(&bytes).await.unwrap() });
            let result = conn.execute(b"", true).await.unwrap();
            assert!(result.error.is_none());
            assert_eq!(result.txn_status, b'I');
            assert_eq!(result.statements.len(), 2);
            assert_eq!(
                result.statements[0].row_desc.as_deref(),
                Some(desc.as_slice())
            );
            assert_eq!(result.statements[0].data_rows.count, 2);
            assert_eq!(
                result.statements[0].data_rows.iter().collect::<Vec<_>>(),
                [&small[..], &large[..]]
            );
            assert_eq!(
                result.statements[1].command_tag.as_deref(),
                Some(b"UPDATE 3\0".as_slice())
            );
            assert_eq!(result.notifications.len(), 1);
            assert_eq!(result.notifications[0].payload, "payload");
            let next = conn.execute(b"", true).await.unwrap();
            assert_eq!(next.txn_status, b'T');
            assert_eq!(
                next.statements[0].command_tag.as_deref(),
                Some(b"DELETE 4\0".as_slice())
            );
            writer.await.unwrap();
        }
    }

    #[tokio::test]
    async fn discard_result_drains_large_rows_and_preserves_error_and_txn_status() {
        let row = data_row(&[Some(&vec![b'x'; 64 * 1024])]);
        let mut wire = wire_frame(b'T', &description(&[("value", 25)]));
        wire.extend(wire_frame(b'D', &row));
        wire.extend(wire_frame(b'C', b"SELECT 1\0"));
        wire.extend(wire_frame(b'E', b"SERROR\0C23505\0Mduplicate\0\0"));
        wire.extend(wire_frame(b'Z', b"E"));
        wire.extend(wire_frame(b'C', b"ROLLBACK\0"));
        wire.extend(wire_frame(b'Z', b"I"));
        for capacity in [7, wire.len()] {
            let (mut conn, mut server) = test_connection(capacity, 5000).await;
            let bytes = wire.clone();
            let writer = tokio::spawn(async move {
                server.write_all(&bytes).await.unwrap();
                // Keep the peer alive until the explicit rollback arrives.
                let mut rollback = [0u8; 14];
                server.read_exact(&mut rollback).await.unwrap();
                assert_eq!(&rollback, b"Q\0\0\0\x0dROLLBACK\0");
            });
            let result = conn.execute(b"", false).await.unwrap();
            assert!(result.statements.is_empty());
            assert!(result.notifications.is_empty());
            assert_eq!(result.error.unwrap().code.as_deref(), Some("23505"));
            assert_eq!(result.txn_status, b'E');
            conn.rollback().await.unwrap();
            writer.await.unwrap();
        }
    }

    #[tokio::test]
    async fn pooled_requests_rollback_open_transactions_and_validate_cleanup() {
        for (status, rollback_status, rollback_error) in [
            (b'T', b'I', false),
            (b'E', b'I', false),
            (b'T', b'T', false),
            (b'T', b'I', true),
        ] {
            let (mut conn, mut server) = test_connection(128, 5000).await;
            let peer = tokio::spawn(async move {
                let mut query = [0; 14];
                server.read_exact(&mut query).await.unwrap();
                assert_eq!(&query, b"Q\0\0\0\x0dROLLBACK\0");
                if rollback_error {
                    server
                        .write_all(&wire_frame(b'E', b"SERROR\0CXX000\0Mfailed\0\0"))
                        .await
                        .unwrap();
                }
                server
                    .write_all(&wire_frame(b'Z', &[rollback_status]))
                    .await
                    .unwrap();
            });
            let mut result = QueryCollector::new(true, 10).finish();
            result.txn_status = status;
            if status == b'E' {
                result.error = Some(Box::new(parse_error(b"SERROR\0C23505\0Mduplicate\0\0")));
            }
            let cleanup = conn.finish_request(&mut result).await;
            assert_eq!(cleanup.is_ok(), rollback_status == b'I' && !rollback_error);
            assert_eq!(
                result.error.as_ref().unwrap().code.as_deref(),
                Some(if status == b'E' { "23505" } else { "25000" })
            );
            peer.await.unwrap();
        }
        let (mut conn, _server) = test_connection(128, 20).await;
        let mut result = QueryCollector::new(true, 10).finish();
        conn.finish_request(&mut result).await.unwrap();
        assert!(result.error.is_none());
    }

    #[test]
    fn row_cap_preserves_completed_statements_and_discards_current_rows() {
        for capture in [true, false] {
            let mut collector = QueryCollector::new(capture, 2);
            collector.accept(b'D', &data_row(&[Some(b"1")])).unwrap();
            collector.accept(b'C', b"SELECT 1\0").unwrap();
            collector.accept(b'D', &data_row(&[Some(b"2")])).unwrap();
            collector.accept(b'D', &data_row(&[Some(b"3")])).unwrap();
            assert!(collector.row_destination(64 * 1024).is_none());
            assert!(collector.current.data_rows.bytes.is_empty());
            collector.accept(b'C', b"SELECT 2\0").unwrap();
            assert!(collector.accept(b'Z', b"I").unwrap());
            let result = collector.finish();
            assert_eq!(result.error.unwrap().code.as_deref(), Some("54000"));
            if capture {
                assert_eq!(result.statements.len(), 2);
                assert_eq!(result.statements[0].data_rows.count, 1);
                assert_eq!(result.statements[1].data_rows.count, 0);
            } else {
                assert!(result.statements.is_empty());
            }
        }
    }

    #[tokio::test]
    async fn reader_rejects_invalid_headers_and_truncated_bodies() {
        let mut oversized = vec![b'D'];
        oversized.extend_from_slice(&((MAX_MESSAGE_LEN + 5) as i32).to_be_bytes());
        for (bytes, expected) in [
            (vec![b'D', 0, 0, 0, 3], "invalid message length"),
            (oversized, "message too large"),
            (vec![b'D', 0, 0], "socket read failed"),
            (vec![b'D', 0, 0, 0, 9, b'x'], "EOF"),
            (wire_frame(b'Z', b""), "invalid ReadyForQuery"),
            (wire_frame(b'Z', b"X"), "invalid ReadyForQuery"),
        ] {
            for capture in [true, false] {
                let (mut conn, mut server) = test_connection(16, 1000).await;
                server.write_all(&bytes).await.unwrap();
                server.shutdown().await.unwrap();
                let error = conn.execute(b"", capture).await.err().unwrap();
                assert!(error.contains(expected), "{error}");
            }
        }
    }

    #[tokio::test]
    async fn startup_auth_and_query_share_buffered_messages() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let params = ConnParams {
            host: "127.0.0.1".to_string(),
            port: listener.local_addr().unwrap().port(),
            user: "tester".to_string(),
            password: String::new(),
            database: "test".to_string(),
            application_name: "pg-test".to_string(),
        };
        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let len = stream.read_u32().await.unwrap() as usize;
            let mut startup = vec![0; len - 4];
            stream.read_exact(&mut startup).await.unwrap();
            assert_eq!(read_i32(&startup, 0), 196608);
            let mut reply = wire_frame(b'R', &0i32.to_be_bytes());
            reply.extend(wire_frame(b'S', b"client_encoding\0UTF8\0"));
            reply.extend(wire_frame(b'K', &[0; 8]));
            reply.extend(wire_frame(b'Z', b"I"));
            stream.write_all(&reply).await.unwrap();
            let mut query = [0u8; 14];
            stream.read_exact(&mut query).await.unwrap();
            assert_eq!(&query, b"Q\0\0\0\x0dSELECT 1\0");
            let mut reply = wire_frame(b'T', &description(&[("one", 23)]));
            reply.extend(wire_frame(b'D', &data_row(&[Some(b"1")])));
            reply.extend(wire_frame(b'C', b"SELECT 1\0"));
            reply.extend(wire_frame(b'Z', b"I"));
            stream.write_all(&reply).await.unwrap();
        });
        let mut conn = PgConn::connect(&params, 1000, 1000).await.unwrap();
        let result = conn
            .execute(&wire_frame(b'Q', b"SELECT 1\0"), true)
            .await
            .unwrap();
        assert_eq!(result.statements[0].data_rows.count, 1);
        assert_eq!(
            result.statements[0].data_rows.iter().next().unwrap(),
            data_row(&[Some(b"1")])
        );
        server.await.unwrap();
    }

    #[tokio::test]
    async fn fully_buffered_response_does_not_rearm_timer() {
        let (mut conn, mut server) = test_connection(1024, 1000).await;
        let mut wire = wire_frame(b'C', b"UPDATE 2\0");
        wire.extend(wire_frame(b'Z', b"I"));
        server.write_all(&wire).await.unwrap();
        assert_eq!(conn.stream.fill_buf().await.unwrap(), wire);
        let deadline = conn.read_timer.deadline();
        assert_eq!(conn.execute(b"", true).await.unwrap().statements.len(), 1);
        assert_eq!(conn.read_timer.deadline(), deadline);
    }

    #[tokio::test]
    async fn body_fragments_do_not_restart_timeout() {
        let (mut conn, mut server) = test_connection(8, 150).await;
        // Header declares a body of 100 bytes; each fragment arrives in less
        // than 150ms, but the body as a whole never completes within its budget.
        server.write_all(&[b'D', 0, 0, 0, 104]).await.unwrap();
        let writer = tokio::spawn(async move {
            loop {
                tokio::time::sleep(Duration::from_millis(40)).await;
                if server.write_all(b"x").await.is_err() {
                    break;
                }
            }
        });
        let result = timeout(Duration::from_secs(2), conn.execute(b"", true))
            .await
            .unwrap();
        writer.abort();
        let _ = writer.await;
        assert_eq!(result.err().unwrap(), "socket read timed out");
    }

    fn test_lua() -> (LuaState, laux::LuaGlobalState) {
        let state = LuaState::new(unsafe { ffi::luaL_newstate() }).unwrap();
        let owner = laux::LuaGlobalState::new(state);
        unsafe { ffi::luaL_openlibs(state.as_ptr()) };
        (state, owner)
    }

    fn check_lua(state: LuaState, code: &str) {
        let code = std::ffi::CString::new(code).unwrap();
        let result = unsafe { ffi::luaL_dostring(state.as_ptr(), code.as_ptr()) };
        assert_eq!(
            result,
            ffi::LUA_OK,
            "{}",
            unsafe { LuaStack::from_raw(state) }.value(-1)
        );
    }

    #[test]
    fn compact_rows_decode_with_cached_keys_nulls_and_returning_counts() {
        let (state, _owner) = test_lua();
        let mut collector = QueryCollector::new(true, 10);
        collector
            .accept(
                b'T',
                &description(&[("id", 20), ("text", 25), ("flag", 16)]),
            )
            .unwrap();
        for values in [
            [
                Some(b"9223372036854775807".as_slice()),
                Some(b"a\0b"),
                Some(b"t"),
            ],
            [Some(b"-42".as_slice()), None, Some(b"f")],
        ] {
            collector.accept(b'D', &data_row(&values)).unwrap();
        }
        collector.accept(b'C', b"INSERT 0 2\0").unwrap();
        collector.accept(b'C', b"BEGIN\0").unwrap();
        collector.accept(b'Z', b"I").unwrap();
        laux::lua_push(state, "sentinel");
        assert_eq!(
            push_pg_response(state, PgResponse::Result(collector.finish())),
            1
        );
        assert_eq!(laux::lua_top(state), 2);
        unsafe { ffi::lua_setglobal(state.as_ptr(), cstr!("result")) };
        check_lua(
            state,
            r#"
            assert(result.num_queries == 2)
            assert(result.data[1].affected_rows == 2)
            assert(result.data[1][1].id == math.maxinteger)
            assert(result.data[1][1].text == "a\0b")
            assert(result.data[1][1].flag == true)
            assert(result.data[1][2].id == -42)
            assert(result.data[1][2].text == nil)
            assert(result.data[1][2].flag == false)
            assert(result.data[2] == true)
        "#,
        );
        assert_eq!(laux::lua_top(state), 1);
    }

    #[test]
    fn malformed_rows_return_protocol_error_and_restore_lua_stack() {
        let good = data_row(&[Some(b"ok")]);
        let mut trailing = good.clone();
        trailing.push(0);
        for row in [
            vec![],
            data_row(&[]),
            vec![0, 1, 0, 0, 0],
            vec![0, 1, 0, 0, 0, 9, b'x'],
            vec![0, 1, 255, 255, 255, 254],
            trailing,
        ] {
            let (state, _owner) = test_lua();
            let mut collector = QueryCollector::new(true, 10);
            collector
                .accept(b'T', &description(&[("value", 25)]))
                .unwrap();
            // Two rows force the cached-key path, including its error cleanup.
            collector.accept(b'D', &good).unwrap();
            collector.accept(b'D', &row).unwrap();
            collector.accept(b'C', b"SELECT 2\0").unwrap();
            laux::lua_push(state, "sentinel");
            assert_eq!(
                push_pg_response(state, PgResponse::Result(collector.finish())),
                1
            );
            assert_eq!(laux::lua_top(state), 2);
            unsafe { ffi::lua_setglobal(state.as_ptr(), cstr!("result")) };
            check_lua(
                state,
                "assert(result.code == 'PROTOCOL'); assert(result.data == nil)",
            );
            assert_eq!(laux::lua_top(state), 1);
        }
    }

    #[test]
    fn row_description_checks_every_truncation_and_borrows_valid_names() {
        let body = description(&[("first", 23), ("second", 25)]);
        for len in 0..body.len() {
            assert!(parse_row_desc(&body[..len]).is_err(), "length {len}");
        }
        let fields = parse_row_desc(&body).unwrap();
        assert!(matches!(fields[0].0, Cow::Borrowed("first")));
        let mut invalid_utf8 = description(&[("x", 25)]);
        invalid_utf8[2] = 255;
        assert_eq!(parse_row_desc(&invalid_utf8).unwrap()[0].0, "\u{fffd}");
        assert!(parse_notification(b"\0\0\0\0channel").is_none());
        assert!(parse_notification(b"\0\0\0\0channel\0payload").is_none());
    }

    #[test]
    fn numeric_parameters_preserve_existing_text_format() {
        let (state, _owner) = test_lua();
        let mut lua = unsafe { LuaStack::from_raw(state) };
        let options = JsonOptions::default();
        let mut encoded = Vec::new();
        for number in [i64::MIN, -1, 0, 1, i64::MAX] {
            laux::lua_push(state, number);
            encoded.clear();
            write_param(&mut encoded, &mut lua, -1, &options).unwrap();
            let expected = number.to_string();
            assert_eq!(read_i32(&encoded, 0) as usize, expected.len());
            assert_eq!(&encoded[4..], expected.as_bytes());
            laux::lua_pop(state, 1);
        }
        for number in [
            0.0,
            -0.0,
            1.25,
            1e-300,
            1e300,
            f64::MAX,
            f64::MIN_POSITIVE,
            f64::from_bits(1),
            f64::NAN,
            f64::INFINITY,
            f64::NEG_INFINITY,
        ] {
            laux::lua_push(state, number);
            encoded.clear();
            write_param(&mut encoded, &mut lua, -1, &options).unwrap();
            let expected = number.to_string();
            assert_eq!(read_i32(&encoded, 0) as usize, expected.len());
            assert_eq!(&encoded[4..], expected.as_bytes());
            laux::lua_pop(state, 1);
        }
        laux::lua_push(state, 42i64);
        laux::lua_push(state, "value");
        encoded.clear();
        append_statement(&mut encoded, &mut lua, b"SELECT $1, $2", 1..3, &options).unwrap();
        let parse_len = read_i32(&encoded, 1) as usize + 1;
        let bind = &encoded[parse_len..];
        assert_eq!(bind[0], b'B');
        assert_eq!(read_u16(bind, 9), 2);
        assert_eq!(&bind[11..26], b"\0\0\0\x0242\0\0\0\x05value");
    }

    #[test]
    fn parse_url_full() {
        let cfg = ConnectConfig::parse(
            "postgres://alice:s3cret@db.host:6543/shop?application_name=svc&name=main\
             &connect_timeout=3000&max_connections=8&read_timeout=20000&queue_capacity=2048",
        )
        .unwrap();
        assert_eq!(cfg.params.host, "db.host");
        assert_eq!(cfg.params.port, 6543);
        assert_eq!(cfg.params.user, "alice");
        assert_eq!(cfg.params.password, "s3cret");
        assert_eq!(cfg.params.database, "shop");
        assert_eq!(cfg.params.application_name, "svc");
        assert_eq!(cfg.name, "main");
        assert_eq!(cfg.connect_timeout_ms, 3000);
        assert_eq!(cfg.max_connections, 8);
        assert_eq!(cfg.read_timeout_ms, 20000);
        assert_eq!(cfg.queue_capacity, 2048);
    }

    #[test]
    fn parse_url_defaults_and_alias() {
        // `postgresql` scheme alias, `pool_size` param alias, defaults elsewhere.
        let cfg = ConnectConfig::parse(
            "postgresql://postgres:123456@127.0.0.1/postgres?name=c&pool_size=3",
        )
        .unwrap();
        assert_eq!(cfg.params.port, 5432);
        assert_eq!(cfg.params.application_name, "moon");
        assert_eq!(cfg.name, "c");
        assert_eq!(cfg.connect_timeout_ms, 5000);
        assert_eq!(cfg.max_connections, 3);
    }

    #[test]
    fn parse_url_percent_encoded_password() {
        let cfg = ConnectConfig::parse("postgres://u:p%40ss%2Fword@h/db?name=c").unwrap();
        assert_eq!(cfg.params.password, "p@ss/word");
    }

    #[test]
    fn parse_url_errors() {
        assert!(ConnectConfig::parse("mysql://u:p@h/db?name=c").is_err()); // bad scheme
        assert!(ConnectConfig::parse("postgres://h/db?name=c").is_err()); // no user
        assert!(ConnectConfig::parse("postgres://u@h?name=c").is_err()); // no database
        assert!(ConnectConfig::parse("postgres://u:p@h/db").is_err()); // no name
        assert!(ConnectConfig::parse("postgres://u:p@h/db?name=c&sslmode=require").is_err()); // unknown param
        assert!(ConnectConfig::parse("not_a_url").is_err()); // not a url
        assert!(ConnectConfig::parse("postgres://u:p@h/db?name=c&port=abc").is_err()); // 'port' is not a query param
        assert!(ConnectConfig::parse("postgres://u:p@h/db?name=c&connect_timeout=abc").is_err()); // bad number
    }

    #[test]
    fn md5_known_vector() {
        assert_eq!(md5_hex(b""), "d41d8cd98f00b204e9800998ecf8427e");
        assert_eq!(md5_hex(b"abc"), "900150983cd24fb0d6963f7d28e17f72");
    }

    #[test]
    fn command_tag_parsing() {
        assert_eq!(
            parse_command_tag(b"INSERT 0 5\0"),
            ("INSERT".into(), Some(5))
        );
        assert_eq!(parse_command_tag(b"UPDATE 2\0"), ("UPDATE".into(), Some(2)));
        assert_eq!(parse_command_tag(b"SELECT 3\0"), ("SELECT".into(), Some(3)));
        assert_eq!(parse_command_tag(b"BEGIN\0"), ("BEGIN".into(), None));
    }

    #[test]
    fn error_response_parsing() {
        // S<severity>\0 C<code>\0 M<message>\0 \0
        let body = b"SERROR\0C23505\0Mduplicate key\0\0";
        let err = parse_error(body);
        assert_eq!(err.severity.as_deref(), Some("ERROR"));
        assert_eq!(err.code.as_deref(), Some("23505"));
        assert_eq!(err.message.as_deref(), Some("duplicate key"));
        assert_eq!(parse_error_string(body), "duplicate key");
    }

    #[test]
    fn row_description_parsing() {
        // 1 field named "id", type OID 23 (int4).
        let mut body = Vec::new();
        body.extend_from_slice(&1u16.to_be_bytes()); // field count
        body.extend_from_slice(b"id\0"); // name
        body.extend_from_slice(&0i32.to_be_bytes()); // table OID
        body.extend_from_slice(&0i16.to_be_bytes()); // column attr
        body.extend_from_slice(&23i32.to_be_bytes()); // type OID
        body.extend_from_slice(&4i16.to_be_bytes()); // type size
        body.extend_from_slice(&(-1i32).to_be_bytes()); // type mod
        body.extend_from_slice(&0i16.to_be_bytes()); // format
        let fields = parse_row_desc(&body).unwrap();
        assert_eq!(fields, vec![(Cow::Borrowed("id"), 23)]);
    }

    #[test]
    fn notification_parsing() {
        let mut body = Vec::new();
        body.extend_from_slice(&42i32.to_be_bytes());
        body.extend_from_slice(b"chan\0payload\0");
        let n = parse_notification(&body).unwrap();
        assert_eq!(n.pid, 42);
        assert_eq!(n.channel, "chan");
        assert_eq!(n.payload, "payload");
    }

    #[test]
    fn conflict_clause_accepts_valid() {
        assert!(
            validate_conflict_clause("ON CONFLICT (uid,key) DO UPDATE SET value = EXCLUDED.value")
                .is_ok()
        );
        // Case-insensitive prefix and surrounding whitespace are allowed.
        assert!(validate_conflict_clause("  on conflict do nothing  ").is_ok());
        assert!(validate_conflict_clause("ON CONFLICT ON CONSTRAINT pk DO NOTHING").is_ok());
    }

    #[test]
    fn conflict_clause_rejects_bad_prefix() {
        assert!(validate_conflict_clause("").is_err());
        assert!(validate_conflict_clause("DO NOTHING").is_err());
        assert!(validate_conflict_clause("ON CONF").is_err()); // shorter than prefix
        // A whole injected statement that doesn't start with ON CONFLICT.
        assert!(validate_conflict_clause("; DROP TABLE users").is_err());
    }

    #[test]
    fn conflict_clause_rejects_injection_tokens() {
        assert!(validate_conflict_clause("ON CONFLICT DO NOTHING; DROP TABLE users").is_err());
        assert!(validate_conflict_clause("ON CONFLICT DO NOTHING -- comment").is_err());
        assert!(validate_conflict_clause("ON CONFLICT DO NOTHING /* block */").is_err());
    }

    #[test]
    fn build_insert_sql_numbers_placeholders_and_appends_conflict() {
        let cols = vec!["a".to_string(), "b".to_string()];
        let sql = build_insert_sql("t", &cols, 2, None);
        assert_eq!(
            sql,
            "INSERT INTO \"t\" (\"a\",\"b\") VALUES ($1,$2),($3,$4)"
        );

        let sql = build_insert_sql("t", &cols, 1, Some("ON CONFLICT DO NOTHING"));
        assert_eq!(
            sql,
            "INSERT INTO \"t\" (\"a\",\"b\") VALUES ($1,$2) ON CONFLICT DO NOTHING"
        );
    }

    #[test]
    fn quote_ident_escapes_double_quotes() {
        assert_eq!(quote_ident("col"), "\"col\"");
        assert_eq!(quote_ident("we\"ird"), "\"we\"\"ird\"");
    }

    // -- quote_ident edge cases -----------------------------------------------

    #[test]
    fn quote_ident_empty_string() {
        assert_eq!(quote_ident(""), "\"\"");
    }

    #[test]
    fn quote_ident_multiple_double_quotes() {
        assert_eq!(quote_ident("a\"b\"c"), "\"a\"\"b\"\"c\"");
    }

    #[test]
    fn quote_ident_special_chars() {
        assert_eq!(quote_ident("my col"), "\"my col\"");
        assert_eq!(quote_ident("table-name"), "\"table-name\"");
    }

    // -- validate_type_name ---------------------------------------------------

    #[test]
    fn validate_type_name_valid() {
        assert!(validate_type_name("bigint").is_ok());
        assert!(validate_type_name("character varying").is_ok());
        assert!(validate_type_name("integer[]").is_ok());
        assert!(validate_type_name("text").is_ok());
        assert!(validate_type_name("double_precision").is_ok());
    }

    #[test]
    fn validate_type_name_rejects_empty() {
        assert!(validate_type_name("").is_err());
    }

    #[test]
    fn validate_type_name_rejects_injection() {
        assert!(validate_type_name("int; DROP TABLE").is_err());
        assert!(validate_type_name("int--comment").is_err());
        assert!(validate_type_name("int'").is_err());
    }

    // -- build_update_sql -----------------------------------------------------

    #[test]
    fn build_update_sql_single_row_with_key_type() {
        let set_cols = vec!["name".to_string(), "value".to_string()];
        let sql = build_update_sql("items", "id", &set_cols, 1, Some("bigint"));
        assert_eq!(
            sql,
            "UPDATE \"items\" AS _t SET \"name\" = _d.\"name\", \"value\" = _d.\"value\" \
             FROM (VALUES ($1,$2,$3)) AS _d(_k, \"name\", \"value\") \
             WHERE _t.\"id\" = _d._k::bigint"
        );
    }

    #[test]
    fn build_update_sql_multi_row_without_key_type() {
        let set_cols = vec!["score".to_string()];
        let sql = build_update_sql("players", "uid", &set_cols, 3, None);
        assert_eq!(
            sql,
            "UPDATE \"players\" AS _t SET \"score\" = _d.\"score\" \
             FROM (VALUES ($1,$2),($3,$4),($5,$6)) AS _d(_k, \"score\") \
             WHERE _t.\"uid\"::text = _d._k"
        );
    }

    #[test]
    fn build_update_sql_quoted_identifiers() {
        let set_cols = vec!["col\"x".to_string()];
        let sql = build_update_sql("my\"table", "k\"ey", &set_cols, 1, None);
        assert!(sql.contains("\"my\"\"table\""));
        assert!(sql.contains("\"k\"\"ey\""));
        assert!(sql.contains("\"col\"\"x\""));
    }

    // -- build_insert_sql edge cases ------------------------------------------

    #[test]
    fn build_insert_sql_single_column_single_row() {
        let cols = vec!["x".to_string()];
        let sql = build_insert_sql("t", &cols, 1, None);
        assert_eq!(sql, "INSERT INTO \"t\" (\"x\") VALUES ($1)");
    }

    #[test]
    fn build_insert_sql_many_rows() {
        let cols = vec!["a".to_string(), "b".to_string(), "c".to_string()];
        let sql = build_insert_sql("t", &cols, 3, None);
        assert_eq!(
            sql,
            "INSERT INTO \"t\" (\"a\",\"b\",\"c\") VALUES ($1,$2,$3),($4,$5,$6),($7,$8,$9)"
        );
    }

    // -- parse_command_tag additional cases ------------------------------------

    #[test]
    fn command_tag_delete() {
        assert_eq!(
            parse_command_tag(b"DELETE 10\0"),
            ("DELETE".into(), Some(10))
        );
    }

    #[test]
    fn command_tag_create_table() {
        assert_eq!(
            parse_command_tag(b"CREATE TABLE\0"),
            ("CREATE".into(), None)
        );
    }

    #[test]
    fn command_tag_copy() {
        assert_eq!(parse_command_tag(b"COPY 100\0"), ("COPY".into(), Some(100)));
    }

    #[test]
    fn command_tag_no_null_terminator() {
        let (cmd, rows) = parse_command_tag(b"UPDATE 5");
        assert_eq!(cmd, "UPDATE");
        assert_eq!(rows, Some(5));
    }

    // -- parse_error additional fields ----------------------------------------

    #[test]
    fn error_response_all_fields() {
        let body = b"SERROR\0C42P01\0Mtable not found\0P15\0Dmore info\0sfoo_schema\0tbar_table\0nmy_constraint\0\0";
        let err = parse_error(body);
        assert_eq!(err.severity.as_deref(), Some("ERROR"));
        assert_eq!(err.code.as_deref(), Some("42P01"));
        assert_eq!(err.message.as_deref(), Some("table not found"));
        assert_eq!(err.position.as_deref(), Some("15"));
        assert_eq!(err.detail.as_deref(), Some("more info"));
        assert_eq!(err.schema.as_deref(), Some("foo_schema"));
        assert_eq!(err.table.as_deref(), Some("bar_table"));
        assert_eq!(err.constraint.as_deref(), Some("my_constraint"));
    }

    #[test]
    fn error_response_empty_body() {
        let err = parse_error(b"\0");
        assert!(err.severity.is_none());
        assert!(err.code.is_none());
        assert!(err.message.is_none());
    }

    #[test]
    fn parse_error_string_missing_message_field() {
        let body = b"SERROR\0C12345\0\0"; // no M field
        assert_eq!(parse_error_string(body), "unknown database error");
    }

    // -- parse_notification edge cases ----------------------------------------

    #[test]
    fn notification_empty_payload() {
        let mut body = Vec::new();
        body.extend_from_slice(&1i32.to_be_bytes());
        body.extend_from_slice(b"test_channel\0\0");
        let n = parse_notification(&body).unwrap();
        assert_eq!(n.pid, 1);
        assert_eq!(n.channel, "test_channel");
        assert_eq!(n.payload, "");
    }

    #[test]
    fn notification_too_short() {
        assert!(parse_notification(b"abc").is_none()); // < 5 bytes
    }

    #[test]
    fn notification_with_unicode() {
        let mut body = Vec::new();
        body.extend_from_slice(&99i32.to_be_bytes());
        body.extend_from_slice("日本語\0メッセージ\0".as_bytes());
        let n = parse_notification(&body).unwrap();
        assert_eq!(n.pid, 99);
        assert_eq!(n.channel, "日本語");
        assert_eq!(n.payload, "メッセージ");
    }

    // -- parse_row_desc edge cases --------------------------------------------

    #[test]
    fn row_description_multiple_fields() {
        let mut body = Vec::new();
        body.extend_from_slice(&2u16.to_be_bytes()); // 2 fields
        // Field 1: "name", OID 25 (text)
        body.extend_from_slice(b"name\0");
        body.extend_from_slice(&0i32.to_be_bytes());
        body.extend_from_slice(&0i16.to_be_bytes());
        body.extend_from_slice(&25i32.to_be_bytes());
        body.extend_from_slice(&(-1i16).to_be_bytes());
        body.extend_from_slice(&(-1i32).to_be_bytes());
        body.extend_from_slice(&0i16.to_be_bytes());
        // Field 2: "age", OID 23 (int4)
        body.extend_from_slice(b"age\0");
        body.extend_from_slice(&0i32.to_be_bytes());
        body.extend_from_slice(&0i16.to_be_bytes());
        body.extend_from_slice(&23i32.to_be_bytes());
        body.extend_from_slice(&4i16.to_be_bytes());
        body.extend_from_slice(&(-1i32).to_be_bytes());
        body.extend_from_slice(&0i16.to_be_bytes());

        let fields = parse_row_desc(&body).unwrap();
        assert_eq!(fields.len(), 2);
        assert_eq!(fields[0], (Cow::Borrowed("name"), 25));
        assert_eq!(fields[1], (Cow::Borrowed("age"), 23));
    }

    #[test]
    fn row_description_empty() {
        assert!(parse_row_desc(b"").is_err());
        assert!(parse_row_desc(b"\x00").is_err()); // < 2 bytes
    }

    #[test]
    fn row_description_zero_fields() {
        let body = 0u16.to_be_bytes();
        let fields = parse_row_desc(&body).unwrap();
        assert!(fields.is_empty());
    }

    // -- parse_url edge cases -------------------------------------------------

    #[test]
    fn parse_url_ipv6_host() {
        let cfg = ConnectConfig::parse("postgres://user:pass@[::1]:5433/mydb?name=n").unwrap();
        assert_eq!(cfg.params.host, "[::1]");
        assert_eq!(cfg.params.port, 5433);
    }

    #[test]
    fn parse_url_clamps_pool_and_queue() {
        let cfg =
            ConnectConfig::parse("postgres://u:p@h/d?name=n&max_connections=0&queue_capacity=0")
                .unwrap();
        assert_eq!(cfg.max_connections, 1);
        assert_eq!(cfg.queue_capacity, 1);
    }

    // -- start_message / end_message ------------------------------------------

    #[test]
    fn start_end_message_encodes_length_correctly() {
        let mut buf = Vec::new();
        let stub = start_message(&mut buf, b'Q');
        buf.extend_from_slice(b"SELECT 1\0");
        end_message(&mut buf, stub);
        // msg_type + 4 bytes length + payload
        assert_eq!(buf[0], b'Q');
        let len = u32::from_be_bytes([buf[1], buf[2], buf[3], buf[4]]);
        // length includes itself (4 bytes) + "SELECT 1\0" (9 bytes) = 13
        assert_eq!(len, 13);
    }

    #[test]
    fn append_sync_produces_5_bytes() {
        let mut buf = Vec::new();
        append_sync(&mut buf);
        assert_eq!(buf.len(), 5); // 'S' + 4-byte length(4)
        assert_eq!(buf[0], PQ_SYNC);
        let len = u32::from_be_bytes([buf[1], buf[2], buf[3], buf[4]]);
        assert_eq!(len, 4);
    }

    #[test]
    fn append_parse_unnamed_format() {
        let mut buf = Vec::new();
        append_parse_unnamed(&mut buf, b"SELECT $1");
        assert_eq!(buf[0], PQ_PARSE);
        // Body: length(4) + "" NUL (1) + "SELECT $1" NUL (10) + 0u16 (2) = 17
        let len = u32::from_be_bytes([buf[1], buf[2], buf[3], buf[4]]);
        assert_eq!(len, 17);
        // stmt name = empty string + NUL
        assert_eq!(buf[5], 0); // empty statement name
        // query starts at 6
        assert_eq!(&buf[6..15], b"SELECT $1");
        assert_eq!(buf[15], 0); // query NUL terminator
    }

    // -- md5_hex additional ---------------------------------------------------

    #[test]
    fn md5_hex_hello_world() {
        assert_eq!(md5_hex(b"hello"), "5d41402abc4b2a76b9719d911017c592");
    }

    // -- conflict_clause edge cases -------------------------------------------

    #[test]
    fn conflict_clause_case_variations() {
        assert!(validate_conflict_clause("On Conflict DO NOTHING").is_ok());
        assert!(validate_conflict_clause("on conflict do nothing").is_ok());
    }

    #[test]
    fn conflict_clause_rejects_block_comment_at_end() {
        assert!(validate_conflict_clause("ON CONFLICT DO NOTHING /**/").is_err());
    }

    // -- write_cstr -----------------------------------------------------------

    #[test]
    fn write_cstr_appends_nul() {
        let mut buf = Vec::new();
        write_cstr(&mut buf, b"hello");
        assert_eq!(buf, b"hello\0");
    }

    #[test]
    fn write_cstr_empty() {
        let mut buf = Vec::new();
        write_cstr(&mut buf, b"");
        assert_eq!(buf, b"\0");
    }

    // -- read_i32 / read_u16 --------------------------------------------------

    #[test]
    fn read_i32_big_endian() {
        let buf = [0x00, 0x01, 0x00, 0x00];
        assert_eq!(read_i32(&buf, 0), 65536);
    }

    #[test]
    fn read_i32_negative() {
        let buf = (-1i32).to_be_bytes();
        assert_eq!(read_i32(&buf, 0), -1);
    }

    #[test]
    fn read_u16_big_endian() {
        let buf = [0x01, 0x00];
        assert_eq!(read_u16(&buf, 0), 256);
    }
}
