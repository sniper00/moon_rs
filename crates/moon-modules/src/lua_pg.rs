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
use moon_lua::laux::LuaState;
use moon_lua::{
    cstr, ffi, laux,
    laux::{LuaTable, LuaValue},
    lreg, lreg_null, luaL_newlib, push_lua_table,
};
use moon_runtime::actor::LuaActor;
use moon_runtime::context::{self, ActorId, CONTEXT};
use std::{ffi::c_int, sync::Arc, time::Duration};
use tokio::io::{AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::net::TcpStream;
use tokio::sync::mpsc;
use tokio::time::timeout;

lazy_static! {
    static ref PG_CONNECTIONS: DashMap<String, PgPool> = DashMap::new();
}

// ---------------------------------------------------------------------------
// Connection parameters (parsed from a sqlx-style URL)
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

impl ConnParams {
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

        let application_name = url
            .query_pairs()
            .find(|(k, _)| k == "application_name")
            .map(|(_, v)| v.into_owned())
            .unwrap_or_else(|| "moon".to_string());

        Ok(ConnParams {
            host,
            port,
            user,
            password,
            database,
            application_name,
        })
    }
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

    fn pending(&self) -> i64 {
        self.inner.pending()
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
struct Statement {
    row_desc: Option<Vec<u8>>,
    data_rows: Vec<Vec<u8>>,
    command_tag: Option<Vec<u8>>,
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

enum PgResponse {
    Connect,
    /// Configuration / URL parse failure.
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

    /// Reads one backend message: 1-byte type + i32 length (incl. itself) + body.
    async fn read_message(&mut self) -> Result<(u8, Vec<u8>), String> {
        let deadline = self.read_timeout;
        let mut header = [0u8; 5];
        timeout(deadline, self.stream.read_exact(&mut header))
            .await
            .map_err(|_| "socket read timed out".to_string())?
            .map_err(|e| format!("socket read failed: {}", e))?;
        let t = header[0];
        let len = i32::from_be_bytes([header[1], header[2], header[3], header[4]]);
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
        let mut body = vec![0u8; body_len];
        if body_len > 0 {
            timeout(deadline, self.stream.read_exact(&mut body))
                .await
                .map_err(|_| "socket read timed out".to_string())?
                .map_err(|e| format!("socket read failed: {}", e))?;
        }
        Ok((t, body))
    }

    /// Send a simple-query ROLLBACK to clear an aborted transaction.
    async fn rollback(&mut self) -> Result<(), String> {
        let sql = b"ROLLBACK\0";
        let len = (sql.len() as u32) + 4;
        let mut buf = Vec::with_capacity(1 + 4 + sql.len());
        buf.push(b'Q');
        buf.extend_from_slice(&len.to_be_bytes());
        buf.extend_from_slice(sql);
        self.write_all(&buf).await?;
        loop {
            let (t, _) = self.read_message().await?;
            if t == b'Z' {
                break;
            }
        }
        Ok(())
    }

    /// Write a prebuilt request buffer, then read the reply until ReadyForQuery.
    async fn execute(&mut self, data: &[u8]) -> Result<QueryResult, String> {
        self.write_all(data).await?;

        let mut statements: Vec<Statement> = Vec::new();
        let mut notifications: Vec<Notification> = Vec::new();
        let mut error: Option<Box<DbError>> = None;
        let mut cur_row_desc: Option<Vec<u8>> = None;
        let mut cur_rows: Vec<Vec<u8>> = Vec::new();
        let mut txn_status = b'I';
        // Total DataRow messages accumulated across all statements in this reply.
        let mut total_rows: usize = 0;

        loop {
            let (t, body) = self.read_message().await?;
            match t {
                b'D' => {
                    total_rows += 1;
                    if total_rows > crate::LIMITS.db_query_rows {
                        return Err(format!(
                            "query returned more than {} rows; use a streaming/paginated query for large result sets",
                            crate::LIMITS.db_query_rows
                        ));
                    }
                    cur_rows.push(body);
                }
                b'T' => cur_row_desc = Some(body),
                b'E' => error = Some(Box::new(parse_error(&body))),
                b'C' => {
                    statements.push(Statement {
                        row_desc: cur_row_desc.take(),
                        data_rows: std::mem::take(&mut cur_rows),
                        command_tag: Some(body),
                    });
                }
                b'A' => {
                    if let Some(n) = parse_notification(&body) {
                        notifications.push(n);
                    }
                }
                b'Z' => {
                    if !body.is_empty() {
                        txn_status = body[0];
                    }
                    break;
                }
                _ => {}
            }
        }

        Ok(QueryResult {
            statements,
            notifications,
            error,
            txn_status,
        })
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
    if body.len() < 5 {
        return None;
    }
    let pid = read_i32(body, 0);
    let mut i = 4;
    let start = i;
    while i < body.len() && body[i] != 0 {
        i += 1;
    }
    let channel = String::from_utf8_lossy(&body[start..i]).into_owned();
    i += 1;
    let pstart = i;
    while i < body.len() && body[i] != 0 {
        i += 1;
    }
    let payload = String::from_utf8_lossy(&body[pstart..i]).into_owned();
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
                                log::error!(
                                    "pg '{}' reconnect failed: {}. retrying. ({}:{})",
                                    name,
                                    e,
                                    file!(),
                                    line!()
                                );
                            }
                            failed_times += 1;
                            tokio::time::sleep(Duration::from_secs(1)).await;
                            continue;
                        }
                    }
                }
            }

            let c = conn.as_mut().unwrap();
            match c.execute(&req.data).await {
                Ok(result) => {
                    if result.txn_status == b'E' {
                        if c.rollback().await.is_err() {
                            conn = None;
                        }
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
                            "pg '{}' execute error: {} ({}:{})",
                            name,
                            err.message.as_deref().unwrap_or("unknown"),
                            file!(),
                            line!()
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
                            log::error!(
                                "pg '{}' socket error: {}. retrying. ({}:{})",
                                name,
                                e,
                                file!(),
                                line!()
                            );
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
    state: LuaState,
    idx: i32,
    options: &JsonOptions,
) -> Result<(), String> {
    let val = LuaValue::from_stack(state, idx);
    let is_null = matches!(val, LuaValue::Nil)
        || matches!(val, LuaValue::None)
        || matches!(val, LuaValue::LightUserData(p) if p.is_null());
    if is_null {
        buf.extend_from_slice(&(-1i32).to_be_bytes());
        return Ok(());
    }

    let stub = buf.len();
    buf.extend_from_slice(&[0u8; 4]); // length placeholder
    match val {
        LuaValue::Integer(v) => buf.extend_from_slice(v.to_string().as_bytes()),
        LuaValue::Number(v) => buf.extend_from_slice(v.to_string().as_bytes()),
        LuaValue::Boolean(v) => buf.extend_from_slice(if v { b"true" } else { b"false" }),
        LuaValue::String(s) => buf.extend_from_slice(s),
        LuaValue::Table(t) => {
            encode_table(buf, &t, 0, false, options)?;
        }
        other => {
            return Err(format!("unsupported parameter type: {}", other.name()));
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
    state: LuaState,
    sql: &[u8],
    param_indices: &[i32],
    options: &JsonOptions,
) -> Result<(), String> {
    // Parse
    let stub = start_message(buf, PQ_PARSE);
    write_cstr(buf, b""); // unnamed statement
    write_cstr(buf, sql);
    buf.extend_from_slice(&0u16.to_be_bytes()); // no parameter type OIDs
    end_message(buf, stub);

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
    for &i in param_indices {
        write_param(buf, state, i, options)?;
    }
    buf.extend_from_slice(&1u16.to_be_bytes()); // one result format code
    buf.extend_from_slice(&0u16.to_be_bytes()); // text
    end_message(buf, stub);

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

    Ok(())
}

fn append_sync(buf: &mut Vec<u8>) {
    let stub = start_message(buf, PQ_SYNC);
    end_message(buf, stub);
}

/// Parse `sql` into the unnamed statement (no parameter type OIDs). Used by
/// `batch`, which parses once and then binds it many times within one Sync.
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
    state: LuaState,
    buf: &mut Vec<u8>,
    rows_idx: i32,
    nrows: usize,
    cols_per_tuple: usize,
    options: &JsonOptions,
    build_sql: &dyn Fn(usize) -> String,
) -> Result<(), String> {
    if cols_per_tuple > MAX_BIND_PARAMS {
        return Err(format!(
            "too many columns per row ({cols_per_tuple}); max is {MAX_BIND_PARAMS}"
        ));
    }
    let max_per_chunk = (MAX_BIND_PARAMS / cols_per_tuple).max(1);
    let total_chunks = nrows.div_ceil(max_per_chunk);
    let multi = total_chunks > 1;

    if multi {
        append_statement(buf, state, b"BEGIN", &[], options)?;
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
                let res = write_param(buf, state, vtop, options);
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

        // Describe (portal) — required so the server sends RowDescription
        // before DataRow when the statement has a RETURNING clause.
        let stub = start_message(buf, PQ_DESCRIBE);
        buf.push(b'P');
        write_cstr(buf, b"");
        end_message(buf, stub);

        // Execute
        let stub = start_message(buf, PQ_EXECUTE);
        write_cstr(buf, b""); // portal
        buf.extend_from_slice(&0u32.to_be_bytes()); // unlimited rows
        end_message(buf, stub);

        start += len;
    }

    if multi {
        append_statement(buf, state, b"COMMIT", &[], options)?;
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
        Some(unsafe { laux::lua_check_str(state, -1) }.to_string())
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
            let cf = unsafe { laux::lua_check_str(state, idx) }.to_string();
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
        let s = unsafe { laux::lua_check_str(state, top) }.to_string();
        laux::lua_pop(state, 1);
        out.push(s);
    }
    Ok(out)
}

// ---------------------------------------------------------------------------
// Lua-facing functions
// ---------------------------------------------------------------------------

const PG_POOL_META: *const std::ffi::c_char = cstr!("pg_pool_metatable");

extern "C-unwind" fn connect(state: LuaState) -> c_int {
    let database_url = unsafe { laux::lua_check_str(state, 1) }.to_string();
    let name = unsafe { laux::lua_check_str(state, 2) }.to_string();
    let timeout_ms: u64 = laux::lua_opt(state, 3).unwrap_or(5000);
    let max_connections: usize =
        laux::lua_opt(state, 4).unwrap_or(crate::LIMITS.db_pool_size as usize);
    let max_connections = max_connections.max(1);
    let read_timeout_ms: u64 = laux::lua_opt(state, 5).unwrap_or(crate::LIMITS.db_read_timeout_ms);
    let queue_capacity: usize =
        laux::lua_opt(state, 6).unwrap_or(crate::LIMITS.request_queue_capacity);
    let queue_capacity = queue_capacity.max(1);

    let actor = LuaActor::from_lua_state(state);
    let owner = unsafe { (*actor).id };
    let session = unsafe { (*actor).next_session() };

    CONTEXT.io_runtime().spawn(async move {
        let params = match ConnParams::parse(&database_url) {
            Ok(p) => p,
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
        if let Some(old) = PG_CONNECTIONS.insert(name, pool) {
            log::warn!(
                "pg '{}' reconnected with the same name; shutting down the previous pool",
                old.inner.name()
            );
            for w in old.inner.workers() {
                let _ = w.tx().send(PgMessage::Shutdown).await;
            }
        }
        let _ = CONTEXT.send_value(context::PTYPE_PG, owner, session, PgResponse::Connect);
    });

    laux::lua_push(state, session);
    1
}

extern "C-unwind" fn find_connection(state: LuaState) -> c_int {
    let name = unsafe { laux::lua_check_str(state, 1) };
    match PG_CONNECTIONS.get(name) {
        Some(pair) => {
            let methods = [
                lreg!("query", query),
                lreg!("query_params", query_params),
                lreg!("pipe", pipe),
                lreg!("insert_many", insert_many),
                lreg!("update_many", update_many),
                lreg!("exec_query", exec_query),
                lreg!("exec_query_params", exec_query_params),
                lreg!("exec_pipe", exec_pipe),
                lreg!("exec_insert_many", exec_insert_many),
                lreg!("exec_update_many", exec_update_many),
                lreg!("len", pool_len),
                lreg!("close", close),
                lreg_null!(),
            ];
            if laux::lua_newuserdata(state, pair.value().clone(), PG_POOL_META, methods.as_ref())
                .is_none()
            {
                laux::lua_pushnil(state);
            }
        }
        None => laux::lua_pushnil(state),
    }
    1
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
extern "C-unwind" fn query(state: LuaState) -> c_int {
    query_impl(state, false)
}
extern "C-unwind" fn exec_query(state: LuaState) -> c_int {
    query_impl(state, true)
}

fn query_impl(state: LuaState, forget: bool) -> c_int {
    let pool = laux::lua_touserdata::<PgPool>(state, 1).expect("invalid pg pool pointer");
    let sql = unsafe { laux::lua_check_lstring(state, 2) };

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
extern "C-unwind" fn query_params(state: LuaState) -> c_int {
    query_params_impl(state, false)
}
extern "C-unwind" fn exec_query_params(state: LuaState) -> c_int {
    query_params_impl(state, true)
}

fn query_params_impl(state: LuaState, forget: bool) -> c_int {
    let pool = laux::lua_touserdata::<PgPool>(state, 1).expect("invalid pg pool pointer");
    let sql_idx = 2;
    let sql = unsafe { laux::lua_check_lstring(state, sql_idx) }.to_vec();

    let top = laux::lua_top(state);
    let param_indices: Vec<i32> = ((sql_idx + 1)..=top).collect();

    let options = JsonOptions::default();
    let mut data = Vec::with_capacity(64 + sql.len());
    if let Err(err) = append_statement(&mut data, state, &sql, &param_indices, &options) {
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
extern "C-unwind" fn pipe(state: LuaState) -> c_int {
    pipe_impl(state, false)
}
extern "C-unwind" fn exec_pipe(state: LuaState) -> c_int {
    pipe_impl(state, true)
}

fn pipe_impl(state: LuaState, forget: bool) -> c_int {
    let pool = laux::lua_touserdata::<PgPool>(state, 1).expect("invalid pg pool pointer");
    let queries_idx = laux::lua_absindex(state, 2);
    laux::lua_checktype(state, queries_idx, ffi::LUA_TTABLE);

    let options = JsonOptions::default();
    let mut data = Vec::with_capacity(256);

    if let Err(err) = append_statement(&mut data, state, b"BEGIN", &[], &options) {
        push_lua_table!(state, "code" => "ENCODE", "message" => err);
        return 1;
    }

    let n = unsafe { ffi::lua_rawlen(state.as_ptr(), queries_idx) };
    for i in 1..=n {
        unsafe { ffi::lua_rawgeti(state.as_ptr(), queries_idx, i as ffi::lua_Integer) };
        let stmt_idx = laux::lua_top(state);
        if laux::lua_type(state, stmt_idx) != laux::LuaType::Table {
            laux::lua_pop(state, 1);
            push_lua_table!(state, "code" => "ENCODE", "message" => format!("pipe: expected table at index {}", i));
            return 1;
        }

        // sql = stmt[1]; params = stmt[2..]
        unsafe { ffi::lua_rawgeti(state.as_ptr(), stmt_idx, 1) };
        let sql_top = laux::lua_top(state);
        let sql = unsafe { laux::lua_check_lstring(state, sql_top) }.to_vec();

        let stmt_len = unsafe { ffi::lua_rawlen(state.as_ptr(), stmt_idx) };
        let mut param_tops = Vec::new();
        for p in 2..=stmt_len {
            unsafe { ffi::lua_rawgeti(state.as_ptr(), stmt_idx, p as ffi::lua_Integer) };
            param_tops.push(laux::lua_top(state));
        }

        let res = append_statement(&mut data, state, &sql, &param_tops, &options);
        laux::lua_pop(state, (param_tops.len() as i32) + 2);
        if let Err(err) = res {
            push_lua_table!(state, "code" => "ENCODE", "message" => err);
            return 1;
        }
    }

    if let Err(err) = append_statement(&mut data, state, b"COMMIT", &[], &options) {
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
extern "C-unwind" fn insert_many(state: LuaState) -> c_int {
    insert_many_impl(state, false)
}
extern "C-unwind" fn exec_insert_many(state: LuaState) -> c_int {
    insert_many_impl(state, true)
}

fn insert_many_impl(state: LuaState, forget: bool) -> c_int {
    let pool = laux::lua_touserdata::<PgPool>(state, 1).expect("invalid pg pool pointer");
    let table = unsafe { laux::lua_check_str(state, 2) }.to_string();

    let columns = match read_string_array(state, 3, "insert_many: columns") {
        Ok(c) => c,
        Err(e) => {
            push_lua_table!(state, "code" => "ENCODE", "message" => e);
            return 1;
        }
    };
    let rows_idx = 4;
    laux::lua_checktype(state, rows_idx, ffi::LUA_TTABLE);
    let nrows = unsafe { ffi::lua_rawlen(state.as_ptr(), rows_idx) };
    if nrows == 0 {
        push_lua_table!(state, "code" => "ENCODE", "message" => "insert_many: rows is empty");
        return 1;
    }
    let conflict: Option<String> = match parse_conflict(state, 5) {
        Ok(c) => c,
        Err(e) => {
            push_lua_table!(state, "code" => "ENCODE", "message" => format!("insert_many: {}", e));
            return 1;
        }
    };

    laux::lua_checkstack(state, 4, std::ptr::null());
    let options = JsonOptions::default();
    let mut data = Vec::with_capacity(128 + table.len() + nrows * columns.len() * 8);
    let build =
        |tuple_count: usize| build_insert_sql(&table, &columns, tuple_count, conflict.as_deref());
    if let Err(err) = encode_many(
        state,
        &mut data,
        rows_idx,
        nrows,
        columns.len(),
        &options,
        &build,
    ) {
        push_lua_table!(state, "code" => "ENCODE", "message" => err);
        return 1;
    }

    if forget {
        dispatch_forget(state, pool, data)
    } else {
        dispatch_async(state, pool, data)
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
extern "C-unwind" fn update_many(state: LuaState) -> c_int {
    update_many_impl(state, false)
}
extern "C-unwind" fn exec_update_many(state: LuaState) -> c_int {
    update_many_impl(state, true)
}

fn update_many_impl(state: LuaState, forget: bool) -> c_int {
    let pool = laux::lua_touserdata::<PgPool>(state, 1).expect("invalid pg pool pointer");
    let table = unsafe { laux::lua_check_str(state, 2) }.to_string();
    let key = unsafe { laux::lua_check_str(state, 3) }.to_string();

    let set_cols = match read_string_array(state, 4, "update_many: set_columns") {
        Ok(c) => c,
        Err(e) => {
            push_lua_table!(state, "code" => "ENCODE", "message" => e);
            return 1;
        }
    };
    let rows_idx = 5;
    laux::lua_checktype(state, rows_idx, ffi::LUA_TTABLE);
    let nrows = unsafe { ffi::lua_rawlen(state.as_ptr(), rows_idx) };
    if nrows == 0 {
        push_lua_table!(state, "code" => "ENCODE", "message" => "update_many: rows is empty");
        return 1;
    }
    let key_type: Option<String> = if laux::lua_type(state, 6) == laux::LuaType::String {
        let kt = unsafe { laux::lua_check_str(state, 6) }.to_string();
        if let Err(e) = validate_type_name(&kt) {
            push_lua_table!(state, "code" => "ENCODE", "message" => format!("update_many: {}", e));
            return 1;
        }
        Some(kt)
    } else {
        None
    };

    laux::lua_checkstack(state, 4, std::ptr::null());
    let options = JsonOptions::default();
    let cols_per_tuple = 1 + set_cols.len();
    let mut data = Vec::with_capacity(128 + table.len() + nrows * cols_per_tuple * 8);
    let build = |tuple_count: usize| {
        build_update_sql(&table, &key, &set_cols, tuple_count, key_type.as_deref())
    };
    if let Err(err) = encode_many(
        state,
        &mut data,
        rows_idx,
        nrows,
        cols_per_tuple,
        &options,
        &build,
    ) {
        push_lua_table!(state, "code" => "ENCODE", "message" => err);
        return 1;
    }

    if forget {
        dispatch_forget(state, pool, data)
    } else {
        dispatch_async(state, pool, data)
    }
}

extern "C-unwind" fn pool_len(state: LuaState) -> c_int {
    let pool = laux::lua_touserdata::<PgPool>(state, 1).expect("invalid pg pool pointer");
    let table = LuaTable::new(state, pool.inner.workers().len(), 0);
    for w in pool.inner.workers() {
        table.push(w.counter().load());
    }
    1
}

extern "C-unwind" fn close(state: LuaState) -> c_int {
    let pool = laux::lua_touserdata::<PgPool>(state, 1).expect("invalid pg pool pointer");
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

extern "C-unwind" fn stats(state: LuaState) -> c_int {
    let table = LuaTable::new(state, 0, PG_CONNECTIONS.len());
    PG_CONNECTIONS.iter().for_each(|pair| {
        table.insert(pair.key().as_str(), pair.value().pending());
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
fn parse_row_desc(body: &[u8]) -> Vec<(String, i32)> {
    let mut fields = Vec::new();
    if body.len() < 2 {
        return fields;
    }
    let num = read_u16(body, 0) as usize;
    let mut offset = 2;
    for _ in 0..num {
        let start = offset;
        while offset < body.len() && body[offset] != 0 {
            offset += 1;
        }
        let name = String::from_utf8_lossy(&body[start..offset]).into_owned();
        offset += 1; // skip NUL
        // table_oid(4) col_attr(2) type_oid(4) type_size(2) type_mod(4) format(2)
        let type_oid = if offset + 10 <= body.len() {
            read_i32(body, offset + 6)
        } else {
            0
        };
        offset += 18;
        fields.push((name, type_oid));
    }
    fields
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
fn push_statement_result(state: LuaState, stmt: &Statement) {
    let (command, affected_rows) = stmt
        .command_tag
        .as_ref()
        .map(|t| parse_command_tag(t))
        .unwrap_or((String::new(), None));

    if let Some(row_desc) = &stmt.row_desc {
        let fields = parse_row_desc(row_desc);
        let table = LuaTable::new(state, stmt.data_rows.len(), 0);
        for (ri, row) in stmt.data_rows.iter().enumerate() {
            let row_table = LuaTable::new(state, 0, fields.len());
            if row.len() >= 2 {
                let ncols = read_u16(row, 0) as usize;
                let mut offset = 2;
                for ci in 0..ncols {
                    if offset + 4 > row.len() {
                        // Truncated DataRow: the server response is shorter than
                        // its own column count claims. Don't silently drop the
                        // rest — log it so the malformed reply is diagnosable.
                        log::warn!(
                            "pg: truncated DataRow (row {}, got {}/{} columns, len {})",
                            ri,
                            ci,
                            ncols,
                            row.len()
                        );
                        break;
                    }
                    let len = read_i32(row, offset);
                    offset += 4;
                    if len < 0 {
                        // SQL NULL -> leave the field absent (nil).
                        continue;
                    }
                    let len = len as usize;
                    let value = &row[offset..(offset + len).min(row.len())];
                    offset += len;
                    if let Some((name, oid)) = fields.get(ci) {
                        laux::lua_push(state, name.as_str());
                        convert_value(state, *oid, value);
                        unsafe { ffi::lua_rawset(state.as_ptr(), row_table.index()) };
                    }
                }
            }
            table.rawseti(ri + 1);
        }
        if let Some(n) = affected_rows {
            if command != "SELECT" {
                table.insert("affected_rows", n);
            }
        }
        return;
    }

    if let Some(n) = affected_rows {
        push_lua_table!(state, "affected_rows" => n);
    } else {
        laux::lua_push(state, true);
    }
}

/// Push the aggregated `data` field across statements (mirrors pg.lua).
fn push_data(state: LuaState, statements: &[Statement]) {
    match statements.len() {
        0 => laux::lua_pushnil(state),
        1 => push_statement_result(state, &statements[0]),
        n => {
            let table = LuaTable::new(state, n, 0);
            for (i, stmt) in statements.iter().enumerate() {
                push_statement_result(state, stmt);
                table.rawseti(i + 1);
            }
        }
    }
}

fn push_notifications(state: LuaState, notifications: &[Notification]) -> bool {
    if notifications.is_empty() {
        return false;
    }
    let table = LuaTable::new(state, notifications.len(), 0);
    for (i, n) in notifications.iter().enumerate() {
        let one = LuaTable::new(state, 0, 4);
        one.insert("operation", "notification");
        one.insert("pid", n.pid as i64);
        one.insert("channel", n.channel.as_str());
        one.insert("payload", n.payload.as_str());
        table.rawseti(i + 1);
    }
    true
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
    match response {
        PgResponse::Connect => {
            // Empty table => no `.code`, signalling success to pg.lua.
            LuaTable::new(state, 0, 0);
            1
        }
        PgResponse::Config(msg) => {
            push_lua_table!(state, "code" => "CONFIG", "message" => msg);
            1
        }
        PgResponse::Socket(msg) => {
            push_lua_table!(state, "code" => "SOCKET", "message" => msg);
            1
        }
        PgResponse::Result(result) => {
            let num_queries = result.statements.len() as i64;
            if let Some(err) = &result.error {
                // Error table carries data/num_queries/notifications too.
                push_db_error(state, err);
                let idx = laux::lua_top(state);
                laux::lua_push(state, "num_queries");
                laux::lua_push(state, num_queries);
                unsafe { ffi::lua_rawset(state.as_ptr(), idx) };

                laux::lua_push(state, "data");
                push_data(state, &result.statements);
                unsafe { ffi::lua_rawset(state.as_ptr(), idx) };

                if !result.notifications.is_empty() {
                    laux::lua_push(state, "notifications");
                    push_notifications(state, &result.notifications);
                    unsafe { ffi::lua_rawset(state.as_ptr(), idx) };
                }
                return 1;
            }

            let table = LuaTable::new(state, 0, 3);
            let idx = table.index();
            laux::lua_push(state, "num_queries");
            laux::lua_push(state, num_queries);
            unsafe { ffi::lua_rawset(state.as_ptr(), idx) };

            laux::lua_push(state, "data");
            push_data(state, &result.statements);
            unsafe { ffi::lua_rawset(state.as_ptr(), idx) };

            if !result.notifications.is_empty() {
                laux::lua_push(state, "notifications");
                push_notifications(state, &result.notifications);
                unsafe { ffi::lua_rawset(state.as_ptr(), idx) };
            }
            1
        }
    }
}

pub unsafe extern "C-unwind" fn decode_pg_message(
    state: LuaState,
    m: *mut moon_runtime::context::Message,
) -> c_int {
    match unsafe { crate::message_decode::take_boxed::<PgResponse>(m) } {
        Ok(response) => push_pg_response(state, response),
        Err(e) => crate::lua_push_error(state, &e),
    }
}

pub extern "C-unwind" fn luaopen_pg(state: LuaState) -> c_int {
    let l = [
        lreg!("connect", connect),
        lreg!("find_connection", find_connection),
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

    #[test]
    fn parse_url_full() {
        let p = ConnParams::parse("postgres://alice:s3cret@db.host:6543/shop?application_name=svc")
            .unwrap();
        assert_eq!(p.host, "db.host");
        assert_eq!(p.port, 6543);
        assert_eq!(p.user, "alice");
        assert_eq!(p.password, "s3cret");
        assert_eq!(p.database, "shop");
        assert_eq!(p.application_name, "svc");
    }

    #[test]
    fn parse_url_defaults_and_alias() {
        let p = ConnParams::parse("postgresql://postgres:123456@127.0.0.1/postgres").unwrap();
        assert_eq!(p.port, 5432);
        assert_eq!(p.application_name, "moon");
    }

    #[test]
    fn parse_url_percent_encoded_password() {
        let p = ConnParams::parse("postgres://u:p%40ss%2Fword@h/db").unwrap();
        assert_eq!(p.password, "p@ss/word");
    }

    #[test]
    fn parse_url_errors() {
        assert!(ConnParams::parse("mysql://u:p@h/db").is_err());
        assert!(ConnParams::parse("postgres://h/db").is_err()); // no user
        assert!(ConnParams::parse("postgres://u@h").is_err()); // no database
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
        let fields = parse_row_desc(&body);
        assert_eq!(fields, vec![("id".to_string(), 23)]);
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

        let fields = parse_row_desc(&body);
        assert_eq!(fields.len(), 2);
        assert_eq!(fields[0], ("name".to_string(), 25));
        assert_eq!(fields[1], ("age".to_string(), 23));
    }

    #[test]
    fn row_description_empty() {
        assert!(parse_row_desc(b"").is_empty());
        assert!(parse_row_desc(b"\x00").is_empty()); // < 2 bytes
    }

    #[test]
    fn row_description_zero_fields() {
        let body = 0u16.to_be_bytes();
        let fields = parse_row_desc(&body);
        assert!(fields.is_empty());
    }

    // -- parse_url edge cases -------------------------------------------------

    #[test]
    fn parse_url_ipv6_host() {
        let p = ConnParams::parse("postgres://user:pass@[::1]:5433/mydb").unwrap();
        assert_eq!(p.host, "[::1]");
        assert_eq!(p.port, 5433);
    }

    #[test]
    fn parse_url_default_host() {
        let p = ConnParams::parse("postgres://user:pass@/mydb");
        // URL parsing with empty host may vary; should not panic
        assert!(p.is_ok() || p.is_err());
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

    // -- percent_decode -------------------------------------------------------

    #[test]
    fn percent_decode_basic() {
        assert_eq!(percent_decode("hello%20world"), "hello world");
        assert_eq!(percent_decode("100%25"), "100%");
        assert_eq!(percent_decode("no_encoding"), "no_encoding");
    }

    #[test]
    fn percent_decode_empty() {
        assert_eq!(percent_decode(""), "");
    }

    #[test]
    fn percent_decode_special_chars() {
        assert_eq!(percent_decode("%40%2F%3A"), "@/:");
    }
}
