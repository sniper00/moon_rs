use crate::net_io::{read_buffer_exact, write_all_vectored};
use dashmap::{DashMap, DashSet};
use lazy_static::lazy_static;
use moon_lua_sys::{
    cstr, ffi,
    laux::{self, LuaStack, LuaState},
    lreg, lreg_null, lreg_try, luaL_newlib,
};
use moon_runtime::{
    actor::LuaActor,
    buffer::Buffer,
    check_buffer,
    context::{self, CONTEXT, Message, MessageBody},
};
use std::{
    ffi::c_int,
    io::IoSlice,
    sync::{
        Arc, RwLock,
        atomic::{AtomicBool, AtomicI64, AtomicU32, AtomicU64, AtomicUsize, Ordering},
    },
    time::{Duration, Instant},
};
use tokio::{
    io::{AsyncReadExt, AsyncWrite, BufReader},
    net::{TcpListener, TcpStream},
    sync::mpsc,
    time::{sleep, timeout},
};

const CONNECT_TIMEOUT_MS: u64 = 5000;
const PING_INTERVAL_MS: u64 = 5000;
const CALL_TIMEOUT_S: u64 = 10;
const MAX_FRAME_SIZE: usize = crate::LIMITS.max_network_read_bytes;
// Cluster policy; the underlying write channel itself is unbounded.
const CLUSTER_WRITE_QUEUE_CAPACITY: usize = 64 * 1024;

type ClusterWriteSender = mpsc::UnboundedSender<ClusterFrame>;
type ClusterWriteReceiver = mpsc::UnboundedReceiver<ClusterFrame>;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ClusterCloseReason {
    Eof,
    SocketError,
    ProtocolError,
    Backpressure,
    QueueClosed,
    Shutdown,
}

impl ClusterCloseReason {
    fn as_str(self) -> &'static str {
        match self {
            ClusterCloseReason::Eof => "EOF",
            ClusterCloseReason::SocketError => "SOCKET_ERROR",
            ClusterCloseReason::ProtocolError => "PROTOCOL_ERROR",
            ClusterCloseReason::Backpressure => "BACKPRESSURE",
            ClusterCloseReason::QueueClosed => "QUEUE_CLOSED",
            ClusterCloseReason::Shutdown => "SHUTDOWN",
        }
    }

    fn from_read_error(err: &str) -> Self {
        if err.contains("eof") {
            ClusterCloseReason::Eof
        } else if err.contains("frame too large") {
            ClusterCloseReason::ProtocolError
        } else {
            ClusterCloseReason::SocketError
        }
    }

    fn from_enqueue_error(err: &str) -> Self {
        if err.contains("write queue full") {
            ClusterCloseReason::Backpressure
        } else {
            ClusterCloseReason::QueueClosed
        }
    }
}

lazy_static! {
    static ref CLUSTER: ClusterState = ClusterState::new();
}

struct ConnectionStateInner {
    tx: ClusterWriteSender,
    cgen: AtomicU64,
    /// Number of frames currently enqueued but not yet written to the socket.
    pending_writes: AtomicUsize,
    write_queue_limit: usize,
    /// Set to true when the connection is draining (no new writes accepted).
    closing: AtomicBool,
}

impl ConnectionStateInner {
    fn cgen(&self) -> u64 {
        self.cgen.load(Ordering::Acquire)
    }
}

type ConnectionState = Arc<ConnectionStateInner>;

struct ClusterState {
    node_id: AtomicU32,
    connections: DashMap<u32, ConnectionState>,
    connecting: DashSet<u32>,
    pending_calls: DashMap<i64, PendingCallInfo>,
    // Keyed by (from_addr, session): `session` comes from a *per-actor* counter
    // (`LuaActor::next_session`), so it is NOT unique across actors. Keying on the
    // session alone lets one actor's outbound call clobber another's entry, which
    // would drop the clobbered actor's cleanup on connection close/timeout and
    // hang its `moon.wait`. The actor id makes the key globally unique.
    outbound_calls: DashMap<(u32, i64), OutboundCallInfo>,
    discovery_url: RwLock<String>,
    session_counter: AtomicI64,
    conn_gen_counter: AtomicI64,
    initialized: AtomicBool,
}

struct PendingCallInfo {
    node_id: u32,
    from_addr: u32,
    session: i64,
    timestamp: Instant,
}

struct OutboundCallInfo {
    to_node: u32,
    /// Connection generation this call was actually sent on. On connection close
    /// we fail exactly the calls whose `cgen` matches the closing connection, so a
    /// call belonging to a newer (replacement) connection is left untouched. A
    /// value of `u64::MAX` means "not yet sent" (still connecting); such a call is
    /// only ever released by the timeout checker, never by a connection close.
    cgen: u64,
    timestamp: Instant,
}

impl ClusterState {
    fn new() -> Self {
        Self {
            node_id: AtomicU32::new(0),
            connections: DashMap::new(),
            connecting: DashSet::new(),
            pending_calls: DashMap::new(),
            outbound_calls: DashMap::new(),
            discovery_url: RwLock::new(String::new()),
            session_counter: AtomicI64::new(1),
            conn_gen_counter: AtomicI64::new(0),
            initialized: AtomicBool::new(false),
        }
    }

    fn next_session(&self) -> i64 {
        self.session_counter.fetch_add(1, Ordering::AcqRel)
    }

    fn node_id(&self) -> u32 {
        self.node_id.load(Ordering::Acquire)
    }
}

// ---------------------------------------------------------------------------
// Wire protocol: 4-byte big-endian length prefix + payload (header\n + body)
// Max single message size: 4GB
// ---------------------------------------------------------------------------

/// A frame to be written to the wire. Header and body are kept separate to avoid copying body data.
struct ClusterFrame {
    header: Vec<u8>,
    body: Option<Box<Buffer>>,
    /// If true, the writer should close the connection after writing this frame
    /// (graceful "close-after-write" semantic: drain this final message then EOF).
    close_after: bool,
}

impl ClusterFrame {
    fn header_only(header: Vec<u8>) -> Self {
        Self {
            header,
            body: None,
            close_after: false,
        }
    }

    fn with_body(header: Vec<u8>, body: Box<Buffer>) -> Self {
        Self {
            header,
            body: Some(body),
            close_after: false,
        }
    }

    #[allow(dead_code)]
    fn close_marker() -> Self {
        Self {
            header: Vec::new(),
            body: None,
            close_after: true,
        }
    }

    fn total_len(&self) -> usize {
        self.header.len() + self.body.as_ref().map_or(0, |b| b.as_slice().len())
    }

    fn is_close_marker(&self) -> bool {
        self.close_after && self.header.is_empty()
    }
}

fn make_send_header(to_sname: &str, from_node: u32, from_addr: u32) -> Vec<u8> {
    format!("SEND {} {} {}\n", to_sname, from_node, from_addr).into_bytes()
}

fn make_call_header(to_sname: &str, from_node: u32, from_addr: u32, session: i64) -> Vec<u8> {
    format!(
        "CALL {} {} {} {}\n",
        to_sname, from_node, from_addr, session
    )
    .into_bytes()
}

fn make_resp_header(from_addr: u32, session: i64) -> Vec<u8> {
    format!("RESP {} {}\n", from_addr, session).into_bytes()
}

fn make_hello_frame(node_id: u32) -> ClusterFrame {
    ClusterFrame::header_only(format!("HELLO {}\n", node_id).into_bytes())
}

fn make_ping_frame() -> ClusterFrame {
    ClusterFrame::header_only(b"PING\n".to_vec())
}

fn make_pong_frame() -> ClusterFrame {
    ClusterFrame::header_only(b"PONG\n".to_vec())
}

// ---------------------------------------------------------------------------
// Wire protocol: frame reading (4-byte big-endian length prefix)
// ---------------------------------------------------------------------------

async fn read_one_frame(
    reader: &mut BufReader<tokio::net::tcp::OwnedReadHalf>,
) -> Result<Box<Buffer>, String> {
    let mut len_buf = [0u8; 4];
    reader.read_exact(&mut len_buf).await.map_err(|e| {
        if e.kind() == std::io::ErrorKind::UnexpectedEof {
            "early eof".to_string()
        } else {
            e.to_string()
        }
    })?;

    let size = u32::from_be_bytes(len_buf) as usize;
    if size > MAX_FRAME_SIZE {
        return Err(format!(
            "frame too large: {} bytes (max {})",
            size, MAX_FRAME_SIZE
        ));
    }
    let mut buf = Box::new(Buffer::with_capacity(size));

    if size > 0 {
        read_buffer_exact(reader, &mut buf, size)
            .await
            .map_err(|e| {
                if e.kind() == std::io::ErrorKind::UnexpectedEof {
                    "early eof".to_string()
                } else {
                    e.to_string()
                }
            })?;
    }

    Ok(buf)
}

// ---------------------------------------------------------------------------
// Protocol parsing & dispatch
// ---------------------------------------------------------------------------

fn parse_hello(frame: &Buffer) -> Option<u32> {
    let data = frame.as_slice();
    let nl = memchr::memchr(b'\n', data)?;
    let header_str = std::str::from_utf8(&data[..nl]).ok()?;
    let mut parts = header_str.split(' ');
    let verb = parts.next()?;
    if verb == "HELLO" {
        parts.next()?.parse().ok()
    } else {
        None
    }
}

fn dispatch_frame(mut frame: Box<Buffer>, remote_node_id: u32) {
    let data = frame.as_slice();
    let Some(nl) = memchr::memchr(b'\n', data) else {
        log::error!(
            "cluster: frame missing header delimiter from node {}",
            remote_node_id
        );
        return;
    };
    let Some(header_str) = std::str::from_utf8(&data[..nl]).ok() else {
        log::error!("cluster: invalid utf8 header from node {}", remote_node_id);
        return;
    };

    let mut parts = header_str.split(' ');
    let Some(verb) = parts.next() else { return };

    match verb {
        "HELLO" => {}
        "PING" => {
            if let Some(entry) = CLUSTER.connections.get(&remote_node_id) {
                let _ = enqueue_cluster_frame(entry.value(), make_pong_frame(), "PONG");
            }
        }
        "PONG" => {}
        "SEND" => {
            let Some(to_sname) = parts.next() else {
                log::error!(
                    "cluster: SEND missing service name from node {}",
                    remote_node_id
                );
                return;
            };
            let Some(_from_node) = parts.next().and_then(|s| s.parse::<u32>().ok()) else {
                log::error!(
                    "cluster: SEND invalid from_node from node {}",
                    remote_node_id
                );
                return;
            };
            let Some(from_addr) = parts.next().and_then(|s| s.parse::<u32>().ok()) else {
                log::error!(
                    "cluster: SEND invalid from_addr from node {}",
                    remote_node_id
                );
                return;
            };

            if let Some(actor_ref) = CONTEXT.query(to_sname) {
                let actor_id = *actor_ref;
                drop(actor_ref);
                frame.consume(nl + 1);
                let _ = CONTEXT.send(Message {
                    from: from_addr,
                    to: actor_id,
                    session: 0,
                    data: MessageBody::Buffer(context::PTYPE_LUA, frame),
                });
            } else {
                log::error!(
                    "cluster SEND: service '{}' not found on node {}",
                    to_sname,
                    CLUSTER.node_id()
                );
            }
        }
        "CALL" => {
            let Some(to_sname) = parts.next() else {
                log::error!(
                    "cluster: CALL missing service name from node {}",
                    remote_node_id
                );
                return;
            };
            let Some(_from_node) = parts.next().and_then(|s| s.parse::<u32>().ok()) else {
                log::error!(
                    "cluster: CALL invalid from_node from node {}",
                    remote_node_id
                );
                return;
            };
            let Some(from_addr) = parts.next().and_then(|s| s.parse::<u32>().ok()) else {
                log::error!(
                    "cluster: CALL invalid from_addr from node {}",
                    remote_node_id
                );
                return;
            };
            let Some(session) = parts.next().and_then(|s| s.parse::<i64>().ok()) else {
                log::error!("cluster: CALL invalid session from node {}", remote_node_id);
                return;
            };

            if let Some(actor_ref) = CONTEXT.query(to_sname) {
                let actor_id = *actor_ref;
                drop(actor_ref);

                let local_session = CLUSTER.next_session();
                CLUSTER.pending_calls.insert(
                    local_session,
                    PendingCallInfo {
                        node_id: remote_node_id,
                        from_addr,
                        session,
                        timestamp: Instant::now(),
                    },
                );

                frame.consume(nl + 1);
                let _ = CONTEXT.send(Message {
                    from: context::CLUSTER_ACTOR_ADDR,
                    to: actor_id,
                    session: -local_session,
                    data: MessageBody::Buffer(context::PTYPE_LUA, frame),
                });
            } else {
                log::error!(
                    "cluster CALL: service '{}' not found on node {}",
                    to_sname,
                    CLUSTER.node_id()
                );
                let err_body = format!(
                    "node {}, service '{}' not found",
                    CLUSTER.node_id(),
                    to_sname
                );
                let err_buf = make_error_seri_buffer(&err_body);
                let mut body_buf = Box::new(Buffer::with_capacity(err_buf.len()));
                body_buf.write_slice(&err_buf);
                let header = make_resp_header(from_addr, session);
                let resp_frame = ClusterFrame::with_body(header, body_buf);
                if let Some(entry) = CLUSTER.connections.get(&remote_node_id) {
                    let _ =
                        enqueue_cluster_frame(entry.value(), resp_frame, "cluster error response");
                }
            }
        }
        "RESP" => {
            let Some(from_addr) = parts.next().and_then(|s| s.parse::<u32>().ok()) else {
                log::error!(
                    "cluster: RESP invalid from_addr from node {}",
                    remote_node_id
                );
                return;
            };
            let Some(session) = parts.next().and_then(|s| s.parse::<i64>().ok()) else {
                log::error!("cluster: RESP invalid session from node {}", remote_node_id);
                return;
            };
            // Deliver only if the call is still pending. If it was already resolved
            // (e.g. failed by a connection close or timeout), drop this late
            // response so the waiting coroutine is not woken a second time.
            if CLUSTER
                .outbound_calls
                .remove(&(from_addr, session))
                .is_none()
            {
                return;
            }
            frame.consume(nl + 1);
            let _ = CONTEXT.send(Message {
                from: 0,
                to: from_addr,
                session,
                data: MessageBody::Buffer(context::PTYPE_LUA, frame),
            });
        }
        _ => {
            log::error!(
                "cluster: unknown verb '{}' from node {}",
                verb,
                remote_node_id
            );
        }
    }
}

// ---------------------------------------------------------------------------
// Connection management
// ---------------------------------------------------------------------------

async fn discover_node_addr(node_id: u32) -> Result<String, String> {
    let url = {
        let u = CLUSTER
            .discovery_url
            .read()
            .map_err(|_| "cluster discovery URL lock poisoned".to_string())?;
        u.replace("{}", &node_id.to_string())
    };

    let response = reqwest::get(&url).await.map_err(|e| {
        format!(
            "discovery node {} request failed: {} (url: {})",
            node_id, e, url
        )
    })?;

    if !response.status().is_success() {
        return Err(format!(
            "discovery node {} returned status {} (url: {})",
            node_id,
            response.status(),
            url,
        ));
    }

    let body = response
        .text()
        .await
        .map_err(|e| format!("discovery node {} read body failed: {}", node_id, e))?;

    Ok(body.trim().to_string())
}

async fn connect_to_node(node_id: u32) -> Result<(), String> {
    if CLUSTER.connections.contains_key(&node_id) {
        return Ok(());
    }

    if !CLUSTER.connecting.insert(node_id) {
        // Another task is already connecting to this node; wait for it
        for _ in 0..50 {
            sleep(Duration::from_millis(100)).await;
            if CLUSTER.connections.contains_key(&node_id) {
                return Ok(());
            }
            if !CLUSTER.connecting.contains(&node_id) {
                break;
            }
        }
        if CLUSTER.connections.contains_key(&node_id) {
            return Ok(());
        }
        return Err(format!(
            "connect to node {} timed out waiting for peer",
            node_id
        ));
    }

    let result = connect_to_node_inner(node_id).await;
    CLUSTER.connecting.remove(&node_id);
    result
}

async fn connect_to_node_inner(node_id: u32) -> Result<(), String> {
    let addr = discover_node_addr(node_id).await?;

    let stream = timeout(
        Duration::from_millis(CONNECT_TIMEOUT_MS),
        TcpStream::connect(&addr),
    )
    .await
    .map_err(|_| format!("connect to node {} ({}) timeout", node_id, addr))?
    .map_err(|e| format!("connect to node {} ({}) failed: {}", node_id, addr, e))?;

    stream
        .set_nodelay(true)
        .map_err(|e| format!("node {} set_nodelay failed: {}", node_id, e))?;

    // Final check: if another path already established a connection, discard ours
    if CLUSTER.connections.contains_key(&node_id) {
        return Ok(());
    }

    let my_node = CLUSTER.node_id();
    setup_connection(stream, node_id, true, my_node);
    Ok(())
}

fn setup_connection(stream: TcpStream, remote_node_id: u32, is_initiator: bool, my_node: u32) {
    let (read_half, write_half) = stream.into_split();
    let (write_tx, write_rx) = mpsc::unbounded_channel::<ClusterFrame>();

    let cgen = CLUSTER.conn_gen_counter.fetch_add(1, Ordering::AcqRel) as u64;

    let conn: ConnectionState = Arc::new(ConnectionStateInner {
        tx: write_tx,
        cgen: AtomicU64::new(cgen),
        pending_writes: AtomicUsize::new(0),
        write_queue_limit: CLUSTER_WRITE_QUEUE_CAPACITY,
        closing: AtomicBool::new(false),
    });

    if remote_node_id != 0 {
        if CLUSTER
            .connections
            .insert(remote_node_id, conn.clone())
            .is_some()
        {
            log::warn!(
                "cluster: replaced an existing connection to node {} (duplicate/reconnect)",
                remote_node_id
            );
        }
    }

    CONTEXT
        .io_runtime()
        .spawn(write_task(write_half, write_rx, conn.clone()));
    CONTEXT
        .io_runtime()
        .spawn(read_task(read_half, remote_node_id, conn.clone(), cgen));

    if is_initiator {
        if let Err(err) = enqueue_cluster_frame(&conn, make_hello_frame(my_node), "HELLO") {
            log::error!("cluster: failed to enqueue HELLO: {}", err);
        }
    }
}

async fn write_cluster_frame<W>(
    writer: &mut W,
    frame: &ClusterFrame,
    total_len: u32,
) -> std::io::Result<()>
where
    W: AsyncWrite + Unpin,
{
    let total_len_bytes = total_len.to_be_bytes();
    let body = frame.body.as_ref().map_or(&[][..], |body| body.as_slice());
    write_all_vectored(
        writer,
        &mut [
            IoSlice::new(&total_len_bytes),
            IoSlice::new(&frame.header),
            IoSlice::new(body),
        ],
        "failed to write cluster frame",
    )
    .await
}

async fn write_task(
    mut writer: tokio::net::tcp::OwnedWriteHalf,
    mut rx: ClusterWriteReceiver,
    conn: ConnectionState,
) {
    while let Some(frame) = rx.recv().await {
        conn.pending_writes.fetch_sub(1, Ordering::AcqRel);
        if frame.is_close_marker() {
            break;
        }
        let total = frame.total_len();
        if total > MAX_FRAME_SIZE {
            log::error!(
                "cluster: outbound frame too large: {} bytes (max {}), dropping it",
                total,
                MAX_FRAME_SIZE
            );
            continue;
        }
        if let Err(e) = write_cluster_frame(&mut writer, &frame, total as u32).await {
            log::error!("cluster write error: {}", e);
            break;
        }
        if frame.close_after {
            break;
        }
    }
}

async fn read_task(
    read_half: tokio::net::tcp::OwnedReadHalf,
    mut remote_node_id: u32,
    conn: ConnectionState,
    mut cgen: u64,
) {
    let mut reader = BufReader::new(read_half);

    // If remote_node_id is 0, we're the acceptor and need to wait for HELLO
    if remote_node_id == 0 {
        match read_one_frame(&mut reader).await {
            Ok(frame) => {
                if let Some(node_id) = parse_hello(&frame) {
                    remote_node_id = node_id;
                    cgen = CLUSTER.conn_gen_counter.fetch_add(1, Ordering::AcqRel) as u64;
                    conn.cgen.store(cgen, Ordering::Release);
                    if CLUSTER.connections.insert(node_id, conn.clone()).is_some() {
                        log::warn!(
                            "cluster: HELLO from node {} replaced an existing connection",
                            node_id
                        );
                    }
                    log::info!("cluster: received HELLO from node {}", node_id);
                } else {
                    log::error!("cluster: expected HELLO as first frame");
                    return;
                }
            }
            Err(e) => {
                log::error!("cluster: read HELLO failed: {}", e);
                return;
            }
        }
    }

    loop {
        match read_one_frame(&mut reader).await {
            Ok(frame) => {
                dispatch_frame(frame, remote_node_id);
            }
            Err(e) => {
                log::warn!(
                    "cluster: connection to node {} closed ({}): {}",
                    remote_node_id,
                    ClusterCloseReason::from_read_error(&e).as_str(),
                    e
                );
                on_connection_closed(
                    remote_node_id,
                    cgen,
                    ClusterCloseReason::from_read_error(&e),
                );
                break;
            }
        }
    }
}

fn on_connection_closed(node_id: u32, cgen: u64, reason: ClusterCloseReason) {
    // Atomically remove the connection entry only if its generation matches.
    // `remove_if` returns None both when the key is absent and when the
    // predicate returns false; we distinguish the two with a separate check.
    let removed = CLUSTER
        .connections
        .remove_if(&node_id, |_, cs| cs.cgen() == cgen);
    let has_newer = removed.is_none() && CLUSTER.connections.contains_key(&node_id);

    // Mark the connection as closing so any racing enqueues are rejected.
    if let Some((_, conn)) = &removed {
        conn.closing.store(true, Ordering::Release);
    }

    if !has_newer {
        // Drop bookkeeping for in-flight *incoming* calls from this node. These are
        // requests we were servicing on the peer's behalf: the local handler is the
        // responder, not a waiter, so there is no local coroutine to release. The
        // peer owns the suspended coroutine and releases it via its own outbound
        // cleanup. We only clear the entries so a late local response is discarded
        // and the map doesn't grow unbounded.
        CLUSTER
            .pending_calls
            .retain(|_, info| info.node_id != node_id);
    }

    // Fail every outbound call that was actually sent on *this* connection
    // generation, even when a newer connection has since replaced it: a cluster
    // call's request and response travel over one bidirectional socket, so once
    // that generation is gone the call can no longer be answered on it. Matching on
    // `cgen` (not just `node_id`) leaves calls belonging to the replacement
    // connection untouched, and the RESP dedup guards the rare case where a late
    // response still arrives over the replacement.
    let mut outbound_to_remove = Vec::new();
    for entry in CLUSTER.outbound_calls.iter() {
        let info = entry.value();
        if info.to_node == node_id && info.cgen == cgen {
            outbound_to_remove.push(*entry.key());
        }
    }

    for (from_addr, session) in outbound_to_remove {
        fail_outbound_call(
            from_addr,
            session,
            format!(
                "cluster connection to node {} closed ({})",
                node_id,
                reason.as_str()
            ),
        );
    }

    // Notify Lua layer: deliver a PTYPE_SYSTEM "_cluster_close,node_id,reason"
    // to all unique actors so they can react to connection loss.
    if !has_newer {
        let payload = format!("_cluster_close,{},{}", node_id, reason.as_str());
        CONTEXT.broadcast_system(context::CLUSTER_ACTOR_ADDR, &payload);
    }
}

fn shutdown_cluster(reason: ClusterCloseReason) {
    CLUSTER.initialized.store(false, Ordering::Release);

    let connections: Vec<(u32, u64)> = CLUSTER
        .connections
        .iter()
        .map(|entry| (*entry.key(), entry.value().cgen()))
        .collect();
    for (node_id, cgen) in connections {
        on_connection_closed(node_id, cgen, reason);
    }

    CLUSTER.connections.clear();
    CLUSTER.connecting.clear();
    CLUSTER.pending_calls.clear();

    let remaining: Vec<(u32, i64)> = CLUSTER
        .outbound_calls
        .iter()
        .map(|entry| *entry.key())
        .collect();
    for (from_addr, session) in remaining {
        fail_outbound_call(
            from_addr,
            session,
            format!("cluster shutdown ({})", reason.as_str()),
        );
    }
}

/// Build a minimal seri-encoded error response: (false, err_msg)
fn make_error_seri_buffer(msg: &str) -> Vec<u8> {
    // seri format: TYPE_BOOLEAN(false) + TYPE_SHORT_STRING(msg) or TYPE_LONG_STRING
    let mut buf = Vec::with_capacity(2 + msg.len());
    // false = TYPE_BOOLEAN | (0 << 3) = 1
    buf.push(1); // combine_type!(TYPE_BOOLEAN, 0)
    // string
    let len = msg.len();
    if len < 32 {
        buf.push(4 | ((len as u8) << 3)); // combine_type!(TYPE_SHORT_STRING, len)
        buf.extend_from_slice(msg.as_bytes());
    } else if len < 0x10000 {
        buf.push(5 | (2 << 3)); // combine_type!(TYPE_LONG_STRING, 2)
        buf.extend_from_slice(&(len as u16).to_le_bytes());
        buf.extend_from_slice(msg.as_bytes());
    } else {
        buf.push(5 | (4 << 3)); // combine_type!(TYPE_LONG_STRING, 4)
        buf.extend_from_slice(&(len as u32).to_le_bytes());
        buf.extend_from_slice(msg.as_bytes());
    }
    buf
}

/// Fill in the connection generation a pending outbound call was sent on. No-op
/// if the entry is already gone (e.g. a fast RESP or close beat us to it).
fn set_outbound_cgen(from_addr: u32, session: i64, cgen: u64) {
    if let Some(mut e) = CLUSTER.outbound_calls.get_mut(&(from_addr, session)) {
        e.cgen = cgen;
    }
}

fn enqueue_cluster_frame(
    conn: &ConnectionState,
    frame: ClusterFrame,
    context: &str,
) -> Result<(), String> {
    if conn.closing.load(Ordering::Acquire) {
        return Err(format!("{}: connection is closing", context));
    }
    // Reserve against the cluster's own limit before publishing the frame.
    // This also prevents the writer from decrementing an uncounted frame.
    conn.pending_writes
        .fetch_update(Ordering::AcqRel, Ordering::Acquire, |pending| {
            (pending < conn.write_queue_limit).then(|| pending + 1)
        })
        .map_err(|_| format!("{}: cluster write queue full", context))?;
    match conn.tx.send(frame) {
        Ok(()) => Ok(()),
        Err(_) => {
            conn.pending_writes.fetch_sub(1, Ordering::AcqRel);
            Err(format!("{}: cluster write queue closed", context))
        }
    }
}

fn close_connection_for_enqueue_error(node_id: u32, err: &str) {
    log::warn!(
        "cluster: closing connection to node {} after enqueue failure: {}",
        node_id,
        err
    );
    let cgen = CLUSTER
        .connections
        .get(&node_id)
        .map(|entry| entry.value().cgen());
    if let Some(cgen) = cgen {
        on_connection_closed(node_id, cgen, ClusterCloseReason::from_enqueue_error(err));
    } else {
        CLUSTER.connections.remove(&node_id);
    }
}

/// Release a pending outbound call with an error, exactly once. Returns without
/// doing anything if the call was already resolved (delivered, closed, or timed
/// out), so a waiting coroutine is never woken twice.
fn fail_outbound_call(from_addr: u32, session: i64, err: String) {
    if CLUSTER
        .outbound_calls
        .remove(&(from_addr, session))
        .is_some()
    {
        CONTEXT.response_error(0, from_addr, -session, err);
    }
}

// ---------------------------------------------------------------------------
// Background tasks
// ---------------------------------------------------------------------------

fn spawn_response_reader(mut rx: mpsc::UnboundedReceiver<Message>) {
    CONTEXT.io_runtime().spawn(async move {
        while let Some(msg) = rx.recv().await {
            if msg.session <= 0 {
                continue;
            }
            let local_session = msg.session;
            if let Some((_, info)) = CLUSTER.pending_calls.remove(&local_session) {
                let body = match msg.data {
                    MessageBody::Buffer(_, buf) => buf,
                    _ => Box::new(Buffer::new()),
                };
                let header = make_resp_header(info.from_addr, info.session);
                let frame = ClusterFrame::with_body(header, body);
                if let Some(conn) = CLUSTER
                    .connections
                    .get(&info.node_id)
                    .map(|e| e.value().clone())
                    && let Err(err) = enqueue_cluster_frame(&conn, frame, "cluster response")
                {
                    log::error!(
                        "cluster response: failed to enqueue response to node {}: {}",
                        info.node_id,
                        err
                    );
                    close_connection_for_enqueue_error(info.node_id, &err);
                }
            }
        }
    });
}

fn spawn_keepalive() {
    CONTEXT.io_runtime().spawn(async move {
        loop {
            sleep(Duration::from_millis(PING_INTERVAL_MS)).await;

            if !CLUSTER.initialized.load(Ordering::Acquire) {
                break;
            }

            let mut conn_count = 0u32;
            let mut failed = Vec::new();
            for entry in CLUSTER.connections.iter() {
                let node_id = *entry.key();
                if let Err(err) =
                    enqueue_cluster_frame(entry.value(), make_ping_frame(), "cluster keepalive")
                {
                    failed.push((node_id, err));
                }
                conn_count += 1;
            }

            for (node_id, err) in failed {
                close_connection_for_enqueue_error(node_id, &err);
            }

            let pending_count = CLUSTER.pending_calls.len();
            if conn_count > 0 || pending_count > 0 {
                log::info!(
                    "cluster stats: connections={}, pending_calls={}",
                    conn_count,
                    pending_count
                );
            }
        }
    });
}

/// Run one pass of call timeout checking. Extracted for testability.
fn check_call_timeouts(timeout_secs: u64) {
    let now = Instant::now();

    // Drop stale *incoming* call bookkeeping. There is no local waiter to
    // notify (the local handler is the responder); the calling node runs
    // its own outbound timeout to release its suspended coroutine. We only
    // reclaim entries whose local handler never responded in time.
    CLUSTER
        .pending_calls
        .retain(|_, info| now.duration_since(info.timestamp).as_secs() < timeout_secs);

    // Also timeout outbound calls
    let mut outbound_expired = Vec::new();
    for entry in CLUSTER.outbound_calls.iter() {
        if now.duration_since(entry.value().timestamp).as_secs() >= timeout_secs {
            outbound_expired.push(*entry.key());
        }
    }

    for key in outbound_expired {
        if let Some((_, info)) = CLUSTER.outbound_calls.remove(&key) {
            let (from_addr, session) = key;
            CONTEXT.response_error(
                0,
                from_addr,
                -session,
                format!("cluster call to node {} timeout", info.to_node),
            );
        }
    }
}

fn spawn_call_timeout_checker() {
    CONTEXT.io_runtime().spawn(async move {
        loop {
            sleep(Duration::from_secs(CALL_TIMEOUT_S)).await;

            if !CLUSTER.initialized.load(Ordering::Acquire) {
                break;
            }

            check_call_timeouts(CALL_TIMEOUT_S);
        }
    });
}

// ---------------------------------------------------------------------------
// Lua-facing C functions
// ---------------------------------------------------------------------------

fn lua_cluster_init(lua: &mut LuaStack<'_>) -> Result<c_int, String> {
    let state = lua.state();
    let node_id: u32 = lua.get(1)?;
    let discovery_url: String = lua.get(2)?;

    if CLUSTER.initialized.load(Ordering::Acquire) {
        return Ok(crate::lua_push_error_tuple(
            state,
            "cluster already initialized",
        ));
    }

    *CLUSTER
        .discovery_url
        .write()
        .map_err(|_| "cluster discovery URL lock poisoned".to_string())? = discovery_url;
    CLUSTER.node_id.store(node_id, Ordering::Release);

    // Register pseudo-actor for receiving call responses
    let (tx, rx) = mpsc::unbounded_channel::<Message>();
    CONTEXT.register_pseudo_actor(context::CLUSTER_ACTOR_ADDR, tx);

    spawn_response_reader(rx);
    spawn_keepalive();
    spawn_call_timeout_checker();

    CLUSTER.initialized.store(true, Ordering::Release);

    laux::lua_push(state, true);
    Ok(1)
}

fn lua_cluster_listen(lua: &mut LuaStack<'_>) -> c_int {
    let state = lua.state();
    if !CLUSTER.initialized.load(Ordering::Acquire) {
        return crate::lua_push_error_tuple(state, "cluster not initialized");
    }

    let node_id = CLUSTER.node_id();

    CONTEXT.io_runtime().spawn(async move {
        match discover_node_addr(node_id).await {
            Ok(addr) => {
                match TcpListener::bind(&addr).await {
                    Ok(listener) => {
                        log::info!("cluster listening on {}", addr);
                        loop {
                            match listener.accept().await {
                                Ok((stream, peer_addr)) => {
                                    log::info!("cluster accepted connection from {}", peer_addr);
                                    let _ = stream.set_nodelay(true);
                                    // remote_node_id = 0 means we wait for HELLO
                                    setup_connection(stream, 0, false, node_id);
                                }
                                Err(e) => {
                                    log::error!("cluster accept error: {}", e);
                                    break;
                                }
                            }
                        }
                    }
                    Err(e) => {
                        log::error!(
                            "cluster node {}: listen bind '{}' failed: {}",
                            node_id,
                            addr,
                            e
                        );
                    }
                }
            }
            Err(e) => {
                log::error!("cluster node {}: listen discovery failed: {}", node_id, e);
            }
        }
    });

    laux::lua_push(state, true);
    1
}

fn lua_cluster_shutdown(lua: &mut LuaStack<'_>) -> c_int {
    let state = lua.state();
    shutdown_cluster(ClusterCloseReason::Shutdown);
    laux::lua_push(state, true);
    1
}

fn lua_cluster_send(lua: &mut LuaStack<'_>) -> Result<c_int, String> {
    let state = lua.state();
    if !CLUSTER.initialized.load(Ordering::Acquire) {
        return Ok(crate::lua_push_error_tuple(
            state,
            "cluster not initialized",
        ));
    }

    let to_node: u32 = lua.get(1)?;
    let to_sname: String = lua.get(2)?;
    let body = check_buffer(lua, 3)?;

    let actor = LuaActor::from_lua_state(state);
    let from_addr = unsafe { (*actor).id };
    let from_node = CLUSTER.node_id();

    let header = make_send_header(&to_sname, from_node, from_addr);
    let frame = ClusterFrame::with_body(header, body);

    if let Some(entry) = CLUSTER.connections.get(&to_node) {
        let conn = entry.value().clone();
        drop(entry);
        if let Err(err) = enqueue_cluster_frame(&conn, frame, "cluster send") {
            log::error!("cluster send: node {} enqueue failed: {}", to_node, err);
            close_connection_for_enqueue_error(to_node, &err);
        }
    } else {
        CONTEXT.io_runtime().spawn(async move {
            if let Err(e) = connect_to_node(to_node).await {
                log::error!("cluster send: connect to node {} failed: {}", to_node, e);
                return;
            }
            if let Some(entry) = CLUSTER.connections.get(&to_node) {
                let conn = entry.value().clone();
                drop(entry);
                if let Err(err) = enqueue_cluster_frame(&conn, frame, "cluster send after connect")
                {
                    log::error!(
                        "cluster send: node {} enqueue failed after connect: {}",
                        to_node,
                        err
                    );
                    close_connection_for_enqueue_error(to_node, &err);
                }
            }
        });
    }

    Ok(0)
}

fn lua_cluster_request(lua: &mut LuaStack<'_>) -> Result<c_int, String> {
    let state = lua.state();
    if !CLUSTER.initialized.load(Ordering::Acquire) {
        return Ok(crate::lua_push_error_tuple(
            state,
            "cluster not initialized",
        ));
    }

    let to_node: u32 = lua.get(1)?;
    let to_sname: String = lua.get(2)?;
    let body = check_buffer(lua, 3)?;

    let actor = LuaActor::from_lua_state(state);
    let from_addr = unsafe { (*actor).id };
    let from_node = CLUSTER.node_id();
    let session = unsafe { (*actor).next_session() };

    let header = make_call_header(&to_sname, from_node, from_addr, session);
    let frame = ClusterFrame::with_body(header, body);

    // A given actor never reuses a session while a call is still outstanding, so
    // the composite key must be unique. A collision here signals a session-
    // management bug (e.g. a reused session), worth catching in debug builds.
    debug_assert!(
        !CLUSTER.outbound_calls.contains_key(&(from_addr, session)),
        "duplicate outbound call key (from_addr={:#x}, session={})",
        from_addr,
        session
    );

    // Register the pending call up-front (cgen unknown) so the timeout checker
    // tracks it even while we are still connecting. `cgen` is filled in with the
    // generation we actually send on, so a later close of that generation fails
    // exactly this call.
    CLUSTER.outbound_calls.insert(
        (from_addr, session),
        OutboundCallInfo {
            to_node,
            cgen: u64::MAX,
            timestamp: Instant::now(),
        },
    );

    if let Some(entry) = CLUSTER.connections.get(&to_node) {
        let conn = entry.value().clone();
        drop(entry);
        set_outbound_cgen(from_addr, session, conn.cgen());
        if let Err(err) = enqueue_cluster_frame(&conn, frame, "cluster call") {
            fail_outbound_call(
                from_addr,
                session,
                format!("cluster call to node {} failed: {}", to_node, err),
            );
            close_connection_for_enqueue_error(to_node, &err);
        }
    } else {
        CONTEXT.io_runtime().spawn(async move {
            if let Err(e) = connect_to_node(to_node).await {
                fail_outbound_call(
                    from_addr,
                    session,
                    format!("cluster call: connect to node {} failed: {}", to_node, e),
                );
                return;
            }
            if let Some(entry) = CLUSTER.connections.get(&to_node) {
                let conn = entry.value().clone();
                drop(entry);
                set_outbound_cgen(from_addr, session, conn.cgen());
                if let Err(err) = enqueue_cluster_frame(&conn, frame, "cluster call after connect")
                {
                    fail_outbound_call(
                        from_addr,
                        session,
                        format!(
                            "cluster call to node {} failed after connect: {}",
                            to_node, err
                        ),
                    );
                    close_connection_for_enqueue_error(to_node, &err);
                }
            } else {
                fail_outbound_call(
                    from_addr,
                    session,
                    format!(
                        "cluster call: connection to node {} lost after connect",
                        to_node
                    ),
                );
            }
        });
    }

    laux::lua_push(state, session);
    Ok(1)
}

// ---------------------------------------------------------------------------
// Module registration
// ---------------------------------------------------------------------------

pub extern "C-unwind" fn luaopen_cluster(state: LuaState) -> c_int {
    let l = [
        lreg_try!("init", lua_cluster_init),
        lreg!("listen", lua_cluster_listen),
        lreg!("shutdown", lua_cluster_shutdown),
        lreg_try!("send", lua_cluster_send),
        lreg_try!("request", lua_cluster_request),
        lreg_null!(),
    ];

    luaL_newlib!(state, l);
    1
}

// ---------------------------------------------------------------------------
// Tests
//
// These drive the global `CLUSTER`/`CONTEXT` singletons directly and exercise
// the pending-wait release paths (composite-key isolation, generation-precise
// close, RESP dedup, incoming-call cleanup) without real TCP. Each test uses a
// disjoint id/node range so they remain independent under parallel execution.
// A "wait" is observed via a pseudo-actor's mailbox: a released wait shows up as
// a `Message` addressed to the caller actor with the original session.
// ---------------------------------------------------------------------------

#[cfg(test)]
#[path = "../../tests/unit/modules/lua_cluster.rs"]
mod tests;
