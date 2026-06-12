use dashmap::{DashMap, DashSet};
use lazy_static::lazy_static;
use moon_base::{
    cstr, ffi,
    laux::{self, LuaState},
    lreg, lreg_null, luaL_newlib,
};
use moon_runtime::{
    actor::LuaActor,
    buffer::Buffer,
    check_buffer,
    context::{self, ActorId, CONTEXT, Message, MessageBody},
};
use std::{
    ffi::c_int,
    io::{Error, ErrorKind, IoSlice},
    sync::{
        Arc, RwLock,
        atomic::{AtomicBool, AtomicI64, AtomicU32, AtomicU64, AtomicUsize, Ordering},
    },
    time::{Duration, Instant},
};
use tokio::{
    io::{AsyncReadExt, AsyncWrite, AsyncWriteExt, BufReader},
    net::{TcpListener, TcpStream},
    sync::mpsc,
    time::{sleep, timeout},
};

const CLUSTER_ACTOR_ID: ActorId = 0xFFFF_FF00;

const CONNECT_TIMEOUT_MS: u64 = 5000;
const PING_INTERVAL_MS: u64 = 5000;
const CALL_TIMEOUT_S: u64 = 10;
const MAX_FRAME_SIZE: usize = crate::LIMITS.cluster_frame_bytes;
const CLUSTER_WRITE_QUEUE_CAPACITY: usize = crate::LIMITS.network_write_queue_capacity;

type ClusterWriteSender = mpsc::Sender<ClusterFrame>;
type ClusterWriteReceiver = mpsc::Receiver<ClusterFrame>;

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
        // SAFETY: `prepare` reserved `size` bytes of spare capacity; `read_exact`
        // fully writes the slice before any read of it.
        let space = unsafe { std::slice::from_raw_parts_mut(buf.prepare(size), size) };
        reader.read_exact(space).await.map_err(|e| {
            if e.kind() == std::io::ErrorKind::UnexpectedEof {
                "early eof".to_string()
            } else {
                e.to_string()
            }
        })?;
        let _ = buf.commit(size);
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
                let _ = entry.value().tx.try_send(make_pong_frame());
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
                    from: CLUSTER_ACTOR_ID,
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
                    let _ = entry.value().tx.try_send(resp_frame);
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
        let u = CLUSTER.discovery_url.read().unwrap();
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
    let (write_tx, write_rx) = mpsc::channel::<ClusterFrame>(CLUSTER_WRITE_QUEUE_CAPACITY);

    let cgen = CLUSTER.conn_gen_counter.fetch_add(1, Ordering::AcqRel) as u64;

    let conn: ConnectionState = Arc::new(ConnectionStateInner {
        tx: write_tx,
        cgen: AtomicU64::new(cgen),
        pending_writes: AtomicUsize::new(0),
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

async fn write_all_vectored<W>(writer: &mut W, parts: &[&[u8]]) -> std::io::Result<()>
where
    W: AsyncWrite + Unpin,
{
    let mut part_index = 0usize;
    let mut part_offset = 0usize;

    while part_index < parts.len() {
        while part_index < parts.len() && part_offset == parts[part_index].len() {
            part_index += 1;
            part_offset = 0;
        }
        if part_index == parts.len() {
            break;
        }

        let mut slices = Vec::with_capacity(parts.len() - part_index);
        slices.push(IoSlice::new(&parts[part_index][part_offset..]));
        for part in &parts[part_index + 1..] {
            if !part.is_empty() {
                slices.push(IoSlice::new(part));
            }
        }

        let mut written = writer.write_vectored(&slices).await?;
        if written == 0 {
            return Err(Error::new(
                ErrorKind::WriteZero,
                "failed to write cluster frame",
            ));
        }

        while written > 0 {
            let remaining_in_part = parts[part_index].len() - part_offset;
            if written < remaining_in_part {
                part_offset += written;
                break;
            }
            written -= remaining_in_part;
            part_index += 1;
            part_offset = 0;
            while part_index < parts.len() && parts[part_index].is_empty() {
                part_index += 1;
            }
        }
    }

    Ok(())
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
        &[total_len_bytes.as_slice(), frame.header.as_slice(), body],
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
        CONTEXT.broadcast_system(CLUSTER_ACTOR_ID, &payload);
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
    match conn.tx.try_send(frame) {
        Ok(()) => {
            conn.pending_writes.fetch_add(1, Ordering::AcqRel);
            Ok(())
        }
        Err(mpsc::error::TrySendError::Full(_)) => {
            Err(format!("{}: cluster write queue full", context))
        }
        Err(mpsc::error::TrySendError::Closed(_)) => {
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

extern "C-unwind" fn lua_cluster_init(state: LuaState) -> c_int {
    let node_id: u32 = laux::lua_get(state, 1);
    let discovery_url: String = laux::lua_get(state, 2);

    if CLUSTER.initialized.load(Ordering::Acquire) {
        return crate::lua_push_error(state, "cluster already initialized");
    }

    CLUSTER.node_id.store(node_id, Ordering::Release);
    *CLUSTER.discovery_url.write().unwrap() = discovery_url;

    // Register pseudo-actor for receiving call responses
    let (tx, rx) = mpsc::unbounded_channel::<Message>();
    CONTEXT.register_pseudo_actor(CLUSTER_ACTOR_ID, tx);

    spawn_response_reader(rx);
    spawn_keepalive();
    spawn_call_timeout_checker();

    CLUSTER.initialized.store(true, Ordering::Release);

    laux::lua_push(state, true);
    1
}

extern "C-unwind" fn lua_cluster_listen(state: LuaState) -> c_int {
    if !CLUSTER.initialized.load(Ordering::Acquire) {
        return crate::lua_push_error(state, "cluster not initialized");
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

extern "C-unwind" fn lua_cluster_shutdown(state: LuaState) -> c_int {
    shutdown_cluster(ClusterCloseReason::Shutdown);
    laux::lua_push(state, true);
    1
}

extern "C-unwind" fn lua_cluster_send(state: LuaState) -> c_int {
    if !CLUSTER.initialized.load(Ordering::Acquire) {
        return crate::lua_push_error(state, "cluster not initialized");
    }

    let to_node: u32 = laux::lua_get(state, 1);
    let to_sname: String = laux::lua_get(state, 2);
    let body = check_buffer(state, 3);

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

    0
}

extern "C-unwind" fn lua_cluster_request(state: LuaState) -> c_int {
    if !CLUSTER.initialized.load(Ordering::Acquire) {
        return crate::lua_push_error(state, "cluster not initialized");
    }

    let to_node: u32 = laux::lua_get(state, 1);
    let to_sname: String = laux::lua_get(state, 2);
    let body = check_buffer(state, 3);

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
    1
}

// ---------------------------------------------------------------------------
// Module registration
// ---------------------------------------------------------------------------

pub extern "C-unwind" fn luaopen_cluster(state: LuaState) -> c_int {
    let l = [
        lreg!("init", lua_cluster_init),
        lreg!("listen", lua_cluster_listen),
        lreg!("shutdown", lua_cluster_shutdown),
        lreg!("send", lua_cluster_send),
        lreg!("request", lua_cluster_request),
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
mod tests {
    use super::*;
    use serial_test::{parallel, serial};

    fn reg_actor(id: ActorId) -> mpsc::UnboundedReceiver<Message> {
        let (tx, rx) = mpsc::unbounded_channel();
        CONTEXT.register_pseudo_actor(id, tx);
        rx
    }

    fn insert_outbound(from_addr: u32, session: i64, to_node: u32, cgen: u64) {
        CLUSTER.outbound_calls.insert(
            (from_addr, session),
            OutboundCallInfo {
                to_node,
                cgen,
                timestamp: Instant::now(),
            },
        );
    }

    fn resp_frame(from_addr: u32, session: i64, body: &[u8]) -> Box<Buffer> {
        let mut v = format!("RESP {} {}\n", from_addr, session).into_bytes();
        v.extend_from_slice(body);
        Box::new(Buffer::from_slice(&v))
    }

    /// Two actors with the *same* per-actor session id must not clobber each
    /// other: closing one node releases only that actor's call.
    #[test]
    #[parallel]
    fn close_releases_matching_actor_not_session_twin() {
        let (from_a, from_b) = (0x7001_0001, 0x7001_0002);
        let (node_x, node_y) = (0x7001_00A1, 0x7001_00A2);

        let mut rx_a = reg_actor(from_a);
        let mut rx_b = reg_actor(from_b);

        insert_outbound(from_a, 1, node_x, 5);
        insert_outbound(from_b, 1, node_y, 5); // same session id, different node

        on_connection_closed(node_x, 5, ClusterCloseReason::Eof);

        let m = rx_a.try_recv().expect("actor A should be released");
        assert_eq!(m.session, 1);
        assert_eq!(m.ptype(), context::PTYPE_ERROR);
        if let MessageBody::Buffer(_, data) = &m.data {
            assert!(data.to_string().contains("(EOF)"));
        } else {
            panic!("expected error buffer");
        }
        assert!(CLUSTER.outbound_calls.get(&(from_a, 1)).is_none());

        assert!(rx_b.try_recv().is_err(), "actor B must be untouched");
        assert!(CLUSTER.outbound_calls.get(&(from_b, 1)).is_some());

        CLUSTER.outbound_calls.remove(&(from_b, 1));
    }

    /// Closing an old (replaced) connection fails only calls sent on that
    /// generation; calls on the current generation and the live connection state
    /// are left intact.
    #[test]
    #[parallel]
    fn close_fails_only_matching_generation() {
        let from = 0x7002_0001;
        let node = 0x7002_00A1;
        let mut rx = reg_actor(from);

        let (conn, _rx2) = make_conn_with_cgen(1, 8);
        CLUSTER.connections.insert(node, conn); // current connection is generation 8
        insert_outbound(from, 10, node, 5); // call on the old generation
        insert_outbound(from, 11, node, 8); // call on the current generation

        on_connection_closed(node, 5, ClusterCloseReason::Eof); // the old connection closes

        let m = rx.try_recv().expect("old-generation call should be failed");
        assert_eq!(m.session, 10);
        assert_eq!(m.ptype(), context::PTYPE_ERROR);
        assert!(CLUSTER.outbound_calls.get(&(from, 10)).is_none());

        assert!(
            rx.try_recv().is_err(),
            "current-generation call must survive"
        );
        assert!(CLUSTER.outbound_calls.get(&(from, 11)).is_some());
        assert_eq!(
            CLUSTER.connections.get(&node).map(|e| e.value().cgen()),
            Some(8)
        );

        CLUSTER.outbound_calls.remove(&(from, 11));
        CLUSTER.connections.remove(&node);
    }

    /// A normal RESP delivers exactly once; a duplicate/late RESP for the same
    /// call is dropped (no second wake-up of the coroutine).
    #[test]
    #[parallel]
    fn resp_delivers_once_then_dedups() {
        let from = 0x7003_0001;
        let node = 0x7003_00A1;
        let mut rx = reg_actor(from);

        insert_outbound(from, 42, node, 5);

        dispatch_frame(resp_frame(from, 42, b"payload"), node);
        let m = rx.try_recv().expect("first RESP should be delivered");
        assert_eq!(m.session, 42);
        assert_eq!(m.ptype(), context::PTYPE_LUA);
        assert!(CLUSTER.outbound_calls.get(&(from, 42)).is_none());

        dispatch_frame(resp_frame(from, 42, b"payload2"), node);
        assert!(rx.try_recv().is_err(), "duplicate RESP must be deduped");
    }

    /// A close failure followed by a late RESP must not double-wake the caller.
    #[test]
    #[parallel]
    fn close_then_late_resp_does_not_double_wake() {
        let from = 0x7004_0001;
        let node = 0x7004_00A1;
        let mut rx = reg_actor(from);

        insert_outbound(from, 7, node, 5);

        on_connection_closed(node, 5, ClusterCloseReason::SocketError);
        let m = rx.try_recv().expect("close should fail the pending call");
        assert_eq!(m.session, 7);
        assert_eq!(m.ptype(), context::PTYPE_ERROR);

        dispatch_frame(resp_frame(from, 7, b"late"), node);
        assert!(rx.try_recv().is_err(), "post-close RESP must be deduped");
    }

    /// Incoming-call bookkeeping is dropped on close *without* sending a
    /// misdirected error to the (remote) caller address: the local handler is the
    /// responder, not a waiter.
    #[test]
    #[parallel]
    fn close_drops_incoming_pending_without_error() {
        let remote_caller = 0x7005_0001;
        let node = 0x7005_00A1;
        let mut rx = reg_actor(remote_caller);

        CLUSTER.pending_calls.insert(
            500,
            PendingCallInfo {
                node_id: node,
                from_addr: remote_caller,
                session: 99,
                timestamp: Instant::now(),
            },
        );

        on_connection_closed(node, 1, ClusterCloseReason::Eof);

        assert!(CLUSTER.pending_calls.get(&500).is_none());
        assert!(
            rx.try_recv().is_err(),
            "incoming-call cleanup must not respond_error"
        );
    }

    /// `fail_outbound_call` releases at most once even if called twice.
    #[test]
    #[parallel]
    fn fail_outbound_call_is_idempotent() {
        let from = 0x7006_0001;
        let mut rx = reg_actor(from);

        insert_outbound(from, 3, 0x7006_00A1, 5);

        fail_outbound_call(from, 3, "boom".to_string());
        let m = rx.try_recv().expect("first failure should notify");
        assert_eq!(m.session, 3);
        assert_eq!(m.ptype(), context::PTYPE_ERROR);

        fail_outbound_call(from, 3, "boom again".to_string());
        assert!(rx.try_recv().is_err(), "second failure must be a no-op");
    }

    fn make_conn(capacity: usize) -> (ConnectionState, ClusterWriteReceiver) {
        let (tx, rx) = mpsc::channel::<ClusterFrame>(capacity);
        let conn = Arc::new(ConnectionStateInner {
            tx,
            cgen: AtomicU64::new(0),
            pending_writes: AtomicUsize::new(0),
            closing: AtomicBool::new(false),
        });
        (conn, rx)
    }

    fn make_conn_with_cgen(capacity: usize, cgen: u64) -> (ConnectionState, ClusterWriteReceiver) {
        let (tx, rx) = mpsc::channel::<ClusterFrame>(capacity);
        let conn = Arc::new(ConnectionStateInner {
            tx,
            cgen: AtomicU64::new(cgen),
            pending_writes: AtomicUsize::new(0),
            closing: AtomicBool::new(false),
        });
        (conn, rx)
    }

    #[test]
    #[parallel]
    fn enqueue_cluster_frame_reports_full_and_closed() {
        let (conn, _rx) = make_conn(1);

        enqueue_cluster_frame(&conn, ClusterFrame::header_only(b"PING\n".to_vec()), "test")
            .expect("first frame should fit");
        assert_eq!(conn.pending_writes.load(Ordering::Acquire), 1);

        let err =
            enqueue_cluster_frame(&conn, ClusterFrame::header_only(b"PING\n".to_vec()), "test")
                .expect_err("second frame should hit bounded queue capacity");
        assert!(err.contains("write queue full"));

        let (conn, rx) = make_conn(1);
        drop(rx);
        let err =
            enqueue_cluster_frame(&conn, ClusterFrame::header_only(b"PING\n".to_vec()), "test")
                .expect_err("closed receiver should reject enqueue");
        assert!(err.contains("write queue closed"));
    }

    #[test]
    #[parallel]
    fn enqueue_rejects_when_closing() {
        let (conn, _rx) = make_conn(16);
        conn.closing.store(true, Ordering::Release);

        let err =
            enqueue_cluster_frame(&conn, ClusterFrame::header_only(b"PING\n".to_vec()), "test")
                .expect_err("should reject when closing");
        assert!(err.contains("closing"));
    }

    /// Outbound calls that have been pending longer than the timeout threshold
    /// are released with a PTYPE_ERROR "timeout" message.
    #[test]
    #[parallel]
    fn timeout_releases_expired_outbound_calls() {
        let from = 0x700B_0001;
        let node = 0x700B_00A1;
        let mut rx = reg_actor(from);

        // Insert an outbound call with a timestamp far in the past
        CLUSTER.outbound_calls.insert(
            (from, 77),
            OutboundCallInfo {
                to_node: node,
                cgen: 1,
                timestamp: Instant::now() - Duration::from_secs(CALL_TIMEOUT_S + 1),
            },
        );

        // Insert another call that is NOT expired
        CLUSTER.outbound_calls.insert(
            (from, 78),
            OutboundCallInfo {
                to_node: node,
                cgen: 1,
                timestamp: Instant::now(),
            },
        );

        check_call_timeouts(CALL_TIMEOUT_S);

        // The expired call should be released
        let m = rx
            .try_recv()
            .expect("expired outbound call should be released");
        assert_eq!(m.session, 77);
        assert_eq!(m.ptype(), context::PTYPE_ERROR);
        if let MessageBody::Buffer(_, data) = &m.data {
            let text = std::str::from_utf8(data.as_slice()).unwrap();
            assert!(text.contains("timeout"), "error should mention timeout");
        }
        assert!(CLUSTER.outbound_calls.get(&(from, 77)).is_none());

        // The non-expired call should still exist
        assert!(CLUSTER.outbound_calls.get(&(from, 78)).is_some());
        assert!(
            rx.try_recv().is_err(),
            "non-expired call must not be released"
        );

        // Cleanup
        CLUSTER.outbound_calls.remove(&(from, 78));
    }

    /// Expired incoming `pending_calls` are silently removed without sending
    /// any error to the (remote) caller.
    #[test]
    #[parallel]
    fn timeout_removes_expired_pending_calls_silently() {
        let remote_caller = 0x700C_0001;
        let node = 0x700C_00A1;
        let mut rx = reg_actor(remote_caller);

        // Expired incoming call
        CLUSTER.pending_calls.insert(
            6000,
            PendingCallInfo {
                node_id: node,
                from_addr: remote_caller,
                session: 50,
                timestamp: Instant::now() - Duration::from_secs(CALL_TIMEOUT_S + 1),
            },
        );

        // Non-expired incoming call
        CLUSTER.pending_calls.insert(
            6001,
            PendingCallInfo {
                node_id: node,
                from_addr: remote_caller,
                session: 51,
                timestamp: Instant::now(),
            },
        );

        check_call_timeouts(CALL_TIMEOUT_S);

        // Expired entry removed
        assert!(CLUSTER.pending_calls.get(&6000).is_none());
        // Non-expired entry retained
        assert!(CLUSTER.pending_calls.get(&6001).is_some());
        // No message sent to any actor (incoming calls don't notify)
        assert!(
            rx.try_recv().is_err(),
            "pending_calls timeout must NOT send error to remote caller"
        );

        // Cleanup
        CLUSTER.pending_calls.remove(&6001);
    }

    /// Timeout does not double-release a call already resolved by RESP or close.
    #[test]
    #[parallel]
    fn timeout_skips_already_resolved_outbound_calls() {
        let from = 0x700D_0001;
        let node = 0x700D_00A1;
        let mut rx = reg_actor(from);

        // Insert an expired outbound call
        CLUSTER.outbound_calls.insert(
            (from, 90),
            OutboundCallInfo {
                to_node: node,
                cgen: 1,
                timestamp: Instant::now() - Duration::from_secs(CALL_TIMEOUT_S + 1),
            },
        );

        // Simulate RESP arriving first (removes the entry)
        CLUSTER.outbound_calls.remove(&(from, 90));

        check_call_timeouts(CALL_TIMEOUT_S);

        // No message should be sent (the call was already resolved)
        assert!(
            rx.try_recv().is_err(),
            "already-resolved call must not be released again"
        );
    }

    #[test]
    #[parallel]
    fn enqueue_failure_close_releases_matching_outbound_wait() {
        let from = 0x7008_0001;
        let node = 0x7008_00A1;
        let cgen = 0x7008_u64;
        let mut rx = reg_actor(from);

        let (conn, _rx2) = make_conn_with_cgen(1, cgen);
        CLUSTER.connections.insert(node, conn);
        insert_outbound(from, 88, node, cgen);

        close_connection_for_enqueue_error(node, "test backpressure");

        let m = rx
            .try_recv()
            .expect("backpressure close should release the pending wait");
        assert_eq!(m.session, 88);
        assert_eq!(m.ptype(), context::PTYPE_ERROR);
        assert!(CLUSTER.outbound_calls.get(&(from, 88)).is_none());
        assert!(CLUSTER.connections.get(&node).is_none());
    }

    /// End-to-end over a real TCP socket: when the peer drops the connection,
    /// `read_task` hits EOF and runs `on_connection_closed`, which must release
    /// the caller's pending wait with an error. Exercises the real read loop +
    /// close path (not just the synchronous cleanup helper).
    #[tokio::test]
    #[parallel]
    async fn real_tcp_close_releases_outbound_wait() {
        let from = 0x7007_0001;
        let node = 0x7007_00A1;
        let cgen = 0x7007_u64;

        let mut rx = reg_actor(from);

        // A real loopback TCP connection pair within this process.
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let client = TcpStream::connect(addr).await.unwrap();
        let (server, _peer) = listener.accept().await.unwrap();

        // Register this generation as the current connection and a pending call on it.
        let (conn, _dummy_rx) = make_conn_with_cgen(1, cgen);
        CLUSTER.connections.insert(node, conn.clone());
        insert_outbound(from, 55, node, cgen);

        // Drive the server's read side exactly as production does.
        let (read_half, _write_half) = server.into_split();
        let handle = tokio::spawn(read_task(read_half, node, conn, cgen));

        // Peer disconnects -> EOF -> read_task breaks -> on_connection_closed.
        drop(client);
        handle.await.unwrap();

        let m = rx
            .try_recv()
            .expect("socket close should release the pending wait");
        assert_eq!(m.session, 55);
        assert_eq!(m.ptype(), context::PTYPE_ERROR);
        assert!(CLUSTER.outbound_calls.get(&(from, 55)).is_none());
        assert!(
            CLUSTER.connections.get(&node).is_none(),
            "connection state torn down"
        );
    }

    #[tokio::test]
    #[parallel]
    async fn write_cluster_frame_preserves_wire_format() {
        let header = b"CALL echo 1 2 3\n".to_vec();
        let body = Box::new(Buffer::from_slice(b"payload"));
        let frame = ClusterFrame::with_body(header.clone(), body);
        let total_len = frame.total_len() as u32;

        let (mut client, mut server) = tokio::io::duplex(128);
        let writer = tokio::spawn(async move {
            write_cluster_frame(&mut client, &frame, total_len)
                .await
                .unwrap();
        });

        let mut out = vec![0u8; 4 + total_len as usize];
        server.read_exact(&mut out).await.unwrap();
        writer.await.unwrap();

        assert_eq!(&out[..4], &total_len.to_be_bytes());
        assert_eq!(&out[4..4 + header.len()], header.as_slice());
        assert_eq!(&out[4 + header.len()..], b"payload");
    }

    /// Full `shutdown_cluster` test: exercises the real function that iterates all
    /// connections, calls `on_connection_closed` per entry, clears global state,
    /// and fails all remaining outbound calls (including "not yet sent" ones).
    ///
    /// Serialized with `#[serial]` because `shutdown_cluster` calls `.clear()` on
    /// global `CLUSTER` maps — cannot run in parallel with other tests that
    /// depend on CLUSTER state.
    #[test]
    #[serial]
    fn shutdown_cluster_full_teardown() {
        // --- Setup: two connections, two outbound calls (one per connection),
        //     one outbound call still connecting (cgen=MAX), one pending_call ---
        let from_a = 0x700A_0001;
        let from_b = 0x700A_0002;
        let from_c = 0x700A_0003;
        let node_x = 0x700A_00A1;
        let node_y = 0x700A_00A2;
        let cgen_x = 0xA001_u64;
        let cgen_y = 0xA002_u64;

        let mut rx_a = reg_actor(from_a);
        let mut rx_b = reg_actor(from_b);
        let mut rx_c = reg_actor(from_c);

        let (conn_x, _rx_x) = make_conn_with_cgen(4, cgen_x);
        let (conn_y, _rx_y) = make_conn_with_cgen(4, cgen_y);
        CLUSTER.connections.insert(node_x, conn_x.clone());
        CLUSTER.connections.insert(node_y, conn_y.clone());
        CLUSTER.connecting.insert(0x700A_00B1); // a "connecting" node

        // Outbound calls: sent on known cgens
        insert_outbound(from_a, 100, node_x, cgen_x);
        insert_outbound(from_b, 200, node_y, cgen_y);
        // Outbound call still connecting (cgen=MAX, not yet associated with a connection)
        insert_outbound(from_c, 300, 0x700A_00B1, u64::MAX);

        // Pending incoming call from node_x
        CLUSTER.pending_calls.insert(
            9000,
            PendingCallInfo {
                node_id: node_x,
                from_addr: 0xDEAD,
                session: 42,
                timestamp: Instant::now(),
            },
        );

        CLUSTER.initialized.store(true, Ordering::Release);

        // --- Act ---
        shutdown_cluster(ClusterCloseReason::Shutdown);

        // --- Assert ---
        // initialized flag cleared
        assert!(!CLUSTER.initialized.load(Ordering::Acquire));

        // All connections removed
        assert!(CLUSTER.connections.is_empty());
        assert!(CLUSTER.connecting.is_empty());
        assert!(CLUSTER.pending_calls.is_empty());
        assert!(CLUSTER.outbound_calls.is_empty());

        // closing flag set on both connection Arcs
        assert!(conn_x.closing.load(Ordering::Acquire));
        assert!(conn_y.closing.load(Ordering::Acquire));

        // Actor A: outbound call on node_x failed
        let m_a = rx_a
            .try_recv()
            .expect("actor A's outbound call should be failed on shutdown");
        assert_eq!(m_a.session, 100);
        assert_eq!(m_a.ptype(), context::PTYPE_ERROR);
        if let MessageBody::Buffer(_, data) = &m_a.data {
            let text = std::str::from_utf8(data.as_slice()).unwrap();
            assert!(
                text.contains("SHUTDOWN"),
                "error should mention SHUTDOWN reason"
            );
        }

        // Actor B: outbound call on node_y failed
        let m_b = rx_b
            .try_recv()
            .expect("actor B's outbound call should be failed on shutdown");
        assert_eq!(m_b.session, 200);
        assert_eq!(m_b.ptype(), context::PTYPE_ERROR);

        // Actor C: "not yet sent" outbound call (cgen=MAX) also failed in the
        // final sweep of remaining outbound_calls
        let m_c = rx_c
            .try_recv()
            .expect("actor C's connecting call should be failed on shutdown");
        assert_eq!(m_c.session, 300);
        assert_eq!(m_c.ptype(), context::PTYPE_ERROR);
        if let MessageBody::Buffer(_, data) = &m_c.data {
            let text = std::str::from_utf8(data.as_slice()).unwrap();
            assert!(text.contains("shutdown"), "error should mention shutdown");
        }

        // No extra messages
        assert!(rx_a.try_recv().is_err());
        assert!(rx_b.try_recv().is_err());
        assert!(rx_c.try_recv().is_err());
    }

    /// The `close_after` flag in ClusterFrame causes write_task to exit after
    /// writing that frame. Uses a real TCP socket pair.
    #[tokio::test]
    #[parallel]
    async fn write_task_exits_on_close_after_frame() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let client = TcpStream::connect(addr).await.unwrap();
        let (server, _) = listener.accept().await.unwrap();

        let (conn, rx) = make_conn(16);
        let (_read_half, write_half) = server.into_split();
        let handle = tokio::spawn(write_task(write_half, rx, conn.clone()));

        // Enqueue a normal frame then a close-after frame
        let normal = ClusterFrame::header_only(b"PING\n".to_vec());
        conn.tx.send(normal).await.unwrap();
        conn.pending_writes.fetch_add(1, Ordering::AcqRel);

        let mut close_frame = ClusterFrame::header_only(b"BYE\n".to_vec());
        close_frame.close_after = true;
        conn.tx.send(close_frame).await.unwrap();
        conn.pending_writes.fetch_add(1, Ordering::AcqRel);

        // write_task should exit after writing the close_after frame
        handle.await.unwrap();
        assert_eq!(conn.pending_writes.load(Ordering::Acquire), 0);

        // Verify both frames were written to the peer. `write_task` has already
        // exited (handle.await above) and dropped its write half, so the peer
        // sees EOF once every byte has arrived. Read to EOF instead of relying on
        // a single `read()` returning both frames — TCP may split them across
        // segments, so one read can legitimately return just the first frame.
        let (mut client_read, _cw) = client.into_split();
        let mut got = Vec::new();
        client_read.read_to_end(&mut got).await.unwrap();
        // PING: 4-byte len (5) + "PING\n" = 9 bytes
        // BYE:  4-byte len (4) + "BYE\n"  = 8 bytes
        assert_eq!(got.len(), 9 + 8, "expected both frames written before close");
    }
}
