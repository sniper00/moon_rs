use dashmap::{DashMap, DashSet};
use lazy_static::lazy_static;
use moon_lua::{
    cstr, ffi,
    laux::{self, LuaState},
    lreg, lreg_null, luaL_newlib,
};
use moon_runtime::{
    actor::LuaActor,
    buffer::Buffer,
    check_buffer,
    context::{self, ActorId, Message, MessageBody, CONTEXT},
};
use std::{
    ffi::c_int,
    sync::{
        atomic::{AtomicBool, AtomicI64, AtomicU32, Ordering},
        RwLock,
    },
    time::{Duration, Instant},
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt, BufReader},
    net::{TcpListener, TcpStream},
    sync::mpsc,
    time::{sleep, timeout},
};

const CLUSTER_ACTOR_ID: ActorId = 0xFFFF_FF00;

const CONNECT_TIMEOUT_MS: u64 = 5000;
const PING_INTERVAL_MS: u64 = 5000;
const CALL_TIMEOUT_S: u64 = 10;
const MAX_FRAME_SIZE: usize = 256 * 1024 * 1024; // 256MB

lazy_static! {
    static ref CLUSTER: ClusterState = ClusterState::new();
}

struct ClusterState {
    node_id: AtomicU32,
    connections: DashMap<u32, mpsc::UnboundedSender<ClusterFrame>>,
    connecting: DashSet<u32>,
    conn_gen: DashMap<u32, u64>,
    pending_calls: DashMap<i64, PendingCallInfo>,
    outbound_calls: DashMap<i64, OutboundCallInfo>,
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
    from_addr: u32,
    timestamp: Instant,
}

impl ClusterState {
    fn new() -> Self {
        Self {
            node_id: AtomicU32::new(0),
            connections: DashMap::new(),
            connecting: DashSet::new(),
            conn_gen: DashMap::new(),
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
}

impl ClusterFrame {
    fn header_only(header: Vec<u8>) -> Self {
        Self { header, body: None }
    }

    fn with_body(header: Vec<u8>, body: Box<Buffer>) -> Self {
        Self {
            header,
            body: Some(body),
        }
    }

    fn total_len(&self) -> usize {
        self.header.len() + self.body.as_ref().map_or(0, |b| b.as_slice().len())
    }
}

fn make_send_header(to_sname: &str, from_node: u32, from_addr: u32) -> Vec<u8> {
    format!("SEND {} {} {}\n", to_sname, from_node, from_addr).into_bytes()
}

fn make_call_header(to_sname: &str, from_node: u32, from_addr: u32, session: i64) -> Vec<u8> {
    format!("CALL {} {} {} {}\n", to_sname, from_node, from_addr, session).into_bytes()
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
    reader
        .read_exact(&mut len_buf)
        .await
        .map_err(|e| if e.kind() == std::io::ErrorKind::UnexpectedEof {
            "early eof".to_string()
        } else {
            e.to_string()
        })?;

    let size = u32::from_be_bytes(len_buf) as usize;
    if size > MAX_FRAME_SIZE {
        return Err(format!("frame too large: {} bytes (max {})", size, MAX_FRAME_SIZE));
    }
    let mut buf = Box::new(Buffer::with_capacity(size));

    if size > 0 {
        let space = buf.prepare(size);
        reader
            .read_exact(&mut space[..size])
            .await
            .map_err(|e| if e.kind() == std::io::ErrorKind::UnexpectedEof {
                "early eof".to_string()
            } else {
                e.to_string()
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
        log::error!("cluster: frame missing header delimiter from node {}", remote_node_id);
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
            if let Some(tx) = CLUSTER.connections.get(&remote_node_id) {
                let _ = tx.value().send(make_pong_frame());
            }
        }
        "PONG" => {}
        "SEND" => {
            let Some(to_sname) = parts.next() else {
                log::error!("cluster: SEND missing service name from node {}", remote_node_id);
                return;
            };
            let Some(_from_node) = parts.next().and_then(|s| s.parse::<u32>().ok()) else {
                log::error!("cluster: SEND invalid from_node from node {}", remote_node_id);
                return;
            };
            let Some(from_addr) = parts.next().and_then(|s| s.parse::<u32>().ok()) else {
                log::error!("cluster: SEND invalid from_addr from node {}", remote_node_id);
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
                log::error!("cluster: CALL missing service name from node {}", remote_node_id);
                return;
            };
            let Some(_from_node) = parts.next().and_then(|s| s.parse::<u32>().ok()) else {
                log::error!("cluster: CALL invalid from_node from node {}", remote_node_id);
                return;
            };
            let Some(from_addr) = parts.next().and_then(|s| s.parse::<u32>().ok()) else {
                log::error!("cluster: CALL invalid from_addr from node {}", remote_node_id);
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
                let err_body = format!("node {}, service '{}' not found", CLUSTER.node_id(), to_sname);
                let err_buf = make_error_seri_buffer(&err_body);
                let mut body_buf = Box::new(Buffer::with_capacity(err_buf.len()));
                let space = body_buf.prepare(err_buf.len());
                space[..err_buf.len()].copy_from_slice(&err_buf);
                let _ = body_buf.commit(err_buf.len());
                let header = make_resp_header(from_addr, session);
                let resp_frame = ClusterFrame::with_body(header, body_buf);
                if let Some(tx) = CLUSTER.connections.get(&remote_node_id) {
                    let _ = tx.value().send(resp_frame);
                }
            }
        }
        "RESP" => {
            let Some(from_addr) = parts.next().and_then(|s| s.parse::<u32>().ok()) else {
                log::error!("cluster: RESP invalid from_addr from node {}", remote_node_id);
                return;
            };
            let Some(session) = parts.next().and_then(|s| s.parse::<i64>().ok()) else {
                log::error!("cluster: RESP invalid session from node {}", remote_node_id);
                return;
            };
            CLUSTER.outbound_calls.remove(&session);
            frame.consume(nl + 1);
            let _ = CONTEXT.send(Message {
                from: 0,
                to: from_addr,
                session,
                data: MessageBody::Buffer(context::PTYPE_LUA, frame),
            });
        }
        _ => {
            log::error!("cluster: unknown verb '{}' from node {}", verb, remote_node_id);
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

    let response = reqwest::get(&url)
        .await
        .map_err(|e| format!("discovery node {} request failed: {} (url: {})", node_id, e, url))?;

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
        return Err(format!("connect to node {} timed out waiting for peer", node_id));
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

    if remote_node_id != 0 {
        CLUSTER.connections.insert(remote_node_id, write_tx.clone());
        CLUSTER.conn_gen.insert(remote_node_id, cgen);
    }

    CONTEXT.io_runtime().spawn(write_task(write_half, write_rx));
    CONTEXT.io_runtime().spawn(read_task(read_half, remote_node_id, write_tx.clone(), cgen));

    if is_initiator {
        let _ = write_tx.send(make_hello_frame(my_node));
    }
}

async fn write_task(
    mut writer: tokio::net::tcp::OwnedWriteHalf,
    mut rx: mpsc::UnboundedReceiver<ClusterFrame>,
) {
    while let Some(frame) = rx.recv().await {
        let total_len = frame.total_len() as u32;
        if let Err(e) = writer.write_all(&total_len.to_be_bytes()).await {
            log::error!("cluster write error (len): {}", e);
            break;
        }
        if let Err(e) = writer.write_all(&frame.header).await {
            log::error!("cluster write error (header): {}", e);
            break;
        }
        if let Some(body) = &frame.body {
            if let Err(e) = writer.write_all(body.as_slice()).await {
                log::error!("cluster write error (body): {}", e);
                break;
            }
        }
    }
}

async fn read_task(
    read_half: tokio::net::tcp::OwnedReadHalf,
    mut remote_node_id: u32,
    write_tx: mpsc::UnboundedSender<ClusterFrame>,
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
                    CLUSTER.connections.insert(node_id, write_tx);
                    CLUSTER.conn_gen.insert(node_id, cgen);
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
    } else {
        drop(write_tx);
    }

    loop {
        match read_one_frame(&mut reader).await {
            Ok(frame) => {
                dispatch_frame(frame, remote_node_id);
            }
            Err(e) => {
                log::warn!("cluster: connection to node {} closed: {}", remote_node_id, e);
                break;
            }
        }
    }

    on_connection_closed(remote_node_id, cgen);
}

fn on_connection_closed(node_id: u32, cgen: u64) {
    // Only remove if this connection is still the current one (not replaced by a newer connection)
    if let Some(current_gen) = CLUSTER.conn_gen.get(&node_id) {
        if *current_gen != cgen {
            return;
        }
    }
    CLUSTER.connections.remove(&node_id);
    CLUSTER.conn_gen.remove(&node_id);

    let mut to_remove = Vec::new();
    for entry in CLUSTER.pending_calls.iter() {
        if entry.value().node_id == node_id {
            to_remove.push(*entry.key());
        }
    }

    for session in to_remove {
        if let Some((_, info)) = CLUSTER.pending_calls.remove(&session) {
            CONTEXT.response_error(
                0,
                info.from_addr,
                -session,
                format!("cluster connection to node {} closed", node_id),
            );
        }
    }

    // Clean up outbound calls (calls THIS node made to the disconnected node)
    let mut outbound_to_remove = Vec::new();
    for entry in CLUSTER.outbound_calls.iter() {
        if entry.value().to_node == node_id {
            outbound_to_remove.push(*entry.key());
        }
    }

    for session in outbound_to_remove {
        if let Some((_, info)) = CLUSTER.outbound_calls.remove(&session) {
            CONTEXT.response_error(
                0,
                info.from_addr,
                -session,
                format!("cluster connection to node {} closed", node_id),
            );
        }
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
                if let Some(tx) = CLUSTER.connections.get(&info.node_id) {
                    let _ = tx.value().send(frame);
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
            for entry in CLUSTER.connections.iter() {
                let _ = entry.value().send(make_ping_frame());
                conn_count += 1;
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

fn spawn_call_timeout_checker() {
    CONTEXT.io_runtime().spawn(async move {
        loop {
            sleep(Duration::from_secs(CALL_TIMEOUT_S)).await;

            if !CLUSTER.initialized.load(Ordering::Acquire) {
                break;
            }

            let now = Instant::now();
            let mut expired = Vec::new();

            for entry in CLUSTER.pending_calls.iter() {
                if now.duration_since(entry.value().timestamp).as_secs() >= CALL_TIMEOUT_S {
                    expired.push(*entry.key());
                }
            }

            for session in expired {
                if let Some((_, info)) = CLUSTER.pending_calls.remove(&session) {
                    CONTEXT.response_error(
                        0,
                        info.from_addr,
                        -session,
                        format!("cluster call to node {} timeout (session {})", info.node_id, info.session),
                    );
                }
            }

            // Also timeout outbound calls
            let mut outbound_expired = Vec::new();
            for entry in CLUSTER.outbound_calls.iter() {
                if now.duration_since(entry.value().timestamp).as_secs() >= CALL_TIMEOUT_S {
                    outbound_expired.push(*entry.key());
                }
            }

            for session in outbound_expired {
                if let Some((_, info)) = CLUSTER.outbound_calls.remove(&session) {
                    CONTEXT.response_error(
                        0,
                        info.from_addr,
                        -session,
                        format!("cluster call to node {} timeout", info.to_node),
                    );
                }
            }
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
                        log::error!("cluster node {}: listen bind '{}' failed: {}", node_id, addr, e);
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

    if let Some(tx) = CLUSTER.connections.get(&to_node) {
        let _ = tx.value().send(frame);
    } else {
        CONTEXT.io_runtime().spawn(async move {
            if let Err(e) = connect_to_node(to_node).await {
                log::error!("cluster send: connect to node {} failed: {}", to_node, e);
                return;
            }
            if let Some(tx) = CLUSTER.connections.get(&to_node) {
                let _ = tx.value().send(frame);
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

    CLUSTER.outbound_calls.insert(session, OutboundCallInfo {
        to_node,
        from_addr,
        timestamp: Instant::now(),
    });

    if let Some(tx) = CLUSTER.connections.get(&to_node) {
        if tx.value().send(frame).is_err() {
            CLUSTER.outbound_calls.remove(&session);
            CONTEXT.io_runtime().spawn(async move {
                CONTEXT.response_error(
                    0,
                    from_addr,
                    -session,
                    format!("cluster call: channel to node {} closed", to_node),
                );
            });
        }
    } else {
        let session_copy = session;
        CONTEXT.io_runtime().spawn(async move {
            if let Err(e) = connect_to_node(to_node).await {
                CLUSTER.outbound_calls.remove(&session_copy);
                CONTEXT.response_error(
                    0,
                    from_addr,
                    -session_copy,
                    format!("cluster call: connect to node {} failed: {}", to_node, e),
                );
                return;
            }
            if let Some(tx) = CLUSTER.connections.get(&to_node) {
                if tx.value().send(frame).is_err() {
                    CLUSTER.outbound_calls.remove(&session_copy);
                    CONTEXT.response_error(
                        0,
                        from_addr,
                        -session_copy,
                        format!("cluster call: channel to node {} closed after connect", to_node),
                    );
                }
            } else {
                CLUSTER.outbound_calls.remove(&session_copy);
                CONTEXT.response_error(
                    0,
                    from_addr,
                    -session_copy,
                    format!("cluster call: connection to node {} lost after connect", to_node),
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
        lreg!("send", lua_cluster_send),
        lreg!("request", lua_cluster_request),
        lreg_null!(),
    ];

    luaL_newlib!(state, l);
    1
}
