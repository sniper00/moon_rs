//! Native gRPC client for moon_rs (`grpc.core`).
//!
//! Design notes
//! ------------
//! This module is a thin, **protobuf-agnostic** gRPC transport built on
//! [`tonic`]'s HTTP/2 `Channel`. Message (de)serialization is intentionally
//! **not** done here — the Lua side encodes the request with
//! `protobuf.encode(...)` (see `lua_protobuf.rs`) and decodes the reply with
//! `protobuf.decode(...)`. The native layer only moves *raw protobuf bytes*
//! across the wire, which keeps a single source of truth for the schema and
//! avoids a second protobuf implementation.
//!
//! To pass raw bytes through tonic we install a [`BytesCodec`] whose
//! encoder/decoder are identity passthroughs over [`bytes::Bytes`].
//!
//! Concurrency model follows the project's session-based async bridge (see the
//! `moon-async-module` skill):
//!   * Lua-facing `extern "C-unwind"` functions capture `(owner, session)`,
//!     hand the work to `CONTEXT.io_runtime()`, and return the `session`
//!     immediately.
//!   * The worker replies via `CONTEXT.send_value(PTYPE_GRPC, owner, session, _)`.
//!   * A registered decoder (`decode_grpc_message`) turns the reply into Lua
//!     values and resumes the coroutine blocked in `moon.wait(session)`.
//!
//! Both unary and streaming RPCs are supported. Streams are exposed as a small
//! handle (`grpc.core.find_stream(fd)`): `recv` reads one message at a time
//! (server/bidi streaming), `send`/`close_send` feed the request stream
//! (client/bidi streaming).

use std::{
    ffi::c_int,
    future::{self, Ready},
    net::SocketAddr,
    pin::Pin,
    sync::Arc,
    sync::atomic::{AtomicBool, Ordering},
    time::Duration,
};

use bytes::{Buf, BufMut, Bytes};
use dashmap::DashMap;
use futures_util::{Stream, StreamExt};
use http::uri::PathAndQuery;
use hyper::{body::Incoming, server::conn::http2, service::service_fn};
use hyper_util::rt::{TokioExecutor, TokioIo};
use lazy_static::lazy_static;
use tokio::net::TcpListener;
use tokio::sync::{Semaphore, mpsc};
use tokio::task::AbortHandle;
use tokio_stream::wrappers::UnboundedReceiverStream;
use tokio_util::sync::CancellationToken;
use tonic::{
    Code, Request, Response, Status, Streaming,
    body::Body as TonicBody,
    client::Grpc,
    codec::{Codec, DecodeBuf, Decoder, EncodeBuf, Encoder},
    metadata::{MetadataKey, MetadataValue},
    server::{Grpc as ServerGrpc, StreamingService},
    transport::{Certificate, Channel, ClientTlsConfig, Identity},
};

use moon_base::{
    cstr, ffi,
    laux::{self, LuaState, LuaTable},
    lreg, lreg_null, luaL_newlib,
};
use moon_runtime::{
    actor::LuaActor,
    context::{self, ActorId, CONTEXT},
};

use crate::LIMITS;

lazy_static! {
    /// Named registry of established channels. A tonic `Channel` multiplexes
    /// concurrent requests over one HTTP/2 connection (and reconnects on its
    /// own), so we keep a single channel per connection name rather than a
    /// worker pool.
    static ref GRPC_CONNECTIONS: DashMap<String, Channel> = DashMap::new();
    /// Active (client-side) streaming RPCs keyed by a process-unique fd.
    static ref GRPC_STREAMS: DashMap<i64, StreamEntry> = DashMap::new();
    /// Running gRPC server listeners keyed by fd; the token cancels the accept
    /// loop on `stop(fd)`.
    static ref GRPC_SERVERS: DashMap<i64, CancellationToken> = DashMap::new();
}

// ---------------------------------------------------------------------------
// Passthrough codec: tonic frames/unframes, we carry raw protobuf bytes.
// ---------------------------------------------------------------------------

#[derive(Default)]
struct BytesCodec;

struct BytesEncoder;
struct BytesDecoder;

impl Codec for BytesCodec {
    type Encode = Bytes;
    type Decode = Bytes;
    type Encoder = BytesEncoder;
    type Decoder = BytesDecoder;

    fn encoder(&mut self) -> Self::Encoder {
        BytesEncoder
    }

    fn decoder(&mut self) -> Self::Decoder {
        BytesDecoder
    }
}

impl Encoder for BytesEncoder {
    type Item = Bytes;
    type Error = Status;

    fn encode(&mut self, item: Bytes, dst: &mut EncodeBuf<'_>) -> Result<(), Status> {
        dst.put_slice(&item);
        Ok(())
    }
}

impl Decoder for BytesDecoder {
    type Item = Bytes;
    type Error = Status;

    fn decode(&mut self, src: &mut DecodeBuf<'_>) -> Result<Option<Bytes>, Status> {
        // tonic hands us exactly one length-delimited message frame; take all
        // of it as the raw protobuf payload.
        let len = src.remaining();
        Ok(Some(src.copy_to_bytes(len)))
    }
}

// ---------------------------------------------------------------------------
// Messages delivered back to Lua via PTYPE_GRPC.
// ---------------------------------------------------------------------------

enum GrpcResponse {
    /// `connect` succeeded; the channel is registered under its name.
    Connect,
    /// Unary reply. `status == 0` means OK and `body` carries the response
    /// bytes; otherwise it is a gRPC status code with `message`.
    Unary {
        status: i32,
        message: String,
        body: Option<Bytes>,
    },
    /// A streaming RPC was opened; `fd` is the handle for `find_stream`.
    StreamOpen(i64),
    /// One message off a response stream, or `None` at a clean end-of-stream.
    StreamMessage(Option<Bytes>),
    /// Generic error (transport/connect/status/etc.) -> `(false, msg)` in Lua.
    Error(String),
    /// **Server side**: a new inbound RPC arrived on a listener. Delivered with
    /// `session == 0` so it routes to the `grpc` protocol's `dispatch` handler.
    /// Decoded into `(path, server_stream_handle)`.
    ServerRpc { path: String, handle: ServerStreamHandle },
}

// ---------------------------------------------------------------------------
// Server side: one inbound RPC is exposed to Lua as a bidirectional handle.
// ---------------------------------------------------------------------------

/// One outbound response item. `Some(Ok)` is a message, `Some(Err)` ends the
/// RPC with a non-OK status, and `None` is the end-of-stream sentinel that
/// terminates the response stream with OK.
type RespItem = Option<Result<Bytes, Status>>;

/// Userdata handle for a single inbound RPC. Every RPC (unary/server/client/
/// bidi) is handled uniformly: `recv` pulls the next request message, `send`
/// pushes a response message, `finish` ends the response stream.
struct ServerStreamHandle {
    /// Ask the request-reader task for the next inbound message. Bounded to 1
    /// so a second concurrent `recv` is rejected rather than queued.
    tx_recv: mpsc::Sender<(ActorId, i64)>,
    /// Feed the outbound response stream.
    tx_resp: mpsc::UnboundedSender<RespItem>,
    /// Set once `finish` runs so the response stream is closed exactly once and
    /// later `send`/`finish` calls are no-ops.
    finished: AtomicBool,
}

/// Parsed, owned TLS options moved into the connect task (PEM bytes are read on
/// the Lua thread because the spawned task has no `lua_State`).
#[derive(Default)]
struct TlsOptions {
    enabled: bool,
    domain: Option<String>,
    ca: Option<Vec<u8>>,
    cert: Option<Vec<u8>>,
    key: Option<Vec<u8>>,
}

/// Userdata handle for a live stream (cheap to clone; shares the channels).
#[derive(Clone)]
struct StreamHandle {
    /// Request "give me the next message" to the recv loop. Bounded to 1 so a
    /// second concurrent `recv` is rejected instead of queueing.
    tx_recv: mpsc::Sender<(ActorId, i64)>,
    /// Feed the outbound request stream (client/bidi streaming). `Some(bytes)`
    /// is a message; `None` half-closes the request stream. `None` for
    /// server-streaming RPCs (no client messages).
    tx_send: Option<mpsc::UnboundedSender<Option<Vec<u8>>>>,
}

/// Registry entry: the handle plus an abort handle so `close()` can tear the
/// background task down deterministically (not just when Lua GCs the handle).
struct StreamEntry {
    handle: StreamHandle,
    abort: AbortHandle,
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn grpc_client(channel: Channel) -> Grpc<Channel> {
    let limit = LIMITS.max_network_read_bytes;
    Grpc::new(channel)
        .max_decoding_message_size(limit)
        .max_encoding_message_size(limit)
}

/// Apply a `{ key = value, ... }` Lua metadata table onto a request. Invalid
/// ascii keys/values are skipped with a warning rather than failing the call.
/// Generic over the request body type so it serves both unary (`Bytes`) and
/// streaming request bodies.
fn apply_metadata<R>(request: &mut Request<R>, metadata: &[(String, String)]) {
    let md = request.metadata_mut();
    for (k, v) in metadata {
        match (
            MetadataKey::from_bytes(k.as_bytes()),
            MetadataValue::try_from(v.as_str()),
        ) {
            (Ok(key), Ok(val)) => {
                md.insert(key, val);
            }
            _ => log::warn!("grpc: skipping invalid metadata entry '{}'", k),
        }
    }
}

/// Read an optional `metadata = { k = v }` table at `index` into owned pairs.
fn read_metadata(state: LuaState, index: i32) -> Vec<(String, String)> {
    let mut out = Vec::new();
    if laux::lua_type(state, index) != laux::LuaType::Table {
        return out;
    }
    let table = LuaTable::from_stack(state, index);
    for (key, value) in table.iter() {
        out.push((key.to_string(), value.to_string()));
    }
    out
}

fn next_grpc_fd() -> i64 {
    crate::next_net_fd()
}

// ---------------------------------------------------------------------------
// connect / close / find_connection
// ---------------------------------------------------------------------------

async fn build_channel(
    uri: String,
    connect_timeout: u64,
    tls: TlsOptions,
) -> Result<Channel, String> {
    let mut endpoint = Channel::from_shared(uri).map_err(|e| e.to_string())?;
    endpoint = endpoint.connect_timeout(Duration::from_millis(connect_timeout));

    if tls.enabled {
        let mut cfg = ClientTlsConfig::new().with_webpki_roots();
        if let Some(domain) = tls.domain {
            cfg = cfg.domain_name(domain);
        }
        if let Some(ca) = tls.ca {
            cfg = cfg.ca_certificate(Certificate::from_pem(ca));
        }
        if let (Some(cert), Some(key)) = (tls.cert, tls.key) {
            cfg = cfg.identity(Identity::from_pem(cert, key));
        }
        endpoint = endpoint.tls_config(cfg).map_err(|e| e.to_string())?;
    }

    endpoint.connect().await.map_err(|e| e.to_string())
}

extern "C-unwind" fn grpc_connect(state: LuaState) -> c_int {
    laux::lua_checktype(state, 1, ffi::LUA_TTABLE);

    let endpoint: String = laux::opt_field(state, 1, "endpoint").unwrap_or_default();
    if endpoint.is_empty() {
        return crate::lua_push_error(state, "grpc.connect: 'endpoint' is required");
    }
    let name: String = laux::opt_field(state, 1, "name").unwrap_or_else(|| "default".to_string());
    let connect_timeout: u64 = laux::opt_field(state, 1, "connect_timeout").unwrap_or(5000);

    // TLS is enabled implicitly for https endpoints, or explicitly via a `tls`
    // table. Read any PEM material now, on the Lua thread.
    let mut tls = TlsOptions {
        enabled: endpoint.starts_with("https"),
        ..Default::default()
    };
    unsafe {
        ffi::lua_getfield(state.as_ptr(), 1, cstr!("tls"));
        if laux::lua_type(state, -1) == laux::LuaType::Table {
            tls.enabled = true;
            let top = laux::lua_top(state);
            tls.domain = laux::opt_field(state, top, "domain");
            tls.ca = laux::opt_field::<&[u8]>(state, top, "ca").map(|b| b.to_vec());
            tls.cert = laux::opt_field::<&[u8]>(state, top, "cert").map(|b| b.to_vec());
            tls.key = laux::opt_field::<&[u8]>(state, top, "key").map(|b| b.to_vec());
        }
        ffi::lua_pop(state.as_ptr(), 1);
    }

    let actor = LuaActor::from_lua_state(state);
    let owner = unsafe { (*actor).id };
    let session = unsafe { (*actor).next_session() };

    CONTEXT.io_runtime().spawn(async move {
        match build_channel(endpoint, connect_timeout, tls).await {
            Ok(channel) => {
                GRPC_CONNECTIONS.insert(name, channel);
                let _ = CONTEXT.send_value(context::PTYPE_GRPC, owner, session, GrpcResponse::Connect);
            }
            Err(err) => {
                let _ = CONTEXT.send_value(
                    context::PTYPE_GRPC,
                    owner,
                    session,
                    GrpcResponse::Error(format!("grpc connect failed: {}", err)),
                );
            }
        }
    });

    laux::lua_push(state, session);
    1
}

extern "C-unwind" fn grpc_close(state: LuaState) -> c_int {
    let name = unsafe { laux::lua_check_str(state, 1) };
    GRPC_CONNECTIONS.remove(name);
    laux::lua_push(state, true);
    1
}

extern "C-unwind" fn grpc_stats(state: LuaState) -> c_int {
    let table = LuaTable::new(state, 0, 3);
    table.insert("connections", GRPC_CONNECTIONS.len() as i64);
    table.insert("streams", GRPC_STREAMS.len() as i64);
    table.insert("servers", GRPC_SERVERS.len() as i64);
    1
}

extern "C-unwind" fn grpc_find_connection(state: LuaState) -> c_int {
    let name = unsafe { laux::lua_check_str(state, 1) };
    match GRPC_CONNECTIONS.get(name) {
        Some(entry) => {
            let l = [
                lreg!("unary", conn_unary),
                lreg!("server_stream", conn_server_stream),
                lreg!("bidi_stream", conn_bidi_stream),
                lreg_null!(),
            ];
            if laux::lua_newuserdata(
                state,
                entry.value().clone(),
                cstr!("grpc_connection_metatable"),
                l.as_ref(),
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

// ---------------------------------------------------------------------------
// Connection-handle methods: unary / server_stream / bidi_stream
// ---------------------------------------------------------------------------

/// Validate & read the request body (raw protobuf bytes) at `index`.
fn read_body(state: LuaState, index: i32) -> Result<Vec<u8>, String> {
    let body = match laux::lua_type(state, index) {
        laux::LuaType::String => unsafe { laux::lua_check_lstring(state, index) }.to_vec(),
        laux::LuaType::LightUserData => {
            let ptr = unsafe { ffi::lua_touserdata(state.as_ptr(), index) };
            if ptr.is_null() {
                return Err("grpc: request body pointer is null".to_string());
            }
            let buf = unsafe { Box::from_raw(ptr as *mut moon_runtime::buffer::Buffer) };
            buf.into_vec()
        }
        _ => return Err("grpc: request body must be a string or buffer".to_string()),
    };
    if body.len() > LIMITS.max_network_read_bytes {
        return Err(format!(
            "grpc: request body too large: {} bytes (max {})",
            body.len(),
            LIMITS.max_network_read_bytes
        ));
    }
    Ok(body)
}

/// `handle:unary(path, request_bytes, timeout?, metadata?)` -> session
extern "C-unwind" fn conn_unary(state: LuaState) -> c_int {
    let channel_ref =
        laux::lua_touserdata::<Channel>(state, 1).expect("invalid grpc connection pointer");
    // Do every operation that can longjmp (arg validation) *before* cloning the
    // owned `Channel`, so a raised Lua error can't leak the clone.
    let path_str = unsafe { laux::lua_check_str(state, 2) }.to_string();
    let path = match PathAndQuery::try_from(path_str.clone()) {
        Ok(p) => p,
        Err(e) => return crate::lua_push_error(state, &format!("grpc: invalid path '{}': {}", path_str, e)),
    };
    let body = match read_body(state, 3) {
        Ok(b) => b,
        Err(e) => return crate::lua_push_error(state, &e),
    };
    let timeout: u64 = laux::lua_opt(state, 4).unwrap_or(0);
    let metadata = read_metadata(state, 5);
    let channel = channel_ref.clone();

    let actor = LuaActor::from_lua_state(state);
    let owner = unsafe { (*actor).id };
    let session = unsafe { (*actor).next_session() };

    CONTEXT.io_runtime().spawn(async move {
        let mut client = grpc_client(channel);
        // tonic's `Channel` is a tower `Buffer`; it must be driven to readiness
        // before dispatch or the underlying service panics ("send_item called
        // without first calling poll_reserve").
        let result = match client.ready().await {
            Ok(_) => {
                let mut request = Request::new(Bytes::from(body));
                apply_metadata(&mut request, &metadata);
                let call = client.unary::<Bytes, Bytes, _>(request, path, BytesCodec);
                if timeout > 0 {
                    match tokio::time::timeout(Duration::from_millis(timeout), call).await {
                        Ok(r) => r,
                        Err(_) => Err(Status::deadline_exceeded("grpc unary timeout")),
                    }
                } else {
                    call.await
                }
            }
            Err(e) => Err(Status::unavailable(format!("grpc not ready: {}", e))),
        };

        let response = match result {
            Ok(resp) => GrpcResponse::Unary {
                status: 0,
                message: String::new(),
                body: Some(resp.into_inner()),
            },
            Err(status) => GrpcResponse::Unary {
                status: status.code() as i32,
                message: status.message().to_string(),
                body: None,
            },
        };
        let _ = CONTEXT.send_value(context::PTYPE_GRPC, owner, session, response);
    });

    laux::lua_push(state, session);
    1
}

/// The body of the recv loop: serially answer `recv` requests by pulling the
/// next message off the response `Streaming`, replying exactly once per
/// `(owner, session)`.
async fn run_recv_loop(
    fd: i64,
    mut streaming: Streaming<Bytes>,
    mut rx_recv: mpsc::Receiver<(ActorId, i64)>,
) {
    let mut ended = false;
    while let Some((owner, session)) = rx_recv.recv().await {
        if ended {
            let _ = CONTEXT.send_value(
                context::PTYPE_GRPC,
                owner,
                session,
                GrpcResponse::Error("grpc stream already ended".to_string()),
            );
            continue;
        }
        let response = match streaming.message().await {
            Ok(Some(bytes)) => GrpcResponse::StreamMessage(Some(bytes)),
            Ok(None) => {
                ended = true;
                GrpcResponse::StreamMessage(None)
            }
            Err(status) => {
                ended = true;
                GrpcResponse::Error(format!(
                    "grpc stream status {}: {}",
                    status.code() as i32,
                    status.message()
                ))
            }
        };
        if ended {
            // Drop the registry entry (and its `tx_recv` sender) as soon as the
            // response stream is done, so the slot is freed even if the caller
            // never calls `close()`. Outstanding userdata handles keep this loop
            // alive to answer any further `recv` (with an "already ended" error)
            // until they are GC'd, at which point `rx_recv` closes and the task
            // exits.
            GRPC_STREAMS.remove(&fd);
        }
        let _ = CONTEXT.send_value(context::PTYPE_GRPC, owner, session, response);
    }
    // All senders (registry entry + userdata handles) dropped: tear down.
    GRPC_STREAMS.remove(&fd);
}

/// Answer every pending `recv` with a fixed error. Used when a streaming RPC
/// was opened (handle handed to Lua) but the underlying call then failed to
/// establish — the failure surfaces on the first `recv`.
async fn run_recv_failed(fd: i64, mut rx_recv: mpsc::Receiver<(ActorId, i64)>, err: String) {
    while let Some((owner, session)) = rx_recv.recv().await {
        let _ = CONTEXT.send_value(
            context::PTYPE_GRPC,
            owner,
            session,
            GrpcResponse::Error(err.clone()),
        );
    }
    GRPC_STREAMS.remove(&fd);
}

/// Spawn `run_recv_loop` on the IO runtime, returning its abort handle so
/// `close()` can tear it down deterministically.
fn spawn_recv_loop(
    fd: i64,
    streaming: Streaming<Bytes>,
    rx_recv: mpsc::Receiver<(ActorId, i64)>,
) -> AbortHandle {
    CONTEXT
        .io_runtime()
        .spawn(run_recv_loop(fd, streaming, rx_recv))
        .abort_handle()
}

/// `handle:server_stream(path, request_bytes, timeout?, metadata?)` -> session.
/// Reply is `StreamOpen(fd)` once the response stream is established.
extern "C-unwind" fn conn_server_stream(state: LuaState) -> c_int {
    let channel_ref =
        laux::lua_touserdata::<Channel>(state, 1).expect("invalid grpc connection pointer");
    let path_str = unsafe { laux::lua_check_str(state, 2) }.to_string();
    let path = match PathAndQuery::try_from(path_str.clone()) {
        Ok(p) => p,
        Err(e) => return crate::lua_push_error(state, &format!("grpc: invalid path '{}': {}", path_str, e)),
    };
    let body = match read_body(state, 3) {
        Ok(b) => b,
        Err(e) => return crate::lua_push_error(state, &e),
    };
    let timeout: u64 = laux::lua_opt(state, 4).unwrap_or(0);
    let metadata = read_metadata(state, 5);
    let channel = channel_ref.clone();

    let actor = LuaActor::from_lua_state(state);
    let owner = unsafe { (*actor).id };
    let session = unsafe { (*actor).next_session() };

    CONTEXT.io_runtime().spawn(async move {
        let mut client = grpc_client(channel);
        let result = match client.ready().await {
            Ok(_) => {
                let mut request = Request::new(Bytes::from(body));
                apply_metadata(&mut request, &metadata);
                let call = client.server_streaming::<Bytes, Bytes, _>(request, path, BytesCodec);
                if timeout > 0 {
                    match tokio::time::timeout(Duration::from_millis(timeout), call).await {
                        Ok(r) => r,
                        Err(_) => Err(Status::deadline_exceeded("grpc server_stream timeout")),
                    }
                } else {
                    call.await
                }
            }
            Err(e) => Err(Status::unavailable(format!("grpc not ready: {}", e))),
        };

        match result {
            Ok(resp) => {
                let fd = next_grpc_fd();
                let (tx_recv, rx_recv) = mpsc::channel::<(ActorId, i64)>(1);
                let abort = spawn_recv_loop(fd, resp.into_inner(), rx_recv);
                GRPC_STREAMS.insert(
                    fd,
                    StreamEntry {
                        handle: StreamHandle {
                            tx_recv,
                            tx_send: None,
                        },
                        abort,
                    },
                );
                let _ = CONTEXT.send_value(
                    context::PTYPE_GRPC,
                    owner,
                    session,
                    GrpcResponse::StreamOpen(fd),
                );
            }
            Err(status) => {
                let _ = CONTEXT.send_value(
                    context::PTYPE_GRPC,
                    owner,
                    session,
                    GrpcResponse::Error(format!(
                        "grpc server_stream status {}: {}",
                        status.code() as i32,
                        status.message()
                    )),
                );
            }
        }
    });

    laux::lua_push(state, session);
    1
}

/// `handle:bidi_stream(path, timeout?, metadata?)` -> session.
/// Covers both client-streaming and bidirectional-streaming methods.
extern "C-unwind" fn conn_bidi_stream(state: LuaState) -> c_int {
    let channel_ref =
        laux::lua_touserdata::<Channel>(state, 1).expect("invalid grpc connection pointer");
    let path_str = unsafe { laux::lua_check_str(state, 2) }.to_string();
    let path = match PathAndQuery::try_from(path_str.clone()) {
        Ok(p) => p,
        Err(e) => return crate::lua_push_error(state, &format!("grpc: invalid path '{}': {}", path_str, e)),
    };
    let timeout: u64 = laux::lua_opt(state, 3).unwrap_or(0);
    let metadata = read_metadata(state, 4);
    let channel = channel_ref.clone();

    let actor = LuaActor::from_lua_state(state);
    let owner = unsafe { (*actor).id };
    let session = unsafe { (*actor).next_session() };

    CONTEXT.io_runtime().spawn(async move {
        let (tx_send, rx_send) = mpsc::unbounded_channel::<Option<Vec<u8>>>();
        // Turn the request channel into a `Stream<Item = Bytes>`; `None` ends it
        // (half-close the request side).
        let req_stream = UnboundedReceiverStream::new(rx_send)
            .take_while(|item: &Option<Vec<u8>>| futures_util::future::ready(item.is_some()))
            .map(|item| Bytes::from(item.unwrap()));

        let mut request = Request::new(req_stream);
        apply_metadata(&mut request, &metadata);

        let mut client = grpc_client(channel);
        if let Err(e) = client.ready().await {
            let _ = CONTEXT.send_value(
                context::PTYPE_GRPC,
                owner,
                session,
                GrpcResponse::Error(format!("grpc bidi_stream not ready: {}", e)),
            );
            return;
        }

        // Open the stream **before** awaiting the call. A bidi/client-streaming
        // server typically does not send response headers until it has read a
        // request, but Lua cannot `send` until it has the handle — so awaiting
        // the call here would deadlock. Instead we register the handle, reply
        // `StreamOpen(fd)` immediately, and let the recv loop drive the call
        // future concurrently with the caller's `send`s.
        let fd = next_grpc_fd();
        let (tx_recv, rx_recv) = mpsc::channel::<(ActorId, i64)>(1);

        let call_future = async move {
            if timeout > 0 {
                match tokio::time::timeout(
                    Duration::from_millis(timeout),
                    client.streaming::<_, Bytes, Bytes, _>(request, path, BytesCodec),
                )
                .await
                {
                    Ok(r) => r,
                    Err(_) => Err(Status::deadline_exceeded("grpc bidi_stream timeout")),
                }
            } else {
                client
                    .streaming::<_, Bytes, Bytes, _>(request, path, BytesCodec)
                    .await
            }
        };

        let abort = CONTEXT
            .io_runtime()
            .spawn(async move {
                match call_future.await {
                    Ok(resp) => run_recv_loop(fd, resp.into_inner(), rx_recv).await,
                    Err(status) => {
                        run_recv_failed(
                            fd,
                            rx_recv,
                            format!(
                                "grpc bidi_stream status {}: {}",
                                status.code() as i32,
                                status.message()
                            ),
                        )
                        .await
                    }
                }
            })
            .abort_handle();

        GRPC_STREAMS.insert(
            fd,
            StreamEntry {
                handle: StreamHandle {
                    tx_recv,
                    tx_send: Some(tx_send),
                },
                abort,
            },
        );
        let _ = CONTEXT.send_value(context::PTYPE_GRPC, owner, session, GrpcResponse::StreamOpen(fd));
    });

    laux::lua_push(state, session);
    1
}

// ---------------------------------------------------------------------------
// Stream-handle methods: recv / send / close_send / close
// ---------------------------------------------------------------------------

extern "C-unwind" fn grpc_find_stream(state: LuaState) -> c_int {
    let fd: i64 = laux::lua_get(state, 1);
    match GRPC_STREAMS.get(&fd) {
        Some(entry) => {
            let l = [
                lreg!("recv", stream_recv),
                lreg!("send", stream_send),
                lreg!("close_send", stream_close_send),
                lreg!("close", stream_close),
                lreg_null!(),
            ];
            // Store the fd alongside the handle so `close()` can find the entry.
            let handle = (fd, entry.value().handle.clone());
            if laux::lua_newuserdata(state, handle, cstr!("grpc_stream_metatable"), l.as_ref())
                .is_none()
            {
                laux::lua_pushnil(state);
            }
        }
        None => laux::lua_pushnil(state),
    }
    1
}

type StreamUserdata = (i64, StreamHandle);

extern "C-unwind" fn stream_recv(state: LuaState) -> c_int {
    let ud = laux::lua_touserdata::<StreamUserdata>(state, 1).expect("invalid grpc stream pointer");

    let actor = LuaActor::from_lua_state(state);
    let owner = unsafe { (*actor).id };
    let session = unsafe { (*actor).next_session() };

    match ud.1.tx_recv.try_send((owner, session)) {
        Ok(_) => {
            laux::lua_push(state, session);
            1
        }
        Err(mpsc::error::TrySendError::Full(_)) => {
            crate::lua_push_error(state, "grpc stream: a recv is already pending")
        }
        Err(_) => crate::lua_push_error(state, "grpc stream: closed"),
    }
}

extern "C-unwind" fn stream_send(state: LuaState) -> c_int {
    let ud = laux::lua_touserdata::<StreamUserdata>(state, 1).expect("invalid grpc stream pointer");

    let tx = match &ud.1.tx_send {
        Some(tx) => tx,
        None => return crate::lua_push_error(state, "grpc stream: this RPC has no request stream"),
    };

    let body = match read_body(state, 2) {
        Ok(b) => b,
        Err(e) => return crate::lua_push_error(state, &e),
    };

    match tx.send(Some(body)) {
        Ok(_) => {
            laux::lua_push(state, true);
            1
        }
        Err(_) => crate::lua_push_error(state, "grpc stream: send failed (stream closed)"),
    }
}

extern "C-unwind" fn stream_close_send(state: LuaState) -> c_int {
    let ud = laux::lua_touserdata::<StreamUserdata>(state, 1).expect("invalid grpc stream pointer");

    match &ud.1.tx_send {
        Some(tx) => {
            let _ = tx.send(None);
            laux::lua_push(state, true);
            1
        }
        None => crate::lua_push_error(state, "grpc stream: this RPC has no request stream"),
    }
}

extern "C-unwind" fn stream_close(state: LuaState) -> c_int {
    let ud = laux::lua_touserdata::<StreamUserdata>(state, 1).expect("invalid grpc stream pointer");
    let fd = ud.0;

    if let Some((_, entry)) = GRPC_STREAMS.remove(&fd) {
        entry.abort.abort();
    }
    laux::lua_push(state, true);
    1
}

// ---------------------------------------------------------------------------
// Server: hyper http2 listener + tonic framing -> Lua actor dispatch
// ---------------------------------------------------------------------------

/// Drives the inbound request `Streaming<Bytes>`, answering each `recv` from
/// Lua with exactly one message (or end-of-stream / error). Mirrors the
/// client-side `run_recv_loop` but without a registry entry — the handle lives
/// in Lua userdata and the task ends when every `tx_recv` sender is dropped.
async fn run_server_recv_loop(
    mut streaming: Streaming<Bytes>,
    mut rx_recv: mpsc::Receiver<(ActorId, i64)>,
) {
    let mut ended = false;
    while let Some((owner, session)) = rx_recv.recv().await {
        let response = if ended {
            GrpcResponse::StreamMessage(None)
        } else {
            match streaming.message().await {
                Ok(Some(bytes)) => GrpcResponse::StreamMessage(Some(bytes)),
                Ok(None) => {
                    ended = true;
                    GrpcResponse::StreamMessage(None)
                }
                Err(status) => {
                    ended = true;
                    GrpcResponse::Error(format!(
                        "grpc recv status {}: {}",
                        status.code() as i32,
                        status.message()
                    ))
                }
            }
        };
        let _ = CONTEXT.send_value(context::PTYPE_GRPC, owner, session, response);
    }
}

/// A codec-level gRPC handler for one method path. We treat *every* RPC as a
/// bidirectional stream — the wire framing is identical for all four RPC kinds,
/// so the Lua handler simply reads/writes as many messages as the method needs.
struct LuaGrpcService {
    owner: ActorId,
    path: String,
}

impl StreamingService<Bytes> for LuaGrpcService {
    type Response = Bytes;
    type ResponseStream = Pin<Box<dyn Stream<Item = Result<Bytes, Status>> + Send>>;
    type Future = Ready<Result<Response<Self::ResponseStream>, Status>>;

    fn call(&mut self, request: Request<Streaming<Bytes>>) -> Self::Future {
        let streaming = request.into_inner();

        let (tx_recv, rx_recv) = mpsc::channel::<(ActorId, i64)>(1);
        let (tx_resp, rx_resp) = mpsc::unbounded_channel::<RespItem>();

        // The response stream tonic will drain: yield messages/errors until the
        // `None` sentinel (or all senders dropped) terminates it.
        let resp_stream = UnboundedReceiverStream::new(rx_resp)
            .take_while(|item: &RespItem| futures_util::future::ready(item.is_some()))
            .map(|item| item.unwrap())
            .boxed();

        let handle = ServerStreamHandle {
            tx_recv,
            tx_resp,
            finished: AtomicBool::new(false),
        };

        // Hand the RPC to the listener's owner actor (session 0 -> dispatch).
        // If delivery fails the actor is gone: drop the handle (which ends the
        // response stream) and report UNAVAILABLE.
        if CONTEXT
            .send_value(
                context::PTYPE_GRPC,
                self.owner,
                0,
                GrpcResponse::ServerRpc {
                    path: self.path.clone(),
                    handle,
                },
            )
            .is_some()
        {
            return future::ready(Err(Status::unavailable("grpc server: actor unavailable")));
        }

        // Pull inbound request messages on demand.
        CONTEXT
            .io_runtime()
            .spawn(run_server_recv_loop(streaming, rx_recv));

        future::ready(Ok(Response::new(resp_stream)))
    }
}

/// hyper service: route by method path and let tonic frame the exchange.
async fn handle_grpc_request(
    req: hyper::Request<Incoming>,
    owner: ActorId,
) -> Result<hyper::Response<TonicBody>, std::convert::Infallible> {
    let path = req.uri().path().to_string();
    let limit = LIMITS.max_network_read_bytes;
    let mut grpc = ServerGrpc::new(BytesCodec)
        .max_decoding_message_size(limit)
        .max_encoding_message_size(limit);
    let svc = LuaGrpcService { owner, path };
    Ok(grpc.streaming(svc, req).await)
}

/// `grpc.core.listen(addr, opts?)` -> listener fd. `opts.max_connections`
/// bounds concurrent connections. Inbound RPCs are delivered to the calling
/// actor via the `grpc` protocol dispatch handler.
extern "C-unwind" fn grpc_listen(state: LuaState) -> c_int {
    let _guard = CONTEXT.io_runtime().enter();

    let addr = unsafe { laux::lua_check_str(state, 1) };

    let max_connections: usize = if laux::lua_type(state, 2) == laux::LuaType::Table {
        laux::opt_field(state, 2, "max_connections").unwrap_or(LIMITS.listener_connections)
    } else {
        LIMITS.listener_connections
    };

    let socket_addr: SocketAddr = match addr.parse() {
        Ok(a) => a,
        Err(e) => laux::lua_error(state, format!("grpc listen '{}' failed: {}", addr, e)),
    };
    let listener = match std::net::TcpListener::bind(socket_addr) {
        Ok(l) => l,
        Err(e) => laux::lua_error(state, format!("grpc listen '{}' failed: {}", addr, e)),
    };
    if let Err(e) = listener.set_nonblocking(true) {
        // Release the bound socket before the longjmp; `lua_error` never returns
        // so the listener's `Drop` would otherwise be skipped.
        let msg = format!("grpc listen '{}' failed: {}", addr, e);
        drop(listener);
        laux::lua_error(state, msg);
    }
    let listener = match TcpListener::from_std(listener) {
        Ok(l) => l,
        Err(e) => laux::lua_error(state, format!("grpc listen '{}' failed: {}", addr, e)),
    };

    let actor = LuaActor::from_lua_state(state);
    let owner = unsafe { (*actor).id };

    let fd = next_grpc_fd();
    let cancel = CancellationToken::new();
    GRPC_SERVERS.insert(fd, cancel.clone());

    let semaphore = Arc::new(Semaphore::new(max_connections));

    CONTEXT.io_runtime().spawn(async move {
        loop {
            tokio::select! {
                _ = cancel.cancelled() => break,
                result = listener.accept() => {
                    match result {
                        Ok((stream, _)) => {
                            let permit = match semaphore.clone().try_acquire_owned() {
                                Ok(p) => p,
                                Err(_) => {
                                    drop(stream);
                                    log::warn!("grpc.listen fd={}: max connections ({}) reached, rejecting", fd, max_connections);
                                    continue;
                                }
                            };
                            stream.set_nodelay(true).unwrap_or_default();
                            let io = TokioIo::new(stream);
                            CONTEXT.io_runtime().spawn(async move {
                                let _permit = permit;
                                let svc = service_fn(move |req| handle_grpc_request(req, owner));
                                if let Err(err) = http2::Builder::new(TokioExecutor::new())
                                    .serve_connection(io, svc)
                                    .await
                                {
                                    log::debug!("grpc server connection error: {}", err);
                                }
                            });
                        }
                        Err(err) => {
                            log::warn!("grpc accept error: {}", err);
                            tokio::time::sleep(Duration::from_millis(100)).await;
                        }
                    }
                }
            }
        }
        GRPC_SERVERS.remove(&fd);
        log::info!("grpc listener fd={} closed.", fd);
    });

    laux::lua_push(state, fd);
    1
}

/// `grpc.core.stop(fd)` -> bool. Cancels the listener accept loop.
extern "C-unwind" fn grpc_server_close(state: LuaState) -> c_int {
    let fd: i64 = laux::lua_get(state, 1);
    if let Some((_, token)) = GRPC_SERVERS.remove(&fd) {
        token.cancel();
        laux::lua_push(state, true);
    } else {
        laux::lua_push(state, false);
    }
    1
}

// ---------------------------------------------------------------------------
// Server stream-handle methods: recv / send / finish
// ---------------------------------------------------------------------------

/// `handle:recv()` -> session. Resolves to the next inbound request message
/// (raw bytes), `nil` at clean end-of-request-stream, or `(nil, err)` on error.
extern "C-unwind" fn server_recv(state: LuaState) -> c_int {
    let ud = laux::lua_touserdata::<ServerStreamHandle>(state, 1)
        .expect("invalid grpc server stream pointer");

    let actor = LuaActor::from_lua_state(state);
    let owner = unsafe { (*actor).id };
    let session = unsafe { (*actor).next_session() };

    match ud.tx_recv.try_send((owner, session)) {
        Ok(_) => {
            laux::lua_push(state, session);
            1
        }
        Err(mpsc::error::TrySendError::Full(_)) => {
            crate::lua_push_error(state, "grpc server stream: a recv is already pending")
        }
        Err(_) => crate::lua_push_error(state, "grpc server stream: request stream closed"),
    }
}

/// `handle:send(bytes)` -> bool. Pushes one response message (raw bytes).
extern "C-unwind" fn server_send(state: LuaState) -> c_int {
    let ud = laux::lua_touserdata::<ServerStreamHandle>(state, 1)
        .expect("invalid grpc server stream pointer");

    if ud.finished.load(Ordering::Acquire) {
        return crate::lua_push_error(state, "grpc server stream: already finished");
    }

    let body = match read_body(state, 2) {
        Ok(b) => b,
        Err(e) => return crate::lua_push_error(state, &e),
    };

    match ud.tx_resp.send(Some(Ok(Bytes::from(body)))) {
        Ok(_) => {
            laux::lua_push(state, true);
            1
        }
        Err(_) => crate::lua_push_error(state, "grpc server stream: send failed (client gone)"),
    }
}

/// `handle:finish(code?, message?)` -> bool. Ends the response stream with the
/// given gRPC status (default OK = 0). Idempotent.
extern "C-unwind" fn server_finish(state: LuaState) -> c_int {
    let ud = laux::lua_touserdata::<ServerStreamHandle>(state, 1)
        .expect("invalid grpc server stream pointer");

    let code: i32 = laux::lua_opt(state, 2).unwrap_or(0);
    // Read the optional message only when it really is a string (avoid longjmp).
    let message = if laux::lua_type(state, 3) == laux::LuaType::String {
        unsafe { laux::lua_check_str(state, 3) }.to_string()
    } else {
        String::new()
    };

    if ud.finished.swap(true, Ordering::AcqRel) {
        laux::lua_push(state, true);
        return 1;
    }

    if code != 0 {
        let _ = ud.tx_resp.send(Some(Err(Status::new(Code::from(code), message))));
    }
    // End-of-stream sentinel: terminates the response stream (OK trailer unless
    // an error item was sent just above).
    let _ = ud.tx_resp.send(None);

    laux::lua_push(state, true);
    1
}

// ---------------------------------------------------------------------------
// Decoder + module open
// ---------------------------------------------------------------------------

fn push_grpc_response(state: LuaState, response: GrpcResponse) -> c_int {
    match response {
        GrpcResponse::Connect => {
            laux::lua_push(state, true);
            1
        }
        GrpcResponse::Unary {
            status,
            message,
            body,
        } => {
            let table = LuaTable::new(state, 0, 3);
            table.insert("status", status as i64);
            table.insert("message", message.as_str());
            if let Some(body) = body {
                table.insert("body", body.as_ref());
            }
            1
        }
        GrpcResponse::StreamOpen(fd) => {
            let table = LuaTable::new(state, 0, 1);
            table.insert("fd", fd);
            1
        }
        GrpcResponse::StreamMessage(Some(bytes)) => {
            laux::lua_push(state, bytes.as_ref());
            1
        }
        GrpcResponse::StreamMessage(None) => {
            laux::lua_pushnil(state);
            1
        }
        GrpcResponse::Error(err) => crate::lua_push_error(state, err.as_str()),
        GrpcResponse::ServerRpc { path, handle } => {
            laux::lua_push(state, path.as_str());
            let l = [
                lreg!("recv", server_recv),
                lreg!("send", server_send),
                lreg!("finish", server_finish),
                lreg_null!(),
            ];
            if laux::lua_newuserdata(
                state,
                handle,
                cstr!("grpc_server_stream_metatable"),
                l.as_ref(),
            )
            .is_none()
            {
                laux::lua_pushnil(state);
            }
            2
        }
    }
}

pub unsafe extern "C-unwind" fn decode_grpc_message(
    state: LuaState,
    m: *mut moon_runtime::context::Message,
) -> c_int {
    match unsafe { crate::message_decode::take_boxed::<GrpcResponse>(m) } {
        Ok(response) => push_grpc_response(state, response),
        Err(e) => crate::lua_push_error(state, &e),
    }
}

pub extern "C-unwind" fn luaopen_grpc(state: LuaState) -> c_int {
    let l = [
        lreg!("connect", grpc_connect),
        lreg!("close", grpc_close),
        lreg!("find_connection", grpc_find_connection),
        lreg!("find_stream", grpc_find_stream),
        lreg!("stats", grpc_stats),
        lreg!("listen", grpc_listen),
        lreg!("stop", grpc_server_close),
        lreg_null!(),
    ];

    luaL_newlib!(state, l);
    1
}
