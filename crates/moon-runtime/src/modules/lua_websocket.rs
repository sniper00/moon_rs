use std::{ffi::c_int, sync::Arc, time::Duration};

use dashmap::DashMap;
use futures_util::{SinkExt, StreamExt, TryStreamExt};
use lazy_static::lazy_static;
use tokio::{
    net::{TcpListener, TcpStream},
    sync::{Semaphore, mpsc},
    time::timeout,
};
use tokio_tungstenite::{
    MaybeTlsStream, WebSocketStream, accept_hdr_async_with_config, connect_async_with_config,
    tungstenite::{
        Message,
        handshake::server::{
            ErrorResponse, Request as HandshakeRequest, Response as HandshakeResponse,
        },
        http::{self, response},
        protocol::{CloseFrame, WebSocketConfig, frame::coding::CloseCode},
    },
};

use moon_base::{
    cstr, ffi,
    laux::{self, LuaStack, LuaState, LuaTable},
    lreg_null, lreg_try, luaL_newlib,
};
use moon_runtime::{
    actor::LuaActor,
    context::{self, ActorId, CONTEXT},
};

use crate::LIMITS;

lazy_static! {
    static ref WS_NET: DashMap<i64, WsChannel> = DashMap::new();
}

#[derive(Clone)]
struct WsChannel {
    tx_reader: mpsc::Sender<WsRequest>,
    tx_writer: mpsc::Sender<WsRequest>,
}

#[derive(Debug)]
enum WsRequest {
    Read(ActorId, i64, u64),
    Write(Message, bool),
    Close(Message),
    Accept(ActorId, i64),
}

enum WsResponse {
    Connect(i64, response::Response<Option<Vec<u8>>>),
    Accept(i64, String),
    Read(Message),
    Error(String),
}

fn next_ws_fd() -> i64 {
    crate::next_net_fd()
}

// ---------- Generic connection handler ----------

async fn handle_read<S>(
    read_timeout: u64,
    reader: &mut futures_util::stream::SplitStream<WebSocketStream<S>>,
) -> Result<Message, String>
where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
{
    let result = if read_timeout > 0 {
        timeout(Duration::from_millis(read_timeout), reader.try_next())
            .await
            .map_err(|err| format!("read timeout: {}", err))?
    } else {
        reader.try_next().await
    };
    result
        .map_err(|err| err.to_string())?
        .ok_or_else(|| "eof".to_string())
}

async fn handle_write<S>(
    mut writer: futures_util::stream::SplitSink<WebSocketStream<S>, Message>,
    mut rx: mpsc::Receiver<WsRequest>,
) -> Result<(), String>
where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
{
    while let Some(op) = rx.recv().await {
        match op {
            WsRequest::Write(data, close) => {
                writer.send(data).await.map_err(|e| e.to_string())?;
                if close {
                    return Ok(());
                }
            }
            WsRequest::Close(data) => {
                writer.send(data).await.map_err(|e| e.to_string())?;
                return Ok(());
            }
            _ => {}
        }
    }
    Err("writer closed".to_string())
}

fn setup_ws_connection() -> (i64, mpsc::Receiver<WsRequest>, mpsc::Receiver<WsRequest>) {
    let fd = next_ws_fd();
    let (tx_reader, rx_reader) = mpsc::channel::<WsRequest>(1);
    let (tx_writer, rx_writer) = mpsc::channel::<WsRequest>(100);
    WS_NET.insert(
        fd,
        WsChannel {
            tx_reader,
            tx_writer,
        },
    );
    (fd, rx_reader, rx_writer)
}

async fn run_connection<S>(
    stream: WebSocketStream<S>,
    fd: i64,
    mut rx_reader: mpsc::Receiver<WsRequest>,
    rx_writer: mpsc::Receiver<WsRequest>,
) where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin + Send + 'static,
{
    let (writer, mut reader) = stream.split();

    let write = handle_write(writer, rx_writer);
    tokio::pin!(write);
    loop {
        let op = tokio::select! {
            _ = &mut write => break,
            op = rx_reader.recv() => match op {
                Some(op) => op,
                None => break,
            },
        };
        if let WsRequest::Read(owner, session, read_timeout) = op {
            let result = tokio::select! {
                result = &mut write => Err(result.err().unwrap_or_else(|| "closed".to_string())),
                result = handle_read(read_timeout, &mut reader) => result,
            };
            let closed = match &result {
                Ok(message) => message.is_close(),
                Err(_) => true,
            };
            let response = match result {
                Ok(message) => WsResponse::Read(message),
                Err(err) => WsResponse::Error(err),
            };
            let _ = CONTEXT.send_value(context::PTYPE_WEBSOCKET, owner, session, response);
            if closed {
                break;
            }
        }
    }

    WS_NET.remove(&fd);
    // Stop accepting reads, then release callers already queued behind the
    // active read. The writer future is dropped together with the connection.
    rx_reader.close();
    while let Some(op) = rx_reader.recv().await {
        if let WsRequest::Read(owner, session, _) = op {
            let _ = CONTEXT.send_value(
                context::PTYPE_WEBSOCKET,
                owner,
                session,
                WsResponse::Error("closed".to_string()),
            );
        }
    }
}

// ---------- Client ----------

async fn on_client_connected(
    stream: WebSocketStream<MaybeTlsStream<TcpStream>>,
    response: response::Response<Option<Vec<u8>>>,
    owner: ActorId,
    session: i64,
) {
    let (fd, rx_reader, rx_writer) = setup_ws_connection();
    let _ = CONTEXT.send_value(
        context::PTYPE_WEBSOCKET,
        owner,
        session,
        WsResponse::Connect(fd, response),
    );
    run_connection(stream, fd, rx_reader, rx_writer).await;
}

// ---------- Server ----------

async fn on_server_accepted(
    stream: WebSocketStream<TcpStream>,
    addr: String,
    owner: ActorId,
    session: i64,
) {
    let (fd, rx_reader, rx_writer) = setup_ws_connection();
    let _ = CONTEXT.send_value(
        context::PTYPE_WEBSOCKET,
        owner,
        session,
        WsResponse::Accept(fd, addr),
    );
    run_connection(stream, fd, rx_reader, rx_writer).await;
}

// ---------- Connection userdata methods ----------

fn ws_read(lua: &mut LuaStack<'_>) -> Result<c_int, String> {
    let state = lua.state();
    let conn_ptr = lua
        .value(1)
        .as_userdata::<WsChannel>()
        .ok_or_else(|| "invalid ws connection pointer".to_string())?;
    let conn = unsafe { conn_ptr.as_ref() };
    let read_timeout: u64 = lua.opt(2).unwrap_or(5000);

    let actor = LuaActor::from_lua_state(state);
    let owner = unsafe { (*actor).id };
    let session = unsafe { (*actor).next_session() };

    match conn
        .tx_reader
        .try_send(WsRequest::Read(owner, session, read_timeout))
    {
        Ok(_) => {
            laux::lua_push(state, session);
            Ok(1)
        }
        Err(err) => {
            let _ = CONTEXT.send_value(
                context::PTYPE_WEBSOCKET,
                owner,
                session,
                WsResponse::Error(format!("ws read error: {}", err)),
            );
            laux::lua_push(state, session);
            Ok(1)
        }
    }
}

fn to_message(data: Vec<u8>, kind: &str) -> Message {
    match kind {
        "c" => Message::Close(Some(CloseFrame {
            code: CloseCode::Normal,
            reason: String::from_utf8_lossy(&data).into_owned().into(),
        })),
        "t" => Message::Text(String::from_utf8_lossy(&data).into_owned().into()),
        "p" => Message::Ping(data.into()),
        _ => Message::Binary(data.into()),
    }
}

fn check_message(lua: &LuaStack<'_>, index: i32, kind: &str) -> Result<Message, String> {
    let value = lua.value(index);
    let data = match value.kind() {
        laux::LuaType::String => value.as_bytes().unwrap_or_default().to_vec(),
        laux::LuaType::LightUserData => {
            let ptr = value
                .as_light_userdata()
                .expect("lightuserdata type checked");
            if ptr.is_null() {
                return Err(format!(
                    "bad argument #{} (non-null lightuserdata expected)",
                    index
                ));
            }
            // Take ownership of the heap `Buffer` created on the Lua side
            // (e.g. `buffer.concat` -> `Box::into_raw`). `to_vec` moves the
            // underlying `Vec` out when nothing has been consumed (zero-copy),
            // and dropping the `Box` at the end of this scope frees the
            // allocation instead of leaking it.
            let buf = unsafe { Box::from_raw(ptr as *mut moon_runtime::buffer::Buffer) };
            buf.into_vec()
        }
        _ => {
            return Err(format!(
                "bad argument #{} (string or lightuserdata expected, got {})",
                index,
                value.name()
            ));
        }
    };
    Ok(to_message(data, kind))
}

fn ws_write(lua: &mut LuaStack<'_>) -> Result<c_int, String> {
    let state = lua.state();
    let conn_ptr = lua
        .value(1)
        .as_userdata::<WsChannel>()
        .ok_or_else(|| "invalid ws connection pointer".to_string())?;
    let conn = unsafe { conn_ptr.as_ref() };
    let kind = match lua.value(2).as_str() {
        Some(kind) => kind,
        None => return Err("bad argument #2 (valid UTF-8 string expected)".to_string()),
    };
    let msg = check_message(lua, 3, kind)?;
    let close = msg.is_close();

    match conn.tx_writer.try_send(WsRequest::Write(msg, close)) {
        Ok(_) => {
            laux::lua_push(state, true);
            Ok(1)
        }
        Err(err) => Ok(crate::lua_push_error_tuple(
            state,
            &format!("ws write error: {}", err),
        )),
    }
}

fn ws_close(lua: &mut LuaStack<'_>) -> Result<c_int, String> {
    let state = lua.state();
    let conn_ptr = lua
        .value(1)
        .as_userdata::<WsChannel>()
        .ok_or_else(|| "invalid ws connection pointer".to_string())?;
    let conn = unsafe { conn_ptr.as_ref() };
    let data = lua.value(2).as_bytes().unwrap_or_default();

    match conn
        .tx_writer
        .try_send(WsRequest::Close(Message::Close(Some(CloseFrame {
            code: CloseCode::Normal,
            reason: String::from_utf8_lossy(data).into_owned().into(),
        })))) {
        Ok(_) => {
            laux::lua_push(state, true);
            Ok(1)
        }
        Err(err) => Ok(crate::lua_push_error_tuple(
            state,
            &format!("ws close error: {}", err),
        )),
    }
}

// ---------- Module-level functions ----------

/// Fallback ceiling for the outbound write buffer when `max_message_size` is
/// uncapped (tungstenite's default incoming message limit is 64 MiB).
const DEFAULT_WS_MESSAGE_CEILING: usize = 64 << 20;

fn read_ws_config(lua: &mut LuaStack<'_>, index: i32) -> WebSocketConfig {
    let mut config = WebSocketConfig::default();
    // Reading fields off a non-table value would make `lua_getfield` raise, so
    // only consult the options table when one was actually provided. The
    // write-buffer bounding below still runs so the unbounded tungstenite
    // default never leaks through, even on the no-options path.
    let has_opts = lua.value(index).kind() == laux::LuaType::Table;
    let explicit_max_write_buffer = if has_opts {
        if let Some(size) = lua.opt_field::<usize>(index, "max_message_size") {
            config.max_message_size = Some(size);
        }
        if let Some(size) = lua.opt_field::<usize>(index, "max_frame_size") {
            config.max_frame_size = Some(size);
        }
        if let Some(size) = lua.opt_field::<usize>(index, "write_buffer_size") {
            config.write_buffer_size = size;
        }
        lua.opt_field::<usize>(index, "max_write_buffer_size")
    } else {
        None
    };

    // tungstenite defaults `max_write_buffer_size` to `usize::MAX`, i.e. the
    // outbound buffer grows without bound when a peer reads slowly — a
    // per-connection memory DoS. Bound it by default (still large enough to hold
    // one max-size message plus the write buffer so normal sends never fail) and
    // let callers tune it via `max_write_buffer_size`.
    config.max_write_buffer_size = explicit_max_write_buffer.unwrap_or_else(|| {
        config
            .max_message_size
            .unwrap_or(DEFAULT_WS_MESSAGE_CEILING)
            .saturating_add(config.write_buffer_size)
    });
    // tungstenite requires `max_write_buffer_size > write_buffer_size`.
    if config.max_write_buffer_size <= config.write_buffer_size {
        config.max_write_buffer_size = config.write_buffer_size.saturating_add(1);
    }
    config
}

/// Read an optional `origins = { "https://a.com", ... }` allow-list from the
/// listen options table. When present (non-empty), the server handshake only
/// upgrades requests whose `Origin` header exactly matches an entry, which
/// blocks cross-site WebSocket hijacking from browsers. When absent, no Origin
/// check is performed (suitable for non-browser / trusted clients).
fn read_allowed_origins(lua: &mut LuaStack<'_>, index: i32) -> Option<Arc<Vec<String>>> {
    let cursor = lua.table_field_cursor(index, "origins")?;
    let mut origins = Vec::new();
    for entry in cursor {
        let value = entry.value();
        if let Some(origin) = value.as_str() {
            origins.push(origin.to_owned());
        }
    }
    if origins.is_empty() {
        None
    } else {
        Some(Arc::new(origins))
    }
}

fn ws_connect(lua: &mut LuaStack<'_>) -> Result<c_int, String> {
    let state = lua.state();
    laux::lua_checktype(state, 1, ffi::LUA_TTABLE)
        .map_err(|err| format!("websocket.connect: argument #1 {err}"))?;

    let url: String = lua.opt_field(1, "url").unwrap_or_default();
    let connect_timeout: u64 = lua.opt_field(1, "connect_timeout").unwrap_or(5000);

    let ws_config = read_ws_config(lua, 1);

    let actor = LuaActor::from_lua_state(state);
    let owner = unsafe { (*actor).id };
    let session = unsafe { (*actor).next_session() };

    CONTEXT.io_runtime().spawn(async move {
        match timeout(
            Duration::from_millis(connect_timeout),
            connect_async_with_config(url, Some(ws_config), false),
        )
        .await
        {
            Ok(Ok((stream, response))) => {
                CONTEXT
                    .io_runtime()
                    .spawn(on_client_connected(stream, response, owner, session));
            }
            Ok(Err(err)) => {
                let _ = CONTEXT.send_value(
                    context::PTYPE_WEBSOCKET,
                    owner,
                    session,
                    WsResponse::Error(err.to_string()),
                );
            }
            Err(err) => {
                let _ = CONTEXT.send_value(
                    context::PTYPE_WEBSOCKET,
                    owner,
                    session,
                    WsResponse::Error(format!("connect timeout: {}", err)),
                );
            }
        }
    });

    laux::lua_push(state, session);
    Ok(1)
}

// The handshake callback's `Err` type (`ErrorResponse`) is fixed by
// tungstenite's `Callback` trait, so the large-err lint is unavoidable here.
#[allow(clippy::result_large_err)]
fn ws_listen(lua: &mut LuaStack<'_>) -> Result<c_int, String> {
    let state = lua.state();
    let _guard = CONTEXT.io_runtime().enter();

    let has_opts = laux::lua_type(state, 2) == laux::LuaType::Table;
    // `read_ws_config` is safe to call even without an options table and always
    // bounds `max_write_buffer_size`, so don't fall back to the raw default
    // (which leaves the write buffer unbounded).
    let ws_config = read_ws_config(lua, 2);
    let allowed_origins = if has_opts {
        read_allowed_origins(lua, 2)
    } else {
        None
    };
    let max_connections: usize = if has_opts {
        lua.opt_field(2, "max_connections")
            .unwrap_or(LIMITS.listener_connections)
    } else {
        LIMITS.listener_connections
    };

    let addr = match lua.value(1).as_str() {
        Some(addr) => addr,
        None => return Err("bad argument #1 (valid UTF-8 string expected)".to_string()),
    };

    let listener = match std::net::TcpListener::bind(addr) {
        Ok(l) => l,
        Err(err) => {
            return Ok(crate::lua_push_error_tuple(
                state,
                &format!("ws listen '{}' failed: {}", addr, err),
            ));
        }
    };

    if let Err(err) = listener.set_nonblocking(true) {
        return Ok(crate::lua_push_error_tuple(
            state,
            &format!("ws listen '{}' failed: {}", addr, err),
        ));
    }

    let listener = match TcpListener::from_std(listener) {
        Ok(l) => l,
        Err(err) => {
            return Ok(crate::lua_push_error_tuple(
                state,
                &format!("ws listen '{}' failed: {}", addr, err),
            ));
        }
    };

    let fd = next_ws_fd();
    let (tx, mut rx) = mpsc::channel::<WsRequest>(1);
    WS_NET.insert(
        fd,
        WsChannel {
            tx_reader: tx.clone(),
            tx_writer: tx,
        },
    );

    // Bound concurrently live accepted connections; the permit is held for the
    // whole connection lifetime (moved into the per-connection task).
    let semaphore = Arc::new(Semaphore::new(max_connections));

    CONTEXT.io_runtime().spawn(async move {
        while let Some(op) = rx.recv().await {
            match op {
                WsRequest::Accept(owner, session) => match listener.accept().await {
                    Ok((stream, addr)) => {
                        let permit = match semaphore.clone().try_acquire_owned() {
                            Ok(permit) => permit,
                            Err(_) => {
                                drop(stream);
                                log::warn!(
                                    "ws listen fd={}: max connections ({}) reached, rejecting",
                                    fd,
                                    max_connections
                                );
                                let _ = CONTEXT.send_value(
                                    context::PTYPE_WEBSOCKET,
                                    owner,
                                    session,
                                    WsResponse::Error(format!(
                                        "ws max connections ({}) reached",
                                        max_connections
                                    )),
                                );
                                continue;
                            }
                        };
                        let addr_str = addr.to_string();
                        let cfg = ws_config;
                        let origins = allowed_origins.clone();
                        CONTEXT.io_runtime().spawn(async move {
                            let _permit = permit;
                            let handshake = accept_hdr_async_with_config(
                                stream,
                                |req: &HandshakeRequest,
                                 resp: HandshakeResponse|
                                 -> Result<HandshakeResponse, ErrorResponse> {
                                    if let Some(allowed) = origins.as_deref() {
                                        let origin = req
                                            .headers()
                                            .get("origin")
                                            .and_then(|v| v.to_str().ok());
                                        let ok = origin
                                            .map(|o| allowed.iter().any(|a| a == o))
                                            .unwrap_or(false);
                                        if !ok {
                                            let body = Some(format!(
                                                "forbidden origin: {}",
                                                origin.unwrap_or("<missing>")
                                            ));
                                            return Err(http::Response::builder()
                                                .status(http::StatusCode::FORBIDDEN)
                                                .body(body)
                                                .expect("static 403 response"));
                                        }
                                    }
                                    Ok(resp)
                                },
                                Some(cfg),
                            )
                            .await;
                            match handshake {
                                Ok(ws_stream) => {
                                    on_server_accepted(ws_stream, addr_str, owner, session).await;
                                }
                                Err(err) => {
                                    let _ = CONTEXT.send_value(
                                        context::PTYPE_WEBSOCKET,
                                        owner,
                                        session,
                                        WsResponse::Error(format!("ws handshake failed: {}", err)),
                                    );
                                }
                            }
                        });
                    }
                    Err(err) => {
                        let _ = CONTEXT.send_value(
                            context::PTYPE_WEBSOCKET,
                            owner,
                            session,
                            WsResponse::Error(format!("ws accept failed: {}", err)),
                        );
                    }
                },
                WsRequest::Close(_) => break,
                _ => log::warn!("ws listen: unexpected request"),
            }
        }
        WS_NET.remove(&fd);
    });

    laux::lua_push(state, fd);
    Ok(1)
}

fn ws_accept(lua: &mut LuaStack<'_>) -> Result<c_int, String> {
    let state = lua.state();
    let fd: i64 = lua.get(1)?;
    let actor = LuaActor::from_lua_state(state);
    let owner = unsafe { (*actor).id };
    let session = unsafe { (*actor).next_session() };

    if let Some(channel) = WS_NET.get(&fd) {
        if let Err(err) = channel
            .tx_reader
            .try_send(WsRequest::Accept(owner, session))
        {
            return Ok(crate::lua_push_error_tuple(
                state,
                &format!("ws accept error: {}", err),
            ));
        }
    } else {
        return Ok(crate::lua_push_error_tuple(
            state,
            &format!("ws: fd {} not found", fd),
        ));
    }
    laux::lua_push(state, session);
    Ok(1)
}

fn ws_find_connection(lua: &mut LuaStack<'_>) -> Result<c_int, String> {
    let state = lua.state();
    let id: i64 = lua.get(1)?;
    match WS_NET.get(&id) {
        Some(pair) => {
            let l = [
                lreg_try!("read", ws_read),
                lreg_try!("write", ws_write),
                lreg_try!("close", ws_close),
                lreg_null!(),
            ];
            if laux::lua_newuserdata(
                state,
                pair.value().clone(),
                cstr!("ws_connection_metatable"),
                l.as_ref(),
            )
            .is_none()
            {
                laux::lua_pushnil(state);
                return Ok(1);
            }
        }
        None => {
            laux::lua_pushnil(state);
        }
    }
    Ok(1)
}

fn version_to_string(version: &tokio_tungstenite::tungstenite::http::Version) -> &'static str {
    match *version {
        tokio_tungstenite::tungstenite::http::Version::HTTP_09 => "HTTP/0.9",
        tokio_tungstenite::tungstenite::http::Version::HTTP_10 => "HTTP/1.0",
        tokio_tungstenite::tungstenite::http::Version::HTTP_11 => "HTTP/1.1",
        tokio_tungstenite::tungstenite::http::Version::HTTP_2 => "HTTP/2.0",
        tokio_tungstenite::tungstenite::http::Version::HTTP_3 => "HTTP/3.0",
        _ => "Unknown",
    }
}

fn push_ws_response(state: LuaState, response: WsResponse) -> c_int {
    match response {
        WsResponse::Connect(fd, resp) => {
            LuaTable::new(state, 0, 6)
                .insert("fd", fd)
                .insert("version", version_to_string(&resp.version()))
                .insert("status_code", resp.status().as_u16() as i64)
                .rawset_x("headers", || {
                    let headers = LuaTable::new(state, 0, resp.headers().len());
                    for (key, value) in resp.headers().iter() {
                        headers.insert(key.as_str(), value.to_str().unwrap_or("").trim());
                    }
                });

            if let Some(body) = resp.body() {
                unsafe {
                    laux::lua_push(state, body.as_slice());
                    ffi::lua_setfield(state.as_ptr(), -2, cstr!("body"));
                }
            }
            1
        }
        WsResponse::Accept(fd, addr) => {
            LuaTable::new(state, 0, 2)
                .insert("fd", fd)
                .insert("addr", addr.as_str());
            1
        }
        WsResponse::Read(data) => {
            let (kind, payload): (&str, &[u8]) = match &data {
                Message::Text(s) => ("t", s.as_bytes()),
                Message::Binary(b) => ("b", b.as_ref()),
                Message::Ping(b) => ("p", b.as_ref()),
                Message::Pong(b) => ("o", b.as_ref()),
                Message::Close(frame) => {
                    if let Some(f) = frame {
                        ("c", f.reason.as_bytes())
                    } else {
                        ("c", &[])
                    }
                }
                Message::Frame(_) => ("f", &[]),
            };
            laux::lua_push(state, payload);
            laux::lua_push(state, kind);
            2
        }
        WsResponse::Error(err) => crate::lua_push_error_tuple(state, err.as_str()),
    }
}

pub unsafe extern "C-unwind" fn decode_websocket_message(
    state: LuaState,
    m: *mut moon_runtime::context::Message,
) -> c_int {
    match unsafe { crate::message_decode::take_boxed::<WsResponse>(m) } {
        Ok(response) => push_ws_response(state, response),
        Err(e) => crate::lua_push_error_tuple(state, &e),
    }
}

pub extern "C-unwind" fn luaopen_websocket(state: LuaState) -> c_int {
    let l = [
        lreg_try!("connect", ws_connect),
        lreg_try!("listen", ws_listen),
        lreg_try!("accept", ws_accept),
        lreg_try!("find_connection", ws_find_connection),
        lreg_null!(),
    ];

    luaL_newlib!(state, l);
    1
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio_tungstenite::tungstenite::protocol::Role;

    #[tokio::test]
    async fn local_close_removes_idle_connection() {
        let (socket, _peer) = tokio::io::duplex(4096);
        let stream = WebSocketStream::from_raw_socket(socket, Role::Server, None).await;
        let (fd, reads, writes) = setup_ws_connection();
        let channels = WS_NET.get(&fd).unwrap().clone();
        channels
            .tx_writer
            .try_send(WsRequest::Close(Message::Close(None)))
            .unwrap();
        timeout(
            Duration::from_secs(2),
            run_connection(stream, fd, reads, writes),
        )
        .await
        .unwrap();
        assert!(!WS_NET.contains_key(&fd));
        assert!(channels.tx_reader.is_closed());
        assert!(channels.tx_writer.is_closed());
    }

    #[tokio::test]
    async fn connection_shutdown_releases_active_and_queued_readers() {
        for mode in 0..4 {
            let owner = 0x7200_0030 + mode;
            let (tx, mut responses) = mpsc::unbounded_channel();
            let mut actor = LuaActor::new(&context::LuaActorParam {
                id: owner,
                unique: false,
                creator: 0,
                session: 0,
                memlimit: 0,
                name: String::new(),
                source: String::new(),
                params: String::new(),
            });
            CONTEXT.add_actor(&mut actor, tx).unwrap();
            let (socket, peer) = tokio::io::duplex(4096);
            let stream = WebSocketStream::from_raw_socket(socket, Role::Server, None).await;
            let mut peer = WebSocketStream::from_raw_socket(peer, Role::Client, None).await;
            let (fd, reads, writes) = setup_ws_connection();
            let channels = WS_NET.get(&fd).unwrap().clone();
            channels
                .tx_reader
                .try_send(WsRequest::Read(owner, 1, if mode == 2 { 100 } else { 0 }))
                .unwrap();
            let task = tokio::spawn(run_connection(stream, fd, reads, writes));
            timeout(Duration::from_secs(2), async {
                while channels.tx_reader.capacity() == 0 {
                    tokio::task::yield_now().await;
                }
            })
            .await
            .unwrap();
            channels
                .tx_reader
                .try_send(WsRequest::Read(owner, 2, 0))
                .unwrap();
            match mode {
                0 => channels
                    .tx_writer
                    .try_send(WsRequest::Close(Message::Close(None)))
                    .unwrap(),
                1 => {
                    drop(peer);
                    channels
                        .tx_writer
                        .try_send(WsRequest::Write(Message::Text("test".into()), false))
                        .unwrap();
                }
                2 => {} // Let the active read time out.
                3 => peer.send(Message::Close(None)).await.unwrap(),
                _ => unreachable!(),
            }
            timeout(Duration::from_secs(2), task)
                .await
                .unwrap()
                .unwrap();
            assert!(!WS_NET.contains_key(&fd));
            for session in [1, 2] {
                let message = responses
                    .try_recv()
                    .expect("waiting reader must be released");
                assert_eq!(message.session, session);
                let context::MessageBody::Boxed(_, mut value) = message.data else {
                    panic!("expected websocket response")
                };
                let response = unsafe { Box::from_raw(value.into_raw() as *mut WsResponse) };
                if mode == 3 && session == 1 {
                    assert!(matches!(*response, WsResponse::Read(Message::Close(_))));
                } else {
                    assert!(matches!(*response, WsResponse::Error(_)));
                }
            }
            assert!(responses.try_recv().is_err());
            CONTEXT.remove_actor(owner, "");
        }
    }
}
