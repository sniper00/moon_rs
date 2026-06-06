use std::{
    ffi::c_int,
    time::Duration,
};

use dashmap::DashMap;
use futures_util::{SinkExt, StreamExt, TryStreamExt};
use lazy_static::lazy_static;
use tokio::{
    net::{TcpListener, TcpStream},
    sync::mpsc,
    time::timeout,
};
use tokio_tungstenite::{
    accept_async_with_config, connect_async_with_config,
    tungstenite::{
        http::response,
        protocol::{frame::coding::CloseCode, CloseFrame, WebSocketConfig},
        Message,
    },
    MaybeTlsStream, WebSocketStream,
};

use moon_runtime::{
    actor::LuaActor,
    context::{self, ActorId, CONTEXT},
};
use moon_lua::{
    cstr, ffi,
    laux::{self, LuaState, LuaTable},
    lreg, lreg_null, luaL_newlib,
};

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
    owner: ActorId,
    session: i64,
    read_timeout: u64,
    reader: &mut futures_util::stream::SplitStream<WebSocketStream<S>>,
) -> Result<(), String>
where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
{
    if read_timeout > 0 {
        match timeout(Duration::from_millis(read_timeout), reader.try_next()).await {
            Ok(Ok(Some(message))) => {
                let _ = CONTEXT.send_value(
                    context::PTYPE_WEBSOCKET,
                    owner,
                    session,
                    WsResponse::Read(message),
                );
                Ok(())
            }
            Ok(Ok(None)) => Err("eof".to_string()),
            Ok(Err(err)) => Err(err.to_string()),
            Err(err) => Err(format!("read timeout: {}", err)),
        }
    } else {
        match reader.try_next().await {
            Ok(Some(message)) => {
                let _ = CONTEXT.send_value(
                    context::PTYPE_WEBSOCKET,
                    owner,
                    session,
                    WsResponse::Read(message),
                );
                Ok(())
            }
            Ok(None) => Err("eof".to_string()),
            Err(err) => Err(err.to_string()),
        }
    }
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
    WS_NET.insert(fd, WsChannel { tx_reader, tx_writer });
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

    let mut read_task = CONTEXT.io_runtime().spawn(async move {
        let mut closed = false;
        while let Some(op) = rx_reader.recv().await {
            if let WsRequest::Read(owner, session, read_timeout) = op {
                if !closed {
                    if let Err(err) =
                        handle_read(owner, session, read_timeout, &mut reader).await
                    {
                        let _ = CONTEXT.send_value(
                            context::PTYPE_WEBSOCKET,
                            owner,
                            session,
                            WsResponse::Error(err),
                        );
                        closed = true;
                    }
                } else {
                    let _ = CONTEXT.send_value(
                        context::PTYPE_WEBSOCKET,
                        owner,
                        session,
                        WsResponse::Error("closed".to_string()),
                    );
                }
            }
        }
    });

    let mut write_task = CONTEXT.io_runtime().spawn(handle_write(writer, rx_writer));

    if tokio::try_join!(&mut read_task, &mut write_task).is_err() {
        read_task.abort();
        write_task.abort();
    }

    WS_NET.remove(&fd);
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

extern "C-unwind" fn ws_read(state: LuaState) -> c_int {
    let conn =
        laux::lua_touserdata::<WsChannel>(state, 1).expect("invalid ws connection pointer");
    let read_timeout: u64 = laux::lua_opt(state, 2).unwrap_or(5000);

    let actor = LuaActor::from_lua_state(state);
    let owner = unsafe { (*actor).id };
    let session = unsafe { (*actor).next_session() };

    match conn
        .tx_reader
        .try_send(WsRequest::Read(owner, session, read_timeout))
    {
        Ok(_) => {
            laux::lua_push(state, session);
            1
        }
        Err(err) => {
            crate::lua_push_error(state, &format!("ws read error: {}", err))
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

fn check_message(state: LuaState, index: i32, kind: &str) -> Message {
    let data = match laux::lua_type(state, index) {
        laux::LuaType::String => unsafe { laux::lua_check_lstring(state, index) }.to_vec(),
        laux::LuaType::LightUserData => {
            let ptr = unsafe { ffi::lua_touserdata(state.as_ptr(), index) };
            if ptr.is_null() {
                laux::lua_error(
                    state,
                    format!("bad argument #{} (non-null lightuserdata expected)", index),
                );
            }
            if laux::lua_type(state, index + 1) == laux::LuaType::Integer {
                let len: usize = laux::lua_get(state, index + 1);
                unsafe { std::slice::from_raw_parts(ptr as *const u8, len) }.to_vec()
            } else {
                let buf = unsafe { &*(ptr as *const moon_runtime::buffer::Buffer) };
                buf.as_slice().to_vec()
            }
        }
        _ => {
            laux::lua_error(
                state,
                format!(
                    "bad argument #{} (string or lightuserdata expected, got {})",
                    index,
                    laux::type_name(state, index)
                ),
            );
        }
    };
    to_message(data, kind)
}

extern "C-unwind" fn ws_write(state: LuaState) -> c_int {
    let conn =
        laux::lua_touserdata::<WsChannel>(state, 1).expect("invalid ws connection pointer");
    let kind = unsafe { laux::lua_check_str(state, 2) };
    let msg = check_message(state, 3, kind);
    let close = msg.is_close();

    match conn.tx_writer.try_send(WsRequest::Write(msg, close)) {
        Ok(_) => {
            laux::lua_push(state, true);
            1
        }
        Err(err) => {
            crate::lua_push_error(state, &format!("ws write error: {}", err))
        }
    }
}

extern "C-unwind" fn ws_close(state: LuaState) -> c_int {
    let conn =
        laux::lua_touserdata::<WsChannel>(state, 1).expect("invalid ws connection pointer");
    let data = unsafe { laux::lua_opt_lstring(state, 2) }.unwrap_or_default();

    match conn.tx_writer.try_send(WsRequest::Close(Message::Close(Some(
        CloseFrame {
            code: CloseCode::Normal,
            reason: String::from_utf8_lossy(data).into_owned().into(),
        },
    )))) {
        Ok(_) => {
            laux::lua_push(state, true);
            1
        }
        Err(err) => {
            crate::lua_push_error(state, &format!("ws close error: {}", err))
        }
    }
}

// ---------- Module-level functions ----------

fn read_ws_config(state: LuaState, index: i32) -> WebSocketConfig {
    let mut config = WebSocketConfig::default();
    if let Some(size) = laux::opt_field::<usize>(state, index, "max_message_size") {
        config.max_message_size = Some(size);
    }
    if let Some(size) = laux::opt_field::<usize>(state, index, "max_frame_size") {
        config.max_frame_size = Some(size);
    }
    config
}

extern "C-unwind" fn ws_connect(state: LuaState) -> c_int {
    laux::lua_checktype(state, 1, ffi::LUA_TTABLE);

    let url: String = laux::opt_field(state, 1, "url").unwrap_or_default();
    let connect_timeout: u64 = laux::opt_field(state, 1, "connect_timeout").unwrap_or(5000);

    let ws_config = read_ws_config(state, 1);

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
                CONTEXT.io_runtime().spawn(on_client_connected(stream, response, owner, session));
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
    1
}

extern "C-unwind" fn ws_listen(state: LuaState) -> c_int {
    let _guard = CONTEXT.io_runtime().enter();

    let addr = unsafe { laux::lua_check_str(state, 1) };

    let ws_config = if laux::lua_type(state, 2) == laux::LuaType::Table {
        read_ws_config(state, 2)
    } else {
        WebSocketConfig::default()
    };

    let listener = match std::net::TcpListener::bind(addr) {
        Ok(l) => l,
        Err(err) => {
            return crate::lua_push_error(state, &format!("ws listen '{}' failed: {}", addr, err));
        }
    };

    if let Err(err) = listener.set_nonblocking(true) {
        return crate::lua_push_error(state, &format!("ws listen '{}' failed: {}", addr, err));
    }

    let listener = match TcpListener::from_std(listener) {
        Ok(l) => l,
        Err(err) => {
            return crate::lua_push_error(state, &format!("ws listen '{}' failed: {}", addr, err));
        }
    };

    let fd = next_ws_fd();
    let (tx, mut rx) = mpsc::channel::<WsRequest>(1);
    WS_NET.insert(fd, WsChannel { tx_reader: tx.clone(), tx_writer: tx });

    CONTEXT.io_runtime().spawn(async move {
        while let Some(op) = rx.recv().await {
            match op {
                WsRequest::Accept(owner, session) => match listener.accept().await {
                    Ok((stream, addr)) => {
                        let addr_str = addr.to_string();
                        let cfg = ws_config;
                        CONTEXT.io_runtime().spawn(async move {
                            match accept_async_with_config(stream, Some(cfg)).await {
                                Ok(ws_stream) => {
                                    on_server_accepted(ws_stream, addr_str, owner, session).await;
                                }
                                Err(err) => {
                                    let _ = CONTEXT.send_value(
                                        context::PTYPE_WEBSOCKET,
                                        owner,
                                        session,
                                        WsResponse::Error(format!(
                                            "ws handshake failed: {}",
                                            err
                                        )),
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
    1
}

extern "C-unwind" fn ws_accept(state: LuaState) -> c_int {
    let fd: i64 = laux::lua_get(state, 1);
    let actor = LuaActor::from_lua_state(state);
    let owner = unsafe { (*actor).id };
    let session = unsafe { (*actor).next_session() };

    if let Some(channel) = WS_NET.get(&fd) {
        if let Err(err) = channel.tx_reader.try_send(WsRequest::Accept(owner, session)) {
            return crate::lua_push_error(state, &format!("ws accept error: {}", err));
        }
    } else {
        return crate::lua_push_error(state, &format!("ws: fd {} not found", fd));
    }
    laux::lua_push(state, session);
    1
}

extern "C-unwind" fn ws_find_connection(state: LuaState) -> c_int {
    let id: i64 = laux::lua_get(state, 1);
    match WS_NET.get(&id) {
        Some(pair) => {
            let l = [
                lreg!("read", ws_read),
                lreg!("write", ws_write),
                lreg!("close", ws_close),
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
                return 1;
            }
        }
        None => {
            laux::lua_pushnil(state);
        }
    }
    1
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

extern "C-unwind" fn ws_decode(state: LuaState) -> c_int {
    laux::lua_checkstack(state, 6, std::ptr::null());
    let response = laux::lua_into_userdata::<WsResponse>(state, 1);
    match *response {
        WsResponse::Connect(fd, ref resp) => {
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
        WsResponse::Accept(fd, ref addr) => {
            LuaTable::new(state, 0, 2)
                .insert("fd", fd)
                .insert("addr", addr.as_str());
            1
        }
        WsResponse::Read(ref data) => {
            let (kind, payload): (&str, &[u8]) = match data {
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
        WsResponse::Error(ref err) => {
            crate::lua_push_error(state, err.as_str())
        }
    }
}

pub extern "C-unwind" fn luaopen_websocket(state: LuaState) -> c_int {
    let l = [
        lreg!("connect", ws_connect),
        lreg!("listen", ws_listen),
        lreg!("accept", ws_accept),
        lreg!("find_connection", ws_find_connection),
        lreg!("decode", ws_decode),
        lreg_null!(),
    ];

    luaL_newlib!(state, l);
    1
}
