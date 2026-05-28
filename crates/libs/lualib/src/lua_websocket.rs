use std::time::Duration;

use bytes::Bytes;
use dashmap::DashMap;
use futures_util::{SinkExt, StreamExt, TryStreamExt};
use lazy_static::lazy_static;
use tokio::{
    net::{TcpListener, TcpStream},
    sync::mpsc,
    time::timeout,
};
use tokio_tungstenite::{
    accept_async, connect_async,
    tungstenite::{
        http::{self, Response},
        protocol::{frame::coding::CloseCode, CloseFrame},
        Message,
    },
    MaybeTlsStream, WebSocketStream,
};

use actor::context::{self, CONTEXT};
use luars::{CFunction, LuaResult, LuaState, LuaValue};

use crate::{lua_actor::ActorRef, lua_check_lightuserdata_bytes};
use crate::{
    lua_check_bytes, lua_check_integer, lua_check_str, lua_newuserdata, lua_opt_integer,
    lua_opt_str, lua_push_error, lua_take_typed_lightuserdata, next_net_fd, opt_field_int,
    opt_field_str,
};

lazy_static! {
    static ref WS_NET: DashMap<i64, WsChannel> = DashMap::new();
}

#[derive(Clone)]
struct WsChannel {
    tx_reader: mpsc::Sender<WsRequest>,
    tx_writer: mpsc::Sender<WsRequest>,
}

enum WsRequest {
    Read(i64, i64, u64),
    Write(Message, bool),
    Close(Message),
    Accept(i64, i64),
}

enum WsResponse {
    Connect(i64, Response<Option<Vec<u8>>>),
    Accept(i64, String),
    Read(Message),
    Error(String),
}

async fn handle_read<S>(
    owner: i64,
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
                CONTEXT.send_value(
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
                CONTEXT.send_value(
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

async fn run_connection<S>(
    stream: WebSocketStream<S>,
    fd: i64,
    mut rx_reader: mpsc::Receiver<WsRequest>,
    rx_writer: mpsc::Receiver<WsRequest>,
) where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin + Send + 'static,
{
    let (writer, mut reader) = stream.split();

    let mut read_task = tokio::spawn(async move {
        let mut closed = false;
        while let Some(op) = rx_reader.recv().await {
            if let WsRequest::Read(owner, session, read_timeout) = op {
                if !closed {
                    if let Err(err) = handle_read(owner, session, read_timeout, &mut reader).await
                    {
                        CONTEXT.send_value(
                            context::PTYPE_WEBSOCKET,
                            owner,
                            session,
                            WsResponse::Error(err),
                        );
                        closed = true;
                    }
                } else {
                    CONTEXT.send_value(
                        context::PTYPE_WEBSOCKET,
                        owner,
                        session,
                        WsResponse::Error("closed".to_string()),
                    );
                }
            }
        }
    });

    let mut write_task = tokio::spawn(handle_write(writer, rx_writer));

    if tokio::try_join!(&mut read_task, &mut write_task).is_err() {
        read_task.abort();
        write_task.abort();
    }

    WS_NET.remove(&fd);
}

fn setup_ws_connection() -> (i64, mpsc::Receiver<WsRequest>, mpsc::Receiver<WsRequest>) {
    let fd = next_net_fd();
    let (tx_reader, rx_reader) = mpsc::channel::<WsRequest>(1);
    let (tx_writer, rx_writer) = mpsc::channel::<WsRequest>(100);
    WS_NET.insert(fd, WsChannel { tx_reader, tx_writer });
    (fd, rx_reader, rx_writer)
}

async fn on_client_connected(
    stream: WebSocketStream<MaybeTlsStream<TcpStream>>,
    response: Response<Option<Vec<u8>>>,
    owner: i64,
    session: i64,
) {
    let (fd, rx_reader, rx_writer) = setup_ws_connection();
    CONTEXT.send_value(
        context::PTYPE_WEBSOCKET,
        owner,
        session,
        WsResponse::Connect(fd, response),
    );
    run_connection(stream, fd, rx_reader, rx_writer).await;
}

async fn on_server_accepted(
    stream: WebSocketStream<TcpStream>,
    addr: String,
    owner: i64,
    session: i64,
) {
    let (fd, rx_reader, rx_writer) = setup_ws_connection();
    CONTEXT.send_value(
        context::PTYPE_WEBSOCKET,
        owner,
        session,
        WsResponse::Accept(fd, addr),
    );
    run_connection(stream, fd, rx_reader, rx_writer).await;
}

// --- Lua-facing functions on the connection userdata ---

fn ws_read(state: &mut LuaState) -> LuaResult<usize> {
    let conn = crate::lua_check_userdata::<WsChannel>(state, 1)?;
    let owner: i64 = lua_check_integer(state, 2)?;
    let session: i64 = lua_check_integer(state, 3)?;
    let read_timeout: u64 = lua_opt_integer(state, 4).unwrap_or(5000);

    match conn
        .tx_reader
        .try_send(WsRequest::Read(owner, session, read_timeout))
    {
        Ok(_) => {
            state.push_value(LuaValue::integer(session))?;
            Ok(1)
        }
        Err(err) => lua_push_error(state, &format!("ws read error: {}", err)),
    }
}

fn to_message(data: Vec<u8>, kind: &str) -> Message {
    match kind {
        "c" => {
            Message::Close(Some(CloseFrame {
                code: CloseCode::Normal,
                reason: String::from_utf8_lossy(&data).into_owned().into(),
            }))
        }
        "t" => Message::Text(String::from_utf8_lossy(&data).into_owned().into()),
        "p" => Message::Ping(Bytes::from(data)),
        _ => Message::Binary(Bytes::from(data)),
    }
}

fn lua_check_message(state: &mut LuaState, index: usize, kind: &str) -> LuaResult<Message> {
    if let Some(value) = state.get_arg(index) {
        if let Some(b) = value.as_bytes() {
            Ok(to_message(b.to_vec(), kind))
        }else if value.is_lightuserdata(){
            if state.get_arg(index + 1).is_some_and(|v| v.is_integer()) {
                let len: usize = lua_opt_integer(state, index + 1).unwrap_or(0);
                let data = lua_check_lightuserdata_bytes(state, index, len)?.to_vec();
                Ok(to_message(data, kind))
            }else {
                let data = lua_check_bytes(state, index)?.to_vec();
                Ok(to_message(data, kind))
            }
        }else {
            Err(state.error(format!("bad argument #{} (string expected, got {})", index, value.type_name())))
        }
    } else {
        Err(state.error(format!("bad argument #{} (string expected, got none)", index)))
    }
}

fn ws_write(state: &mut LuaState) -> LuaResult<usize> {
    let conn = crate::lua_check_userdata::<WsChannel>(state, 1)?;
    let kind = lua_check_str(state, 2)?;
    let msg = lua_check_message(state, 3, kind)?;

    let close = msg.is_close();
    match conn.tx_writer.try_send(WsRequest::Write(msg, close)) {
        Ok(_) => {
            state.push_value(LuaValue::boolean(true))?;
            Ok(1)
        }
        Err(err) => lua_push_error(state, &format!("ws write error: {}", err)),
    }
}

fn ws_close(state: &mut LuaState) -> LuaResult<usize> {
    let conn = crate::lua_check_userdata::<WsChannel>(state, 1)?;
    let data = lua_opt_str(state, 2).unwrap_or("");

    match conn.tx_writer.try_send(WsRequest::Close(Message::Close(Some(CloseFrame {
        code: CloseCode::Normal,
        reason: data.to_string().into(),
    })))) {
        Ok(_) => {
            state.push_value(LuaValue::boolean(true))?;
            Ok(1)
        }
        Err(err) => lua_push_error(state, &format!("ws close error: {}", err)),
    }
}

// --- Module-level functions ---

fn connect(state: &mut LuaState) -> LuaResult<usize> {
    let table = state
        .get_arg(1)
        .filter(|v| v.is_table())
        .ok_or_else(|| state.error("bad argument #1 (table expected)".to_string()))?;

    let url = opt_field_str(state, &table, "url").unwrap_or_default();
    let connect_timeout: u64 =
        opt_field_int(state, &table, "connect_timeout").unwrap_or(5000) as u64;

    let actor = ActorRef::from_state(state);
    let owner = actor.id();
    let session = actor.next_session();

    tokio::spawn(async move {
        match timeout(Duration::from_millis(connect_timeout), connect_async(url)).await {
            Ok(Ok((stream, response))) => {
                tokio::spawn(on_client_connected(stream, response, owner, session));
            }
            Ok(Err(err)) => {
                CONTEXT.send_value(
                    context::PTYPE_WEBSOCKET,
                    owner,
                    session,
                    WsResponse::Error(err.to_string()),
                );
            }
            Err(err) => {
                CONTEXT.send_value(
                    context::PTYPE_WEBSOCKET,
                    owner,
                    session,
                    WsResponse::Error(format!("connect timeout: {}", err)),
                );
            }
        }
    });

    state.push_value(LuaValue::integer(session))?;
    Ok(1)
}

fn ws_listen(state: &mut LuaState) -> LuaResult<usize> {
    let addr = lua_check_str(state, 1)?.to_string();

    let listener = std::net::TcpListener::bind(&addr)
        .map_err(|e| state.error(format!("ws listen '{}' failed: {}", addr, e)))?;
    listener
        .set_nonblocking(true)
        .map_err(|e| state.error(format!("ws listen '{}' failed: {}", addr, e)))?;
    let listener = TcpListener::from_std(listener)
        .map_err(|e| state.error(format!("ws listen '{}' failed: {}", addr, e)))?;

    let fd = next_net_fd();
    let (tx, mut rx) = mpsc::channel::<WsRequest>(1);
    WS_NET.insert(fd, WsChannel { tx_reader: tx.clone(), tx_writer: tx });

    tokio::spawn(async move {
        while let Some(op) = rx.recv().await {
            match op {
                WsRequest::Accept(owner, session) => match listener.accept().await {
                    Ok((stream, addr)) => {
                        let addr_str = addr.to_string();
                        tokio::spawn(async move {
                            match accept_async(stream).await {
                                Ok(ws_stream) => {
                                    on_server_accepted(ws_stream, addr_str, owner, session).await;
                                }
                                Err(err) => {
                                    CONTEXT.send_value(
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
                        CONTEXT.send_value(
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

    state.push_value(LuaValue::integer(fd))?;
    Ok(1)
}

fn ws_accept(state: &mut LuaState) -> LuaResult<usize> {
    let fd: i64 = lua_check_integer(state, 1)?;
    let actor = ActorRef::from_state(state);
    let owner = actor.id();
    let session = actor.next_session();

    if let Some(channel) = WS_NET.get(&fd) {
        if let Err(err) = channel.tx_reader.try_send(WsRequest::Accept(owner, session)) {
            return lua_push_error(state, &format!("ws accept error: {}", err));
        }
    } else {
        return lua_push_error(state, &format!("ws: fd {} not found", fd));
    }
    state.push_value(LuaValue::integer(session))?;
    Ok(1)
}

fn find_connection(state: &mut LuaState) -> LuaResult<usize> {
    let id: i64 = lua_check_integer(state, 1)?;
    match WS_NET.get(&id) {
        Some(pair) => {
            static METHODS: &[(&str, CFunction)] = &[
                ("read", ws_read),
                ("write", ws_write),
                ("close", ws_close),
            ];
            let ud = lua_newuserdata(state, pair.value().clone(), "ws_connection", METHODS)?;
            state.push_value(ud)?;
        }
        None => {
            state.push_value(LuaValue::nil())?;
        }
    }
    Ok(1)
}

fn version_to_string(version: &http::Version) -> &'static str {
    match *version {
        http::Version::HTTP_09 => "HTTP/0.9",
        http::Version::HTTP_10 => "HTTP/1.0",
        http::Version::HTTP_11 => "HTTP/1.1",
        http::Version::HTTP_2 => "HTTP/2.0",
        http::Version::HTTP_3 => "HTTP/3.0",
        _ => "Unknown",
    }
}

fn decode(state: &mut LuaState) -> LuaResult<usize> {
    let response = lua_take_typed_lightuserdata::<WsResponse>(state, 1)?;
    match *response {
        WsResponse::Connect(id, ref resp) => {
            let table = state.create_table(0, 6)?;

            let k = state.create_string("fd")?;
            state.raw_set(&table, k, LuaValue::integer(id));

            let k = state.create_string("version")?;
            let v = state.create_string(version_to_string(&resp.version()))?;
            state.raw_set(&table, k, v);

            let k = state.create_string("status_code")?;
            state.raw_set(&table, k, LuaValue::integer(resp.status().as_u16() as i64));

            if let Some(body) = resp.body() {
                let k = state.create_string("body")?;
                let v = state.create_bytes(body)?;
                state.raw_set(&table, k, v);
            }

            let headers_table = state.create_table(0, resp.headers().len())?;
            for (key, value) in resp.headers().iter() {
                let k = state.create_string(key.as_str())?;
                let v = state.create_string(value.to_str().unwrap_or("").trim())?;
                state.raw_set(&headers_table, k, v);
            }
            let k = state.create_string("headers")?;
            state.raw_set(&table, k, headers_table);

            state.push_value(table)?;
            Ok(1)
        }
        WsResponse::Accept(fd, ref addr) => {
            let table = state.create_table(0, 2)?;
            let k = state.create_string("fd")?;
            state.raw_set(&table, k, LuaValue::integer(fd));
            let k = state.create_string("addr")?;
            let v = state.create_string(addr)?;
            state.raw_set(&table, k, v);
            state.push_value(table)?;
            Ok(1)
        }
        WsResponse::Read(data) => {
            let (kind, payload) = match data {
                Message::Text(s) => ("t", s.as_bytes().to_vec()),
                Message::Binary(b) => ("b", b.to_vec()),
                Message::Ping(b) => ("p", b.to_vec()),
                Message::Pong(b) => ("o", b.to_vec()),
                Message::Close(ref frame) => {
                    let reason = frame
                        .as_ref()
                        .map(|f| f.reason.as_bytes().to_vec())
                        .unwrap_or_default();
                    ("c", reason)
                }
                Message::Frame(_) => ("f", Vec::new()),
            };
            let v = state.create_bytes(&payload)?;
            state.push_value(v)?;
            let k = state.create_string(kind)?;
            state.push_value(k)?;
            Ok(2)
        }
        WsResponse::Error(ref err) => {
            state.push_value(LuaValue::boolean(false))?;
            let v = state.create_string(err)?;
            state.push_value(v)?;
            Ok(2)
        }
    }
}

pub fn register_websocket() -> luars::LibraryModule {
    luars::lua_module!("ws.core", {
        "connect" => connect,
        "listen" => ws_listen,
        "accept" => ws_accept,
        "find_connection" => find_connection,
        "decode" => decode,
    })
}
