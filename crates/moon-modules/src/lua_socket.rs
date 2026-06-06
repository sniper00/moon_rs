use dashmap::DashMap;
use lazy_static::lazy_static;
use moon_runtime::{buffer::Buffer, check_arc_buffer, context::MessageBody};
use moon_lua::{
    cstr, ffi,
    laux::{self, LuaState, LuaType},
    lreg, lreg_null, luaL_newlib,
};
use std::ffi::c_void;
use std::net::TcpStream;
use std::sync::Arc;
use std::time::Duration;
use std::ffi::c_int;
use tokio::io::AsyncReadExt;

use tokio::{
    io::{AsyncBufReadExt, AsyncWriteExt, BufReader, Result},
    net::{
        TcpListener,
        tcp::{OwnedReadHalf, OwnedWriteHalf},
    },
    sync::mpsc,
    time::{sleep, timeout},
};

use moon_runtime::{
    actor::LuaActor,
    context::{self, ActorId, CONTEXT, Message},
};

use crate::next_net_fd;

use crate::ShortBytes;

lazy_static! {
    static ref NET: DashMap<i64, NetChannel> = DashMap::new();
}

/// Delimiter type used in ReadUntil: max 7 bytes, no heap allocation.
pub type Delimiter = ShortBytes<8>;

#[derive(Debug)]
pub enum NetOp {
    ReadUntil(ActorId, i64, usize, Delimiter, u64), //owner,session,max_size,delim,timeout_ms
    ReadBytes(ActorId, i64, usize, u64),             //owner,session,size,timeout_ms
    ReadFrame(ActorId, i64, u64),                    //owner,session,read_timeout
    Write(ActorId, Arc<Buffer>, bool),               //owner,data,close
    Close(),
}

pub struct NetChannel(pub mpsc::Sender<NetOp>, pub mpsc::Sender<NetOp>);

const SOCKET_DATA_ACCEPT: u8 = 2;
const SOCKET_DATA_MESSAGE: u8 = 3;
const SOCKET_DATA_CLOSE: u8 = 4;

const MESSAGE_CONTINUED_FLAG: u16 = u16::MAX;

pub enum SocketEvent {
    Accept(i64, i64, String), // listen_fd, conn_fd, remote_addr
    Message(i64, Box<Buffer>),
    Close(i64, String, String),
}

async fn read_until(
    reader: &mut BufReader<OwnedReadHalf>,
    owner: ActorId,
    session: i64,
    max_size: usize,
    delim: Delimiter,
    read_timeout: u64,
) -> bool {
    let mut with_delim = false;
    let raw = delim.as_slice();
    let delim_bytes = if raw[0] == b'^' {
        if raw.len() < 2 {
            CONTEXT.response_error(
                0,
                owner,
                -session,
                "read_until: delim is empty".to_string(),
            );
            return false;
        }
        with_delim = true;
        &raw[1..]
    } else {
        raw
    };

    let mut buffer = Box::new(Buffer::with_capacity(std::cmp::min(max_size, 512)));
    let last_byte = *delim_bytes.last().unwrap();
    let delim_len = delim_bytes.len();
    loop {
        let read_res = if read_timeout > 0 {
            match timeout(
                Duration::from_millis(read_timeout),
                reader.read_until(last_byte, buffer.as_mut_vec()),
            )
            .await
            {
                Ok(res) => res,
                Err(_) => {
                    CONTEXT.response_error(0, owner, -session, "read timeout".to_string());
                    return false;
                }
            }
        } else {
            reader
                .read_until(last_byte, buffer.as_mut_vec())
                .await
        };

        match read_res {
            Ok(0) => {
                CONTEXT.response_error(0, owner, -session, "eof".to_string());
                return false;
            }
            Ok(_) => {
                if buffer.len() >= max_size {
                    CONTEXT.response_error(0, owner, -session, "read_until: max size exceeded".to_string());
                    return false;
                }
                let buf_len = buffer.len();
                if buf_len >= delim_len && buffer.as_vec()[buf_len - delim_len..] == *delim_bytes {
                    if !with_delim {
                        buffer.revert(delim_len);
                    }
                    if CONTEXT
                        .send(Message {
                            from: 0,
                            to: owner,
                            session,
                            data: MessageBody::Buffer(context::PTYPE_SOCKET_TCP, buffer),
                        })
                        .is_some()
                    {
                        return false;
                    }
                    break;
                }
            }
            Err(err) => {
                CONTEXT.response_error(0, owner, -session, err.to_string());
                return false;
            }
        }
    }
    true
}

async fn read_bytes(
    reader: &mut BufReader<OwnedReadHalf>,
    owner: ActorId,
    session: i64,
    size: usize,
    read_timeout: u64,
) -> bool {
    if size == 0 {
        CONTEXT.response_error(
            0,
            owner,
            -session,
            "read_bytes: size must be greater than 0".to_string(),
        );
        return false;
    }
    let mut buffer = Box::new(Buffer::with_capacity(size));
    let space = buffer.prepare(size);
    let read_res = if read_timeout > 0 {
        match timeout(
            Duration::from_millis(read_timeout),
            reader.read_exact(space),
        )
        .await
        {
            Ok(res) => res,
            Err(_) => {
                CONTEXT.response_error(0, owner, -session, "read timeout".to_string());
                return false;
            }
        }
    } else {
        reader.read_exact(space).await
    };

    match read_res {
        Ok(_) => {
            let _ = buffer.commit(size);
            if CONTEXT
                .send(Message {
                    from: 0,
                    to: owner,
                    session,
                    data: MessageBody::Buffer(context::PTYPE_SOCKET_TCP, buffer),
                })
                .is_some()
            {
                return false;
            }
        }
        Err(err) => {
            CONTEXT.response_error(0, owner, -session, err.to_string());
            return false;
        }
    }
    true
}

/// Wrapper that drains pending read operations on drop, sending error responses
/// for any sessions that would otherwise be silently lost (e.g. when the read task
/// is aborted because the write side closed first).
struct ReadOpGuard(mpsc::Receiver<NetOp>);

impl Drop for ReadOpGuard {
    fn drop(&mut self) {
        while let Ok(op) = self.0.try_recv() {
            match op {
                NetOp::ReadUntil(owner, session, ..)
                | NetOp::ReadBytes(owner, session, ..)
                | NetOp::ReadFrame(owner, session, ..) => {
                    if session > 0 {
                        CONTEXT.response_error(0, owner, -session, "closed".to_string());
                    }
                }
                _ => {}
            }
        }
    }
}

/// Unified read handler: supports ReadUntil, ReadBytes, and ReadFrame on any fd.
/// Returns `Some(reason)` when the connection ended due to an I/O error (the reason
/// should appear in the close event). Returns `None` for clean exits (explicit close,
/// owner dead, channel closed).
async fn handle_read(reader: OwnedReadHalf, fd: i64, _addr: String, rx: mpsc::Receiver<NetOp>) -> Option<String> {
    let mut rx = ReadOpGuard(rx);
    let mut reader = BufReader::new(reader);
    while let Some(op) = rx.0.recv().await {
        match op {
            NetOp::ReadUntil(owner, session, max_size, delim, read_timeout) => {
                if !read_until(&mut reader, owner, session, max_size, delim, read_timeout).await {
                    return None;
                }
            }
            NetOp::ReadBytes(owner, session, size, read_timeout) => {
                if !read_bytes(&mut reader, owner, session, size, read_timeout).await {
                    return None;
                }
            }
            NetOp::ReadFrame(owner, session, read_timeout) => {
                if session > 0 {
                    match read_one_frame(&mut reader, read_timeout).await {
                        Ok(buf) => {
                            if CONTEXT.send(Message {
                                from: 0,
                                to: owner,
                                session,
                                data: MessageBody::Buffer(context::PTYPE_SOCKET_EVENT, buf),
                            }).is_some() {
                                return None;
                            }
                        }
                        Err(err_msg) => {
                            CONTEXT.response_error(0, owner, -session, err_msg.clone());
                            return Some(err_msg);
                        }
                    }
                } else {
                    return frame_read_loop(&mut reader, owner, fd, read_timeout).await;
                }
            }
            NetOp::Close() => return None,
            _ => {}
        }
    }
    None
}

async fn handle_write(mut writer: OwnedWriteHalf, mut rx: mpsc::Receiver<NetOp>) -> Option<String> {
    while let Some(op) = rx.recv().await {
        match op {
            NetOp::Write(_owner, data, close) => {
                if let Err(err) = writer.write_all(data.as_slice()).await {
                    return Some(format!("write: {}", err));
                }
                if close {
                    return None;
                }
            }
            NetOp::Close() => {
                return None;
            }
            _ => {}
        }
    }
    None
}

/// Read one complete framed message from the reader.
/// Returns Ok(Some(buf)) on success, Ok(None) if owner is dead, Err(msg) on I/O error.
async fn read_one_frame(
    reader: &mut BufReader<OwnedReadHalf>,
    read_timeout: u64,
) -> std::result::Result<Box<Buffer>, String> {
    let mut data: Option<Box<Buffer>> = None;

    loop {
        let mut header_buf = [0u8; 2];
        let header_res = if read_timeout > 0 {
            match timeout(Duration::from_millis(read_timeout), reader.read_exact(&mut header_buf)).await {
                Ok(res) => res,
                Err(_) => return Err("read timeout".to_string()),
            }
        } else {
            reader.read_exact(&mut header_buf).await
        };
        if let Err(err) = header_res {
            let err_msg = if err.kind() == std::io::ErrorKind::UnexpectedEof {
                "eof".to_string()
            } else {
                err.to_string()
            };
            return Err(err_msg);
        }

        let header = u16::from_be_bytes(header_buf);
        let fin = header != MESSAGE_CONTINUED_FLAG;
        let size = header as usize;

        if size == 0 && fin {
            if let Some(buf) = data.take() {
                return Ok(buf);
            }
            continue;
        }

        let buf = data.get_or_insert_with(|| {
            let alloc_size = if fin { size } else { size * 2 };
            Box::new(Buffer::with_capacity(alloc_size))
        });

        if size > 0 {
            let space = buf.prepare(size);
            let body_res = if read_timeout > 0 {
                match timeout(Duration::from_millis(read_timeout), reader.read_exact(&mut space[..size])).await {
                    Ok(res) => res,
                    Err(_) => return Err("read timeout".to_string()),
                }
            } else {
                reader.read_exact(&mut space[..size]).await
            };
            if let Err(err) = body_res {
                let err_msg = if err.kind() == std::io::ErrorKind::UnexpectedEof {
                    "eof".to_string()
                } else {
                    err.to_string()
                };
                return Err(err_msg);
            }
            let _ = buf.commit(size);
        }

        if fin {
            if let Some(buf) = data.take() {
                return Ok(buf);
            }
        }
    }
}

/// Auto-read loop: continuously reads framed messages and dispatches to owner via callback.
/// Returns `Some(reason)` on I/O error, `None` if the owner is dead (send failed).
async fn frame_read_loop(
    reader: &mut BufReader<OwnedReadHalf>,
    owner: ActorId,
    fd: i64,
    read_timeout: u64,
) -> Option<String> {
    loop {
        match read_one_frame(reader, read_timeout).await {
            Ok(buf) => {
                if CONTEXT.send_value(
                    context::PTYPE_SOCKET_EVENT,
                    owner,
                    0,
                    SocketEvent::Message(fd, buf),
                ).is_some() {
                    return None;
                }
            }
            Err(err_msg) => {
                return Some(err_msg);
            }
        }
    }
}

/// Set up a connection: start read/write tasks. Assumes NET entry is already inserted.
async fn run_connection(socket: tokio::net::TcpStream, owner: ActorId, fd: i64, rx_reader: mpsc::Receiver<NetOp>, rx_writer: mpsc::Receiver<NetOp>) {
    let addr = socket
        .peer_addr()
        .map(|a| a.to_string())
        .unwrap_or_default();

    socket.set_nodelay(true).unwrap_or_default();
    let (reader, writer) = socket.into_split();

    let addr_clone = addr.clone();
    let mut read_task = CONTEXT.io_runtime().spawn(
        handle_read(reader, fd, addr_clone, rx_reader)
    );
    let mut write_task = CONTEXT.io_runtime().spawn(handle_write(writer, rx_writer));

    let close_reason = tokio::select! {
        res = &mut read_task => {
            write_task.abort();
            res.ok().flatten().unwrap_or_else(|| "closed".to_string())
        }
        res = &mut write_task => {
            read_task.abort();
            res.ok().flatten().unwrap_or_else(|| "closed".to_string())
        }
    };

    let _ = CONTEXT.send_value(
        context::PTYPE_SOCKET_EVENT,
        owner,
        0,
        SocketEvent::Close(fd, addr, close_reason),
    );
    NET.remove(&fd);
}

fn setup_net_channel(fd: i64) -> (mpsc::Receiver<NetOp>, mpsc::Receiver<NetOp>) {
    let (tx_reader, rx_reader) = mpsc::channel::<NetOp>(1);
    let (tx_writer, rx_writer) = mpsc::channel::<NetOp>(64 * 1024);
    NET.insert(fd, NetChannel(tx_reader, tx_writer));
    (rx_reader, rx_writer)
}

fn listen(addr: &str, owner: ActorId) -> Result<i64> {
    let listener = std::net::TcpListener::bind(addr)?;
    listener.set_nonblocking(true)?;
    let listener = TcpListener::from_std(listener)?;

    let fd = next_net_fd();
    let (tx, mut rx) = mpsc::channel::<NetOp>(1);
    NET.insert(fd, NetChannel(tx.clone(), tx));

    CONTEXT.io_runtime().spawn(async move {
        loop {
            tokio::select! {
                result = listener.accept() => {
                    match result {
                        Ok((socket, _)) => {
                            let conn_fd = next_net_fd();
                            let remote_addr = socket
                                .peer_addr()
                                .map(|a| a.to_string())
                                .unwrap_or_default();

                            let (rx_reader, rx_writer) = setup_net_channel(conn_fd);

                            if CONTEXT.send_value(
                                context::PTYPE_SOCKET_EVENT,
                                owner,
                                0,
                                SocketEvent::Accept(fd, conn_fd, remote_addr),
                            ).is_some() {
                                NET.remove(&conn_fd);
                                break;
                            }

                            CONTEXT.io_runtime().spawn(
                                run_connection(socket, owner, conn_fd, rx_reader, rx_writer)
                            );
                        }
                        Err(err) => {
                            log::warn!("accept error: {}", err);
                            sleep(Duration::new(1, 0)).await;
                        }
                    }
                }
                op = rx.recv() => {
                    match op {
                        Some(NetOp::Close()) | None => break,
                        _ => {}
                    }
                }
            }
        }
        NET.remove(&fd);
    });

    Ok(fd)
}

extern "C-unwind" fn lua_socket_listen(state: LuaState) -> c_int {
    let _guard = CONTEXT.io_runtime().enter();

    let addr = unsafe { laux::lua_check_str(state, 1) };
    let actor = LuaActor::from_lua_state(state);
    let owner = unsafe { (*actor).id };

    match listen(addr, owner) {
        Ok(fd) => {
            laux::lua_push(state, fd);
            1
        }
        Err(err) => {
            crate::lua_push_error(state, &format!("Listen '{}' failed: {}", addr, err))
        }
    }
}

extern "C-unwind" fn lua_socket_read(state: LuaState) -> c_int {
    let fd = laux::lua_get(state, 1);

    let actor = LuaActor::from_lua_state(state);
    let owner = unsafe { (*actor).id };
    let session = unsafe { (*actor).next_session() };

    let op = if laux::lua_type(state, 2) == LuaType::Integer {
        let size = laux::lua_get(state, 2);
        let read_timeout: u64 = laux::lua_opt(state, 3).unwrap_or(0);
        NetOp::ReadBytes(owner, session, size, read_timeout)
    } else {
        let delim = unsafe { laux::lua_check_lstring(state, 2) };
        let delim = match Delimiter::new(delim) {
            Some(d) => d,
            None => {
                laux::lua_error(state, format!("read: delim is empty or too long (max {} bytes)", 7));
            }
        };
        let max_size = laux::lua_opt(state, 3).unwrap_or(0x1000000);
        let read_timeout: u64 = laux::lua_opt(state, 4).unwrap_or(0);
        NetOp::ReadUntil(owner, session, max_size, delim, read_timeout)
    };

    if let Some(channel) = NET.get(&fd) {
        if let Err(err) = channel.value().0.try_send(op) {
            return crate::lua_push_error(state, &format!("read: channel full (fd={}): {}", fd, err));
        }
    } else {
        return crate::lua_push_error(state, &format!("read: fd {} not found", fd));
    }

    laux::lua_push(state, session);

    1
}

extern "C-unwind" fn lua_socket_write(state: LuaState) -> c_int {
    let actor = LuaActor::from_lua_state(state);
    let owner = unsafe { (*actor).id };

    let fd = laux::lua_get(state, 1);
    let data = check_arc_buffer(state, 2);
    let max_write_capacity = laux::lua_opt(state, 3).unwrap_or(u16::MAX);
    let close = laux::lua_opt(state, 4).unwrap_or(false);

    if let Some(channel) = NET.get(&fd) {
        if max_write_capacity != u16::MAX {
            let pending = channel.value().1.max_capacity() - channel.value().1.capacity();
            if pending > max_write_capacity as usize {
                let _ = channel.value().1.try_send(NetOp::Close());
                return crate::lua_push_error(state, &format!("write: backpressure (fd={})", fd));
            }
        }
        match channel
            .value()
            .1
            .try_send(NetOp::Write(owner, data, close))
        {
            Ok(_) => {
                laux::lua_push(state, true);
                1
            }
            Err(err) => {
                crate::lua_push_error(state, &format!("write: channel full (fd={}): {}", fd, err))
            }
        }
    } else {
        crate::lua_push_error(state, &format!("write: fd {} not found", fd))
    }
}

extern "C-unwind" fn lua_socket_connect(state: LuaState) -> c_int {
    let addr = unsafe { laux::lua_check_str(state, 1) }.to_string();
    let connect_timeout: u64 = laux::lua_opt(state, 2).unwrap_or(5000);

    let actor = LuaActor::from_lua_state(state);
    let owner = unsafe { (*actor).id };
    let session = unsafe { (*actor).next_session() };

    CONTEXT.io_runtime().spawn(async move {
        match timeout(
            Duration::from_millis(connect_timeout),
            tokio::net::TcpStream::connect(addr.as_str()),
        )
        .await
        {
            Ok(Ok(socket)) => {
                let fd = next_net_fd();
                let (rx_reader, rx_writer) = setup_net_channel(fd);
                if CONTEXT.send(Message {
                    from: 0,
                    to: owner,
                    session,
                    data: MessageBody::ISize(context::PTYPE_INTEGER, fd as isize),
                }).is_some() {
                    NET.remove(&fd);
                    return;
                }
                CONTEXT.io_runtime().spawn(run_connection(socket, owner, fd, rx_reader, rx_writer));
            }
            Ok(Err(err)) => {
                CONTEXT.response_error(0, owner, -session, format!("connect '{}': {}", addr, err));
            }
            Err(_) => {
                CONTEXT.response_error(
                    0,
                    owner,
                    -session,
                    format!("connect '{}': timeout ({}ms)", addr, connect_timeout),
                );
            }
        }
    });

    laux::lua_push(state, session);
    1
}

extern "C-unwind" fn lua_read_frame(state: LuaState) -> c_int {
    let fd: i64 = laux::lua_get(state, 1);
    let actor = LuaActor::from_lua_state(state);
    let owner = unsafe { (*actor).id };
    let session = unsafe { (*actor).next_session() };
    let read_timeout: u64 = laux::lua_opt(state, 2).unwrap_or(0);

    if let Some(channel) = NET.get(&fd) {
        match channel.value().0.try_send(NetOp::ReadFrame(owner, session, read_timeout)) {
            Ok(_) => {}
            Err(err) => {
                return crate::lua_push_error(
                    state,
                    &format!("read_frame: channel full (fd={}): {}", fd, err),
                );
            }
        }
    } else {
        return crate::lua_push_error(state, &format!("read_frame: fd {} not found", fd));
    }

    laux::lua_push(state, session);
    1
}

/// Start auto-read mode for a framed fd (session=0, callback-based).
extern "C-unwind" fn lua_start_read_frame(state: LuaState) -> c_int {
    let fd: i64 = laux::lua_get(state, 1);
    let actor = LuaActor::from_lua_state(state);
    let owner = unsafe { (*actor).id };
    let read_timeout: u64 = laux::lua_opt(state, 2).unwrap_or(0);

    if let Some(channel) = NET.get(&fd) {
        match channel.value().0.try_send(NetOp::ReadFrame(owner, 0, read_timeout)) {
            Ok(_) => {
                laux::lua_push(state, true);
                1
            }
            Err(err) => {
                crate::lua_push_error(
                    state,
                    &format!("start_read_frame: channel full (fd={}): {}", fd, err),
                )
            }
        }
    } else {
        crate::lua_push_error(state, &format!("start_read_frame: fd {} not found", fd))
    }
}

extern "C-unwind" fn lua_socket_close(state: LuaState) -> c_int {
    let fd = laux::lua_get(state, 1);

    if let Some(channel) = NET.get(&fd) {
        match channel.value().1.try_send(NetOp::Close()) {
            Ok(_) => {
                laux::lua_push(state, true);
                return 1;
            }
            Err(_) => {
                return 0;
            }
        }
    }
    0
}

extern "C-unwind" fn lua_host(state: LuaState) -> c_int {
    if let Ok(addr) = laux::lua_opt(state, 1).unwrap_or("1.1.1.1:80").parse()
        && let Ok(socket) = TcpStream::connect_timeout(&addr, Duration::from_millis(1000))
        && let Ok(local_addr) = socket.local_addr()
    {
        laux::lua_push(state, local_addr.ip().to_string());
        return 1;
    }
    0
}

extern "C-unwind" fn lua_write_frame(state: LuaState) -> c_int {
    let actor = LuaActor::from_lua_state(state);
    let owner = unsafe { (*actor).id };

    let fd: i64 = laux::lua_get(state, 1);
    let data = check_arc_buffer(state, 2);
    let max_write_capacity = laux::lua_opt(state, 3).unwrap_or(u16::MAX);
    let close = laux::lua_opt(state, 4).unwrap_or(false);

    if data.is_empty() {
        laux::lua_push(state, true);
        return 1;
    }

    let src = data.as_slice();
    let src_len = src.len();
    let num_full_chunks = src_len / MESSAGE_CONTINUED_FLAG as usize;
    let remainder = src_len % MESSAGE_CONTINUED_FLAG as usize;
    let needs_end_marker = remainder == 0 && src_len > 0;

    let out_size = src_len + (num_full_chunks + 1) * 2 + if needs_end_marker { 2 } else { 0 };
    let mut out = Buffer::with_capacity(out_size);

    let mut offset = 0usize;
    while src_len - offset >= MESSAGE_CONTINUED_FLAG as usize {
        out.write_slice(&MESSAGE_CONTINUED_FLAG.to_be_bytes());
        out.write_slice(&src[offset..offset + MESSAGE_CONTINUED_FLAG as usize]);
        offset += MESSAGE_CONTINUED_FLAG as usize;
    }

    let remaining = src_len - offset;
    if remaining > 0 {
        out.write_slice(&(remaining as u16).to_be_bytes());
        out.write_slice(&src[offset..]);
    } else if needs_end_marker {
        out.write_slice(&0u16.to_be_bytes());
    }

    let arc_out = Arc::new(out);

    if let Some(channel) = NET.get(&fd) {
        if max_write_capacity != u16::MAX {
            let pending = channel.value().1.max_capacity() - channel.value().1.capacity();
            if pending > max_write_capacity as usize {
                let _ = channel.value().1.try_send(NetOp::Close());
                return crate::lua_push_error(state, &format!("write_frame: backpressure (fd={})", fd));
            }
        }
        match channel.value().1.try_send(NetOp::Write(owner, arc_out, close)) {
            Ok(_) => {
                laux::lua_push(state, true);
                1
            }
            Err(err) => {
                crate::lua_push_error(state, &format!("write_frame: channel full (fd={}): {}", fd, err))
            }
        }
    } else {
        crate::lua_push_error(state, &format!("write_frame: fd {} not found", fd))
    }
}

extern "C-unwind" fn lua_decode_socket_event(state: LuaState) -> c_int {
    let event = laux::lua_into_userdata::<SocketEvent>(state, 1);

    match *event {
        SocketEvent::Accept(listen_fd, conn_fd, addr) => {
            laux::lua_push(state, listen_fd);
            laux::lua_push(state, SOCKET_DATA_ACCEPT as i64);
            laux::lua_push(state, conn_fd);
            laux::lua_push(state, addr.as_str());
            4
        }
        SocketEvent::Message(fd, data) => {
            laux::lua_push(state, fd);
            laux::lua_push(state, SOCKET_DATA_MESSAGE as i64);
            laux::lua_pushlightuserdata(state, Box::into_raw(data) as *mut c_void);
            3
        }
        SocketEvent::Close(fd, addr, err) => {
            laux::lua_push(state, fd);
            laux::lua_push(state, SOCKET_DATA_CLOSE as i64);
            laux::lua_push(state, addr.as_str());
            laux::lua_push(state, err.as_str());
            4
        }
    }
}

pub extern "C-unwind" fn luaopen_socket(state: LuaState) -> c_int {
    let l = [
        lreg!("listen", lua_socket_listen),
        lreg!("read", lua_socket_read),
        lreg!("read_frame", lua_read_frame),
        lreg!("start_read_frame", lua_start_read_frame),
        lreg!("write", lua_socket_write),
        lreg!("write_frame", lua_write_frame),
        lreg!("connect", lua_socket_connect),
        lreg!("close", lua_socket_close),
        lreg!("decode_socket_event", lua_decode_socket_event),
        lreg!("host", lua_host),
        lreg_null!(),
    ];

    luaL_newlib!(state, l);
    1
}
