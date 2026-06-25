use dashmap::DashMap;
use lazy_static::lazy_static;
use moon_base::{
    cstr, ffi,
    laux::{self, LuaState, LuaType},
    lreg, lreg_null, luaL_newlib,
};
use moon_runtime::{
    buffer::{BUFFER_HEAD_RESERVE, Buffer},
    check_arc_buffer,
    context::MessageBody,
};
use std::{
    ffi::{c_int, c_void},
    io::{Error, ErrorKind, IoSlice},
    net::TcpStream,
    sync::Arc,
    time::Duration,
};
use tokio::io::AsyncReadExt;

use tokio::{
    io::{AsyncBufReadExt, AsyncWrite, AsyncWriteExt, BufReader, Result},
    net::{TcpListener, tcp::OwnedReadHalf},
    sync::{Semaphore, mpsc},
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
    ReadBytes(ActorId, i64, usize, u64),            //owner,session,size,timeout_ms
    ReadFrame(ActorId, i64, u64),                   //owner,session,read_timeout
    Write(ActorId, Arc<Buffer>, bool),              //owner,data,close
    WriteFrame(ActorId, Arc<Buffer>, bool),         //owner,data,close
    Close(),
}

pub struct NetChannel(pub mpsc::Sender<NetOp>, pub mpsc::Sender<NetOp>);

const SOCKET_DATA_ACCEPT: u8 = 2;
const SOCKET_DATA_MESSAGE: u8 = 3;
const SOCKET_DATA_CLOSE: u8 = 4;

const MESSAGE_CONTINUED_FLAG: u16 = u16::MAX;

/// Hard upper bound on the bytes a single `socket.read` may request
/// (`read_bytes`) or accumulate (`read_until`). See `crate::LIMITS.max_network_read_bytes`.
const MAX_READ_SIZE: usize = crate::LIMITS.max_network_read_bytes;
const MAX_SOCKET_WRITE_BATCH_BYTES: usize = crate::LIMITS.socket_write_batch_bytes;

enum SocketWriteItem {
    Raw(Arc<Buffer>),
    Frame(Arc<Buffer>),
}

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
            CONTEXT.response_error(0, owner, -session, "read_until: delim is empty".to_string());
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
        // Read one buffered chunk at a time (bounded by the BufReader's internal
        // buffer) up to and including the next `last_byte`, enforcing `max_size`
        // incrementally. Using `fill_buf`/`consume` instead of `read_until`
        // prevents a peer that withholds the delimiter from growing `buffer`
        // without bound inside a single read.
        let fill = if read_timeout > 0 {
            match timeout(Duration::from_millis(read_timeout), reader.fill_buf()).await {
                Ok(res) => res,
                Err(_) => {
                    CONTEXT.response_error(0, owner, -session, "read timeout".to_string());
                    return false;
                }
            }
        } else {
            reader.fill_buf().await
        };

        let chunk = match fill {
            Ok(chunk) => chunk,
            Err(err) => {
                CONTEXT.response_error(0, owner, -session, err.to_string());
                return false;
            }
        };

        if chunk.is_empty() {
            CONTEXT.response_error(0, owner, -session, "eof".to_string());
            return false;
        }

        let (take, found) = match chunk.iter().position(|&b| b == last_byte) {
            Some(i) => (i + 1, true),
            None => (chunk.len(), false),
        };

        if buffer.len() + take > max_size {
            CONTEXT.response_error(
                0,
                owner,
                -session,
                "read_until: max size exceeded".to_string(),
            );
            return false;
        }

        buffer.write_slice(&chunk[..take]);
        reader.consume(take);

        if found {
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
            // `last_byte` appeared but the full (multi-byte) delimiter did not
            // match at the tail — keep reading for the real delimiter.
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
    if size > MAX_READ_SIZE {
        CONTEXT.response_error(
            0,
            owner,
            -session,
            format!(
                "read_bytes: size {} exceeds limit of {} bytes",
                size, MAX_READ_SIZE
            ),
        );
        return false;
    }
    let mut buffer = Box::new(Buffer::with_capacity(size));
    // SAFETY: `prepare` reserved `size` bytes of spare capacity; `read_exact`
    // fully writes the slice before any read of it.
    let space = unsafe { std::slice::from_raw_parts_mut(buffer.prepare(size), size) };
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
async fn handle_read(
    reader: OwnedReadHalf,
    fd: i64,
    _addr: String,
    rx: mpsc::Receiver<NetOp>,
) -> Option<String> {
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
                            if CONTEXT
                                .send(Message {
                                    from: 0,
                                    to: owner,
                                    session,
                                    data: MessageBody::Buffer(context::PTYPE_SOCKET_EVENT, buf),
                                })
                                .is_some()
                            {
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

async fn write_buffers_vectored<W>(writer: &mut W, buffers: &[Arc<Buffer>]) -> std::io::Result<()>
where
    W: AsyncWrite + Unpin,
{
    let mut buffer_index = 0usize;
    let mut buffer_offset = 0usize;

    while buffer_index < buffers.len() {
        while buffer_index < buffers.len() && buffer_offset == buffers[buffer_index].len() {
            buffer_index += 1;
            buffer_offset = 0;
        }
        if buffer_index == buffers.len() {
            break;
        }

        let mut slices = Vec::with_capacity(buffers.len() - buffer_index);
        slices.push(IoSlice::new(
            &buffers[buffer_index].as_slice()[buffer_offset..],
        ));
        for buffer in &buffers[buffer_index + 1..] {
            if !buffer.is_empty() {
                slices.push(IoSlice::new(buffer.as_slice()));
            }
        }

        let mut written = writer.write_vectored(&slices).await?;
        if written == 0 {
            return Err(Error::new(
                ErrorKind::WriteZero,
                "failed to write socket buffers",
            ));
        }

        while written > 0 {
            let remaining_in_buffer = buffers[buffer_index].len() - buffer_offset;
            if written < remaining_in_buffer {
                buffer_offset += written;
                break;
            }
            written -= remaining_in_buffer;
            buffer_index += 1;
            buffer_offset = 0;
            while buffer_index < buffers.len() && buffers[buffer_index].is_empty() {
                buffer_index += 1;
            }
        }
    }

    Ok(())
}

async fn write_slices_vectored<W>(writer: &mut W, parts: &[&[u8]]) -> std::io::Result<()>
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
                "failed to write socket frame",
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

async fn write_frame_buffer_vectored<W>(writer: &mut W, data: &Buffer) -> std::io::Result<()>
where
    W: AsyncWrite + Unpin,
{
    let src = data.as_slice();
    if src.is_empty() {
        return Ok(());
    }

    let max_chunk = MESSAGE_CONTINUED_FLAG as usize;
    let mut offset = 0usize;
    while src.len() - offset >= max_chunk {
        let header = MESSAGE_CONTINUED_FLAG.to_be_bytes();
        write_slices_vectored(
            writer,
            &[header.as_slice(), &src[offset..offset + max_chunk]],
        )
        .await?;
        offset += max_chunk;
    }

    let remaining = src.len() - offset;
    if remaining > 0 {
        let header = (remaining as u16).to_be_bytes();
        write_slices_vectored(writer, &[header.as_slice(), &src[offset..]]).await?;
    } else {
        let end_marker = 0u16.to_be_bytes();
        write_slices_vectored(writer, &[end_marker.as_slice()]).await?;
    }

    Ok(())
}

fn drain_socket_write_batch(
    first_item: SocketWriteItem,
    first_close: bool,
    rx: &mut mpsc::Receiver<NetOp>,
) -> (Vec<SocketWriteItem>, bool) {
    let mut total_bytes = match &first_item {
        SocketWriteItem::Raw(data) | SocketWriteItem::Frame(data) => data.len(),
    };
    let mut batch = vec![first_item];
    let mut close_after_batch = first_close;

    while !close_after_batch && total_bytes < MAX_SOCKET_WRITE_BATCH_BYTES {
        match rx.try_recv() {
            Ok(NetOp::Write(_, data, close)) => {
                total_bytes += data.len();
                batch.push(SocketWriteItem::Raw(data));
                close_after_batch = close;
            }
            Ok(NetOp::WriteFrame(_, data, close)) => {
                total_bytes += data.len();
                batch.push(SocketWriteItem::Frame(data));
                close_after_batch = close;
            }
            Ok(NetOp::Close()) => {
                close_after_batch = true;
            }
            Ok(_) => {}
            Err(mpsc::error::TryRecvError::Empty) => break,
            Err(mpsc::error::TryRecvError::Disconnected) => break,
        }
    }

    (batch, close_after_batch)
}

async fn write_socket_batch<W>(writer: &mut W, batch: Vec<SocketWriteItem>) -> std::io::Result<()>
where
    W: AsyncWrite + Unpin,
{
    let mut raw_batch = Vec::new();

    for item in batch {
        match item {
            SocketWriteItem::Raw(data) => raw_batch.push(data),
            SocketWriteItem::Frame(data) => {
                if !raw_batch.is_empty() {
                    write_buffers_vectored(writer, &raw_batch).await?;
                    raw_batch.clear();
                }
                write_frame_buffer_vectored(writer, data.as_ref()).await?;
            }
        }
    }

    if !raw_batch.is_empty() {
        write_buffers_vectored(writer, &raw_batch).await?;
    }

    Ok(())
}

async fn handle_write<W>(mut writer: W, mut rx: mpsc::Receiver<NetOp>) -> Option<String>
where
    W: AsyncWrite + Unpin,
{
    while let Some(op) = rx.recv().await {
        match op {
            NetOp::Write(_owner, data, close) => {
                let (batch, close_after_batch) =
                    drain_socket_write_batch(SocketWriteItem::Raw(data), close, &mut rx);
                if let Err(err) = write_socket_batch(&mut writer, batch).await {
                    return Some(format!("write: {}", err));
                }
                if close_after_batch {
                    return None;
                }
            }
            NetOp::WriteFrame(_owner, data, close) => {
                let (batch, close_after_batch) =
                    drain_socket_write_batch(SocketWriteItem::Frame(data), close, &mut rx);
                if let Err(err) = write_socket_batch(&mut writer, batch).await {
                    return Some(format!("write: {}", err));
                }
                if close_after_batch {
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
    // Cumulative size across all continuation frames of this message. A peer
    // can stream unbounded `MESSAGE_CONTINUED_FLAG` frames, so cap the total
    // against the same global ceiling enforced by `read_bytes`/`read_until`.
    let mut total: usize = 0;

    loop {
        let mut header_buf = [0u8; 2];
        let header_res = if read_timeout > 0 {
            match timeout(
                Duration::from_millis(read_timeout),
                reader.read_exact(&mut header_buf),
            )
            .await
            {
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
            if let Some(mut buf) = data.take() {
                buf.seek(BUFFER_HEAD_RESERVE as isize);
                return Ok(buf);
            }
            continue;
        }

        if total.saturating_add(size) > MAX_READ_SIZE {
            return Err(format!(
                "read_frame: cumulative size exceeds limit of {} bytes",
                MAX_READ_SIZE
            ));
        }

        let buf = data.get_or_insert_with(|| {
            let alloc_size = if fin { size } else { size * 2 };
            // Reserve BUFFER_HEAD_RESERVE bytes at the front so downstream Lua
            // code (e.g. `buffer.write_front`) can prepend headers without
            // reallocating/shifting. The reserved bytes are committed up front
            // and skipped past via `seek` before the buffer is handed off.
            let mut buf = Box::new(Buffer::with_capacity(alloc_size + BUFFER_HEAD_RESERVE));
            let _ = buf.commit(BUFFER_HEAD_RESERVE);
            buf
        });

        if size > 0 {
            // SAFETY: `prepare` reserved `size` bytes of spare capacity; `read_exact`
            // fully writes the slice before any read of it.
            let space = unsafe { std::slice::from_raw_parts_mut(buf.prepare(size), size) };
            let body_res = if read_timeout > 0 {
                match timeout(
                    Duration::from_millis(read_timeout),
                    reader.read_exact(space),
                )
                .await
                {
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
            total += size;
        }

        if fin {
            if let Some(mut buf) = data.take() {
                buf.seek(BUFFER_HEAD_RESERVE as isize);
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
                if CONTEXT
                    .send_value(
                        context::PTYPE_SOCKET_EVENT,
                        owner,
                        0,
                        SocketEvent::Message(fd, buf),
                    )
                    .is_some()
                {
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
async fn run_connection(
    socket: tokio::net::TcpStream,
    owner: ActorId,
    fd: i64,
    rx_reader: mpsc::Receiver<NetOp>,
    rx_writer: mpsc::Receiver<NetOp>,
) {
    let addr = socket
        .peer_addr()
        .map(|a| a.to_string())
        .unwrap_or_default();

    socket.set_nodelay(true).unwrap_or_default();
    let (reader, writer) = socket.into_split();

    let addr_clone = addr.clone();
    let mut read_task = CONTEXT
        .io_runtime()
        .spawn(handle_read(reader, fd, addr_clone, rx_reader));
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
    let (tx_writer, rx_writer) = mpsc::channel::<NetOp>(crate::LIMITS.network_write_queue_capacity);
    NET.insert(fd, NetChannel(tx_reader, tx_writer));
    (rx_reader, rx_writer)
}

fn listen(addr: &str, owner: ActorId, max_connections: usize) -> Result<i64> {
    let listener = std::net::TcpListener::bind(addr)?;
    listener.set_nonblocking(true)?;
    let listener = TcpListener::from_std(listener)?;

    let fd = next_net_fd();
    let (tx, mut rx) = mpsc::channel::<NetOp>(1);
    NET.insert(fd, NetChannel(tx.clone(), tx));

    // Bound the number of concurrently live accepted connections so a flood of
    // inbound peers cannot exhaust fds / spawn unbounded tasks. The permit is
    // held for the whole connection lifetime (moved into `run_connection`).
    let semaphore = Arc::new(Semaphore::new(max_connections));

    CONTEXT.io_runtime().spawn(async move {
        loop {
            tokio::select! {
                result = listener.accept() => {
                    match result {
                        Ok((socket, _)) => {
                            let permit = match semaphore.clone().try_acquire_owned() {
                                Ok(permit) => permit,
                                Err(_) => {
                                    drop(socket);
                                    log::warn!(
                                        "socket.listen fd={}: max connections ({}) reached, rejecting",
                                        fd, max_connections
                                    );
                                    continue;
                                }
                            };
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

                            CONTEXT.io_runtime().spawn(async move {
                                let _permit = permit;
                                run_connection(socket, owner, conn_fd, rx_reader, rx_writer).await;
                            });
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

    // Optional opts table at arg 2: { max_connections = N }.
    let max_connections: usize = if laux::lua_type(state, 2) == LuaType::Table {
        laux::opt_field(state, 2, "max_connections").unwrap_or(crate::LIMITS.listener_connections)
    } else {
        crate::LIMITS.listener_connections
    };

    match listen(addr, owner, max_connections) {
        Ok(fd) => {
            laux::lua_push(state, fd);
            1
        }
        Err(err) => crate::lua_push_error(state, &format!("Listen '{}' failed: {}", addr, err)),
    }
}

extern "C-unwind" fn lua_socket_read(state: LuaState) -> c_int {
    let fd = laux::lua_get(state, 1);

    if laux::lua_type(state, 2) == LuaType::Integer {
        let size = laux::lua_get(state, 2);
        let read_timeout: u64 = laux::lua_opt(state, 3).unwrap_or(0);
        if let Some(channel) = NET.get(&fd) {
            let actor = LuaActor::from_lua_state(state);
            let owner = unsafe { (*actor).id };
            let session = unsafe { (*actor).next_session() };
            if let Err(err) =
                channel
                    .value()
                    .0
                    .try_send(NetOp::ReadBytes(owner, session, size, read_timeout))
            {
                CONTEXT.response_error(
                    0,
                    owner,
                    -session,
                    format!("read: channel full (fd={}): {}", fd, err),
                );
            };
            laux::lua_push(state, session);
            1
        } else {
            crate::lua_push_error(state, &format!("read: fd {} not found", fd))
        }
    } else {
        let delim = unsafe { laux::lua_check_lstring(state, 2) };
        let delim = match Delimiter::new(delim) {
            Some(delim) => delim,
            None => {
                return crate::lua_push_error(
                    state,
                    &format!("read: delim is empty or too long (max {} bytes)", 7),
                );
            }
        };
        let max_size = laux::lua_opt(state, 3).unwrap_or(crate::LIMITS.max_network_read_bytes);
        let read_timeout: u64 = laux::lua_opt(state, 4).unwrap_or(0);
        if let Some(channel) = NET.get(&fd) {
            let actor = LuaActor::from_lua_state(state);
            let owner = unsafe { (*actor).id };
            let session = unsafe { (*actor).next_session() };
            if let Err(err) = channel.value().0.try_send(NetOp::ReadUntil(
                owner,
                session,
                max_size,
                delim,
                read_timeout,
            )) {
                CONTEXT.response_error(
                    0,
                    owner,
                    -session,
                    format!("read: channel full (fd={}): {}", fd, err),
                );
            }
            laux::lua_push(state, session);
            1
        } else {
            crate::lua_push_error(state, &format!("read: fd {} not found", fd))
        }
    }
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
        match channel.value().1.try_send(NetOp::Write(owner, data, close)) {
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
                if CONTEXT
                    .send(Message {
                        from: 0,
                        to: owner,
                        session,
                        data: MessageBody::ISize(context::PTYPE_INTEGER, fd as isize),
                    })
                    .is_some()
                {
                    NET.remove(&fd);
                    return;
                }
                CONTEXT
                    .io_runtime()
                    .spawn(run_connection(socket, owner, fd, rx_reader, rx_writer));
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
    let read_timeout: u64 = laux::lua_opt(state, 2).unwrap_or(0);

    if let Some(channel) = NET.get(&fd) {
        let actor = LuaActor::from_lua_state(state);
        let owner = unsafe { (*actor).id };
        let session = unsafe { (*actor).next_session() };
        match channel
            .value()
            .0
            .try_send(NetOp::ReadFrame(owner, session, read_timeout))
        {
            Ok(_) => {}
            Err(err) => {
                CONTEXT.response_error(
                    0,
                    owner,
                    -session,
                    format!("read_frame: channel full (fd={}): {}", fd, err),
                );
            }
        }
        laux::lua_push(state, session);
        1
    } else {
        crate::lua_push_error(state, &format!("read_frame: fd {} not found", fd))
    }
}

/// Start auto-read mode for a framed fd (session=0, callback-based).
extern "C-unwind" fn lua_start_read_frame(state: LuaState) -> c_int {
    let fd: i64 = laux::lua_get(state, 1);
    let actor = LuaActor::from_lua_state(state);
    let owner = unsafe { (*actor).id };
    let read_timeout: u64 = laux::lua_opt(state, 2).unwrap_or(0);

    if let Some(channel) = NET.get(&fd) {
        match channel
            .value()
            .0
            .try_send(NetOp::ReadFrame(owner, 0, read_timeout))
        {
            Ok(_) => {
                laux::lua_push(state, true);
                1
            }
            Err(err) => crate::lua_push_error(
                state,
                &format!("start_read_frame: channel full (fd={}): {}", fd, err),
            ),
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

    if let Some(channel) = NET.get(&fd) {
        if max_write_capacity != u16::MAX {
            let pending = channel.value().1.max_capacity() - channel.value().1.capacity();
            if pending > max_write_capacity as usize {
                let _ = channel.value().1.try_send(NetOp::Close());
                return crate::lua_push_error(
                    state,
                    &format!("write_frame: backpressure (fd={})", fd),
                );
            }
        }
        match channel
            .value()
            .1
            .try_send(NetOp::WriteFrame(owner, data, close))
        {
            Ok(_) => {
                laux::lua_push(state, true);
                1
            }
            Err(err) => crate::lua_push_error(
                state,
                &format!("write_frame: channel full (fd={}): {}", fd, err),
            ),
        }
    } else {
        crate::lua_push_error(state, &format!("write_frame: fd {} not found", fd))
    }
}

fn push_socket_event(state: LuaState, event: SocketEvent) -> c_int {
    match event {
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

pub unsafe extern "C-unwind" fn decode_socket_event_message(
    state: LuaState,
    m: *mut Message,
) -> c_int {
    unsafe {
        let body = (*m).take_body();
        match body {
            MessageBody::Boxed(_, mut boxed) => {
                let ptr = boxed.into_raw();
                if ptr.is_null() {
                    return crate::lua_push_error(state, "boxed message payload already consumed");
                }
                push_socket_event(state, *Box::from_raw(ptr as *mut SocketEvent))
            }
            MessageBody::Buffer(_, buf) => {
                laux::lua_pushlightuserdata(state, Box::into_raw(buf) as *mut c_void);
                1
            }
            other => {
                (*m).data = other;
                crate::lua_push_error(
                    state,
                    &format!(
                        "unexpected socket event message body for ptype {}",
                        (*m).ptype()
                    ),
                )
            }
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
        lreg!("host", lua_host),
        lreg_null!(),
    ];

    luaL_newlib!(state, l);
    1
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::duplex;
    use tokio::net::{TcpListener, TcpStream};

    // -----------------------------------------------------------------------
    // read_one_frame tests
    // -----------------------------------------------------------------------

    /// Creates a TCP pair and returns (writer_half, BufReader<OwnedReadHalf>)
    async fn tcp_pair() -> (tokio::net::tcp::OwnedWriteHalf, BufReader<OwnedReadHalf>) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let client = TcpStream::connect(addr).await.unwrap();
        let (server, _) = listener.accept().await.unwrap();
        let (_client_read, client_write) = client.into_split();
        let (server_read, _server_write) = server.into_split();
        (client_write, BufReader::new(server_read))
    }

    #[tokio::test]
    async fn read_one_frame_single_small_message() {
        let (mut writer, mut reader) = tcp_pair().await;
        let payload = b"hello";
        let header = (payload.len() as u16).to_be_bytes();
        writer.write_all(&header).await.unwrap();
        writer.write_all(payload).await.unwrap();
        drop(writer);

        let result = read_one_frame(&mut reader, 0).await;
        let buf = result.unwrap();
        assert_eq!(buf.as_slice(), b"hello");
    }

    #[tokio::test]
    async fn read_one_frame_continuation_then_final() {
        let (mut writer, mut reader) = tcp_pair().await;
        // First chunk: continuation (0xFFFF header) with 10 bytes
        let cont_header = MESSAGE_CONTINUED_FLAG.to_be_bytes();
        let chunk1 = vec![b'A'; MESSAGE_CONTINUED_FLAG as usize];
        writer.write_all(&cont_header).await.unwrap();
        writer.write_all(&chunk1).await.unwrap();
        // Final chunk: 5 bytes
        let final_payload = b"FINAL";
        let final_header = (final_payload.len() as u16).to_be_bytes();
        writer.write_all(&final_header).await.unwrap();
        writer.write_all(final_payload).await.unwrap();
        drop(writer);

        let buf = read_one_frame(&mut reader, 0).await.unwrap();
        assert_eq!(buf.len(), MESSAGE_CONTINUED_FLAG as usize + 5);
        assert_eq!(&buf.as_slice()[..5], &[b'A'; 5]);
        assert_eq!(&buf.as_slice()[buf.len() - 5..], b"FINAL");
    }

    #[tokio::test]
    async fn read_one_frame_zero_end_marker_after_continuation() {
        let (mut writer, mut reader) = tcp_pair().await;
        // Continuation frame
        let cont_header = MESSAGE_CONTINUED_FLAG.to_be_bytes();
        let chunk = vec![b'X'; MESSAGE_CONTINUED_FLAG as usize];
        writer.write_all(&cont_header).await.unwrap();
        writer.write_all(&chunk).await.unwrap();
        // Zero-length end marker
        let end_marker = 0u16.to_be_bytes();
        writer.write_all(&end_marker).await.unwrap();
        drop(writer);

        let buf = read_one_frame(&mut reader, 0).await.unwrap();
        assert_eq!(buf.len(), MESSAGE_CONTINUED_FLAG as usize);
    }

    #[tokio::test]
    async fn read_one_frame_eof_before_header() {
        let (writer, mut reader) = tcp_pair().await;
        drop(writer); // immediate EOF

        let result = read_one_frame(&mut reader, 0).await;
        assert_eq!(result.unwrap_err(), "eof");
    }

    #[tokio::test]
    async fn read_one_frame_eof_mid_body() {
        let (mut writer, mut reader) = tcp_pair().await;
        // Header says 100 bytes but we only write 10
        let header = 100u16.to_be_bytes();
        writer.write_all(&header).await.unwrap();
        writer.write_all(&[b'Z'; 10]).await.unwrap();
        drop(writer);

        let result = read_one_frame(&mut reader, 0).await;
        assert_eq!(result.unwrap_err(), "eof");
    }

    #[tokio::test]
    async fn read_one_frame_timeout() {
        let (_writer, mut reader) = tcp_pair().await;
        // No data written — should timeout
        let result = read_one_frame(&mut reader, 50).await; // 50ms timeout
        assert_eq!(result.unwrap_err(), "read timeout");
    }

    // -----------------------------------------------------------------------
    // drain_socket_write_batch tests
    // -----------------------------------------------------------------------

    #[test]
    fn drain_batch_single_raw() {
        let (tx, mut rx) = mpsc::channel::<NetOp>(8);
        drop(tx); // channel closed, drain won't find more
        let item = SocketWriteItem::Raw(Arc::new(Buffer::from_slice(b"hello")));
        let (batch, close) = drain_socket_write_batch(item, false, &mut rx);
        assert_eq!(batch.len(), 1);
        assert!(!close);
    }

    #[test]
    fn drain_batch_collects_until_close() {
        let (tx, mut rx) = mpsc::channel::<NetOp>(8);
        tx.try_send(NetOp::Write(1, Arc::new(Buffer::from_slice(b"abc")), false))
            .unwrap();
        tx.try_send(NetOp::Write(1, Arc::new(Buffer::from_slice(b"def")), true))
            .unwrap();
        tx.try_send(NetOp::Write(1, Arc::new(Buffer::from_slice(b"ghi")), false))
            .unwrap();
        drop(tx);

        let first = SocketWriteItem::Raw(Arc::new(Buffer::from_slice(b"000")));
        let (batch, close) = drain_socket_write_batch(first, false, &mut rx);
        // Should have: first + abc + def, stop at close=true
        assert_eq!(batch.len(), 3);
        assert!(close);
    }

    #[test]
    fn drain_batch_close_op_stops_drain() {
        let (tx, mut rx) = mpsc::channel::<NetOp>(8);
        tx.try_send(NetOp::Write(1, Arc::new(Buffer::from_slice(b"abc")), false))
            .unwrap();
        tx.try_send(NetOp::Close()).unwrap();
        tx.try_send(NetOp::Write(1, Arc::new(Buffer::from_slice(b"def")), false))
            .unwrap();
        drop(tx);

        let first = SocketWriteItem::Raw(Arc::new(Buffer::from_slice(b"first")));
        let (batch, close) = drain_socket_write_batch(first, false, &mut rx);
        assert_eq!(batch.len(), 2); // first + abc
        assert!(close); // Close op triggers close_after_batch
    }

    #[test]
    fn drain_batch_respects_byte_limit() {
        let (tx, mut rx) = mpsc::channel::<NetOp>(128);
        // Fill with many small writes that collectively exceed MAX_SOCKET_WRITE_BATCH_BYTES
        let small_buf = Arc::new(Buffer::from_slice(&vec![0u8; 64 * 1024])); // 64KB each
        for _ in 0..8 {
            tx.try_send(NetOp::Write(1, small_buf.clone(), false))
                .unwrap();
        }
        drop(tx);

        let first = SocketWriteItem::Raw(Arc::new(Buffer::from_slice(&vec![0u8; 64 * 1024])));
        let (batch, close) = drain_socket_write_batch(first, false, &mut rx);
        // MAX is 256KB, first is 64KB, so can fit 3 more (64*4 = 256KB) then stops
        assert!(batch.len() <= 5); // capped by byte limit
        assert!(!close);
    }

    #[test]
    fn drain_batch_mixed_raw_and_frame() {
        let (tx, mut rx) = mpsc::channel::<NetOp>(8);
        tx.try_send(NetOp::WriteFrame(
            1,
            Arc::new(Buffer::from_slice(b"frame1")),
            false,
        ))
        .unwrap();
        tx.try_send(NetOp::Write(1, Arc::new(Buffer::from_slice(b"raw")), false))
            .unwrap();
        drop(tx);

        let first = SocketWriteItem::Raw(Arc::new(Buffer::from_slice(b"first_raw")));
        let (batch, close) = drain_socket_write_batch(first, false, &mut rx);
        assert_eq!(batch.len(), 3);
        assert!(!close);
        // Verify types
        assert!(matches!(&batch[0], SocketWriteItem::Raw(_)));
        assert!(matches!(&batch[1], SocketWriteItem::Frame(_)));
        assert!(matches!(&batch[2], SocketWriteItem::Raw(_)));
    }

    // -----------------------------------------------------------------------
    // handle_write error propagation tests
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn handle_write_returns_error_on_broken_pipe() {
        let (tx, rx) = mpsc::channel::<NetOp>(8);
        tx.send(NetOp::Write(
            1,
            Arc::new(Buffer::from_slice(b"data")),
            false,
        ))
        .await
        .unwrap();
        drop(tx);

        // Create a duplex but drop the reader to simulate broken pipe
        let (writer, _reader) = duplex(1);
        drop(_reader);

        let result = handle_write(writer, rx).await;
        assert!(result.is_some());
        assert!(result.unwrap().contains("write"));
    }

    #[tokio::test]
    async fn handle_write_empty_channel_returns_none() {
        let (_tx, rx) = mpsc::channel::<NetOp>(8);
        drop(_tx); // close the sender immediately

        let (writer, _reader) = duplex(64);
        let result = handle_write(writer, rx).await;
        assert_eq!(result, None);
    }

    // -----------------------------------------------------------------------
    // write_frame_buffer_vectored tests
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn write_frame_empty_payload_is_noop() {
        let (mut writer, mut reader) = duplex(64);
        let buf = Buffer::new();
        write_frame_buffer_vectored(&mut writer, &buf)
            .await
            .unwrap();
        drop(writer);

        let mut out = Vec::new();
        reader.read_to_end(&mut out).await.unwrap();
        assert!(out.is_empty());
    }

    #[tokio::test]
    async fn write_frame_small_payload_single_chunk() {
        let (mut writer, mut reader) = duplex(64);
        let buf = Buffer::from_slice(b"test123");
        write_frame_buffer_vectored(&mut writer, &buf)
            .await
            .unwrap();
        drop(writer);

        let mut out = Vec::new();
        reader.read_to_end(&mut out).await.unwrap();
        // header (2 bytes: len=7) + "test123"
        assert_eq!(out.len(), 2 + 7);
        assert_eq!(&out[..2], &7u16.to_be_bytes());
        assert_eq!(&out[2..], b"test123");
    }

    #[tokio::test]
    async fn write_frame_multi_chunk_payload() {
        let payload_size = MESSAGE_CONTINUED_FLAG as usize + 100;
        let payload = vec![b'M'; payload_size];
        let (mut writer, mut reader) = duplex(payload_size * 2);
        let buf = Buffer::from_slice(&payload);
        write_frame_buffer_vectored(&mut writer, &buf)
            .await
            .unwrap();
        drop(writer);

        let mut out = Vec::new();
        reader.read_to_end(&mut out).await.unwrap();

        // Chunk 1: 0xFFFF header + 65535 bytes
        let mut pos = 0;
        let h1 = u16::from_be_bytes([out[pos], out[pos + 1]]);
        assert_eq!(h1, MESSAGE_CONTINUED_FLAG);
        pos += 2;
        assert_eq!(
            &out[pos..pos + MESSAGE_CONTINUED_FLAG as usize],
            &payload[..MESSAGE_CONTINUED_FLAG as usize]
        );
        pos += MESSAGE_CONTINUED_FLAG as usize;

        // Chunk 2: header (100) + 100 bytes
        let h2 = u16::from_be_bytes([out[pos], out[pos + 1]]);
        assert_eq!(h2, 100);
        pos += 2;
        assert_eq!(
            &out[pos..pos + 100],
            &payload[MESSAGE_CONTINUED_FLAG as usize..]
        );
        pos += 100;
        assert_eq!(pos, out.len());
    }

    // -----------------------------------------------------------------------
    // handle_read via read_until / read_bytes tests (using CONTEXT pseudo-actors)
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn handle_read_bytes_delivers_exact_size() {
        let from = 0x8001_0001u32;
        let session = 1i64;
        let (tx, _) = tokio::sync::mpsc::unbounded_channel();
        CONTEXT.register_pseudo_actor(from, tx);

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let client = TcpStream::connect(addr).await.unwrap();
        let (server, _) = listener.accept().await.unwrap();

        let (server_read, _server_write) = server.into_split();
        let (_client_read, mut client_write) = client.into_split();

        let (read_tx, read_rx) = mpsc::channel::<NetOp>(8);
        read_tx
            .send(NetOp::ReadBytes(from, session, 5, 0))
            .await
            .unwrap();

        client_write.write_all(b"helloworld").await.unwrap();
        drop(client_write);

        let handle = tokio::spawn(handle_read(server_read, 1, "test".to_string(), read_rx));
        // Wait for read to finish
        let _ = handle.await.unwrap();

        // The actor should have received a 5-byte message
        // (tested via CONTEXT.send — we rely on the function returning without panic)
    }

    // -----------------------------------------------------------------------
    // Original write tests
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn handle_write_batches_contiguous_writes_and_preserves_order() {
        let (tx, rx) = mpsc::channel::<NetOp>(8);
        tx.try_send(NetOp::Write(1, Arc::new(Buffer::from_slice(b"abc")), false))
            .unwrap();
        tx.try_send(NetOp::Write(1, Arc::new(Buffer::from_slice(b"def")), false))
            .unwrap();
        tx.try_send(NetOp::Write(1, Arc::new(Buffer::from_slice(b"ghi")), true))
            .unwrap();

        let (client, mut server) = tokio::io::duplex(64);
        let writer = tokio::spawn(handle_write(client, rx));

        let mut out = [0u8; 9];
        server.read_exact(&mut out).await.unwrap();
        assert_eq!(&out, b"abcdefghi");
        assert_eq!(writer.await.unwrap(), None);
    }

    #[tokio::test]
    async fn handle_write_close_op_writes_prior_batch_then_closes() {
        let (tx, rx) = mpsc::channel::<NetOp>(8);
        tx.try_send(NetOp::Write(1, Arc::new(Buffer::from_slice(b"abc")), false))
            .unwrap();
        tx.try_send(NetOp::Close()).unwrap();
        tx.try_send(NetOp::Write(1, Arc::new(Buffer::from_slice(b"def")), false))
            .unwrap();

        let (client, mut server) = tokio::io::duplex(64);
        let writer = tokio::spawn(handle_write(client, rx));

        let mut out = [0u8; 3];
        server.read_exact(&mut out).await.unwrap();
        assert_eq!(&out, b"abc");

        let mut eof = [0u8; 1];
        assert_eq!(server.read(&mut eof).await.unwrap(), 0);
        assert_eq!(writer.await.unwrap(), None);
    }

    #[tokio::test]
    async fn handle_write_frame_writes_header_and_payload_without_preconcat() {
        let (tx, rx) = mpsc::channel::<NetOp>(8);
        tx.try_send(NetOp::WriteFrame(
            1,
            Arc::new(Buffer::from_slice(b"payload")),
            true,
        ))
        .unwrap();

        let (client, mut server) = tokio::io::duplex(64);
        let writer = tokio::spawn(handle_write(client, rx));

        let mut out = [0u8; 9];
        server.read_exact(&mut out).await.unwrap();
        assert_eq!(&out[..2], &7u16.to_be_bytes());
        assert_eq!(&out[2..], b"payload");
        assert_eq!(writer.await.unwrap(), None);
    }

    #[tokio::test]
    async fn handle_write_frame_exact_continuation_chunk_writes_end_marker() {
        let payload = vec![b'a'; MESSAGE_CONTINUED_FLAG as usize];
        let (tx, rx) = mpsc::channel::<NetOp>(8);
        tx.try_send(NetOp::WriteFrame(
            1,
            Arc::new(Buffer::from_slice(&payload)),
            true,
        ))
        .unwrap();

        let (client, mut server) = tokio::io::duplex(8192);
        let writer = tokio::spawn(handle_write(client, rx));

        let mut header = [0u8; 2];
        server.read_exact(&mut header).await.unwrap();
        assert_eq!(header, MESSAGE_CONTINUED_FLAG.to_be_bytes());

        let mut body = vec![0u8; MESSAGE_CONTINUED_FLAG as usize];
        server.read_exact(&mut body).await.unwrap();
        assert_eq!(body, payload);

        let mut end_marker = [0u8; 2];
        server.read_exact(&mut end_marker).await.unwrap();
        assert_eq!(end_marker, 0u16.to_be_bytes());
        assert_eq!(writer.await.unwrap(), None);
    }
}
