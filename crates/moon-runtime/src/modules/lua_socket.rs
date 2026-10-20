use dashmap::DashMap;
use moon_base::{
    cstr, ffi,
    laux::{self, LuaStack, LuaState, LuaType},
    lreg, lreg_null, lreg_try, luaL_newlib,
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
    pin::Pin,
    sync::{
        Arc, LazyLock,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};
use tokio::io::AsyncReadExt;

use tokio::{
    io::{AsyncBufReadExt, AsyncWrite, BufReader, Result},
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
use crate::net_io::{read_buffer_exact, write_all_vectored};

static NET: LazyLock<DashMap<i64, NetChannel>> = LazyLock::new(DashMap::new);

/// Delimiter type used in ReadUntil: max 7 bytes, no heap allocation.
pub type Delimiter = ShortBytes<8>;

type SocketResult<T> = std::result::Result<T, String>;

#[derive(Debug)]
struct ReadRequest {
    owner: ActorId,
    session: i64,
    read_timeout: u64,
    op: ReadOp,
}

#[derive(Debug)]
enum ReadOp {
    Until(usize, Delimiter), // max_size, delim
    Bytes(usize),
    Frame,
}

#[derive(Debug)]
enum WriteOp {
    Write(SocketWriteItem, bool), // data, close
    Close,
}

struct NetChannel(mpsc::Sender<ReadRequest>, SocketWriteSender);

struct SocketWriteSender {
    tx: mpsc::UnboundedSender<WriteOp>,
    pending: Arc<AtomicUsize>,
}

struct SocketWriteReceiver {
    rx: mpsc::UnboundedReceiver<WriteOp>,
    pending: Arc<AtomicUsize>,
}

fn socket_write_channel() -> (SocketWriteSender, SocketWriteReceiver) {
    let (tx, rx) = mpsc::unbounded_channel();
    let pending = Arc::new(AtomicUsize::new(0));
    (
        SocketWriteSender {
            tx,
            pending: pending.clone(),
        },
        SocketWriteReceiver { rx, pending },
    )
}

impl SocketWriteSender {
    fn send(&self, op: WriteOp) -> std::result::Result<(), mpsc::error::SendError<WriteOp>> {
        // Count before publishing so the consumer cannot decrement first.
        self.pending.fetch_add(1, Ordering::AcqRel);
        self.tx.send(op).inspect_err(|_| {
            self.pending.fetch_sub(1, Ordering::AcqRel);
        })
    }

    fn len(&self) -> usize {
        self.pending.load(Ordering::Acquire)
    }
}

impl SocketWriteReceiver {
    async fn recv(&mut self) -> Option<WriteOp> {
        let op = self.rx.recv().await?;
        self.pending.fetch_sub(1, Ordering::AcqRel);
        Some(op)
    }

    fn try_recv(&mut self) -> std::result::Result<WriteOp, mpsc::error::TryRecvError> {
        let op = self.rx.try_recv()?;
        self.pending.fetch_sub(1, Ordering::AcqRel);
        Ok(op)
    }
}

const SOCKET_DATA_ACCEPT: u8 = 2;
const SOCKET_DATA_MESSAGE: u8 = 3;
const SOCKET_DATA_CLOSE: u8 = 4;

const MESSAGE_CONTINUED_FLAG: u16 = u16::MAX;

/// Hard upper bound on the bytes a single `socket.read` may request
/// (`read_bytes`) or accumulate (`read_until`). See `crate::LIMITS.max_network_read_bytes`.
const MAX_READ_SIZE: usize = crate::LIMITS.max_network_read_bytes;
const MAX_SOCKET_WRITE_BATCH_BYTES: usize = crate::LIMITS.socket_write_batch_bytes;
const MAX_SOCKET_WRITE_BATCH_ITEMS: usize = 256;
// Each framed chunk needs at most two iovecs: its header and its payload.
const MAX_SOCKET_WRITE_CHUNKS: usize = 32;
const MAX_SOCKET_WRITE_IOV: usize = MAX_SOCKET_WRITE_CHUNKS * 2;

#[derive(Debug)]
enum SocketWriteItem {
    Raw(Arc<Buffer>),
    Frame(Arc<Buffer>),
}

impl SocketWriteItem {
    fn len(&self) -> usize {
        match self {
            Self::Raw(data) | Self::Frame(data) => data.len(),
        }
    }
}

pub enum SocketEvent {
    Accept(i64, i64, String), // listen_fd, conn_fd, remote_addr
    Message(i64, Box<Buffer>),
    Close(i64, String, String),
}

type ReadTimer = Option<Pin<Box<tokio::time::Sleep>>>;

fn read_timer(read_timeout: u64) -> ReadTimer {
    (read_timeout > 0).then(|| Box::pin(sleep(Duration::from_millis(read_timeout))))
}

async fn read_until(
    reader: &mut BufReader<OwnedReadHalf>,
    max_size: usize,
    delim: Delimiter,
    read_timeout: u64,
) -> SocketResult<Box<Buffer>> {
    let mut with_delim = false;
    let raw = delim.as_slice();
    let delim_bytes = if raw[0] == b'^' {
        if raw.len() < 2 {
            return Err("read_until: delim is empty".to_string());
        }
        with_delim = true;
        &raw[1..]
    } else {
        raw
    };

    let mut buffer = Box::new(Buffer::with_capacity(std::cmp::min(max_size, 512)));
    let last_byte = *delim_bytes.last().unwrap();
    let delim_len = delim_bytes.len();
    let mut timer = read_timer(read_timeout);
    loop {
        // Read one buffered chunk at a time (bounded by the BufReader's internal
        // buffer) up to and including the next `last_byte`, enforcing `max_size`
        // incrementally. Using `fill_buf`/`consume` instead of `read_until`
        // prevents a peer that withholds the delimiter from growing `buffer`
        // without bound inside a single read.
        let fill = if let Some(timer) = &mut timer {
            timer
                .as_mut()
                .reset(tokio::time::Instant::now() + Duration::from_millis(read_timeout));
            tokio::select! {
                result = reader.fill_buf() => result,
                _ = timer.as_mut() => Err(Error::new(ErrorKind::TimedOut, "read timeout")),
            }
        } else {
            reader.fill_buf().await
        };

        let chunk = fill.map_err(|err| err.to_string())?;

        if chunk.is_empty() {
            return Err("eof".to_string());
        }

        let (take, found) = match chunk.iter().position(|&b| b == last_byte) {
            Some(i) => (i + 1, true),
            None => (chunk.len(), false),
        };

        if buffer.len() + take > max_size {
            return Err("read_until: max size exceeded".to_string());
        }

        buffer.write_slice(&chunk[..take]);
        reader.consume(take);

        if found {
            let buf_len = buffer.len();
            if buf_len >= delim_len && buffer.as_vec()[buf_len - delim_len..] == *delim_bytes {
                if !with_delim {
                    buffer.revert(delim_len);
                }
                return Ok(buffer);
            }
            // `last_byte` appeared but the full (multi-byte) delimiter did not
            // match at the tail — keep reading for the real delimiter.
        }
    }
}

async fn read_bytes(
    reader: &mut BufReader<OwnedReadHalf>,
    size: usize,
    read_timeout: u64,
) -> SocketResult<Box<Buffer>> {
    if size == 0 {
        return Err("read_bytes: size must be greater than 0".to_string());
    }
    if size > MAX_READ_SIZE {
        return Err(format!(
            "read_bytes: size {} exceeds limit of {} bytes",
            size, MAX_READ_SIZE
        ));
    }
    let mut buffer = Box::new(Buffer::with_capacity(size));
    let read_res = if read_timeout > 0 {
        timeout(
            Duration::from_millis(read_timeout),
            read_buffer_exact(reader, &mut buffer, size),
        )
        .await
        .map_err(|_| "read timeout".to_string())?
    } else {
        read_buffer_exact(reader, &mut buffer, size).await
    };
    read_res.map_err(|err| err.to_string())?;
    Ok(buffer)
}

/// Respond to both in-flight and queued reads when the read task is cancelled.
struct ReadOpGuard {
    receiver: mpsc::Receiver<ReadRequest>,
    in_flight: Option<(ActorId, i64)>,
}

impl Drop for ReadOpGuard {
    fn drop(&mut self) {
        self.receiver.close();
        if let Some((owner, session)) = self.in_flight.take() {
            CONTEXT.response_error(0, owner, -session, "closed".to_string());
        }
        while let Ok(ReadRequest { owner, session, .. }) = self.receiver.try_recv() {
            if session > 0 {
                CONTEXT.response_error(0, owner, -session, "closed".to_string());
            }
        }
    }
}

/// Read requests share one response path. Framed errors also become the close
/// reason; raw read errors are reported through their session only.
async fn handle_read(
    reader: OwnedReadHalf,
    fd: i64,
    rx: mpsc::Receiver<ReadRequest>,
) -> Option<String> {
    let mut rx = ReadOpGuard {
        receiver: rx,
        in_flight: None,
    };
    let mut reader = BufReader::new(reader);
    while let Some(ReadRequest {
        owner,
        session,
        read_timeout,
        op,
    }) = rx.receiver.recv().await
    {
        // Automatic framed reads have no caller waiting for a response.
        rx.in_flight = (session > 0).then_some((owner, session));
        let (ptype, result) = match op {
            ReadOp::Until(max_size, delim) => (
                context::PTYPE_SOCKET_TCP,
                read_until(&mut reader, max_size, delim, read_timeout).await,
            ),
            ReadOp::Bytes(size) => (
                context::PTYPE_SOCKET_TCP,
                read_bytes(&mut reader, size, read_timeout).await,
            ),
            ReadOp::Frame if session > 0 => (
                context::PTYPE_SOCKET_EVENT,
                read_one_frame(&mut reader, read_timeout).await,
            ),
            ReadOp::Frame => {
                return frame_read_loop(&mut reader, owner, fd, read_timeout).await;
            }
        };
        // Completion and reply are synchronous: cancellation cannot interrupt
        // between clearing the guard and delivering the result.
        rx.in_flight = None;
        match result {
            Ok(buffer) => {
                if CONTEXT
                    .send(Message {
                        from: 0,
                        to: owner,
                        session,
                        data: MessageBody::Buffer(ptype, buffer),
                    })
                    .is_some()
                {
                    return None;
                }
            }
            Err(err) => {
                CONTEXT.response_error(0, owner, -session, err.clone());
                // Raw reads historically report their error through the session
                // only; framed reads also include it in the close event.
                return (ptype == context::PTYPE_SOCKET_EVENT).then_some(err);
            }
        }
    }
    None
}

#[derive(Clone, Copy)]
struct SocketWriteChunk<'a> {
    header: Option<[u8; 2]>,
    data: &'a [u8],
}

fn socket_write_chunks<'a>(
    batch: &'a [SocketWriteItem],
) -> impl Iterator<Item = SocketWriteChunk<'a>> + Send + 'a {
    let max_chunk = MESSAGE_CONTINUED_FLAG as usize;
    batch.iter().flat_map(move |item| {
        let (data, framed): (&'a [u8], bool) = match item {
            SocketWriteItem::Raw(data) => (data.as_slice(), false),
            SocketWriteItem::Frame(data) => (data.as_slice(), true),
        };
        let chunk_size = if framed { max_chunk } else { data.len().max(1) };
        let end_marker = (framed && !data.is_empty() && data.len() % max_chunk == 0).then_some(
            SocketWriteChunk {
                header: Some([0; 2]),
                data: &[],
            },
        );
        data.chunks(chunk_size)
            .map(move |data: &'a [u8]| SocketWriteChunk {
                header: framed.then(|| (data.len() as u16).to_be_bytes()),
                data,
            })
            .chain(end_marker)
    })
}

async fn write_socket_batch<W>(writer: &mut W, batch: &[SocketWriteItem]) -> std::io::Result<()>
where
    W: AsyncWrite + Unpin,
{
    let mut chunks = socket_write_chunks(batch);
    let mut window = [SocketWriteChunk {
        header: None,
        data: &[],
    }; MAX_SOCKET_WRITE_CHUNKS];
    loop {
        let mut count = 0;
        for slot in &mut window {
            let Some(chunk) = chunks.next() else { break };
            *slot = chunk;
            count += 1;
        }
        if count == 0 {
            return Ok(());
        }

        // Stack storage bounds each write window and is reused after short writes.
        let mut slices = [IoSlice::new(&[]); MAX_SOCKET_WRITE_IOV];
        let mut slice_count = 0;
        for chunk in &window[..count] {
            if let Some(header) = &chunk.header {
                slices[slice_count] = IoSlice::new(header);
                slice_count += 1;
            }
            if !chunk.data.is_empty() {
                slices[slice_count] = IoSlice::new(chunk.data);
                slice_count += 1;
            }
        }
        let write_zero_message = if window[0].header.is_some() {
            "failed to write socket frame"
        } else {
            "failed to write socket buffers"
        };
        write_all_vectored(writer, &mut slices[..slice_count], write_zero_message).await?;
    }
}

fn drain_socket_write_batch(
    first_item: SocketWriteItem,
    first_close: bool,
    rx: &mut SocketWriteReceiver,
    batch: &mut Vec<SocketWriteItem>,
) -> bool {
    let mut total_bytes = first_item.len();
    batch.clear();
    batch.push(first_item);
    let mut close_after_batch = first_close;

    while !close_after_batch
        && total_bytes < MAX_SOCKET_WRITE_BATCH_BYTES
        && batch.len() < MAX_SOCKET_WRITE_BATCH_ITEMS
    {
        match rx.try_recv() {
            Ok(WriteOp::Write(item, close)) => {
                total_bytes += item.len();
                batch.push(item);
                close_after_batch = close;
            }
            Ok(WriteOp::Close) => {
                close_after_batch = true;
            }
            Err(mpsc::error::TryRecvError::Empty) => break,
            Err(mpsc::error::TryRecvError::Disconnected) => break,
        }
    }

    close_after_batch
}

async fn handle_write<W>(mut writer: W, mut rx: SocketWriteReceiver) -> Option<String>
where
    W: AsyncWrite + Unpin,
{
    let mut batch = Vec::new();
    while let Some(op) = rx.recv().await {
        let (item, close) = match op {
            WriteOp::Write(item, close) => (item, close),
            WriteOp::Close => return None,
        };
        let close_after_batch = drain_socket_write_batch(item, close, &mut rx, &mut batch);
        let result = write_socket_batch(&mut writer, &batch).await;
        // Release payloads before waiting for the next operation; retain capacity.
        batch.clear();
        if let Err(err) = result {
            return Some(format!("write: {}", err));
        }
        if close_after_batch {
            return None;
        }
    }
    None
}

/// Read one complete framed message from the reader.
/// Returns the message buffer on success, or an error string on failure.
async fn read_one_frame(
    reader: &mut BufReader<OwnedReadHalf>,
    read_timeout: u64,
) -> SocketResult<Box<Buffer>> {
    let mut timer = read_timer(read_timeout);
    read_one_frame_with_timer(reader, read_timeout, &mut timer).await
}

async fn read_frame_part<T>(
    read: impl std::future::Future<Output = std::io::Result<T>>,
    read_timeout: u64,
    timer: &mut ReadTimer,
) -> SocketResult<()> {
    let result = if let Some(timer) = timer {
        timer
            .as_mut()
            .reset(tokio::time::Instant::now() + Duration::from_millis(read_timeout));
        tokio::select! {
            result = read => result,
            _ = timer.as_mut() => Err(Error::new(ErrorKind::TimedOut, "read timeout")),
        }
    } else {
        read.await
    };
    result.map(|_| ()).map_err(|err| {
        if err.kind() == ErrorKind::UnexpectedEof {
            "eof".to_string()
        } else {
            err.to_string()
        }
    })
}

async fn read_one_frame_with_timer(
    reader: &mut BufReader<OwnedReadHalf>,
    read_timeout: u64,
    timer: &mut ReadTimer,
) -> SocketResult<Box<Buffer>> {
    let mut data: Option<Box<Buffer>> = None;
    // Cumulative size across all continuation frames of this message. A peer
    // can stream unbounded `MESSAGE_CONTINUED_FLAG` frames, so cap the total
    // against the same global ceiling enforced by `read_bytes`/`read_until`.
    let mut total: usize = 0;

    loop {
        let mut header_buf = [0u8; 2];
        read_frame_part(reader.read_exact(&mut header_buf), read_timeout, timer).await?;

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
            // reallocating/shifting. The reserved bytes are initialized up front
            // and skipped past via `seek` before the buffer is handed off.
            let mut buf = Box::new(Buffer::with_capacity(alloc_size + BUFFER_HEAD_RESERVE));
            buf.write_slice(&[0; BUFFER_HEAD_RESERVE]);
            buf
        });

        if size > 0 {
            read_frame_part(read_buffer_exact(reader, buf, size), read_timeout, timer).await?;
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
    let mut timer = read_timer(read_timeout);
    loop {
        match read_one_frame_with_timer(reader, read_timeout, &mut timer).await {
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
    rx_reader: mpsc::Receiver<ReadRequest>,
    rx_writer: SocketWriteReceiver,
) {
    let addr = socket
        .peer_addr()
        .map(|a| a.to_string())
        .unwrap_or_default();

    socket.set_nodelay(true).unwrap_or_default();
    let (reader, writer) = socket.into_split();

    let mut read_task = CONTEXT
        .io_runtime()
        .spawn(handle_read(reader, fd, rx_reader));
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

fn setup_net_channel(fd: i64) -> (mpsc::Receiver<ReadRequest>, SocketWriteReceiver) {
    let (tx_reader, rx_reader) = mpsc::channel::<ReadRequest>(1);
    let (tx_writer, rx_writer) = socket_write_channel();
    NET.insert(fd, NetChannel(tx_reader, tx_writer));
    (rx_reader, rx_writer)
}

fn listen(addr: &str, owner: ActorId, max_connections: usize) -> Result<i64> {
    let listener = std::net::TcpListener::bind(addr)?;
    listener.set_nonblocking(true)?;
    let listener = TcpListener::from_std(listener)?;

    let fd = next_net_fd();
    let (rx_reader, mut rx) = setup_net_channel(fd);

    // Bound the number of concurrently live accepted connections so a flood of
    // inbound peers cannot exhaust fds / spawn unbounded tasks. The permit is
    // held for the whole connection lifetime (moved into `run_connection`).
    let semaphore = Arc::new(Semaphore::new(max_connections));

    CONTEXT.io_runtime().spawn(async move {
        let _rx_reader = rx_reader;
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
                        Some(WriteOp::Close) | None => break,
                        _ => {}
                    }
                }
            }
        }
        NET.remove(&fd);
    });

    Ok(fd)
}

fn lua_socket_listen(lua: &mut LuaStack<'_>) -> SocketResult<c_int> {
    let _guard = CONTEXT.io_runtime().enter();
    let state = lua.state();

    let actor = LuaActor::from_lua_state(state);
    let owner = unsafe { (*actor).id };

    // Optional opts table at arg 2: { max_connections = N }.
    let max_connections: usize = if laux::lua_type(state, 2) == LuaType::Table {
        lua.opt_field(2, "max_connections")
            .unwrap_or(crate::LIMITS.listener_connections)
    } else {
        crate::LIMITS.listener_connections
    };

    let result = {
        let addr = lua
            .value(1)
            .as_str()
            .ok_or_else(|| "bad argument #1 (valid UTF-8 string expected)".to_string())?;
        listen(addr, owner, max_connections)
            .map_err(|err| format!("Listen '{}' failed: {}", addr, err))
    };
    match result {
        Ok(fd) => {
            laux::lua_push(state, fd);
            Ok(1)
        }
        Err(err) => Ok(crate::lua_push_error_tuple(state, &err)),
    }
}

fn submit_read(
    lua: &mut LuaStack<'_>,
    fd: i64,
    name: &str,
    op: ReadOp,
    read_timeout: u64,
) -> SocketResult<c_int> {
    let state = lua.state();
    let Some(channel) = NET.get(&fd) else {
        return Ok(crate::lua_push_error_tuple(
            state,
            &format!("{}: fd {} not found", name, fd),
        ));
    };
    let actor = LuaActor::from_lua_state(state);
    let owner = unsafe { (*actor).id };
    let session = unsafe { (*actor).next_session() };
    if let Err(err) = channel.0.try_send(ReadRequest {
        owner,
        session,
        read_timeout,
        op,
    }) {
        CONTEXT.response_error(
            0,
            owner,
            -session,
            format!("{}: channel full (fd={}): {}", name, fd, err),
        );
    }
    laux::lua_push(state, session);
    Ok(1)
}

fn lua_socket_read(lua: &mut LuaStack<'_>) -> SocketResult<c_int> {
    let state = lua.state();
    let fd = lua.get(1)?;

    if laux::lua_type(state, 2) == LuaType::Integer {
        let size = lua.get(2)?;
        let read_timeout: u64 = lua.opt(3).unwrap_or(0);
        submit_read(lua, fd, "read", ReadOp::Bytes(size), read_timeout)
    } else {
        let delim = lua
            .value(2)
            .as_bytes()
            .ok_or_else(|| "bad argument #2 (string expected)".to_string())?;
        let Some(delim) = Delimiter::new(delim) else {
            return Ok(crate::lua_push_error_tuple(
                state,
                "read: delim is empty or too long (max 7 bytes)",
            ));
        };
        let max_size = lua.opt(3).unwrap_or(MAX_READ_SIZE).min(MAX_READ_SIZE);
        let read_timeout: u64 = lua.opt(4).unwrap_or(0);
        submit_read(
            lua,
            fd,
            "read",
            ReadOp::Until(max_size, delim),
            read_timeout,
        )
    }
}

fn enqueue_socket_write(
    fd: i64,
    item: SocketWriteItem,
    max_write_capacity: u16,
    close: bool,
) -> SocketResult<()> {
    let name = match &item {
        SocketWriteItem::Raw(_) => "write",
        SocketWriteItem::Frame(data) if data.is_empty() => return Ok(()),
        SocketWriteItem::Frame(_) => "write_frame",
    };
    let channel = NET
        .get(&fd)
        .ok_or_else(|| format!("{}: fd {} not found", name, fd))?;
    if max_write_capacity != u16::MAX && channel.1.len() > max_write_capacity as usize {
        let _ = channel.1.send(WriteOp::Close);
        return Err(format!("{}: backpressure (fd={})", name, fd));
    }
    channel
        .1
        .send(WriteOp::Write(item, close))
        .map_err(|err| format!("{}: channel closed (fd={}): {}", name, fd, err))
}

fn socket_write(lua: &mut LuaStack<'_>, framed: bool) -> SocketResult<c_int> {
    let state = lua.state();
    let fd: i64 = lua.get(1)?;
    let data = check_arc_buffer(lua, 2)?;
    let max_write_capacity = lua.opt(3).unwrap_or(u16::MAX);
    let close = lua.opt_truthy(4).unwrap_or(false);
    let item = if framed {
        SocketWriteItem::Frame(data)
    } else {
        SocketWriteItem::Raw(data)
    };
    match enqueue_socket_write(fd, item, max_write_capacity, close) {
        Ok(()) => {
            laux::lua_push(state, true);
            Ok(1)
        }
        Err(err) => Ok(crate::lua_push_error_tuple(state, &err)),
    }
}

fn lua_socket_write(lua: &mut LuaStack<'_>) -> SocketResult<c_int> {
    socket_write(lua, false)
}

fn lua_socket_connect(lua: &mut LuaStack<'_>) -> SocketResult<c_int> {
    let state = lua.state();
    let addr = match lua.value(1).as_str() {
        Some(addr) => addr.to_owned(),
        None => return Err("bad argument #1 (valid UTF-8 string expected)".to_string()),
    };
    let connect_timeout: u64 = lua.opt(2).unwrap_or(5000);

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
    Ok(1)
}

fn lua_read_frame(lua: &mut LuaStack<'_>) -> SocketResult<c_int> {
    let fd: i64 = lua.get(1)?;
    let read_timeout: u64 = lua.opt(2).unwrap_or(0);
    submit_read(lua, fd, "read_frame", ReadOp::Frame, read_timeout)
}

/// Start auto-read mode for a framed fd (session=0, callback-based).
fn lua_start_read_frame(lua: &mut LuaStack<'_>) -> SocketResult<c_int> {
    let state = lua.state();
    let fd: i64 = lua.get(1)?;
    let actor = LuaActor::from_lua_state(state);
    let owner = unsafe { (*actor).id };
    let read_timeout: u64 = lua.opt(2).unwrap_or(0);

    if let Some(channel) = NET.get(&fd) {
        match channel.value().0.try_send(ReadRequest {
            owner,
            session: 0,
            read_timeout,
            op: ReadOp::Frame,
        }) {
            Ok(_) => {
                laux::lua_push(state, true);
                Ok(1)
            }
            Err(err) => Ok(crate::lua_push_error_tuple(
                state,
                &format!("start_read_frame: channel full (fd={}): {}", fd, err),
            )),
        }
    } else {
        Ok(crate::lua_push_error_tuple(
            state,
            &format!("start_read_frame: fd {} not found", fd),
        ))
    }
}

fn lua_socket_close(lua: &mut LuaStack<'_>) -> SocketResult<c_int> {
    let state = lua.state();
    let fd = lua.get(1)?;

    if let Some(channel) = NET.get(&fd) {
        if channel.1.send(WriteOp::Close).is_ok() {
            laux::lua_push(state, true);
            return Ok(1);
        }
    }
    Ok(0)
}

fn lua_host(lua: &mut LuaStack<'_>) -> c_int {
    let state = lua.state();
    let host: String = lua.opt(1).unwrap_or_else(|| "1.1.1.1:80".to_string());
    if let Ok(addr) = host.parse()
        && let Ok(socket) = TcpStream::connect_timeout(&addr, Duration::from_millis(1000))
        && let Ok(local_addr) = socket.local_addr()
    {
        laux::lua_push(state, local_addr.ip().to_string());
        return 1;
    }
    0
}

fn lua_write_frame(lua: &mut LuaStack<'_>) -> SocketResult<c_int> {
    socket_write(lua, true)
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
            MessageBody::Boxed(_, boxed) => {
                push_socket_event(state, boxed.into_inner::<SocketEvent>())
            }
            MessageBody::Buffer(_, buf) => {
                laux::lua_pushlightuserdata(state, Box::into_raw(buf) as *mut c_void);
                1
            }
            other => {
                (*m).data = other;
                crate::lua_push_error_tuple(
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
        lreg_try!("listen", lua_socket_listen),
        lreg_try!("read", lua_socket_read),
        lreg_try!("read_frame", lua_read_frame),
        lreg_try!("start_read_frame", lua_start_read_frame),
        lreg_try!("write", lua_socket_write),
        lreg_try!("write_frame", lua_write_frame),
        lreg_try!("connect", lua_socket_connect),
        lreg_try!("close", lua_socket_close),
        lreg!("host", lua_host),
        lreg_null!(),
    ];

    luaL_newlib!(state, l);
    1
}

#[cfg(test)]
#[path = "../../tests/unit/modules/lua_socket.rs"]
mod tests;
