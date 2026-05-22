use dashmap::DashMap;
use lazy_static::lazy_static;
use actor::{buffer::Buffer, context::MessageBody};
use luars::{LuaResult, LuaState, LuaValue};
use std::net::TcpStream;
use std::time::Duration;
use std::sync::atomic::{AtomicI64, Ordering};
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

use actor::{
    context::{self, CONTEXT, Message},
};

use crate::{lua_check_integer, lua_check_str, lua_opt_boolean, lua_check_buffer, lua_opt_integer, lua_opt_str, lua_push_error};
use crate::lua_actor::ActorRef;

lazy_static! {
    static ref NET: DashMap<i64, NetChannel> = DashMap::new();
    static ref NET_UUID: AtomicI64 = AtomicI64::new(1);
}

#[derive(Debug)]
pub enum NetOp {
    Accept(i64, i64),                         //owner,session
    ReadUntil(i64, i64, usize, Vec<u8>, u64), //owner,session,max_size
    ReadBytes(i64, i64, usize, u64),          //owner,session,size
    Write(i64, Box<Buffer>, bool),            //owner,data
    Close(),
}

pub struct NetChannel(pub mpsc::Sender<NetOp>, pub mpsc::Sender<NetOp>);

fn next_net_fd() -> i64 {
    let fd = NET_UUID.fetch_add(1, Ordering::AcqRel);
    if fd == i64::MAX {
        panic!("net fd overflow");
    }
    fd
}

async fn read_until(
    reader: &mut BufReader<OwnedReadHalf>,
    owner: i64,
    session: i64,
    max_size: usize,
    delim: Vec<u8>,
    read_timeout: u64,
) -> bool {
    let mut with_delim = false;
    let mut delim = delim.as_slice();
    if delim.is_empty() || (delim[0] == b'^' && delim.len() < 2) {
        CONTEXT.response_error(
            0,
            owner,
            -session,
            "socket: read delimiter is empty".to_string(),
        );
        return false;
    }

    if delim[0] == b'^' {
        delim = &delim[1..];
        with_delim = true;
    }

    let mut buffer = Box::new(Buffer::with_capacity(std::cmp::min(max_size, 512)));
    loop {
        let read_res;
        if read_timeout > 0 {
            match timeout(
                Duration::from_millis(read_timeout),
                reader.read_until(*delim.last().unwrap(), buffer.as_mut_vec()),
            )
            .await
            {
                Ok(res) => {
                    read_res = res;
                }
                Err(err) => {
                    CONTEXT.response_error(0, owner, -session, format!("read timeout: {}", err));
                    return false;
                }
            }
        } else {
            read_res = reader
                .read_until(*delim.last().unwrap(), buffer.as_mut_vec())
                .await;
        }

        match read_res {
            Ok(0) => {
                CONTEXT.response_error(0, owner, -session, "eof".to_string());
                return false;
            }
            Ok(_) => {
                if buffer.len() >= max_size {
                    CONTEXT.response_error(0, owner, -session, "socket: read size limit exceeded".to_string());
                    return false;
                }
                if buffer.as_vec().ends_with(delim.as_ref()) {
                    if !with_delim {
                        buffer.revert(delim.len());
                    }
                    if CONTEXT
                        .send(Message {
                            ptype: context::PTYPE_SOCKET_TCP,
                            from: 0,
                            to: owner,
                            session,
                            data: MessageBody::Buffer(buffer),
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
    owner: i64,
    session: i64,
    size: usize,
    read_timeout: u64,
) -> bool {
    if size == 0 {
        CONTEXT.response_error(
            0,
            owner,
            -session,
            "socket op:ReadBytes size must be greater than 0".to_string(),
        );
        return false;
    }
    let mut buffer = Box::new(Buffer::with_capacity(size));
    let space = buffer.prepare(size);
    let read_res;
    if read_timeout > 0 {
        match timeout(
            Duration::from_millis(read_timeout),
            reader.read_exact(space),
        )
        .await
        {
            Ok(res) => {
                read_res = res;
            }
            Err(err) => {
                CONTEXT.response_error(0, owner, -session, format!("read timeout: {}", err));
                return false;
            }
        }
    } else {
        read_res = reader.read_exact(space).await;
    }

    match read_res {
        Ok(_) => {
            buffer.commit(size);
            if CONTEXT
                .send(Message {
                    ptype: context::PTYPE_SOCKET_TCP,
                    from: 0,
                    to: owner,
                    session,
                    data: MessageBody::Buffer(buffer),
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

async fn handle_read(reader: OwnedReadHalf, rx: &mut mpsc::Receiver<NetOp>) {
    let mut reader = BufReader::new(reader);
    while let Some(op) = rx.recv().await {
        match op {
            NetOp::ReadUntil(owner, session, max_size, delim, read_timeout) => {
                if !read_until(&mut reader, owner, session, max_size, delim, read_timeout).await {
                    return;
                }
            }
            NetOp::ReadBytes(owner, session, size, read_timeout) => {
                if !read_bytes(&mut reader, owner, session, size, read_timeout).await {
                    return;
                }
            }
            _ => {}
        }
    }
}

async fn handle_write(mut writer: OwnedWriteHalf, mut rx: mpsc::Receiver<NetOp>) {
    while let Some(op) = rx.recv().await {
        match op {
            NetOp::Write(owner, data, close) => {
                if let Err(err) = writer.write_all(data.as_slice()).await {
                    CONTEXT.response_error(0, owner, 0, format!("socket: write failed: {}", err));
                    return;
                }
                if close {
                    return;
                }
            }
            NetOp::Close() => {
                return;
            }
            _ => {
                log::error!("write: {:?}", op);
            }
        }
    }
}

async fn handle_client(socket: tokio::net::TcpStream, owner: i64, session: i64) {
    let fd = next_net_fd();
    let (tx_reader, rx_reader) = mpsc::channel::<NetOp>(1);
    let (tx_writer, rx_writer) = mpsc::channel::<NetOp>(100);
    NET.insert(fd, NetChannel(tx_reader, tx_writer));

    if CONTEXT
        .send(Message {
            ptype: context::PTYPE_INTEGER,
            from: 0,
            to: owner,
            session,
            data: MessageBody::ISize(fd as isize),
        })
        .is_some()
    {
        return;
    }

    socket.set_nodelay(true).unwrap_or_default();

    let (reader, writer) = socket.into_split();

    let mut read_task = tokio::spawn(async move {
        let mut rx = rx_reader;
        handle_read(reader, &mut rx).await;
        let mut left = Vec::with_capacity(10);
        rx.recv_many(&mut left, 10).await;
        for op in left {
            match op {
                NetOp::ReadUntil(owner, session, ..) | NetOp::ReadBytes(owner, session, ..) => {
                    CONTEXT.response_error(0, owner, -session, "closed".to_string());
                }
                _ => {}
            }
        }
    });

    let mut write_task = tokio::spawn(handle_write(writer, rx_writer));

    if tokio::try_join!(&mut read_task, &mut write_task).is_err() {
        read_task.abort();
        write_task.abort();
    };
    NET.remove(&fd);
}

fn listen(addr: &str) -> Result<i64> {
    let listener = std::net::TcpListener::bind(addr)?;
    listener.set_nonblocking(true)?;
    let listener = TcpListener::from_std(listener)?;

    let (tx, mut rx) = mpsc::channel::<NetOp>(1);
    let fd = next_net_fd();
    NET.insert(fd, NetChannel(tx.clone(), tx));

    tokio::spawn(async move {
        while let Some(op) = rx.recv().await {
            match op {
                NetOp::Accept(owner, session) => match listener.accept().await {
                    Ok((socket, _)) => {
                        tokio::spawn(handle_client(socket, owner, session));
                    }
                    Err(err) => {
                        log::warn!("accept error: {}", err);
                        sleep(Duration::new(1, 0)).await;
                    }
                },
                NetOp::Close() => {
                    break;
                }
                _ => {
                    log::warn!("listen recv unknown op: {:?}", op);
                }
            }
        }
        NET.remove(&fd);
    });

    Ok(fd)
}

fn lua_socket_listen(state: &mut LuaState) -> LuaResult<usize> {
    let addr = lua_check_str(state, 1)?.to_string();
    match listen(&addr) {
        Ok(fd) => {
            state.push_value(LuaValue::integer(fd))?;
            Ok(1)
        }
        Err(err) => lua_push_error(state, &format!("socket: listen '{}' failed: {}", addr, err)),
    }
}

fn lua_socket_accept(state: &mut LuaState) -> LuaResult<usize> {
    let fd: i64 = lua_check_integer(state, 1)?;
    let actor = ActorRef::from_state(state);
    let owner = actor.id();
    let session = actor.next_session();

    if let Some(channel) = NET.get(&fd) {
        match channel.value().0.try_send(NetOp::Accept(owner, session)) {
            Ok(_) => {}
            Err(err) => {
                return lua_push_error(state, &format!("socket: accept channel closed: {}", err));
            }
        }
    } else {
        return lua_push_error(state, &format!("socket: fd {} not found", fd));
    }
    state.push_value(LuaValue::integer(session))?;
    Ok(1)
}

fn lua_socket_read(state: &mut LuaState) -> LuaResult<usize> {
    let fd: i64 = lua_check_integer(state, 1)?;

    let actor = ActorRef::from_state(state);
    let owner = actor.id();
    let session = actor.next_session();

    let arg2 = state.get_arg(2);
    let op = if arg2.as_ref().is_some_and(|v| v.is_integer()) {
        let max_size = arg2.and_then(|v| v.as_integer()).unwrap_or(0) as usize;
        let read_timeout: u64 = lua_opt_integer(state, 3).unwrap_or(0);

        NetOp::ReadBytes(owner, session, max_size, read_timeout)
    } else {
        let delim = arg2
            .and_then(|v| v.as_bytes().map(|b| b.to_vec()))
            .unwrap_or_default();
        if delim.is_empty() {
            return Err(state.error("socket: read delimiter is empty".to_string()));
        }

        let max_size: usize = lua_opt_integer(state, 3).unwrap_or(0xFFFFFFFF);
        let read_timeout: u64 = lua_opt_integer(state, 4).unwrap_or(0);

        NetOp::ReadUntil(owner, session, max_size, delim, read_timeout)
    };

    if let Some(channel) = NET.get(&fd) {
        if let Err(err) = channel.value().0.try_send(op) {
            return lua_push_error(state, &format!("socket: read channel closed: {}", err));
        }
    } else {
        return lua_push_error(state, &format!("socket: fd {} not found", fd));
    }

    state.push_value(LuaValue::integer(session))?;
    Ok(1)
}

fn lua_socket_write(state: &mut LuaState) -> LuaResult<usize> {
    let actor = ActorRef::from_state(state);
    let owner_id = actor.id();

    let fd: i64 = lua_check_integer(state, 1)?;
    let data = lua_check_buffer(state, 2)?; 
    let close = lua_opt_boolean(state, 3).unwrap_or(false);

    if data.is_none() {
        return Err(state.error("bad argument #2 (string expected, got nil)".to_string()));
    }

    if let Some(channel) = NET.get(&fd) {
        match channel
            .value()
            .1
            .try_send(NetOp::Write(owner_id, data.unwrap(), close))
        {
            Ok(_) => {
                state.push_value(LuaValue::boolean(true))?;
                return Ok(1);
            }
            Err(err) => {
                return lua_push_error(state, &format!("socket: write channel closed: {}", err));
            }
        }
    } else {
        lua_push_error(state, &format!("socket: fd {} not found", fd))
    }
}

fn lua_socket_connect(state: &mut LuaState) -> LuaResult<usize> {
    let addr = lua_check_str(state, 1)?.to_string();
    let connect_timeout: u64 = lua_opt_integer(state, 2).unwrap_or(5000);

    let actor = ActorRef::from_state(state);
    let owner = actor.id();
    let session = actor.next_session();

    tokio::spawn(async move {
        match timeout(
            Duration::from_millis(connect_timeout),
            tokio::net::TcpStream::connect(addr),
        )
        .await
        {
            Ok(Ok(socket)) => {
                tokio::spawn(async move {
                    handle_client(socket, owner, session).await;
                });
            }
            Ok(Err(err)) => {
                CONTEXT.response_error(0, owner, -session, format!("socket: connect failed: {}", err));
            }
            Err(err) => {
                CONTEXT.response_error(0, owner, -session, format!("socket: connect timeout: {}", err));
            }
        }
    });

    state.push_value(LuaValue::integer(session))?;
    Ok(1)
}

fn lua_socket_close(state: &mut LuaState) -> LuaResult<usize> {
    let fd: i64 = lua_check_integer(state, 1)?;

    if let Some((_, channel)) = NET.remove(&fd) {
        match channel.1.try_send(NetOp::Close()) {
            Ok(_) => {
                state.push_value(LuaValue::boolean(true))?;
                return Ok(1);
            }
            Err(_) => {
                return Ok(0);
            }
        }
    }
    Ok(0)
}

fn lua_host(state: &mut LuaState) -> LuaResult<usize> {
    let addr_str = lua_opt_str(state, 1).unwrap_or("1.1.1.1:80");

    if let Ok(addr) = addr_str.parse()
        && let Ok(socket) = TcpStream::connect_timeout(&addr, Duration::from_millis(1000))
        && let Ok(local_addr) = socket.local_addr()
    {
        let val = state.create_string(&local_addr.ip().to_string())?;
        state.push_value(val)?;
        return Ok(1);
    }
    Ok(0)
}

pub fn register_socket() -> luars::LibraryModule {
    luars::lua_module!("net.core", {
        "listen" => lua_socket_listen,
        "accept" => lua_socket_accept,
        "read" => lua_socket_read,
        "write" => lua_socket_write,
        "connect" => lua_socket_connect,
        "close" => lua_socket_close,
        "host" => lua_host,
    })
}
