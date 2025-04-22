use dashmap::DashMap;
use lazy_static::lazy_static;
use lib_core::{buffer::Buffer, check_buffer, context::MessageData};
use lib_lua::{
    self, cstr,
    ffi::{self, luaL_Reg},
    laux::{self, LuaType},
    lreg, lreg_null, luaL_newlib,
};
use std::net::TcpStream;
use std::time::Duration;
use std::{
    ffi::c_int,
    sync::atomic::{AtomicI64, Ordering},
};
use tokio::io::AsyncReadExt;

use tokio::{
    io::{AsyncBufReadExt, AsyncWriteExt, BufReader, Result},
    net::{
        tcp::{OwnedReadHalf, OwnedWriteHalf},
        TcpListener,
    },
    sync::mpsc,
    time::{sleep, timeout},
};

use lib_core::{
    actor::LuaActor,
    context::{self, Message, CONTEXT},
};

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

// pub const SOCKET_ACCEPT: i8 = 1;
// pub const SOCKET_READ: i8 = 2;
// pub const SOCKET_WRITE: i8 = 3;
// pub const SOCKET_CONNECT: i8 = 4;

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
            "socket op:ReadUntil delim is empty".to_string(),
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
                    CONTEXT.response_error(0, owner, -session, "max read size limit".to_string());
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
                            data: MessageData::Buffer(buffer),
                        })
                        .is_some()
                    {
                        return false;
                    }
                    break;
                }
                //log::warn!("continue read {}", session);
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
                    data: MessageData::Buffer(buffer),
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
                    CONTEXT.response_error(0, owner, 0, format!("socket write error {}", err));
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
            data: MessageData::ISize(fd as isize),
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
                    return;
                }
                _ => {
                    log::warn!("listen recv unknown op: {:?}", op);
                }
            }
        }
    });

    Ok(fd)
}

extern "C-unwind" fn lua_socket_listen(state: *mut ffi::lua_State) -> c_int {
    let addr: &str = laux::lua_get(state, 1);
    match listen(addr) {
        Ok(fd) => {
            laux::lua_push(state, fd);
            1
        }
        Err(err) => {
            laux::lua_push(state, false);
            laux::lua_push(state, format!("Listen '{}' failed: {}", addr, err));
            2
        }
    }
}

extern "C-unwind" fn lua_socket_accept(state: *mut ffi::lua_State) -> c_int {
    let fd = laux::lua_get(state, 1);
    let actor = LuaActor::from_lua_state(state);
    let owner = actor.id;
    let session = actor.next_session();

    if let Some(channel) = NET.get(&fd) {
        match channel.value().0.try_send(NetOp::Accept(owner, session)) {
            Ok(_) => {}
            Err(err) => {
                laux::lua_push(state, false);
                laux::lua_push(state, format!("socket_accept channel send error: {}", err));
                return 2;
            }
        }
    } else {
        laux::lua_push(state, false);
        laux::lua_push(state, format!("socket_accept error fd not found: {}", fd));
        return 2;
    }
    laux::lua_push(state, session);
    1
}

extern "C-unwind" fn lua_socket_read(state: *mut ffi::lua_State) -> c_int {
    let fd = laux::lua_get(state, 1);

    let actor = LuaActor::from_lua_state(state);
    let owner = actor.id;
    let session = actor.next_session();

    let op = if laux::lua_type(state, 2) == LuaType::Integer {
        let max_size = laux::lua_get(state, 2);
        let read_timeout = laux::lua_opt(state, 3).unwrap_or(0);

        NetOp::ReadBytes(owner, session, max_size, read_timeout)
    } else {
        let delim = laux::lua_get::<&[u8]>(state, 2);
        if delim.is_empty() {
            laux::lua_error(state, "socket_read error: delim is empty");
        }

        let max_size = laux::lua_opt(state, 3).unwrap_or(0xFFFFFFFF);
        let read_timeout = laux::lua_opt(state, 4).unwrap_or(0);

        NetOp::ReadUntil(owner, session, max_size, delim.to_vec(), read_timeout)
    };

    if let Some(channel) = NET.get(&fd) {
        if let Err(err) = channel.value().0.try_send(op) {
            laux::lua_push(state, false);
            laux::lua_push(state, format!("socket_read channel send error: {}", err));
            return 2;
        }
    } else {
        laux::lua_push(state, false);
        laux::lua_push(state, format!("socket_read error: fd not found: {}", fd));
        return 2;
    }

    laux::lua_push(state, session);

    1
}

extern "C-unwind" fn lua_socket_write(state: *mut ffi::lua_State) -> c_int {
    let actor = LuaActor::from_lua_state(state);

    let fd = laux::lua_get(state, 1);
    let data = check_buffer(state, 2);
    let close = laux::lua_opt(state, 3).unwrap_or_default();

    if let Some(channel) = NET.get(&fd) {
        match channel
            .value()
            .1
            .try_send(NetOp::Write(actor.id, data.unwrap(), close))
        {
            Ok(_) => {
                laux::lua_push(state, true);
                return 1;
            }
            Err(err) => {
                laux::lua_push(state, false);
                laux::lua_push(state, format!("socket_write channel send error: {}", err));
            }
        }
    } else {
        laux::lua_push(state, false);
        laux::lua_push(state, format!("socket_write error: fd not found: {}", fd));
    }
    2
}

extern "C-unwind" fn lua_socket_connect(state: *mut ffi::lua_State) -> c_int {
    let addr: &str = laux::lua_get(state, 1);
    let connect_timeout: u64 = laux::lua_opt(state, 2).unwrap_or(5000);

    let actor = LuaActor::from_lua_state(state);
    let owner = actor.id;
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
                CONTEXT.response_error(0, owner, -session, format!("connect error: {}", err));
            }
            Err(err) => {
                CONTEXT.response_error(0, owner, -session, format!("connect timeout: {}", err));
            }
        }
    });

    laux::lua_push(state, session);

    1
}

extern "C-unwind" fn lua_socket_close(state: *mut ffi::lua_State) -> c_int {
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

extern "C-unwind" fn lua_host(state: *mut ffi::lua_State) -> c_int {
    if let Ok(addr) = laux::lua_opt(state, 1).unwrap_or("1.1.1.1:80").parse() {
        if let Ok(socket) = TcpStream::connect_timeout(&addr, Duration::from_millis(1000)) {
            if let Ok(local_addr) = socket.local_addr() {
                laux::lua_push(state, local_addr.ip().to_string());
                return 1;
            }
        }
    }
    0
}

pub extern "C-unwind" fn luaopen_socket(state: *mut ffi::lua_State) -> c_int {
    let l = [
        lreg!("listen", lua_socket_listen),
        lreg!("accept", lua_socket_accept),
        lreg!("read", lua_socket_read),
        lreg!("write", lua_socket_write),
        lreg!("connect", lua_socket_connect),
        lreg!("close", lua_socket_close),
        lreg!("host", lua_host),
        lreg_null!(),
    ];

    luaL_newlib!(state, l);
    1
}
