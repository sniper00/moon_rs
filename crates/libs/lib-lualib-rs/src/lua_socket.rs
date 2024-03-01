use lib_lua::{ffi, ffi::luaL_Reg};
use std::ffi::c_int;
use std::time::Duration;
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
    c_str,
    context::{self, Message, NetChannel, NetOp, CONTEXT},
    laux::{self, LuaStateRef},
    lreg, lreg_null,
};

// pub const SOCKET_ACCEPT: i8 = 1;
// pub const SOCKET_READ: i8 = 2;
// pub const SOCKET_WRITE: i8 = 3;
// pub const SOCKET_CONNECT: i8 = 4;

fn response_error(owner: i64, session: i64, err: Vec<u8>) {
    if session == 0 {
        log::error!("error: {:?}", String::from_utf8_lossy(err.as_ref()));
        return;
    }
    tokio::spawn(async move {
        CONTEXT.send(Message {
            ptype: context::PTYPE_ERROR,
            from: 0,
            to: owner,
            session,
            data: Some(Box::new(err.into())),
        });
    });
}

async fn handle_read(reader: OwnedReadHalf, rx: &mut mpsc::Receiver<NetOp>) {
    let mut reader = BufReader::new(reader);
    while let Some(op) = rx.recv().await {
        match op {
            NetOp::ReadUntil(owner, session, max_size, delim, read_timeout) => {
                let mut buffer = Vec::<u8>::with_capacity(512);
                loop {
                    let read_res;
                    if read_timeout > 0 {
                        match timeout(
                            Duration::from_millis(read_timeout),
                            reader.read_until(*delim.last().unwrap(), &mut buffer),
                        )
                        .await
                        {
                            Ok(res) => {
                                read_res = res;
                            }
                            Err(err) => {
                                response_error(
                                    owner,
                                    session,
                                    format!("read timeout: {}", err).into_bytes(),
                                );
                                return;
                            }
                        }
                    } else {
                        read_res = reader.read_until(*delim.last().unwrap(), &mut buffer).await;
                    }

                    match read_res {
                        Ok(0) => {
                            response_error(owner, session, b"eof".to_vec());
                            return;
                        }
                        Ok(_) => {
                            if buffer.len() >= max_size {
                                response_error(owner, session, b"max read size limit".to_vec());
                                return;
                            }
                            if buffer.ends_with(delim.as_ref()) {
                                buffer.truncate(buffer.len() - delim.len());
                                if CONTEXT
                                    .send(Message {
                                        ptype: context::PTYPE_SOCKET_TCP,
                                        from: 0,
                                        to: owner,
                                        session,
                                        data: Some(Box::new(buffer.into())),
                                    })
                                    .is_some()
                                {
                                    return;
                                }
                                break;
                            }
                            log::warn!("continue read {}", session);
                        }
                        Err(err) => {
                            response_error(owner, session, err.to_string().into_bytes());
                            return;
                        }
                    }
                }
            }
            NetOp::ReadBytes(owner, session, size, read_timeout) => {
                let mut buffer = vec![0; size];
                let read_res;
                if read_timeout > 0 {
                    match timeout(
                        Duration::from_millis(read_timeout),
                        reader.read_exact(&mut buffer),
                    )
                    .await
                    {
                        Ok(res) => {
                            read_res = res;
                        }
                        Err(err) => {
                            response_error(
                                owner,
                                session,
                                format!("read timeout: {}", err).into_bytes(),
                            );
                            return;
                        }
                    }
                } else {
                    read_res = reader.read_exact(&mut buffer).await;
                }

                if let Err(err) = read_res {
                    response_error(owner, session, err.to_string().into_bytes());
                    return;
                } else {
                    CONTEXT.send(Message {
                        ptype: context::PTYPE_SOCKET_TCP,
                        from: 0,
                        to: owner,
                        session,
                        data: Some(Box::new(buffer.into())),
                    });
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
                    response_error(owner, 0, format!("socket write error {}", err).into_bytes());
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
    let fd = CONTEXT.next_net_fd();
    let (tx0, rx0) = mpsc::channel::<NetOp>(1);
    let (tx1, rx1) = mpsc::channel::<NetOp>(100);
    CONTEXT.net.insert(fd, NetChannel(tx0, tx1));

    if CONTEXT
        .send(Message {
            ptype: context::PTYPE_INTEGER,
            from: 0,
            to: owner,
            session,
            data: Some(Box::new(fd.to_string().into())),
        })
        .is_some()
    {
        return;
    }

    socket.set_nodelay(true).unwrap_or_default();

    let (reader, writer) = socket.into_split();

    let mut read_task = tokio::spawn(async move {
        let mut rx = rx0;
        handle_read(reader, &mut rx).await;
        let mut left = Vec::with_capacity(10);
        rx.recv_many(&mut left, 10).await;
        for op in left {
            match op {
                NetOp::ReadUntil(owner, session, _, _, _) => {
                    response_error(owner, session, b"closed".to_vec());
                }
                NetOp::ReadBytes(owner, session, _, _) => {
                    response_error(owner, session, b"closed".to_vec());
                }
                _ => {}
            }
        }
    });

    let mut write_task = tokio::spawn(handle_write(writer, rx1));

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
    let fd = CONTEXT.next_net_fd();
    CONTEXT.net.insert(fd, NetChannel(tx.clone(), tx));

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
    let lua = LuaStateRef::new(state);
    let addr: &str = lua.get(1);
    match listen(addr) {
        Ok(fd) => {
            lua.push(fd);
            1
        }
        Err(err) => {
            lua.push(false);
            lua.push(format!("socket_listen error: {}", err));
            2
        }
    }
}

extern "C-unwind" fn lua_socket_accept(state: *mut ffi::lua_State) -> c_int {
    let lua = LuaStateRef::new(state);

    let fd = lua.get(1);
    let actor = LuaActor::from_lua_state(state);
    let owner = actor.id;
    let session = actor.next_uuid();

    if let Some(channel) = CONTEXT.net.get(&fd) {
        match channel.value().0.try_send(NetOp::Accept(owner, session)) {
            Ok(_) => {}
            Err(err) => {
                response_error(
                    owner,
                    session,
                    format!("socket_accept channel send error: {}", err).into_bytes(),
                );
            }
        }
    } else {
        response_error(
            owner,
            session,
            format!("socket_accept error: fd not found: {}", fd).into_bytes(),
        );
    }
    lua.push(session);
    1
}

extern "C-unwind" fn lua_socket_read(state: *mut ffi::lua_State) -> c_int {
    let lua = LuaStateRef::new(state);

    let fd = lua.get(1);

    let actor = LuaActor::from_lua_state(state);
    let owner = actor.id;
    let session = actor.next_uuid();

    let op = if lua.ltype(2) == ffi::LUA_TNUMBER {
        let max_size = lua.get(2);
        let read_timeout = lua.opt(3).unwrap_or(0);

        NetOp::ReadBytes(owner, session, max_size, read_timeout)
    } else {
        let delim = lua.check_slice(2);
        if delim.is_empty() {
            lua.error("socket_read error: delim is empty");
        }

        let max_size = lua.opt(3).unwrap_or(0xFFFFFFFF);
        let read_timeout = lua.opt(4).unwrap_or(0);

        NetOp::ReadUntil(owner, session, max_size, delim.to_vec(), read_timeout)
    };

    if let Some(channel) = CONTEXT.net.get(&fd) {
        if let Err(err) = channel.value().0.try_send(op) {
            response_error(
                owner,
                session,
                format!("socket_read channel send error: {}", err).into_bytes(),
            );
        }
    } else {
        response_error(
            owner,
            session,
            format!("socket_read error: fd not found: {}", fd).into_bytes(),
        );
    }

    lua.push(session);

    1
}

extern "C-unwind" fn lua_socket_write(state: *mut ffi::lua_State) -> c_int {
    let lua = LuaStateRef::new(state);
    let actor = LuaActor::from_lua_state(state);

    let fd = lua.get(1);
    let data = laux::check_buffer(state, 2);
    let close = lua.opt(3).unwrap_or_default();

    if let Some(channel) = CONTEXT.net.get(&fd) {
        match channel
            .value()
            .1
            .try_send(NetOp::Write(actor.id, data.unwrap(), close))
        {
            Ok(_) => {
                lua.push(true);
                return 1;
            }
            Err(err) => {
                lua.push(false);
                lua.push(format!("socket_write channel send error: {}", err));
            }
        }
    } else {
        lua.push(false);
        lua.push(format!("socket_write error: fd not found: {}", fd));
    }
    2
}

extern "C-unwind" fn lua_socket_connect(state: *mut ffi::lua_State) -> c_int {
    let lua = LuaStateRef::new(state);

    let addr: &str = lua.get(1);
    let connect_timeout: u64 = lua.opt(2).unwrap_or(5000);

    let actor = LuaActor::from_lua_state(state);
    let owner = actor.id;
    let session = actor.next_uuid();

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
                response_error(
                    owner,
                    session,
                    format!("connect error: {}", err).into_bytes(),
                );
            }
            Err(err) => {
                response_error(
                    owner,
                    session,
                    format!("connect timeout: {}", err).into_bytes(),
                );
            }
        }
    });

    lua.push(session);

    1
}

extern "C-unwind" fn lua_socket_close(state: *mut ffi::lua_State) -> c_int {
    let lua = LuaStateRef::new(state);

    let fd = lua.get(1);

    if let Some(channel) = CONTEXT.net.get(&fd) {
        match channel.value().1.try_send(NetOp::Close()) {
            Ok(_) => {
                lua.push(true);
                return 1;
            }
            Err(_) => {
                return 0;
            }
        }
    }
    0
}

pub unsafe extern "C-unwind" fn luaopen_socket(state: *mut ffi::lua_State) -> c_int {
    let l = [
        lreg!("listen", lua_socket_listen),
        lreg!("accept", lua_socket_accept),
        lreg!("read", lua_socket_read),
        lreg!("write", lua_socket_write),
        lreg!("connect", lua_socket_connect),
        lreg!("close", lua_socket_close),
        lreg_null!(),
    ];

    ffi::lua_createtable(state, 0, l.len() as c_int);
    ffi::luaL_setfuncs(state, l.as_ptr(), 0);
    1
}
