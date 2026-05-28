use std::net::SocketAddr;
use std::time::Duration;

use bytes::Bytes;
use dashmap::DashMap;
use http_body_util::{BodyExt, Full};
use hyper::{Request, Response, body::Incoming, server::conn::http1, service::service_fn};
use hyper_util::rt::TokioIo;
use lazy_static::lazy_static;
use tokio::{net::TcpListener, sync::oneshot, time::timeout};
use tokio_util::sync::CancellationToken;

use actor::context::{self, CONTEXT};
use luars::{LuaResult, LuaState, LuaValue};

use crate::lua_actor::ActorRef;
use crate::{lua_check_integer, lua_check_lightuserdata, lua_check_str, lua_push_error, next_net_fd};

lazy_static! {
    static ref HTTP_SERVERS: DashMap<i64, CancellationToken> = DashMap::new();
}

struct HttpSrvRequest {
    method: String,
    path: String,
    query_string: String,
    headers: Vec<(String, String)>,
    body: Vec<u8>,
    response_tx: oneshot::Sender<HttpSrvResponse>,
}

struct HttpSrvResponse {
    status: u16,
    headers: Vec<(String, String)>,
    body: Vec<u8>,
}

async fn handle_request(
    req: Request<Incoming>,
    owner: i64,
) -> Result<Response<Full<Bytes>>, hyper::Error> {
    let method = req.method().to_string();
    let uri = req.uri().clone();
    let path = uri.path().to_string();
    let query_string = uri.query().unwrap_or("").to_string();

    let headers: Vec<(String, String)> = req
        .headers()
        .iter()
        .map(|(k, v)| (k.as_str().to_string(), v.to_str().unwrap_or("").to_string()))
        .collect();

    let body = req.collect().await?.to_bytes().to_vec();

    let (tx, rx) = oneshot::channel::<HttpSrvResponse>();

    CONTEXT.send_value(
        context::PTYPE_HTTP_SRV,
        owner,
        0,
        HttpSrvRequest {
            method,
            path,
            query_string,
            headers,
            body,
            response_tx: tx,
        },
    );

    match timeout(Duration::from_secs(30), rx).await {
        Ok(Ok(resp)) => {
            let mut builder = Response::builder().status(resp.status);
            for (k, v) in &resp.headers {
                builder = builder.header(k.as_str(), v.as_str());
            }
            Ok(builder
                .body(Full::new(Bytes::from(resp.body)))
                .unwrap_or_else(|_| {
                    Response::builder()
                        .status(500)
                        .body(Full::new(Bytes::from("Internal Server Error")))
                        .unwrap()
                }))
        }
        Ok(Err(_)) => Ok(Response::builder()
            .status(500)
            .body(Full::new(Bytes::from("Handler dropped")))
            .unwrap()),
        Err(_) => Ok(Response::builder()
            .status(504)
            .body(Full::new(Bytes::from("Gateway Timeout")))
            .unwrap()),
    }
}

fn listen(state: &mut LuaState) -> LuaResult<usize> {
    let addr = lua_check_str(state, 1)?.to_string();
    let actor = ActorRef::from_state(state);
    let owner = actor.id();

    let socket_addr: SocketAddr = addr
        .parse()
        .map_err(|e| state.error(format!("httpsrv listen '{}' failed: {}", addr, e)))?;

    let listener = std::net::TcpListener::bind(socket_addr)
        .map_err(|e| state.error(format!("httpsrv listen '{}' failed: {}", addr, e)))?;
    listener
        .set_nonblocking(true)
        .map_err(|e| state.error(format!("httpsrv listen '{}' failed: {}", addr, e)))?;
    let listener = TcpListener::from_std(listener)
        .map_err(|e| state.error(format!("httpsrv listen '{}' failed: {}", addr, e)))?;

    let fd = next_net_fd();
    let cancel = CancellationToken::new();
    HTTP_SERVERS.insert(fd, cancel.clone());

    tokio::spawn(async move {
        loop {
            tokio::select! {
                _ = cancel.cancelled() => break,
                result = listener.accept() => {
                    match result {
                        Ok((stream, _)) => {
                            let io = TokioIo::new(stream);
                            let owner = owner;
                            tokio::spawn(async move {
                                let svc = service_fn(move |req| handle_request(req, owner));
                                if let Err(err) = http1::Builder::new()
                                    .serve_connection(io, svc)
                                    .await
                                {
                                    log::error!("httpsrv connection error: {}", err);
                                }
                            });
                        }
                        Err(err) => {
                            log::error!("httpsrv accept error: {}", err);
                            tokio::time::sleep(Duration::from_millis(100)).await;
                        }
                    }
                }
            }
        }
        HTTP_SERVERS.remove(&fd);
        log::info!("httpsrv listener fd={} closed.", fd);
    });

    state.push_value(LuaValue::integer(fd))?;
    Ok(1)
}

fn decode(state: &mut LuaState) -> LuaResult<usize> {
    let ptr = lua_check_lightuserdata(state, 1)?;
    if ptr.is_null() {
        return lua_push_error(state, "httpsrv decode: null pointer");
    }
    let req = unsafe { Box::from_raw(ptr as *mut HttpSrvRequest) };

    let table = state.create_table(0, 5)?;

    let k = state.create_string("method")?;
    let v = state.create_string(&req.method)?;
    state.raw_set(&table, k, v);

    let k = state.create_string("path")?;
    let v = state.create_string(&req.path)?;
    state.raw_set(&table, k, v);

    let k = state.create_string("query_string")?;
    let v = state.create_string(&req.query_string)?;
    state.raw_set(&table, k, v);

    let headers_table = state.create_table(0, req.headers.len())?;
    for (hk, hv) in &req.headers {
        let k = state.create_string(hk)?;
        let v = state.create_string(hv)?;
        state.raw_set(&headers_table, k, v);
    }
    let k = state.create_string("headers")?;
    state.raw_set(&table, k, headers_table);

    let k = state.create_string("body")?;
    let v = state.create_bytes(&req.body)?;
    state.raw_set(&table, k, v);

    state.push_value(table)?;

    let tx_ptr = Box::into_raw(Box::new(req.response_tx)) as *mut std::ffi::c_void;
    state.push_value(LuaValue::lightuserdata(tx_ptr))?;

    Ok(2)
}

fn response(state: &mut LuaState) -> LuaResult<usize> {
    let ptr = lua_check_lightuserdata(state, 1)?;
    if ptr.is_null() {
        return lua_push_error(state, "httpsrv response: null handle");
    }
    let tx = unsafe { Box::from_raw(ptr as *mut oneshot::Sender<HttpSrvResponse>) };

    let status: u16 = crate::lua_opt_integer(state, 2).unwrap_or(200);

    let mut headers = Vec::new();
    if let Some(val) = state.get_arg(3) {
        if let Some(t) = val.as_table() {
            let mut key = LuaValue::nil();
            while let Ok(Some((k, v))) = t.next(&key) {
                let hk = k.as_str().unwrap_or("").to_string();
                let hv = v.as_str().unwrap_or("").to_string();
                headers.push((hk, hv));
                key = k;
            }
        }
    }

    let body = state
        .get_arg(4)
        .and_then(|v| v.as_bytes().map(|b| b.to_vec()))
        .unwrap_or_default();

    let _ = tx.send(HttpSrvResponse {
        status,
        headers,
        body,
    });

    state.push_value(LuaValue::boolean(true))?;
    Ok(1)
}

fn close(state: &mut LuaState) -> LuaResult<usize> {
    let fd: i64 = lua_check_integer(state, 1)?;
    if let Some((_, token)) = HTTP_SERVERS.remove(&fd) {
        token.cancel();
        state.push_value(LuaValue::boolean(true))?;
    } else {
        state.push_value(LuaValue::boolean(false))?;
    }
    Ok(1)
}

pub fn register_httpd() -> luars::LibraryModule {
    luars::lua_module!("httpd.core", {
        "listen" => listen,
        "decode" => decode,
        "response" => response,
        "close" => close,
    })
}
