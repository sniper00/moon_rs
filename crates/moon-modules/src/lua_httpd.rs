use std::ffi::c_int;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, Instant, UNIX_EPOCH};

use bytes::Bytes;
use dashmap::DashMap;
use futures_util::StreamExt;
use http_body_util::combinators::BoxBody;
use http_body_util::{BodyExt, Full, Limited, StreamBody};
use hyper::body::Frame;
use hyper::header::HeaderMap;
use hyper::{Request, Response, body::Incoming, server::conn::http1, service::service_fn};
use hyper_util::rt::TokioIo;
use lazy_static::lazy_static;
use tokio::io::{AsyncReadExt, AsyncSeekExt};
use tokio::net::TcpListener;
use tokio::sync::{Semaphore, oneshot};
use tokio::time::timeout;
use tokio_util::io::ReaderStream;
use tokio_util::sync::CancellationToken;

use moon_lua::laux::LuaState;
use moon_lua::{
    cstr, ffi, laux,
    laux::{LuaTable, LuaValue},
    lreg, lreg_null, luaL_newlib,
};
use moon_runtime::actor::LuaActor;
use moon_runtime::context::{self, CONTEXT};

use crate::lua_socket::next_net_fd;

const DEFAULT_MAX_BODY_SIZE: usize = 10 * 1024 * 1024; // 10 MB
const DEFAULT_MAX_CONNECTIONS: usize = 10000;
const STREAM_THRESHOLD: u64 = 1024 * 1024; // files > 1 MB are streamed
const CACHE_TTL: Duration = Duration::from_secs(5);
const MAX_CACHE_ENTRIES: usize = 10000;

type HttpBody = BoxBody<Bytes, std::io::Error>;

fn full_body(data: impl Into<Bytes>) -> HttpBody {
    Full::new(data.into())
        .map_err(|never| match never {})
        .boxed()
}

fn error_response(status: u16, msg: &'static str) -> Response<HttpBody> {
    Response::builder()
        .status(status)
        .body(full_body(msg))
        .unwrap()
}

fn mime_from_ext(ext: &str) -> &'static str {
    match ext {
        "html" | "htm" => "text/html; charset=utf-8",
        "css" => "text/css; charset=utf-8",
        "js" | "mjs" => "application/javascript; charset=utf-8",
        "json" => "application/json; charset=utf-8",
        "xml" => "application/xml; charset=utf-8",
        "txt" => "text/plain; charset=utf-8",
        "csv" => "text/csv; charset=utf-8",
        "svg" => "image/svg+xml",
        "png" => "image/png",
        "jpg" | "jpeg" => "image/jpeg",
        "gif" => "image/gif",
        "webp" => "image/webp",
        "ico" => "image/x-icon",
        "bmp" => "image/bmp",
        "woff" => "font/woff",
        "woff2" => "font/woff2",
        "ttf" => "font/ttf",
        "otf" => "font/otf",
        "eot" => "application/vnd.ms-fontobject",
        "pdf" => "application/pdf",
        "zip" => "application/zip",
        "gz" | "gzip" => "application/gzip",
        "tar" => "application/x-tar",
        "mp3" => "audio/mpeg",
        "mp4" => "video/mp4",
        "webm" => "video/webm",
        "ogg" => "audio/ogg",
        "wav" => "audio/wav",
        "wasm" => "application/wasm",
        "map" => "application/json",
        _ => "application/octet-stream",
    }
}

fn generate_etag(metadata: &std::fs::Metadata) -> String {
    let mtime = metadata
        .modified()
        .ok()
        .and_then(|t| t.duration_since(UNIX_EPOCH).ok())
        .map(|d| d.as_secs())
        .unwrap_or(0);
    format!("\"{:x}-{:x}\"", mtime, metadata.len())
}

fn etag_matches(header_value: &str, etag: &str) -> bool {
    if header_value.trim() == "*" {
        return true;
    }
    header_value.split(',').any(|t| t.trim() == etag)
}

/// Parse a single-range `Range: bytes=start-end` header.
/// Returns `(start, end_inclusive)` or `None` for invalid/unsupported ranges.
fn parse_range(header: &str, file_size: u64) -> Option<(u64, u64)> {
    let s = header.strip_prefix("bytes=")?;
    let s = s.split(',').next()?.trim();
    let (start_s, end_s) = s.split_once('-')?;

    let (start, end);
    if start_s.is_empty() {
        let suffix: u64 = end_s.parse().ok()?;
        if suffix == 0 {
            return None;
        }
        start = file_size.checked_sub(suffix)?;
        end = file_size - 1;
    } else {
        start = start_s.parse().ok()?;
        end = if end_s.is_empty() {
            file_size - 1
        } else {
            end_s.parse().ok()?
        };
    }

    if start > end || start >= file_size {
        return None;
    }
    Some((start, end.min(file_size - 1)))
}

// ---------------------------------------------------------------------------
// File metadata cache
// ---------------------------------------------------------------------------

#[derive(Clone)]
struct CachedFileMeta {
    canonical: PathBuf,
    file_size: u64,
    etag: String,
    last_modified: Option<String>,
    content_type: &'static str,
    mtime_secs: u64,
}

struct CacheEntry {
    meta: Option<CachedFileMeta>,
    cached_at: Instant,
}

lazy_static! {
    static ref HTTP_SERVERS: DashMap<i64, CancellationToken> = DashMap::new();
    static ref FILE_META_CACHE: DashMap<PathBuf, CacheEntry> = DashMap::new();
}

/// Full path resolution: metadata -> canonicalize -> metadata.
async fn resolve_file_meta(root: &PathBuf, file_path: &PathBuf) -> Option<CachedFileMeta> {
    let resolved = if tokio::fs::metadata(file_path)
        .await
        .map(|m| m.is_dir())
        .unwrap_or(false)
    {
        file_path.join("index.html")
    } else {
        file_path.clone()
    };

    let canonical = tokio::fs::canonicalize(&resolved).await.ok()?;
    if !canonical.starts_with(root.as_path()) {
        return None;
    }

    let metadata = tokio::fs::metadata(&canonical).await.ok()?;
    if !metadata.is_file() {
        return None;
    }

    let file_size = metadata.len();
    let ext = canonical
        .extension()
        .and_then(|e| e.to_str())
        .unwrap_or("");
    let content_type = mime_from_ext(ext);
    let etag = generate_etag(&metadata);
    let mtime_secs = metadata
        .modified()
        .ok()
        .and_then(|t| t.duration_since(UNIX_EPOCH).ok())
        .map(|d| d.as_secs())
        .unwrap_or(0);
    let last_modified = metadata.modified().ok().map(httpdate::fmt_http_date);

    Some(CachedFileMeta {
        canonical,
        file_size,
        etag,
        last_modified,
        content_type,
        mtime_secs,
    })
}

/// Quick revalidation: single metadata() call on the cached canonical path.
/// Avoids the expensive canonicalize() when the file hasn't changed.
async fn revalidate_meta(prev: &CachedFileMeta) -> Option<CachedFileMeta> {
    let metadata = tokio::fs::metadata(&prev.canonical).await.ok()?;
    if !metadata.is_file() {
        return None;
    }

    let new_mtime = metadata
        .modified()
        .ok()
        .and_then(|t| t.duration_since(UNIX_EPOCH).ok())
        .map(|d| d.as_secs())
        .unwrap_or(0);

    if new_mtime == prev.mtime_secs && metadata.len() == prev.file_size {
        return Some(prev.clone());
    }

    let etag = generate_etag(&metadata);
    let last_modified = metadata.modified().ok().map(httpdate::fmt_http_date);
    Some(CachedFileMeta {
        canonical: prev.canonical.clone(),
        file_size: metadata.len(),
        etag,
        last_modified,
        content_type: prev.content_type,
        mtime_secs: new_mtime,
    })
}

fn update_cache(file_path: &PathBuf, meta: Option<CachedFileMeta>) {
    if FILE_META_CACHE.len() >= MAX_CACHE_ENTRIES {
        FILE_META_CACHE.retain(|_, e| e.cached_at.elapsed() < CACHE_TTL);
    }
    FILE_META_CACHE.insert(
        file_path.clone(),
        CacheEntry {
            meta,
            cached_at: Instant::now(),
        },
    );
}

async fn get_cached_meta(root: &PathBuf, file_path: &PathBuf) -> Option<CachedFileMeta> {
    if let Some(entry) = FILE_META_CACHE.get(file_path) {
        if entry.cached_at.elapsed() < CACHE_TTL {
            return entry.meta.clone();
        }
        // Expired — try quick revalidation if we have previous metadata
        if let Some(ref prev) = entry.meta {
            let prev = prev.clone();
            drop(entry);
            if let Some(refreshed) = revalidate_meta(&prev).await {
                update_cache(file_path, Some(refreshed.clone()));
                return Some(refreshed);
            }
        } else {
            drop(entry);
        }
    }

    // Cache miss or revalidation failed — full resolution
    let meta = resolve_file_meta(root, file_path).await;
    update_cache(file_path, meta.clone());
    meta
}

// ---------------------------------------------------------------------------
// Static file serving
// ---------------------------------------------------------------------------

async fn serve_static_file(
    root: &PathBuf,
    req_path: &str,
    req_headers: &HeaderMap,
) -> Option<Response<HttpBody>> {
    let decoded = percent_decode(req_path.trim_start_matches('/'));
    if decoded.contains("..") {
        return Some(error_response(403, "Forbidden"));
    }

    let file_path = root.join(&decoded);
    let meta = get_cached_meta(root, &file_path).await?;

    // --- Cache: If-None-Match ---
    if let Some(inm) = req_headers
        .get("if-none-match")
        .and_then(|v| v.to_str().ok())
    {
        if etag_matches(inm, &meta.etag) {
            let mut b = Response::builder().status(304).header("etag", &meta.etag);
            if let Some(ref lm) = meta.last_modified {
                b = b.header("last-modified", lm.as_str());
            }
            return Some(b.body(full_body(Bytes::new())).unwrap());
        }
    }

    // --- Cache: If-Modified-Since (only when no If-None-Match) ---
    if !req_headers.contains_key("if-none-match") {
        if let Some(ims) = req_headers
            .get("if-modified-since")
            .and_then(|v| v.to_str().ok())
        {
            if let Ok(ims_time) = httpdate::parse_http_date(ims) {
                let ims_secs = ims_time
                    .duration_since(UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_secs();
                if meta.mtime_secs <= ims_secs {
                    let mut b =
                        Response::builder().status(304).header("etag", &meta.etag);
                    if let Some(ref lm) = meta.last_modified {
                        b = b.header("last-modified", lm.as_str());
                    }
                    return Some(b.body(full_body(Bytes::new())).unwrap());
                }
            }
        }
    }

    // --- Range request ---
    if let Some(range_hdr) = req_headers.get("range").and_then(|v| v.to_str().ok()) {
        if let Some((start, end)) = parse_range(range_hdr, meta.file_size) {
            let length = end - start + 1;

            let mut file = tokio::fs::File::open(&meta.canonical).await.ok()?;
            file.seek(std::io::SeekFrom::Start(start)).await.ok()?;
            let limited = file.take(length);
            let stream = ReaderStream::with_capacity(limited, 65536);
            let mapped = stream.map(|r| r.map(Frame::data));

            let mut b = Response::builder()
                .status(206)
                .header("content-type", meta.content_type)
                .header("content-length", length)
                .header(
                    "content-range",
                    format!("bytes {}-{}/{}", start, end, meta.file_size),
                )
                .header("accept-ranges", "bytes")
                .header("etag", &meta.etag);
            if let Some(ref lm) = meta.last_modified {
                b = b.header("last-modified", lm.as_str());
            }
            return Some(
                b.body(BodyExt::boxed(StreamBody::new(mapped))).unwrap(),
            );
        } else {
            return Some(
                Response::builder()
                    .status(416)
                    .header("content-range", format!("bytes */{}", meta.file_size))
                    .body(full_body("Range Not Satisfiable"))
                    .unwrap(),
            );
        }
    }

    // --- Full response ---
    let mut b = Response::builder()
        .status(200)
        .header("content-type", meta.content_type)
        .header("content-length", meta.file_size)
        .header("accept-ranges", "bytes")
        .header("etag", &meta.etag);
    if let Some(ref lm) = meta.last_modified {
        b = b.header("last-modified", lm.as_str());
    }

    if meta.file_size <= STREAM_THRESHOLD {
        let content = tokio::fs::read(&meta.canonical).await.ok()?;
        Some(b.body(full_body(content)).unwrap())
    } else {
        let file = tokio::fs::File::open(&meta.canonical).await.ok()?;
        let stream = ReaderStream::with_capacity(file, 65536);
        let mapped = stream.map(|r| r.map(Frame::data));
        Some(b.body(BodyExt::boxed(StreamBody::new(mapped))).unwrap())
    }
}

fn percent_decode(input: &str) -> String {
    let mut result = Vec::with_capacity(input.len());
    let bytes = input.as_bytes();
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'%' && i + 2 < bytes.len() {
            if let (Some(hi), Some(lo)) = (hex_val(bytes[i + 1]), hex_val(bytes[i + 2])) {
                result.push(hi << 4 | lo);
                i += 3;
                continue;
            }
        }
        result.push(bytes[i]);
        i += 1;
    }
    String::from_utf8(result).unwrap_or_default()
}

fn hex_val(b: u8) -> Option<u8> {
    match b {
        b'0'..=b'9' => Some(b - b'0'),
        b'a'..=b'f' => Some(b - b'a' + 10),
        b'A'..=b'F' => Some(b - b'A' + 10),
        _ => None,
    }
}

// ---------------------------------------------------------------------------
// HTTP server core
// ---------------------------------------------------------------------------

struct ResponseHandle(Option<oneshot::Sender<HttpSrvResponse>>);

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
    max_body_size: usize,
    static_dir: Option<Arc<PathBuf>>,
) -> Result<Response<HttpBody>, hyper::Error> {
    let method = req.method().to_string();
    let uri = req.uri().clone();
    let path = uri.path().to_string();
    let query_string = uri.query().unwrap_or("").to_string();

    if let Some(ref root) = static_dir {
        if method == "GET" || method == "HEAD" {
            if let Some(resp) = serve_static_file(root, &path, req.headers()).await {
                return Ok(resp);
            }
        }
    }

    let headers: Vec<(String, String)> = req
        .headers()
        .iter()
        .map(|(k, v)| (k.as_str().to_string(), v.to_str().unwrap_or("").to_string()))
        .collect();

    let limited = Limited::new(req.into_body(), max_body_size);
    let body = match limited.collect().await {
        Ok(collected) => collected.to_bytes().to_vec(),
        Err(_) => {
            return Ok(error_response(413, "Payload Too Large"));
        }
    };

    let (tx, rx) = oneshot::channel::<HttpSrvResponse>();

    let _ = CONTEXT.send_value(
        context::PTYPE_HTTPD,
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
                .body(full_body(resp.body))
                .unwrap_or_else(|_| error_response(500, "Internal Server Error")))
        }
        Ok(Err(_)) => Ok(error_response(500, "Handler dropped")),
        Err(_) => Ok(error_response(504, "Gateway Timeout")),
    }
}

extern "C-unwind" fn listen(state: LuaState) -> c_int {
    let addr = unsafe { laux::lua_check_str(state, 1) };

    let has_opts = laux::lua_type(state, 2) == laux::LuaType::Table;
    let max_body_size: usize = if has_opts {
        laux::opt_field(state, 2, "max_body_size").unwrap_or(DEFAULT_MAX_BODY_SIZE)
    } else {
        DEFAULT_MAX_BODY_SIZE
    };
    let max_connections: usize = if has_opts {
        laux::opt_field(state, 2, "max_connections").unwrap_or(DEFAULT_MAX_CONNECTIONS)
    } else {
        DEFAULT_MAX_CONNECTIONS
    };
    let static_dir: Option<Arc<PathBuf>> = if has_opts {
        laux::opt_field::<String>(state, 2, "static_dir").map(|s| {
            let p = PathBuf::from(&s);
            let canonical = p.canonicalize().unwrap_or_else(|e| {
                laux::lua_error(state, format!("httpd static_dir '{}' invalid: {}", s, e));
            });
            Arc::new(canonical)
        })
    } else {
        None
    };

    let socket_addr: SocketAddr = match addr.parse() {
        Ok(a) => a,
        Err(e) => {
            laux::lua_error(state, format!("httpd listen '{}' failed: {}", addr, e));
        }
    };

    let listener = match std::net::TcpListener::bind(socket_addr) {
        Ok(l) => l,
        Err(e) => {
            laux::lua_error(state, format!("httpd listen '{}' failed: {}", addr, e));
        }
    };
    if let Err(e) = listener.set_nonblocking(true) {
        laux::lua_error(state, format!("httpd listen '{}' failed: {}", addr, e));
    }
    let listener = match TcpListener::from_std(listener) {
        Ok(l) => l,
        Err(e) => {
            laux::lua_error(state, format!("httpd listen '{}' failed: {}", addr, e));
        }
    };

    let actor = LuaActor::from_lua_state(state);
    let owner = unsafe { (*actor).id };

    let fd = next_net_fd();
    let cancel = CancellationToken::new();
    HTTP_SERVERS.insert(fd, cancel.clone());

    let semaphore = Arc::new(Semaphore::new(max_connections));

    tokio::spawn(async move {
        loop {
            tokio::select! {
                _ = cancel.cancelled() => break,
                result = listener.accept() => {
                    match result {
                        Ok((stream, _)) => {
                            let permit = match semaphore.clone().try_acquire_owned() {
                                Ok(permit) => permit,
                                Err(_) => {
                                    drop(stream);
                                    log::warn!("httpd: max connections ({}) reached, rejecting", max_connections);
                                    continue;
                                }
                            };
                            let io = TokioIo::new(stream);
                            let static_dir = static_dir.clone();
                            tokio::spawn(async move {
                                let _permit = permit;
                                let svc = service_fn(move |req| {
                                    let static_dir = static_dir.clone();
                                    handle_request(req, owner, max_body_size, static_dir)
                                });
                                if let Err(err) = http1::Builder::new()
                                    .serve_connection(io, svc)
                                    .await
                                {
                                    log::error!("httpd connection error: {}", err);
                                }
                            });
                        }
                        Err(err) => {
                            log::error!("httpd accept error: {}", err);
                            tokio::time::sleep(Duration::from_millis(100)).await;
                        }
                    }
                }
            }
        }
        HTTP_SERVERS.remove(&fd);
        log::info!("httpd listener fd={} closed.", fd);
    });

    laux::lua_push(state, fd);
    1
}

extern "C-unwind" fn decode(state: LuaState) -> c_int {
    laux::lua_checkstack(state, 6, std::ptr::null());
    let p_as_isize: isize = laux::lua_get(state, 1);
    let req = unsafe { Box::from_raw(p_as_isize as *mut HttpSrvRequest) };

    LuaTable::new(state, 0, 5)
        .insert("method", req.method.as_str())
        .insert("path", req.path.as_str())
        .insert("query_string", req.query_string.as_str())
        .insert("body", req.body.as_slice())
        .rawset_x("headers", || {
            let headers = LuaTable::new(state, 0, req.headers.len());
            for (k, v) in &req.headers {
                headers.insert(k.as_str(), v.as_str());
            }
        });

    let methods = [lreg_null!()];
    laux::lua_newuserdata(
        state,
        ResponseHandle(Some(req.response_tx)),
        cstr!("httpd_response_handle"),
        &methods,
    );

    2
}

extern "C-unwind" fn response(state: LuaState) -> c_int {
    let handle = match laux::lua_touserdata::<ResponseHandle>(state, 1) {
        Some(h) => h,
        None => {
            laux::lua_push(state, false);
            laux::lua_push(state, "httpd response: null handle");
            return 2;
        }
    };
    let tx = match handle.0.take() {
        Some(tx) => tx,
        None => {
            laux::lua_push(state, false);
            laux::lua_push(state, "httpd response: already consumed");
            return 2;
        }
    };

    let status: u16 = laux::lua_opt(state, 2).unwrap_or(200);

    let mut headers = Vec::new();
    if laux::lua_type(state, 3) == laux::LuaType::Table {
        let header_table = LuaTable::from_stack(state, 3);
        for (key, value) in header_table.iter() {
            let hk = key.to_string();
            let hv = value.to_string();
            headers.push((hk, hv));
        }
    }

    let body = match LuaValue::from_stack(state, 4) {
        LuaValue::String(s) => s.to_vec(),
        _ => Vec::new(),
    };

    let _ = tx.send(HttpSrvResponse {
        status,
        headers,
        body,
    });

    laux::lua_push(state, true);
    1
}

extern "C-unwind" fn close(state: LuaState) -> c_int {
    let fd: i64 = laux::lua_get(state, 1);
    if let Some((_, token)) = HTTP_SERVERS.remove(&fd) {
        token.cancel();
        laux::lua_push(state, true);
    } else {
        laux::lua_push(state, false);
    }
    1
}

pub extern "C-unwind" fn luaopen_httpd(state: LuaState) -> c_int {
    let l = [
        lreg!("listen", listen),
        lreg!("decode", decode),
        lreg!("response", response),
        lreg!("close", close),
        lreg_null!(),
    ];

    luaL_newlib!(state, l);

    1
}
