use crate::request_pool::{PendingCounter, QueuedRequest, drain_queued_requests, try_send_counted};
use dashmap::DashMap;
use futures_util::stream::TryStreamExt;
use lazy_static::lazy_static;
use mongodb::{
    Client, Collection, IndexModel,
    bson::{Bson, Document, doc, oid},
    error::{Error, ErrorKind},
    options::{ClientOptions, CreateIndexOptions, FindOptions, IndexOptions, ReadConcern},
    results,
};
use moon_base::{
    cstr, ffi,
    laux::{self, LuaArgs, LuaStack, LuaState, LuaTable, LuaType},
    lreg, lreg_null, lreg_try, luaL_newlib, push_lua_table,
};
use moon_runtime::actor::LuaActor;
use moon_runtime::context::{self, ActorId, CONTEXT};
use std::{ffi::c_int, str::FromStr, time::Duration};
use tokio::sync::{mpsc, oneshot};

lazy_static! {
    static ref DATABASE_CONNECTIONSS: DashMap<String, DatabaseConnection> = DashMap::new();
}

/// Drain a find cursor into a `Vec`, failing fast once it would exceed
/// `crate::LIMITS.db_query_rows`.
async fn collect_docs_capped(mut cur: mongodb::Cursor<Document>) -> Result<Vec<Document>, Error> {
    let mut docs = Vec::new();
    while let Some(doc) = cur.try_next().await? {
        if docs.len() >= crate::LIMITS.db_query_rows {
            return Err(Error::from(std::io::Error::other(format!(
                "find returned more than {} documents; use find_stream for large result sets",
                crate::LIMITS.db_query_rows
            ))));
        }
        docs.push(doc);
    }
    Ok(docs)
}

enum DatabaseRequest {
    CreateCollection(ActorId, i64, String, String), // owner, session, db_name, collection_name
    InsertOne(ActorId, i64, String, String, Document), // owner, session, db_name, collection_name, doc
    InsertMany(ActorId, i64, String, String, Vec<Document>), // owner, session, db_name, collection_name, docs
    DeleteOne(ActorId, i64, String, String, Document), // owner, session, db_name, collection_name, filter
    DeleteMany(ActorId, i64, String, String, Document), // owner, session, db_name, collection_name, filter
    UpdateOne(ActorId, i64, String, String, Document, Document), // owner, session, db_name, collection_name, filter, update
    UpdateMany(ActorId, i64, String, String, Document, Document), // owner, session, db_name, collection_name, filter, update
    FindOne(ActorId, i64, String, String, Document), // owner, session, db_name, collection_name, filter
    Find(
        ActorId,
        i64,
        String,
        String,
        Document,
        Box<Option<FindOptions>>,
    ), // owner, session, db_name, collection_name, filter
    ReplacOne(ActorId, i64, String, String, Document, Document), // owner, session, db_name, collection_name, filter, replacement
    Count(ActorId, i64, String, String, Document), // owner, session, db_name, collection_name, filter
    Exists(ActorId, i64, String, String, Document), // owner, session, db_name, collection_name, filter,
    CreateIndex(
        ActorId,
        i64,
        String,
        String,
        Box<IndexModel>,
        Box<Option<CreateIndexOptions>>,
    ), // owner, session, db_name, collection_name, keys, options
    FindStream(
        ActorId,
        i64,
        String,
        String,
        Document,
        Box<Option<FindOptions>>,
        usize,
    ),
    Close(),
}

impl DatabaseRequest {
    /// `(owner, session)` for a request that expects a reply, or `None` for
    /// control messages (`Close`). Used to fail requests that are still queued
    /// when the handler shuts down so their callers don't hang forever.
    fn owner_session(&self) -> Option<(ActorId, i64)> {
        match self {
            DatabaseRequest::CreateCollection(o, s, ..)
            | DatabaseRequest::InsertOne(o, s, ..)
            | DatabaseRequest::InsertMany(o, s, ..)
            | DatabaseRequest::DeleteOne(o, s, ..)
            | DatabaseRequest::DeleteMany(o, s, ..)
            | DatabaseRequest::UpdateOne(o, s, ..)
            | DatabaseRequest::UpdateMany(o, s, ..)
            | DatabaseRequest::FindOne(o, s, ..)
            | DatabaseRequest::Find(o, s, ..)
            | DatabaseRequest::ReplacOne(o, s, ..)
            | DatabaseRequest::Count(o, s, ..)
            | DatabaseRequest::Exists(o, s, ..)
            | DatabaseRequest::CreateIndex(o, s, ..)
            | DatabaseRequest::FindStream(o, s, ..) => Some((*o, *s)),
            DatabaseRequest::Close() => None,
        }
    }
}

impl QueuedRequest for DatabaseRequest {
    fn owner_session(&self) -> Option<(ActorId, i64)> {
        DatabaseRequest::owner_session(self)
    }
}

enum CursorSignal {
    Next(ActorId, i64),
    Close,
}

struct CursorBatch {
    docs: Vec<Document>,
    next_tx: Option<oneshot::Sender<CursorSignal>>,
}

struct CursorHandle(Option<oneshot::Sender<CursorSignal>>);

impl Drop for CursorHandle {
    fn drop(&mut self) {
        if let Some(tx) = self.0.take() {
            let _ = tx.send(CursorSignal::Close);
        }
    }
}

enum DatabaseResponse {
    Connect,
    CreateCollection,
    InsertOne(results::InsertOneResult),
    InsertMany(results::InsertManyResult),
    DeleteOne(results::DeleteResult),
    DeleteMany(results::DeleteResult),
    UpdateOne(results::UpdateResult),
    UpdateMany(results::UpdateResult),
    FindOne(Option<Document>),
    Find(Vec<Document>),
    FindBatch(CursorBatch),
    ReplacOne(results::UpdateResult),
    Count(u64),
    Exists(bool),

    CreateIndex(results::CreateIndexResult),
    Error(Error),
    // Timeout(String),
}

#[derive(Clone)]
struct DatabaseConnection {
    name: String,
    tx: mpsc::Sender<DatabaseRequest>,
    counter: PendingCounter,
}

struct DatabaseState {
    protocol_type: u8,
    database_url: String,
    client: Client,
}

impl DatabaseState {
    async fn connect(protocol_type: u8, database_url: String) -> Result<Self, Error> {
        let options = ClientOptions::parse(&database_url).await?;
        let client = Client::with_options(options)?;
        client
            .database("admin")
            .run_command(doc! { "ping": 1 })
            .await?;
        Ok(DatabaseState {
            protocol_type,
            database_url,
            client,
        })
    }

    fn send_result(&self, owner: ActorId, session: i64, res: Result<DatabaseResponse, Error>) {
        match res {
            Ok(res) => {
                if session != 0 {
                    let _ = CONTEXT.send_value(self.protocol_type, owner, session, res);
                }
            }
            Err(err) => {
                if session != 0 {
                    let _ = CONTEXT.send_value(
                        self.protocol_type,
                        owner,
                        session,
                        DatabaseResponse::Error(err),
                    );
                } else {
                    // Fire-and-forget request (session == 0): there is no caller
                    // to receive the error and the handler does not retry, so the
                    // failure is dropped after logging.
                    log::error!(
                        "Database '{}' error: '{:?}'. Dropped (fire-and-forget, no retry).",
                        self.database_url,
                        err.to_string()
                    );
                }
            }
        }
    }
}

/// Whether an error is a transient network/connectivity failure that is worth
/// retrying (as opposed to a logical error like a duplicate key or bad filter,
/// which will keep failing). The `mongodb` driver already retries reads/writes
/// internally; this gates the additional fire-and-forget self-heal retry.
fn is_transient_network_error(err: &Error) -> bool {
    if matches!(
        *err.kind,
        ErrorKind::Io(_)
            | ErrorKind::ServerSelection { .. }
            | ErrorKind::ConnectionPoolCleared { .. }
            | ErrorKind::DnsResolve { .. }
    ) {
        return true;
    }
    err.contains_label("RetryableWriteError")
}

/// Run a MongoDB operation, mirroring the pg/redis worker retry convention:
///
/// - **Awaited** requests (`session != 0`) get the result or error exactly once
///   — the caller is responsible for handling/retrying.
/// - **Fire-and-forget** requests (`session == 0`) self-heal: a *transient
///   network* error is retried with a fixed backoff until it succeeds; any
///   non-network (logical) error is returned so it is logged and dropped rather
///   than retried forever.
///
/// `make_fut` rebuilds the operation future on each attempt (futures are
/// single-use), so callers clone any owned inputs inside the closure.
async fn run_with_retry<F, Fut>(
    database_url: &str,
    session: i64,
    mut make_fut: F,
) -> Result<DatabaseResponse, Error>
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = Result<DatabaseResponse, Error>>,
{
    let mut failed_times: u32 = 0;
    loop {
        match make_fut().await {
            Ok(v) => return Ok(v),
            Err(err) => {
                if session != 0 || !is_transient_network_error(&err) {
                    return Err(err);
                }
                if failed_times == 0 {
                    log::error!(
                        "mongodb '{}' network error: {}. retrying.",
                        database_url,
                        err
                    );
                }
                failed_times += 1;
                tokio::time::sleep(Duration::from_secs(1)).await;
            }
        }
    }
}

async fn database_handler(
    state: DatabaseState,
    mut rx: mpsc::Receiver<DatabaseRequest>,
    counter: PendingCounter,
) {
    while let Some(op) = rx.recv().await {
        // let mut failed_times = 0;
        match op {
            DatabaseRequest::CreateCollection(owner, session, db_name, collection_name) => {
                let client = state.client.clone();
                let res = run_with_retry(&state.database_url, session, move || {
                    let db = client.database(&db_name);
                    let collection_name = collection_name.clone();
                    async move {
                        db.create_collection(collection_name)
                            .await
                            .map(|_| DatabaseResponse::CreateCollection)
                    }
                })
                .await;
                state.send_result(owner, session, res);
            }
            DatabaseRequest::InsertOne(owner, session, db_name, collection_name, doc) => {
                let client = state.client.clone();
                let res = run_with_retry(&state.database_url, session, move || {
                    let coll: Collection<Document> =
                        client.database(&db_name).collection(&collection_name);
                    let doc = doc.clone();
                    async move { coll.insert_one(doc).await.map(DatabaseResponse::InsertOne) }
                })
                .await;
                state.send_result(owner, session, res);
            }
            DatabaseRequest::InsertMany(owner, session, db_name, collection_name, docs) => {
                let client = state.client.clone();
                let res = run_with_retry(&state.database_url, session, move || {
                    let coll: Collection<Document> =
                        client.database(&db_name).collection(&collection_name);
                    let docs = docs.clone();
                    async move {
                        coll.insert_many(docs)
                            .await
                            .map(DatabaseResponse::InsertMany)
                    }
                })
                .await;
                state.send_result(owner, session, res);
            }
            DatabaseRequest::DeleteOne(owner, session, db_name, collection_name, filter) => {
                let client = state.client.clone();
                let res = run_with_retry(&state.database_url, session, move || {
                    let coll: Collection<Document> =
                        client.database(&db_name).collection(&collection_name);
                    let filter = filter.clone();
                    async move {
                        coll.delete_one(filter)
                            .await
                            .map(DatabaseResponse::DeleteOne)
                    }
                })
                .await;
                state.send_result(owner, session, res);
            }
            DatabaseRequest::DeleteMany(owner, session, db_name, collection_name, filter) => {
                let client = state.client.clone();
                let res = run_with_retry(&state.database_url, session, move || {
                    let coll: Collection<Document> =
                        client.database(&db_name).collection(&collection_name);
                    let filter = filter.clone();
                    async move {
                        coll.delete_many(filter)
                            .await
                            .map(DatabaseResponse::DeleteMany)
                    }
                })
                .await;
                state.send_result(owner, session, res);
            }
            DatabaseRequest::UpdateOne(
                owner,
                session,
                db_name,
                collection_name,
                filter,
                update,
            ) => {
                let client = state.client.clone();
                let res = run_with_retry(&state.database_url, session, move || {
                    let coll: Collection<Document> =
                        client.database(&db_name).collection(&collection_name);
                    let filter = filter.clone();
                    let update = update.clone();
                    async move {
                        coll.update_one(filter, update)
                            .await
                            .map(DatabaseResponse::UpdateOne)
                    }
                })
                .await;
                state.send_result(owner, session, res);
            }
            DatabaseRequest::UpdateMany(
                owner,
                session,
                db_name,
                collection_name,
                filter,
                update,
            ) => {
                let client = state.client.clone();
                let res = run_with_retry(&state.database_url, session, move || {
                    let coll: Collection<Document> =
                        client.database(&db_name).collection(&collection_name);
                    let filter = filter.clone();
                    let update = update.clone();
                    async move {
                        coll.update_many(filter, update)
                            .await
                            .map(DatabaseResponse::UpdateMany)
                    }
                })
                .await;
                state.send_result(owner, session, res);
            }
            DatabaseRequest::FindOne(owner, session, db_name, collection_name, filter) => {
                let client = state.client.clone();
                let res = run_with_retry(&state.database_url, session, move || {
                    let coll: Collection<Document> =
                        client.database(&db_name).collection(&collection_name);
                    let filter = filter.clone();
                    async move {
                        coll.find_one(filter)
                            .await
                            .map(|doc: Option<Document>| DatabaseResponse::FindOne(doc))
                    }
                })
                .await;
                state.send_result(owner, session, res);
            }
            DatabaseRequest::Find(owner, session, db_name, collection_name, filter, options) => {
                let client = state.client.clone();
                let res = run_with_retry(&state.database_url, session, move || {
                    let coll: Collection<Document> =
                        client.database(&db_name).collection(&collection_name);
                    let filter = filter.clone();
                    let options = options.clone();
                    async move {
                        let cur = coll.find(filter).with_options(*options).await?;
                        collect_docs_capped(cur).await.map(DatabaseResponse::Find)
                    }
                })
                .await;
                state.send_result(owner, session, res);
            }
            DatabaseRequest::ReplacOne(
                owner,
                session,
                db_name,
                collection_name,
                filter,
                replacement,
            ) => {
                let client = state.client.clone();
                let res = run_with_retry(&state.database_url, session, move || {
                    let coll: Collection<Document> =
                        client.database(&db_name).collection(&collection_name);
                    let filter = filter.clone();
                    let replacement = replacement.clone();
                    async move {
                        coll.replace_one(filter, replacement)
                            .await
                            .map(DatabaseResponse::ReplacOne)
                    }
                })
                .await;
                state.send_result(owner, session, res);
            }
            DatabaseRequest::Count(owner, session, db_name, collection_name, filter) => {
                let client = state.client.clone();
                let res = run_with_retry(&state.database_url, session, move || {
                    let coll: Collection<Document> =
                        client.database(&db_name).collection(&collection_name);
                    let filter = filter.clone();
                    async move {
                        coll.count_documents(filter)
                            .await
                            .map(DatabaseResponse::Count)
                    }
                })
                .await;
                state.send_result(owner, session, res);
            }
            DatabaseRequest::Exists(owner, session, db_name, collection_name, filter) => {
                let client = state.client.clone();
                let res = run_with_retry(&state.database_url, session, move || {
                    let coll: Collection<Document> =
                        client.database(&db_name).collection(&collection_name);
                    let filter = filter.clone();
                    async move {
                        coll.find_one(filter)
                            .await
                            .map(|doc: Option<Document>| DatabaseResponse::Exists(doc.is_some()))
                    }
                })
                .await;
                state.send_result(owner, session, res);
            }
            DatabaseRequest::CreateIndex(
                owner,
                session,
                db_name,
                collection_name,
                index,
                options,
            ) => {
                let client = state.client.clone();
                let res = run_with_retry(&state.database_url, session, move || {
                    let coll: Collection<Document> =
                        client.database(&db_name).collection(&collection_name);
                    let index = index.clone();
                    let options = options.clone();
                    async move {
                        coll.create_index(*index)
                            .with_options(*options)
                            .await
                            .map(DatabaseResponse::CreateIndex)
                    }
                })
                .await;
                state.send_result(owner, session, res);
            }

            DatabaseRequest::FindStream(
                owner,
                session,
                db_name,
                collection_name,
                filter,
                options,
                batch_size,
            ) => {
                let db = state.client.database(&db_name);
                let coll: Collection<Document> = db.collection(&collection_name);
                match coll.find(filter.clone()).with_options(*options).await {
                    Ok(mut cur) => {
                        let mut current_owner = owner;
                        let mut current_session = session;
                        loop {
                            let mut batch = Vec::with_capacity(batch_size);
                            let mut errored = false;
                            for _ in 0..batch_size {
                                match cur.try_next().await {
                                    Ok(Some(doc)) => batch.push(doc),
                                    Ok(None) => break,
                                    Err(err) => {
                                        CONTEXT.response_error(
                                            0,
                                            current_owner,
                                            -current_session,
                                            err.to_string(),
                                        );
                                        errored = true;
                                        break;
                                    }
                                }
                            }
                            if errored {
                                break;
                            }

                            let cursor_exhausted = batch.len() < batch_size;
                            let (next_tx, next_rx) = if !cursor_exhausted {
                                let (tx, rx) = oneshot::channel();
                                (Some(tx), Some(rx))
                            } else {
                                (None, None)
                            };

                            let _ = CONTEXT.send_value(
                                state.protocol_type,
                                current_owner,
                                current_session,
                                DatabaseResponse::FindBatch(CursorBatch {
                                    docs: batch,
                                    next_tx,
                                }),
                            );

                            // `next_rx` is `Some` iff the cursor isn't exhausted.
                            let Some(next_rx) = next_rx else { break };
                            match next_rx.await {
                                Ok(CursorSignal::Next(new_owner, new_session)) => {
                                    current_owner = new_owner;
                                    current_session = new_session;
                                }
                                _ => break,
                            }
                        }
                    }
                    Err(err) => {
                        CONTEXT.response_error(0, owner, -session, err.to_string());
                    }
                }
            }

            DatabaseRequest::Close() => {
                drain_queued_requests(&mut rx, &counter, |owner, session| {
                    CONTEXT.response_error(
                        0,
                        owner,
                        -session,
                        "mongodb connection closed".to_string(),
                    );
                });
                break;
            }
        }
        counter.dec();
    }
}

fn connect(lua: &mut LuaStack<'_>) -> Result<c_int, String> {
    let state = lua.state();
    let mut args = LuaArgs::new(1);
    let database_url = lua
        .get::<String>(args.iter_arg())
        .map_err(|err| format!("mongodb.connect: {err}"))?;
    let name = lua
        .get::<String>(args.iter_arg())
        .map_err(|err| format!("mongodb.connect: {err}"))?;
    let queue_capacity: usize = lua
        .opt(args.iter_arg())
        .unwrap_or(crate::LIMITS.request_queue_capacity);
    let queue_capacity = queue_capacity.max(1);

    let actor = LuaActor::from_lua_state(state);
    let owner = unsafe { (*actor).id };
    let session = unsafe { (*actor).next_session() };

    CONTEXT.io_runtime().spawn(async move {
        match DatabaseState::connect(context::PTYPE_MONGODB, database_url).await {
            Ok(state) => {
                let (tx, rx) = mpsc::channel(queue_capacity);
                let counter = PendingCounter::new();
                // Replacing an existing connection of the same name: tell the
                // previous handler to close so its task and mongodb Client don't
                // leak (it drains and fails any queued requests, then exits).
                if let Some(old) = DATABASE_CONNECTIONSS.insert(
                    name.clone(),
                    DatabaseConnection {
                        name: name.to_string(),
                        tx: tx.clone(),
                        counter: counter.clone(),
                    },
                ) {
                    log::warn!(
                        "mongodb '{}' reconnected with the same name; closing the previous connection",
                        old.name
                    );
                    let _ = old.tx.send(DatabaseRequest::Close()).await;
                }

                let _ = CONTEXT.send_value(
                    context::PTYPE_MONGODB,
                    owner,
                    session,
                    DatabaseResponse::Connect,
                );

                database_handler(state, rx, counter).await;
            }
            Err(err) => {
                let _ = CONTEXT.send_value(
                    context::PTYPE_MONGODB,
                    owner,
                    session,
                    DatabaseResponse::Error(err),
                );
            }
        }
    });

    laux::lua_push(state, session);
    Ok(1)
}

fn extract_find_options(lua: &mut LuaStack<'_>, index: i32) -> Result<FindOptions, String> {
    let mut find_options = FindOptions::default();
    for mut entry in lua.table_cursor(index) {
        let key = entry.key();
        let Some(key) = key.as_bytes() else {
            continue;
        };
        match key {
            b"limit" => {
                let value = entry.value();
                if let Some(value) = value.as_integer() {
                    find_options.limit = Some(value);
                }
            }
            b"skip" => {
                let value = entry.value();
                if let Some(value) = value.as_integer() {
                    find_options.skip = Some(value as u64);
                }
            }
            b"sort" => {
                let index = {
                    let value = entry.value();
                    if value.kind() != LuaType::Table {
                        return Err(format!("Invalid sort value type: {:?}", value.name()));
                    }
                    value.index()
                };
                find_options.sort = Some(unsafe { table_to_doc(entry.lua_mut(), index) }?);
            }
            b"projection" => {
                let index = {
                    let value = entry.value();
                    if value.kind() != LuaType::Table {
                        return Err(format!("Invalid projection value type: {:?}", value.name()));
                    }
                    value.index()
                };
                find_options.projection = Some(unsafe { table_to_doc(entry.lua_mut(), index) }?);
            }
            b"max_time" => {
                let value = entry.value();
                if let Some(value) = value.as_integer() {
                    find_options.max_time = Some(Duration::from_millis(value as u64));
                }
            }
            b"batch_size" => {
                let value = entry.value();
                if let Some(value) = value.as_integer() {
                    find_options.batch_size = Some(value as u32);
                }
            }
            b"allow_partial_results" => {
                let value = entry.value();
                if let Some(value) = value.as_bool() {
                    find_options.allow_partial_results = Some(value);
                }
            }
            b"no_cursor_timeout" => {
                let value = entry.value();
                if let Some(value) = value.as_bool() {
                    find_options.no_cursor_timeout = Some(value);
                }
            }
            b"cursor_type" => {
                let value = entry.value();
                if let Some(value) = value.as_bytes() {
                    find_options.cursor_type = Some(match value {
                        b"NonTailable" => mongodb::options::CursorType::NonTailable,
                        b"Tailable" => mongodb::options::CursorType::Tailable,
                        b"TailableAwait" => mongodb::options::CursorType::TailableAwait,
                        _ => {
                            return Err(format!(
                                "Invalid cursor type: {}",
                                String::from_utf8_lossy(value)
                            ));
                        }
                    });
                }
            }
            b"read_concern" => {
                let value = entry.value();
                if let Some(value) = value.as_bytes() {
                    find_options.read_concern =
                        Some(ReadConcern::custom(String::from_utf8_lossy(value)));
                }
            }
            _ => {
                return Err(format!(
                    "Invalid find_options key: '{}'",
                    String::from_utf8_lossy(key)
                ));
            }
        }
    }
    Ok(find_options)
}

fn extract_create_index_options(
    lua: &mut LuaStack<'_>,
    index: i32,
) -> Result<CreateIndexOptions, String> {
    let mut create_index_options = CreateIndexOptions::default();
    for entry in lua.table_cursor(index) {
        let key = entry.key();
        let Some(key) = key.as_bytes() else {
            continue;
        };
        match key {
            b"max_time" => {
                let value = entry.value();
                if let Some(value) = value.as_integer() {
                    create_index_options.max_time = Some(Duration::from_secs(value as u64));
                }
            }
            _ => return Err(format!("Invalid key: {}", String::from_utf8_lossy(key))),
        }
    }
    Ok(create_index_options)
}

fn extract_index_options(lua: &mut LuaStack<'_>, index: i32) -> Result<IndexOptions, String> {
    let mut index_options = IndexOptions::default();
    for mut entry in lua.table_cursor(index) {
        let key = entry.key();
        let Some(key) = key.as_bytes() else {
            continue;
        };
        match key {
            b"name" => {
                let value = entry.value();
                if let Some(value) = value.as_bytes() {
                    index_options.name = Some(String::from_utf8_lossy(value).into_owned());
                }
            }
            b"unique" => {
                let value = entry.value();
                if let Some(value) = value.as_bool() {
                    index_options.unique = Some(value);
                }
            }
            b"background" => {
                let value = entry.value();
                if let Some(value) = value.as_bool() {
                    index_options.background = Some(value);
                }
            }
            b"sparse" => {
                let value = entry.value();
                if let Some(value) = value.as_bool() {
                    index_options.sparse = Some(value);
                }
            }
            b"storage_engine" => {
                let index = {
                    let value = entry.value();
                    if value.kind() != LuaType::Table {
                        return Err(format!(
                            "Invalid storage_engine value type: {:?}",
                            value.name()
                        ));
                    }
                    value.index()
                };
                index_options.storage_engine =
                    Some(unsafe { table_to_doc(entry.lua_mut(), index) }?);
            }
            b"partial_filter_expression" => {
                let index = {
                    let value = entry.value();
                    if value.kind() != LuaType::Table {
                        return Err(format!(
                            "Invalid partial_filter_expression value type: {:?}",
                            value.name()
                        ));
                    }
                    value.index()
                };
                index_options.partial_filter_expression =
                    Some(unsafe { table_to_doc(entry.lua_mut(), index) }?);
            }
            b"wildcard_projection" => {
                let index = {
                    let value = entry.value();
                    if value.kind() != LuaType::Table {
                        return Err(format!(
                            "Invalid wildcard_projection value type: {:?}",
                            value.name()
                        ));
                    }
                    value.index()
                };
                index_options.wildcard_projection =
                    Some(unsafe { table_to_doc(entry.lua_mut(), index) }?);
            }
            b"hidden" => {
                let value = entry.value();
                if let Some(value) = value.as_bool() {
                    index_options.hidden = Some(value);
                }
            }
            b"default_language" => {
                let value = entry.value();
                if let Some(value) = value.as_bytes() {
                    index_options.default_language =
                        Some(String::from_utf8_lossy(value).into_owned());
                }
            }
            b"language_override" => {
                let value = entry.value();
                if let Some(value) = value.as_bytes() {
                    index_options.language_override =
                        Some(String::from_utf8_lossy(value).into_owned());
                }
            }
            b"weights" => {
                let index = {
                    let value = entry.value();
                    if value.kind() != LuaType::Table {
                        return Err(format!("Invalid weights value type: {:?}", value.name()));
                    }
                    value.index()
                };
                index_options.weights = Some(unsafe { table_to_doc(entry.lua_mut(), index) }?);
            }
            b"bits" => {
                let value = entry.value();
                if let Some(value) = value.as_integer() {
                    index_options.bits = Some(value as u32);
                }
            }
            b"max" => {
                let value = entry.value();
                if value.kind() == LuaType::Number {
                    index_options.max = value.as_number();
                }
            }
            b"min" => {
                let value = entry.value();
                if value.kind() == LuaType::Number {
                    index_options.min = value.as_number();
                }
            }
            b"bucket_size" => {
                let value = entry.value();
                if let Some(value) = value.as_integer() {
                    index_options.bucket_size = Some(value as u32);
                }
            }
            _ => return Err(format!("Invalid key: {}", String::from_utf8_lossy(key))),
        }
    }
    Ok(index_options)
}

fn make_request(
    owner: ActorId,
    session: i64,
    db_name: String,
    collection_name: String,
    op_name: &str,
    lua: &mut LuaStack<'_>,
    args: &mut LuaArgs,
) -> Result<DatabaseRequest, String> {
    let request = match op_name {
        "create_coll" => {
            DatabaseRequest::CreateCollection(owner, session, db_name, collection_name)
        }
        "insert_one" => {
            let doc = table_to_doc(lua, args.iter_arg())?;
            DatabaseRequest::InsertOne(owner, session, db_name, collection_name, doc)
        }
        "insert_many" => {
            let mut docs = Vec::new();
            for mut entry in lua.table_cursor(args.iter_arg()) {
                let index = entry.value().index();
                docs.push(unsafe { lua_to_doc(entry.lua_mut(), index) }?);
            }
            DatabaseRequest::InsertMany(owner, session, db_name, collection_name, docs)
        }
        "delete_one" => {
            let filter = table_to_doc(lua, args.iter_arg())?;
            DatabaseRequest::DeleteOne(owner, session, db_name, collection_name, filter)
        }
        "delete_many" => {
            let filter = table_to_doc(lua, args.iter_arg())?;
            DatabaseRequest::DeleteMany(owner, session, db_name, collection_name, filter)
        }
        "update_one" => {
            let filter = table_to_doc(lua, args.iter_arg())?;
            let update = table_to_doc(lua, args.iter_arg())?;
            DatabaseRequest::UpdateOne(owner, session, db_name, collection_name, filter, update)
        }
        "update_many" => {
            let filter = table_to_doc(lua, args.iter_arg())?;
            let update = table_to_doc(lua, args.iter_arg())?;
            DatabaseRequest::UpdateMany(owner, session, db_name, collection_name, filter, update)
        }
        "find_one" => {
            let filter = table_to_doc(lua, args.iter_arg())?;
            DatabaseRequest::FindOne(owner, session, db_name, collection_name, filter)
        }
        "find" => {
            let filter = table_to_doc(lua, args.iter_arg())?;
            let options_index = args.iter_arg();
            let find_options = if lua.value(options_index).kind() == LuaType::Table {
                Some(extract_find_options(lua, options_index)?)
            } else {
                None
            };

            DatabaseRequest::Find(
                owner,
                session,
                db_name,
                collection_name,
                filter,
                Box::new(find_options),
            )
        }
        "replace_one" => {
            let filter = table_to_doc(lua, args.iter_arg())?;
            let replacement = table_to_doc(lua, args.iter_arg())?;
            DatabaseRequest::ReplacOne(
                owner,
                session,
                db_name,
                collection_name,
                filter,
                replacement,
            )
        }
        "count" => {
            let filter = table_to_doc(lua, args.iter_arg())?;
            DatabaseRequest::Count(owner, session, db_name, collection_name, filter)
        }
        "exists" => {
            let filter = table_to_doc(lua, args.iter_arg())?;
            DatabaseRequest::Exists(owner, session, db_name, collection_name, filter)
        }
        "create_index" => {
            let keys = table_to_doc(lua, args.iter_arg())?;

            let index_options_index = args.iter_arg();
            let index_options = if lua.value(index_options_index).kind() == LuaType::Table {
                Some(extract_index_options(lua, index_options_index)?)
            } else {
                None
            };

            let options_index = args.iter_arg();
            let options = if lua.value(options_index).kind() == LuaType::Table {
                Some(extract_create_index_options(lua, options_index)?)
            } else {
                None
            };

            let index = IndexModel::builder()
                .keys(keys)
                .options(index_options)
                .build();
            DatabaseRequest::CreateIndex(
                owner,
                session,
                db_name,
                collection_name,
                Box::new(index),
                Box::new(options),
            )
        }
        "find_stream" => {
            let filter = table_to_doc(lua, args.iter_arg())?;
            let options_index = args.iter_arg();
            let find_options = if lua.value(options_index).kind() == LuaType::Table {
                Some(extract_find_options(lua, options_index)?)
            } else {
                None
            };
            // Parse as i64 so a negative Lua integer is rejected rather than
            // wrapping to a huge `usize`. `batch_size == 0` would make the
            // handler loop emit empty batches forever, so require >= 1.
            let batch_size: i64 = lua
                .opt(args.iter_arg())
                .unwrap_or(crate::LIMITS.db_stream_batch_rows);
            if batch_size < 1 || batch_size as u64 > crate::LIMITS.db_query_rows as u64 {
                return Err(format!(
                    "find_stream: batch_size must be between 1 and {}",
                    crate::LIMITS.db_query_rows
                ));
            }
            let batch_size = batch_size as usize;

            DatabaseRequest::FindStream(
                owner,
                session,
                db_name,
                collection_name,
                filter,
                Box::new(find_options),
                batch_size,
            )
        }
        "close" => DatabaseRequest::Close(),
        _ => {
            return Err(format!("Invalid operation: {}", op_name));
        }
    };

    Ok(request)
}

fn lua_mongodb_close(lua: &mut LuaStack<'_>) -> c_int {
    let conn_ptr = lua
        .value(1)
        .as_userdata::<DatabaseConnection>()
        .expect("Invalid database connect pointer");
    let conn = unsafe { conn_ptr.as_ref() };
    // Stop the handler task (drops the mongodb Client) and drop the registry
    // entry so a later reconnect with the same name doesn't collide with a
    // stale, dead handle.
    let tx = conn.tx.clone();
    CONTEXT.io_runtime().spawn(async move {
        let _ = tx.send(DatabaseRequest::Close()).await;
    });
    // Only remove our own entry: if a `connect()` with the same name has already
    // replaced this connection, closing through this (now stale) handle must not
    // delete the newer entry. Match on our own pending counter to identify it.
    DATABASE_CONNECTIONSS.remove_if(&conn.name, |_, v| v.counter.ptr_eq(&conn.counter));
    0
}

fn cursor_next(lua: &mut LuaStack<'_>) -> c_int {
    let state = lua.state();
    let mut handle_ptr = lua
        .value(1)
        .as_userdata::<CursorHandle>()
        .expect("invalid cursor handle");
    let handle = unsafe { handle_ptr.as_mut() };
    if let Some(tx) = handle.0.take() {
        let actor = LuaActor::from_lua_state(state);
        let owner = unsafe { (*actor).id };
        let session = unsafe { (*actor).next_session() };
        // If the stream handler has already exited, its receiver is gone and the
        // send fails. Surface that as an error rather than pushing a session that
        // would never be answered (which would hang the awaiting coroutine).
        if tx.send(CursorSignal::Next(owner, session)).is_err() {
            return crate::lua_push_error_tuple(state, "cursor: stream handler is gone");
        }
        laux::lua_push(state, session);
        1
    } else {
        crate::lua_push_error_tuple(state, "cursor: already consumed or closed")
    }
}

fn cursor_close(lua: &mut LuaStack<'_>) -> c_int {
    let mut handle_ptr = lua
        .value(1)
        .as_userdata::<CursorHandle>()
        .expect("invalid cursor handle");
    let handle = unsafe { handle_ptr.as_mut() };
    if let Some(tx) = handle.0.take() {
        let _ = tx.send(CursorSignal::Close);
    }
    0
}

fn operators(lua: &mut LuaStack<'_>) -> Result<c_int, String> {
    let state = lua.state();
    let mut args = LuaArgs::new(1);

    let conn_ptr = lua
        .value(args.iter_arg())
        .as_userdata::<DatabaseConnection>()
        .expect("Invalid database connect pointer");
    let conn = unsafe { conn_ptr.as_ref() };

    let op_name = lua
        .get::<String>(args.iter_arg())
        .map_err(|err| format!("mongodb.operators: {err}"))?;
    let db_name: String = lua.get(args.iter_arg())?;
    let collection_name: String = lua.get(args.iter_arg())?;

    let actor = LuaActor::from_lua_state(state);
    let owner = unsafe { (*actor).id };
    let session = unsafe { (*actor).next_session() };

    let request = match make_request(
        owner,
        session,
        db_name,
        collection_name,
        &op_name,
        lua,
        &mut args,
    ) {
        Ok(request) => request,
        Err(err) => {
            push_lua_table!(
                state,
                "kind" => "ERROR",
                "message" => err
            );
            return Ok(1);
        }
    };

    if matches!(request, DatabaseRequest::Close()) {
        match conn.tx.try_send(request) {
            Ok(()) => {
                laux::lua_push(state, true);
                Ok(1)
            }
            Err(err) => {
                push_lua_table!(
                    state,
                    "kind" => "ERROR",
                    "message" => err.to_string()
                );
                Ok(1)
            }
        }
    } else {
        match try_send_counted(&conn.tx, &conn.counter, request) {
            Ok(_) => {
                laux::lua_push(state, session);
                Ok(1)
            }
            Err(err) => {
                push_lua_table!(
                    state,
                    "kind" => "ERROR",
                    "message" => err.to_string()
                );
                Ok(1)
            }
        }
    }
}

fn push_mongodb_response(state: LuaState, result: DatabaseResponse) -> c_int {
    match result {
        DatabaseResponse::Connect => {
            push_lua_table!(
                state,
                "message" => "Ok"
            );
            1
        }
        DatabaseResponse::CreateCollection => {
            push_lua_table!(
                state,
                "message" => "Ok"
            );
            1
        }
        DatabaseResponse::InsertOne(res) => {
            push_lua_table!(
                state,
                "inserted_id" => res.inserted_id.to_string()
            );
            1
        }
        DatabaseResponse::InsertMany(res) => {
            LuaTable::new(state, 0, res.inserted_ids.len());
            for (i, id) in res.inserted_ids.iter() {
                // The driver keys `inserted_ids` by the 0-based position of the
                // document in the input array; Lua arrays are 1-based, so shift
                // so `result[k]` corresponds to the k-th document passed to
                // `insert_many` (its own `_id` when supplied, else the one the
                // server generated). Without the shift every id landed under a
                // 0..n-1 key that `#result`/`ipairs(result)` would ignore.
                laux::lua_push(state, *i as i64 + 1);
                if let Err(err) = bson_to_lua(state, id) {
                    push_lua_table!(
                        state,
                        "kind" => "ERROR",
                        "message" => err
                    );
                    return 1;
                }
                unsafe { ffi::lua_rawset(state.as_ptr(), -3) };
            }
            1
        }
        DatabaseResponse::DeleteOne(res) => {
            push_lua_table!(
                state,
                "deleted_count" => res.deleted_count
            );
            1
        }
        DatabaseResponse::DeleteMany(res) => {
            push_lua_table!(
                state,
                "deleted_count" => res.deleted_count
            );
            1
        }
        DatabaseResponse::UpdateOne(res) | DatabaseResponse::UpdateMany(res) => {
            let table = LuaTable::new(state, 0, 3);
            table.insert("matched_count", res.matched_count);
            table.insert("modified_count", res.modified_count);

            if let Some(id) = res.upserted_id {
                laux::lua_push(state, "upserted_id");
                if let Err(err) = bson_to_lua(state, &id) {
                    push_lua_table!(
                        state,
                        "kind" => "ERROR",
                        "message" => err
                    );
                    return 1;
                }
                unsafe { ffi::lua_rawset(state.as_ptr(), -3) };
            }

            1
        }
        DatabaseResponse::FindOne(Some(doc)) => {
            if let Err(err) = bson_to_lua(state, &Bson::Document(doc)) {
                push_lua_table!(
                    state,
                    "kind" => "ERROR",
                    "message" => err
                );
            }
            1
        }
        DatabaseResponse::Find(docs) => {
            let table = laux::LuaTable::new(state, 0, docs.len());
            for (i, doc) in docs.into_iter().enumerate() {
                if let Err(err) = bson_to_lua(state, &Bson::Document(doc)) {
                    push_lua_table!(
                        state,
                        "kind" => "ERROR",
                        "message" => err
                    );
                    return 1;
                }
                table.rawseti(i + 1);
            }
            1
        }
        DatabaseResponse::FindBatch(batch) => {
            let table = laux::LuaTable::new(state, 0, batch.docs.len());
            for (i, doc) in batch.docs.into_iter().enumerate() {
                if let Err(err) = bson_to_lua(state, &Bson::Document(doc)) {
                    push_lua_table!(
                        state,
                        "kind" => "ERROR",
                        "message" => err
                    );
                    return 1;
                }
                table.rawseti(i + 1);
            }
            if let Some(next_tx) = batch.next_tx {
                let methods = [
                    lreg!("next", cursor_next),
                    lreg!("close", cursor_close),
                    lreg_null!(),
                ];
                laux::lua_newuserdata(
                    state,
                    CursorHandle(Some(next_tx)),
                    cstr!("mongodb_cursor_handle"),
                    &methods,
                );
            } else {
                laux::lua_pushnil(state);
            }
            2
        }
        DatabaseResponse::ReplacOne(res) => {
            push_lua_table!(
                state,
                "matched_count" => res.matched_count,
                "modified_count" => res.modified_count
            );
            1
        }
        DatabaseResponse::CreateIndex(res) => {
            push_lua_table!(
                state,
                "name" => res.index_name
            );
            1
        }
        DatabaseResponse::Count(count) => {
            push_lua_table!(
                state,
                "count" => count
            );
            1
        }
        DatabaseResponse::Exists(exists) => {
            push_lua_table!(
                state,
                "exists" => exists
            );
            1
        }
        DatabaseResponse::Error(err) => {
            push_lua_table!(
                state,
                "kind" => "ERROR",
                "message" => err.to_string()
            );
            1
        }
        // DatabaseResponse::Timeout(err) => {
        //     push_lua_table!(
        //         state,
        //         "kind" => "ERROR",
        //         "message" => err
        //     );
        //     1
        // }
        _ => {
            laux::lua_pushnil(state);
            1
        }
    }
}

fn find_connection(lua: &mut LuaStack<'_>) -> Result<c_int, String> {
    let state = lua.state();
    let connection = {
        let name = lua
            .value(1)
            .as_str()
            .ok_or_else(|| "mongodb.find_connection: UTF-8 string expected".to_string())?;
        DATABASE_CONNECTIONSS
            .get(name)
            .map(|pair| pair.value().clone())
    };
    match connection {
        Some(connection) => {
            let l = [
                lreg_try!("operators", operators),
                lreg!("close", lua_mongodb_close),
                lreg_null!(),
            ];
            if laux::lua_newuserdata(
                state,
                connection,
                cstr!("mongodb_connection_metatable"),
                l.as_ref(),
            )
            .is_none()
            {
                laux::lua_pushnil(state);
                return Ok(1);
            }
        }
        None => {
            laux::lua_pushnil(state);
        }
    }
    Ok(1)
}

fn stats(lua: &mut LuaStack<'_>) -> c_int {
    let state = lua.state();
    let table = LuaTable::new(state, 0, DATABASE_CONNECTIONSS.len());
    DATABASE_CONNECTIONSS.iter().for_each(|pair| {
        let counter = &pair.value().counter;
        table.rawset_x(pair.key().as_str(), || {
            crate::request_pool::push_pool_stats(
                state,
                counter.load(),
                counter.total(),
                counter.peak(),
                1,
            );
        });
    });
    1
}

fn lua_to_doc(lua: &mut LuaStack<'_>, index: i32) -> Result<Document, String> {
    if lua.value(index).kind() == LuaType::Table {
        table_to_doc(lua, index)
    } else {
        Err(format!("Invalid type: {}", lua.value(index).name()))
    }
}

fn table_to_doc(lua: &mut LuaStack<'_>, index: i32) -> Result<Document, String> {
    let mut doc = Document::new();
    for mut entry in lua.table_cursor(index) {
        let key = {
            let key = entry.key();
            match key.kind() {
                LuaType::String => {
                    String::from_utf8(key.as_bytes().unwrap_or_default().to_vec())
                        .map_err(|_| "Invalid document key: not valid UTF-8".to_string())?
                }
                LuaType::Number => key.as_number().unwrap_or_default().to_string(),
                LuaType::Integer => key.as_integer().unwrap_or_default().to_string(),
                _ => return Err(format!("Invalid key type: {}", key.name())),
            }
        };
        let is_object_id = key == "_id";
        let index = entry.value().index();
        let value = unsafe { lua_to_bson(entry.lua_mut(), index, is_object_id) }?;
        doc.insert(key, value);
    }

    Ok(doc)
}

fn table_to_bson(lua: &mut LuaStack<'_>, index: i32) -> Result<Bson, String> {
    let len = lua.array_len(index);
    if len > 0 {
        let mut arr = Vec::with_capacity(len);
        let mut cursor = lua.array_cursor_len(index, len);
        while let Some(value) = cursor.next() {
            let index = value.index();
            arr.push(unsafe { lua_to_bson(cursor.lua_mut(), index, false) }?);
        }
        return Ok(Bson::Array(arr));
    }

    let doc = table_to_doc(lua, index)?;

    Ok(Bson::Document(doc))
}

fn lua_to_bson(lua: &mut LuaStack<'_>, index: i32, is_object_id: bool) -> Result<Bson, String> {
    let value = lua.value(index);
    match value.kind() {
        LuaType::Nil => Ok(Bson::Null),
        LuaType::Boolean => Ok(Bson::Boolean(value.as_bool().unwrap_or(false))),
        LuaType::Number => Ok(Bson::Double(value.as_number().unwrap_or_default())),
        LuaType::Integer => Ok(Bson::Int64(value.as_integer().unwrap_or_default())),
        LuaType::String => {
            let bytes = value.as_bytes().unwrap_or_default();
            let s = std::str::from_utf8(bytes)
                .map_err(|err| format!("Invalid UTF-8 in string: {err}"))?;
            if is_object_id {
                Ok(Bson::ObjectId(
                    oid::ObjectId::from_str(s).map_err(|err| err.to_string())?,
                ))
            } else {
                Ok(Bson::String(s.to_string()))
            }
        }
        LuaType::Table => table_to_bson(lua, index),
        _ => Err(format!("Invalid type: {}", value.name())),
    }
}

fn bson_to_lua(state: LuaState, value: &Bson) -> Result<(), String> {
    match value {
        Bson::Double(val) => laux::lua_push(state, *val),
        Bson::String(val) => laux::lua_push(state, val.as_str()),
        Bson::Array(bsons) => {
            laux::LuaTable::new(state, bsons.len(), 0);
            for (i, bson) in bsons.iter().enumerate() {
                bson_to_lua(state, bson)?;
                unsafe { ffi::lua_rawseti(state.as_ptr(), -2, (i + 1) as ffi::lua_Integer) };
            }
        }
        Bson::Document(document) => {
            laux::LuaTable::new(state, 0, document.len());
            for (key, value) in document {
                laux::lua_push(state, key.as_str());
                bson_to_lua(state, value)?;
                unsafe { ffi::lua_rawset(state.as_ptr(), -3) };
            }
        }
        Bson::Boolean(val) => laux::lua_push(state, *val),
        Bson::Null => laux::lua_pushnil(state),
        Bson::Int32(val) => laux::lua_push(state, *val),
        Bson::Int64(val) => laux::lua_push(state, *val),
        Bson::Binary(val) => laux::lua_push(state, val.bytes.as_slice()),
        Bson::ObjectId(object_id) => laux::lua_push(state, object_id.to_string()),
        Bson::DateTime(date_time) => laux::lua_push(state, date_time.to_string()),
        Bson::Timestamp(timestamp) => laux::lua_push(state, timestamp.to_string()),
        Bson::Decimal128(decimal128) => laux::lua_push(state, decimal128.to_string()),
        _ => return Err(format!("Unsupported BSON type: {:?}", value)),
    }

    Ok(())
}

fn tt(lua: &mut LuaStack<'_>) -> Result<c_int, String> {
    let state = lua.state();
    let doc = table_to_doc(lua, 1)?;
    let bson = Bson::Document(doc);
    bson_to_lua(state, &bson)?;
    Ok(1)
}

pub unsafe extern "C-unwind" fn decode_mongodb_message(
    state: LuaState,
    m: *mut moon_runtime::context::Message,
) -> c_int {
    match unsafe { crate::message_decode::take_boxed::<DatabaseResponse>(m) } {
        Ok(response) => push_mongodb_response(state, response),
        Err(e) => crate::lua_push_error_tuple(state, &e),
    }
}

pub extern "C-unwind" fn luaopen_mongodb(state: LuaState) -> c_int {
    let l = [
        lreg_try!("connect", connect),
        lreg_try!("find_connection", find_connection),
        lreg!("stats", stats),
        lreg_try!("tt", tt),
        lreg_null!(),
    ];

    luaL_newlib!(state, l);

    1
}

#[cfg(test)]
mod tests {
    use super::*;
    use moon_base::laux::LuaGlobalState;
    use std::ptr::{NonNull, null_mut};

    fn new_state() -> (LuaState, LuaGlobalState) {
        let state = NonNull::new(unsafe { ffi::luaL_newstate() }).expect("Lua state allocation");
        let owner = LuaGlobalState::new(state);
        (state, owner)
    }

    unsafe fn rawset(state: LuaState, key: &str, push_value: impl FnOnce()) {
        laux::lua_push(state, key);
        push_value();
        unsafe { ffi::lua_rawset(state.as_ptr(), -3) };
    }

    #[test]
    fn nested_bson_conversion_restores_the_lua_stack() {
        let (state, _owner) = new_state();
        unsafe {
            ffi::lua_createtable(state.as_ptr(), 0, 4);
            rawset(state, "_id", || {
                laux::lua_push(state, "507f1f77bcf86cd799439011")
            });
            rawset(state, "name", || laux::lua_push(state, "moon"));
            rawset(state, "nested", || {
                ffi::lua_createtable(state.as_ptr(), 0, 3);
                rawset(state, "enabled", || laux::lua_push(state, true));
                rawset(state, "values", || {
                    ffi::lua_createtable(state.as_ptr(), 3, 0);
                    for value in 1_i64..=3 {
                        laux::lua_push(state, value);
                        ffi::lua_rawseti(state.as_ptr(), -2, value);
                    }
                });
                rawset(state, "empty", || {
                    ffi::lua_createtable(state.as_ptr(), 0, 0)
                });
            });
        }

        let mut lua = unsafe { LuaStack::from_raw(state) };
        let top = lua.top();
        let document = table_to_doc(&mut lua, 1).expect("nested document conversion");

        assert_eq!(lua.top(), top);
        assert_eq!(
            document,
            doc! {
                "_id": oid::ObjectId::from_str("507f1f77bcf86cd799439011").unwrap(),
                "name": "moon",
                "nested": {
                    "enabled": true,
                    "values": [1_i64, 2_i64, 3_i64],
                    "empty": {},
                },
            }
        );
    }

    #[test]
    fn nested_bson_error_restores_the_lua_stack() {
        let (state, _owner) = new_state();
        unsafe {
            ffi::lua_createtable(state.as_ptr(), 0, 1);
            rawset(state, "nested", || {
                ffi::lua_createtable(state.as_ptr(), 0, 1);
                rawset(state, "bad", || {
                    ffi::lua_pushlightuserdata(state.as_ptr(), null_mut())
                });
            });
        }

        let mut lua = unsafe { LuaStack::from_raw(state) };
        let top = lua.top();
        let error = table_to_doc(&mut lua, 1).expect_err("unsupported nested Lua value");

        assert_eq!(error, "Invalid type: lightuserdata");
        assert_eq!(lua.top(), top);
    }
}
