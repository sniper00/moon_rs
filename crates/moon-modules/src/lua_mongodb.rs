use crate::request_pool::{PendingCounter, QueuedRequest, drain_queued_requests};
use dashmap::DashMap;
use futures::stream::TryStreamExt;
use lazy_static::lazy_static;
use mongodb::{
    Client, Collection, IndexModel,
    bson::{Bson, Document, doc, oid},
    error::{Error, ErrorKind},
    options::{ClientOptions, CreateIndexOptions, FindOptions, IndexOptions, ReadConcern},
    results,
};
use moon_lua::{
    cstr, ffi,
    laux::{self, LuaArgs, LuaState, LuaTable, LuaValue},
    lreg, lreg_null, luaL_newlib, push_lua_table,
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
                        "Database '{}' error: '{:?}'. Dropped (fire-and-forget, no retry). ({}:{})",
                        self.database_url,
                        err.to_string(),
                        file!(),
                        line!()
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
                        "mongodb '{}' network error: {}. retrying. ({}:{})",
                        database_url,
                        err,
                        file!(),
                        line!()
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

extern "C-unwind" fn connect(state: LuaState) -> c_int {
    let mut args = LuaArgs::new(1);
    let database_url = unsafe { laux::lua_check_str(state, args.iter_arg()) };
    let name = unsafe { laux::lua_check_str(state, args.iter_arg()) };
    let queue_capacity: usize =
        laux::lua_opt(state, args.iter_arg()).unwrap_or(crate::LIMITS.request_queue_capacity);
    let queue_capacity = queue_capacity.max(1);

    let actor = LuaActor::from_lua_state(state);
    let owner = unsafe { (*actor).id };
    let session = unsafe { (*actor).next_session() };

    CONTEXT.io_runtime().spawn(async move {
        match DatabaseState::connect(context::PTYPE_MONGODB, database_url.to_string()).await {
            Ok(state) => {
                let (tx, rx) = mpsc::channel(queue_capacity);
                let counter = PendingCounter::new();
                // Replacing an existing connection of the same name: tell the
                // previous handler to close so its task and mongodb Client don't
                // leak (it drains and fails any queued requests, then exits).
                if let Some(old) = DATABASE_CONNECTIONSS.insert(
                    name.to_string(),
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
    1
}

fn extract_find_options(options: LuaTable) -> Result<FindOptions, String> {
    let mut find_options = FindOptions::default();
    for (key, value) in options.iter() {
        if let LuaValue::String(name) = key {
            let key = String::from_utf8_lossy(name).into_owned();
            match key.as_str() {
                "limit" => {
                    if let LuaValue::Integer(val) = value {
                        find_options.limit = Some(val);
                    }
                }
                "skip" => {
                    if let LuaValue::Integer(val) = value {
                        find_options.skip = Some(val as u64);
                    }
                }
                "sort" => {
                    if let LuaValue::Table(val) = value {
                        let sort = table_to_doc(val)?;

                        find_options.sort = Some(sort);
                    } else {
                        return Err(format!("Invalid sort value type: {:?}", value.name()));
                    }
                }
                "projection" => {
                    if let LuaValue::Table(val) = value {
                        let projection = table_to_doc(val)?;
                        find_options.projection = Some(projection);
                    } else {
                        return Err(format!("Invalid projection value type: {:?}", value.name()));
                    }
                }
                "max_time" => {
                    if let LuaValue::Integer(val) = value {
                        find_options.max_time = Some(Duration::from_millis(val as u64));
                    }
                }
                "batch_size" => {
                    if let LuaValue::Integer(val) = value {
                        find_options.batch_size = Some(val as u32);
                    }
                }
                "allow_partial_results" => {
                    if let LuaValue::Boolean(val) = value {
                        find_options.allow_partial_results = Some(val);
                    }
                }
                "no_cursor_timeout" => {
                    if let LuaValue::Boolean(val) = value {
                        find_options.no_cursor_timeout = Some(val);
                    }
                }
                "cursor_type" => {
                    if let LuaValue::String(val) = value {
                        match val {
                            b"NonTailable" => {
                                find_options.cursor_type =
                                    Some(mongodb::options::CursorType::NonTailable);
                            }
                            b"Tailable" => {
                                find_options.cursor_type =
                                    Some(mongodb::options::CursorType::Tailable);
                            }
                            b"TailableAwait" => {
                                find_options.cursor_type =
                                    Some(mongodb::options::CursorType::TailableAwait);
                            }
                            _ => {
                                return Err(format!(
                                    "Invalid cursor type: {}",
                                    String::from_utf8_lossy(val)
                                ));
                            }
                        }
                    }
                }
                "read_concern" => {
                    if let LuaValue::String(val) = value {
                        find_options.read_concern =
                            Some(ReadConcern::custom(String::from_utf8_lossy(val)));
                    }
                }
                _ => {
                    return Err(format!("Invalid find_options key: '{}'", key));
                }
            }
        }
    }
    Ok(find_options)
}

fn extract_create_index_options(options: LuaTable) -> Result<CreateIndexOptions, String> {
    let mut create_index_options = CreateIndexOptions::default();
    for (key, value) in options.iter() {
        if let LuaValue::String(name) = key {
            let key = String::from_utf8_lossy(name).into_owned();
            match key.as_str() {
                "max_time" => {
                    if let LuaValue::Integer(val) = value {
                        create_index_options.max_time = Some(Duration::from_secs(val as u64));
                    }
                }
                _ => {
                    return Err(format!("Invalid key: {}", key));
                }
            }
        }
    }
    Ok(create_index_options)
}

fn extract_index_options(table: LuaTable) -> Result<IndexOptions, String> {
    let mut index_options = IndexOptions::default();
    for (key, value) in table.iter() {
        if let LuaValue::String(name) = key {
            let key = String::from_utf8_lossy(name).into_owned();
            match key.as_str() {
                "name" => {
                    if let LuaValue::String(val) = value {
                        index_options.name = Some(String::from_utf8_lossy(val).into_owned());
                    }
                }
                "unique" => {
                    if let LuaValue::Boolean(val) = value {
                        index_options.unique = Some(val);
                    }
                }
                "background" => {
                    if let LuaValue::Boolean(val) = value {
                        index_options.background = Some(val);
                    }
                }
                "sparse" => {
                    if let LuaValue::Boolean(val) = value {
                        index_options.sparse = Some(val);
                    }
                }
                "storage_engine" => {
                    if let LuaValue::Table(val) = value {
                        index_options.storage_engine = Some(table_to_doc(val)?);
                    } else {
                        return Err(format!(
                            "Invalid storage_engine value type: {:?}",
                            value.name()
                        ));
                    }
                }
                "partial_filter_expression" => {
                    if let LuaValue::Table(val) = value {
                        let partial_filter_expression = table_to_doc(val)?;
                        index_options.partial_filter_expression = Some(partial_filter_expression);
                    } else {
                        return Err(format!(
                            "Invalid partial_filter_expression value type: {:?}",
                            value.name()
                        ));
                    }
                }
                "wildcard_projection" => {
                    if let LuaValue::Table(val) = value {
                        index_options.wildcard_projection = Some(table_to_doc(val)?);
                    } else {
                        return Err(format!(
                            "Invalid wildcard_projection value type: {:?}",
                            value.name()
                        ));
                    }
                }
                "hidden" => {
                    if let LuaValue::Boolean(val) = value {
                        index_options.hidden = Some(val);
                    }
                }
                "default_language" => {
                    if let LuaValue::String(val) = value {
                        index_options.default_language =
                            Some(String::from_utf8_lossy(val).into_owned());
                    }
                }
                "language_override" => {
                    if let LuaValue::String(val) = value {
                        index_options.language_override =
                            Some(String::from_utf8_lossy(val).into_owned());
                    }
                }
                "weights" => {
                    if let LuaValue::Table(val) = value {
                        let weights = table_to_doc(val)?;
                        index_options.weights = Some(weights);
                    } else {
                        return Err(format!("Invalid weights value type: {:?}", value.name()));
                    }
                }
                "bits" => {
                    if let LuaValue::Integer(val) = value {
                        index_options.bits = Some(val as u32);
                    }
                }
                "max" => {
                    if let LuaValue::Number(val) = value {
                        index_options.max = Some(val);
                    }
                }
                "min" => {
                    if let LuaValue::Number(val) = value {
                        index_options.min = Some(val);
                    }
                }
                "bucket_size" => {
                    if let LuaValue::Integer(val) = value {
                        index_options.bucket_size = Some(val as u32);
                    }
                }
                _ => {
                    return Err(format!("Invalid key: {}", key));
                }
            }
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
    state: LuaState,
    args: &mut LuaArgs,
) -> Result<DatabaseRequest, String> {
    let request = match op_name {
        "create_coll" => {
            DatabaseRequest::CreateCollection(owner, session, db_name, collection_name)
        }
        "insert_one" => {
            let doc = table_to_doc(LuaTable::from_stack(state, args.iter_arg()))?;
            DatabaseRequest::InsertOne(owner, session, db_name, collection_name, doc)
        }
        "insert_many" => {
            let docs = LuaTable::from_stack(state, args.iter_arg())
                .iter()
                .map(|(_, doc)| lua_to_doc(doc))
                .collect::<Result<Vec<Document>, String>>()?;
            DatabaseRequest::InsertMany(owner, session, db_name, collection_name, docs)
        }
        "delete_one" => {
            let filter = table_to_doc(LuaTable::from_stack(state, args.iter_arg()))?;
            DatabaseRequest::DeleteOne(owner, session, db_name, collection_name, filter)
        }
        "delete_many" => {
            let filter = table_to_doc(LuaTable::from_stack(state, args.iter_arg()))?;
            DatabaseRequest::DeleteMany(owner, session, db_name, collection_name, filter)
        }
        "update_one" => {
            let filter = table_to_doc(LuaTable::from_stack(state, args.iter_arg()))?;
            let update = table_to_doc(LuaTable::from_stack(state, args.iter_arg()))?;
            DatabaseRequest::UpdateOne(owner, session, db_name, collection_name, filter, update)
        }
        "update_many" => {
            let filter = table_to_doc(LuaTable::from_stack(state, args.iter_arg()))?;
            let update = table_to_doc(LuaTable::from_stack(state, args.iter_arg()))?;
            DatabaseRequest::UpdateMany(owner, session, db_name, collection_name, filter, update)
        }
        "find_one" => {
            let filter = table_to_doc(LuaTable::from_stack(state, args.iter_arg()))?;
            DatabaseRequest::FindOne(owner, session, db_name, collection_name, filter)
        }
        "find" => {
            let filter = table_to_doc(LuaTable::from_stack(state, args.iter_arg()))?;
            let find_options =
                if let LuaValue::Table(options) = LuaValue::from_stack(state, args.iter_arg()) {
                    Some(extract_find_options(options)?)
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
            let filter = table_to_doc(LuaTable::from_stack(state, args.iter_arg()))?;
            let replacement = table_to_doc(LuaTable::from_stack(state, args.iter_arg()))?;
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
            let filter = table_to_doc(LuaTable::from_stack(state, args.iter_arg()))?;
            DatabaseRequest::Count(owner, session, db_name, collection_name, filter)
        }
        "exists" => {
            let filter = table_to_doc(LuaTable::from_stack(state, args.iter_arg()))?;
            DatabaseRequest::Exists(owner, session, db_name, collection_name, filter)
        }
        "create_index" => {
            let keys = table_to_doc(LuaTable::from_stack(state, args.iter_arg()))?;

            let index_options =
                if let LuaValue::Table(options) = LuaValue::from_stack(state, args.iter_arg()) {
                    Some(extract_index_options(options)?)
                } else {
                    None
                };

            let options =
                if let LuaValue::Table(options) = LuaValue::from_stack(state, args.iter_arg()) {
                    Some(extract_create_index_options(options)?)
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
            let filter = table_to_doc(LuaTable::from_stack(state, args.iter_arg()))?;
            let find_options =
                if let LuaValue::Table(options) = LuaValue::from_stack(state, args.iter_arg()) {
                    Some(extract_find_options(options)?)
                } else {
                    None
                };
            // Parse as i64 so a negative Lua integer is rejected rather than
            // wrapping to a huge `usize`. `batch_size == 0` would make the
            // handler loop emit empty batches forever, so require >= 1.
            let batch_size: i64 =
                laux::lua_opt(state, args.iter_arg()).unwrap_or(crate::LIMITS.db_stream_batch_rows);
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

extern "C-unwind" fn lua_mongodb_close(state: LuaState) -> c_int {
    let conn = laux::lua_touserdata::<DatabaseConnection>(state, 1)
        .expect("Invalid database connect pointer");
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

extern "C-unwind" fn cursor_next(state: LuaState) -> c_int {
    let handle = laux::lua_touserdata::<CursorHandle>(state, 1).expect("invalid cursor handle");
    if let Some(tx) = handle.0.take() {
        let actor = LuaActor::from_lua_state(state);
        let owner = unsafe { (*actor).id };
        let session = unsafe { (*actor).next_session() };
        // If the stream handler has already exited, its receiver is gone and the
        // send fails. Surface that as an error rather than pushing a session that
        // would never be answered (which would hang the awaiting coroutine).
        if tx.send(CursorSignal::Next(owner, session)).is_err() {
            return crate::lua_push_error(state, "cursor: stream handler is gone");
        }
        laux::lua_push(state, session);
        1
    } else {
        crate::lua_push_error(state, "cursor: already consumed or closed")
    }
}

extern "C-unwind" fn cursor_close(state: LuaState) -> c_int {
    let handle = laux::lua_touserdata::<CursorHandle>(state, 1).expect("invalid cursor handle");
    if let Some(tx) = handle.0.take() {
        let _ = tx.send(CursorSignal::Close);
    }
    0
}

extern "C-unwind" fn operators(state: LuaState) -> c_int {
    let mut args = LuaArgs::new(1);

    let conn = laux::lua_touserdata::<DatabaseConnection>(state, args.iter_arg())
        .expect("Invalid database connect pointer");

    let op_name = unsafe { laux::lua_check_str(state, args.iter_arg()) };
    let db_name = laux::lua_get(state, args.iter_arg());
    let collection_name = laux::lua_get(state, args.iter_arg());

    let actor = LuaActor::from_lua_state(state);
    let owner = unsafe { (*actor).id };
    let session = unsafe { (*actor).next_session() };

    let request = match make_request(
        owner,
        session,
        db_name,
        collection_name,
        op_name,
        state,
        &mut args,
    ) {
        Ok(request) => request,
        Err(err) => {
            push_lua_table!(
                state,
                "kind" => "ERROR",
                "message" => err
            );
            return 1;
        }
    };

    if matches!(request, DatabaseRequest::Close()) {
        match conn.tx.try_send(request) {
            Ok(()) => {
                laux::lua_push(state, true);
                1
            }
            Err(err) => {
                push_lua_table!(
                    state,
                    "kind" => "ERROR",
                    "message" => err.to_string()
                );
                1
            }
        }
    } else {
        match conn.tx.try_send(request) {
            Ok(_) => {
                conn.counter.inc();
                laux::lua_push(state, session);
                1
            }
            Err(err) => {
                push_lua_table!(
                    state,
                    "kind" => "ERROR",
                    "message" => err.to_string()
                );
                1
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
                laux::lua_push(state, *i);
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

extern "C-unwind" fn find_connection(state: LuaState) -> c_int {
    let name = unsafe { laux::lua_check_str(state, 1) };
    match DATABASE_CONNECTIONSS.get(name) {
        Some(pair) => {
            let l = [
                lreg!("operators", operators),
                lreg!("close", lua_mongodb_close),
                lreg_null!(),
            ];
            if laux::lua_newuserdata(
                state,
                pair.value().clone(),
                cstr!("mongodb_connection_metatable"),
                l.as_ref(),
            )
            .is_none()
            {
                laux::lua_pushnil(state);
                return 1;
            }
        }
        None => {
            laux::lua_pushnil(state);
        }
    }
    1
}

fn lua_to_doc(value: LuaValue) -> Result<Document, String> {
    match value {
        LuaValue::Table(val) => table_to_doc(val),
        val => Err(format!("Invalid type: {}", val.name())),
    }
}

fn table_to_doc(table: LuaTable) -> Result<Document, String> {
    let mut doc = Document::new();
    for (key, value) in table.iter() {
        let key = match key {
            LuaValue::String(val) => String::from_utf8(val.to_vec())
                .map_err(|_| "Invalid document key: not valid UTF-8".to_string())?,
            LuaValue::Number(val) => val.to_string(),
            LuaValue::Integer(val) => val.to_string(),
            val => return Err(format!("Invalid key type: {}", val.name())),
        };
        let is_object_id = key == "_id";
        let value = lua_to_bson(value, is_object_id)?;
        doc.insert(key, value);
    }

    Ok(doc)
}

fn table_to_bson(table: LuaTable) -> Result<Bson, String> {
    let len = table.array_len();
    if len > 0 {
        let mut arr = Vec::with_capacity(len);
        for val in table.array_iter() {
            arr.push(lua_to_bson(val, false)?);
        }
        return Ok(Bson::Array(arr));
    }

    let doc = table_to_doc(table)?;

    Ok(Bson::Document(doc))
}

fn lua_to_bson(value: LuaValue, is_object_id: bool) -> Result<Bson, String> {
    match value {
        LuaValue::Nil => Ok(Bson::Null),
        LuaValue::Boolean(val) => Ok(Bson::Boolean(val)),
        LuaValue::Number(val) => Ok(Bson::Double(val)),
        LuaValue::Integer(val) => Ok(Bson::Int64(val)),
        LuaValue::String(val) => {
            let s =
                std::str::from_utf8(val).map_err(|e| format!("Invalid UTF-8 in string: {}", e))?;
            if is_object_id {
                Ok(Bson::ObjectId(
                    oid::ObjectId::from_str(s).map_err(|e| e.to_string())?,
                ))
            } else {
                Ok(Bson::String(s.to_string()))
            }
        }
        LuaValue::Table(val) => Ok(table_to_bson(val)?),
        val => Err(format!("Invalid type: {}", val.name())),
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

extern "C-unwind" fn tt(state: LuaState) -> c_int {
    let table = LuaTable::from_stack(state, 1);
    let doc = table_to_doc(table).unwrap();
    let bson = Bson::Document(doc);
    bson_to_lua(state, &bson).unwrap();
    1
}

pub unsafe extern "C-unwind" fn decode_mongodb_message(
    state: LuaState,
    m: *mut moon_runtime::context::Message,
) -> c_int {
    match unsafe { crate::message_decode::take_boxed::<DatabaseResponse>(m) } {
        Ok(response) => push_mongodb_response(state, response),
        Err(e) => crate::lua_push_error(state, &e),
    }
}

pub extern "C-unwind" fn luaopen_mongodb(state: LuaState) -> c_int {
    let l = [
        lreg!("connect", connect),
        lreg!("find_connection", find_connection),
        lreg!("tt", tt),
        lreg_null!(),
    ];

    luaL_newlib!(state, l);

    1
}
