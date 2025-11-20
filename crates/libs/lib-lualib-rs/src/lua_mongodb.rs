use dashmap::DashMap;
use futures::stream::TryStreamExt;
use lazy_static::lazy_static;
use lib_core::actor::LuaActor;
use lib_core::context::{self, CONTEXT};
use lib_lua::{
    cstr, ffi,
    laux::{self, LuaArgs, LuaState, LuaTable, LuaValue},
    lreg, lreg_null, luaL_newlib, push_lua_table,
};
use mongodb::{
    bson::{doc, oid, Bson, Document},
    error::Error,
    options::{ClientOptions, CreateIndexOptions, FindOptions, IndexOptions, ReadConcern},
    results, Client, Collection, IndexModel,
};
use std::{
    ffi::c_int,
    str::FromStr,
    sync::{atomic::AtomicI64, Arc},
    time::Duration,
};
use tokio::sync::mpsc;

lazy_static! {
    static ref DATABASE_CONNECTIONSS: DashMap<String, DatabaseConnection> = DashMap::new();
}

enum DatabaseRequest {
    CreateCollection(i64, i64, String, String), // owner, session, db_name, collection_name
    InsertOne(i64, i64, String, String, Document), // owner, session, db_name, collection_name, doc
    InsertMany(i64, i64, String, String, Vec<Document>), // owner, session, db_name, collection_name, docs
    DeleteOne(i64, i64, String, String, Document), // owner, session, db_name, collection_name, filter
    DeleteMany(i64, i64, String, String, Document), // owner, session, db_name, collection_name, filter
    UpdateOne(i64, i64, String, String, Document, Document), // owner, session, db_name, collection_name, filter, update
    UpdateMany(i64, i64, String, String, Document, Document), // owner, session, db_name, collection_name, filter, update
    FindOne(i64, i64, String, String, Document), // owner, session, db_name, collection_name, filter
    Find(i64, i64, String, String, Document, Box<Option<FindOptions>>), // owner, session, db_name, collection_name, filter
    ReplacOne(i64, i64, String, String, Document, Document), // owner, session, db_name, collection_name, filter, replacement
    Count(i64, i64, String, String, Document), // owner, session, db_name, collection_name, filter
    Exists(i64, i64, String, String, Document), // owner, session, db_name, collection_name, filter,
    CreateIndex(
        i64,
        i64,
        String,
        String,
        Box<IndexModel>,
        Box<Option<CreateIndexOptions>>,
    ), // owner, session, db_name, collection_name, keys, options
    Close(),
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
    ReplacOne(results::UpdateResult),
    Count(u64),
    Exists(bool),

    CreateIndex(results::CreateIndexResult),
    Error(Error),
    // Timeout(String),
}

#[derive(Clone)]
struct DatabaseConnection {
    tx: mpsc::UnboundedSender<DatabaseRequest>,
    counter: Arc<AtomicI64>,
}

struct DatabaseState {
    protocol_type: u8,
    database_url: String,
    client: Client,
}

impl DatabaseState {
    async fn connect(protocol_type: u8, database_url: String) -> Result<Self, Error> {
        let options = ClientOptions::parse(&database_url).await?;

        Ok(DatabaseState {
            protocol_type,
            database_url,
            client: Client::with_options(options)?,
        })
    }

    fn send_result(&self, owner: i64, session: i64, res: Result<DatabaseResponse, Error>) {
        match res {
            Ok(res) => {
                if session != 0 {
                    CONTEXT.send_value(self.protocol_type, owner, session, res);
                }
            }
            Err(err) => {
                if session != 0 {
                    CONTEXT.send_value(
                        self.protocol_type,
                        owner,
                        session,
                        DatabaseResponse::Error(err),
                    );
                } else {
                    log::error!(
                        "Database '{}' error: '{:?}'. Will retry. ({}:{})",
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

async fn database_handler(
    state: DatabaseState,
    mut rx: mpsc::UnboundedReceiver<DatabaseRequest>,
    counter: Arc<AtomicI64>,
) {
    while let Some(op) = rx.recv().await {
        // let mut failed_times = 0;
        match op {
            DatabaseRequest::CreateCollection(owner, session, db_name, collection_name) => {
                let db = state.client.database(&db_name);
                state.send_result(
                    owner,
                    session,
                    db.create_collection(collection_name)
                        .await
                        .map(|_| DatabaseResponse::CreateCollection),
                );
            }
            DatabaseRequest::InsertOne(owner, session, db_name, collection_name, doc) => {
                let db = state.client.database(&db_name);
                let coll: Collection<Document> = db.collection(&collection_name);
                state.send_result(
                    owner,
                    session,
                    coll.insert_one(doc).await.map(DatabaseResponse::InsertOne),
                );
            }
            DatabaseRequest::InsertMany(owner, session, db_name, collection_name, docs) => {
                let db = state.client.database(&db_name);
                let coll: Collection<Document> = db.collection(&collection_name);
                state.send_result(
                    owner,
                    session,
                    coll.insert_many(docs)
                        .await
                        .map(DatabaseResponse::InsertMany),
                );
            }
            DatabaseRequest::DeleteOne(owner, session, db_name, collection_name, filter) => {
                let db = state.client.database(&db_name);
                let coll: Collection<Document> = db.collection(&collection_name);
                state.send_result(
                    owner,
                    session,
                    coll.delete_one(filter.clone())
                        .await
                        .map(DatabaseResponse::DeleteOne),
                );
            }
            DatabaseRequest::DeleteMany(owner, session, db_name, collection_name, filter) => {
                let db = state.client.database(&db_name);
                let coll: Collection<Document> = db.collection(&collection_name);
                state.send_result(
                    owner,
                    session,
                    coll.delete_many(filter.clone())
                        .await
                        .map(DatabaseResponse::DeleteMany),
                );
            }
            DatabaseRequest::UpdateOne(
                owner,
                session,
                db_name,
                collection_name,
                filter,
                update,
            ) => {
                let db = state.client.database(&db_name);
                let coll: Collection<Document> = db.collection(&collection_name);
                state.send_result(
                    owner,
                    session,
                    coll.update_one(filter.clone(), update.clone())
                        .await
                        .map(DatabaseResponse::UpdateOne),
                );
            }
            DatabaseRequest::UpdateMany(
                owner,
                session,
                db_name,
                collection_name,
                filter,
                update,
            ) => {
                let db = state.client.database(&db_name);
                let coll: Collection<Document> = db.collection(&collection_name);
                state.send_result(
                    owner,
                    session,
                    coll.update_many(filter.clone(), update.clone())
                        .await
                        .map(DatabaseResponse::UpdateMany),
                );
            }
            DatabaseRequest::FindOne(owner, session, db_name, collection_name, filter) => {
                let db = state.client.database(&db_name);
                let coll: Collection<Document> = db.collection(&collection_name);
                state.send_result(
                    owner,
                    session,
                    coll.find_one(filter.clone())
                        .await
                        .map(|doc: Option<Document>| DatabaseResponse::FindOne(doc)),
                );
            }
            DatabaseRequest::Find(owner, session, db_name, collection_name, filter, options) => {
                let db = state.client.database(&db_name);
                let coll: Collection<Document> = db.collection(&collection_name);
                let result = coll.find(filter.clone()).with_options(*options).await;
                match result {
                    Ok(cur) => {
                        state.send_result(
                            owner,
                            session,
                            cur.try_collect().await.map(DatabaseResponse::Find),
                        );
                    }
                    Err(err) => {
                        state.send_result(owner, session, Err(err));
                    }
                }
            }
            DatabaseRequest::ReplacOne(
                owner,
                session,
                db_name,
                collection_name,
                filter,
                replacement,
            ) => {
                let db = state.client.database(&db_name);
                let coll: Collection<Document> = db.collection(&collection_name);
                state.send_result(
                    owner,
                    session,
                    coll.replace_one(filter.clone(), replacement.clone())
                        .await
                        .map(DatabaseResponse::ReplacOne),
                );
            }
            DatabaseRequest::Count(owner, session, db_name, collection_name, filter) => {
                let db = state.client.database(&db_name);
                let coll: Collection<Document> = db.collection(&collection_name);
                state.send_result(
                    owner,
                    session,
                    coll.count_documents(filter.clone())
                        .await
                        .map(DatabaseResponse::Count),
                );
            }
            DatabaseRequest::Exists(owner, session, db_name, collection_name, filter) => {
                let db = state.client.database(&db_name);
                let coll: Collection<Document> = db.collection(&collection_name);
                state.send_result(
                    owner,
                    session,
                    coll.find_one(filter.clone())
                        .await
                        .map(|doc: Option<Document>| {
                            if doc.is_some() {
                                DatabaseResponse::Exists(true)
                            } else {
                                DatabaseResponse::Exists(false)
                            }
                        }),
                );
            }
            DatabaseRequest::CreateIndex(
                owner,
                session,
                db_name,
                collection_name,
                index,
                options,
            ) => {
                let db = state.client.database(&db_name);
                let coll: Collection<Document> = db.collection(&collection_name);
                state.send_result(
                    owner,
                    session,
                    coll.create_index(*index)
                        .with_options(*options)
                        .await
                        .map(DatabaseResponse::CreateIndex),
                );
            }

            DatabaseRequest::Close() => {
                break;
            }
        }
        counter.fetch_sub(1, std::sync::atomic::Ordering::Release);
    }
}

extern "C-unwind" fn connect(state: LuaState) -> c_int {
    let mut args = LuaArgs::new(1);
    let database_url: &str = laux::lua_get(state, args.iter_arg());
    let name: &str = laux::lua_get(state, args.iter_arg());

    let actor = LuaActor::from_lua_state(state);
    let owner = actor.id;
    let session = actor.next_session();

    tokio::spawn(async move {
        match DatabaseState::connect(context::PTYPE_MONGODB, database_url.to_string()).await {
            Ok(state) => {
                let (tx, rx) = mpsc::unbounded_channel();
                let counter = Arc::new(AtomicI64::new(0));
                DATABASE_CONNECTIONSS.insert(
                    name.to_string(),
                    DatabaseConnection {
                        tx: tx.clone(),
                        counter: counter.clone(),
                    },
                );

                CONTEXT.send_value(
                    context::PTYPE_MONGODB,
                    owner,
                    session,
                    DatabaseResponse::Connect,
                );

                database_handler(state, rx, counter).await;
            }
            Err(err) => {
                CONTEXT.send_value(
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
    owner: i64,
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
        "close" => DatabaseRequest::Close(),
        _ => {
            return Err(format!("Invalid operation: {}", op_name));
        }
    };

    Ok(request)
}

extern "C-unwind" fn operators(state: LuaState) -> c_int {
    let mut args = LuaArgs::new(1);

    let conn = laux::lua_touserdata::<DatabaseConnection>(state, args.iter_arg())
        .expect("Invalid database connect pointer");

    let session = laux::lua_get(state, args.iter_arg());
    let op_name = laux::lua_get::<&str>(state, args.iter_arg());
    let db_name = laux::lua_get(state, args.iter_arg());
    let collection_name = laux::lua_get(state, args.iter_arg());

    let actor = LuaActor::from_lua_state(state);
    let owner = actor.id;

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

    match conn.tx.send(request) {
        Ok(_) => {
            conn.counter
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
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

extern "C-unwind" fn decode(state: LuaState) -> c_int {
    laux::lua_checkstack(state, 6, std::ptr::null());
    let result = laux::lua_into_userdata::<DatabaseResponse>(state, 1);
    match *result {
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
    let name = laux::lua_get::<&str>(state, 1);
    match DATABASE_CONNECTIONSS.get(name) {
        Some(pair) => {
            let l = [lreg!("operators", operators), lreg_null!()];
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
            LuaValue::String(val) => String::from_utf8(val.to_vec()).unwrap(),
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
            let s = unsafe { std::str::from_utf8_unchecked(val) };
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

pub extern "C-unwind" fn luaopen_mongodb(state: LuaState) -> c_int {
    let l = [
        lreg!("connect", connect),
        lreg!("find_connection", find_connection),
        lreg!("decode", decode),
        lreg!("tt", tt),
        lreg_null!(),
    ];

    luaL_newlib!(state, l);

    1
}
