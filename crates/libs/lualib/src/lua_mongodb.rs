use dashmap::DashMap;
use futures::stream::TryStreamExt;
use lazy_static::lazy_static;
use actor::context::{self, CONTEXT};
use luars::{LuaResult, LuaState, LuaValue};
use mongodb::{
    bson::{doc, oid, Bson, Document},
    error::Error,
    options::{ClientOptions, CreateIndexOptions, FindOptions, IndexOptions, ReadConcern},
    results, Client, Collection, IndexModel,
};
use std::{
    ffi::c_void,
    str::FromStr,
    sync::{atomic::AtomicI64, Arc},
    time::Duration,
};
use tokio::sync::mpsc;

use crate::{
    lua_check_str,
    lua_check_typed_lightuserdata_ref,
    lua_take_typed_lightuserdata,
    lua_opt_integer,
    push_error_table,
    push_message_table,
};
use crate::lua_actor::ActorRef;

lazy_static! {
    static ref DATABASE_CONNECTIONS: DashMap<String, DatabaseConnection> = DashMap::new();
}

struct DbOp {
    owner: i64,
    session: i64,
    db_name: String,
    collection_name: String,
    kind: DbOpKind,
}

enum DbOpKind {
    CreateCollection,
    InsertOne(Document),
    InsertMany(Vec<Document>),
    DeleteOne(Document),
    DeleteMany(Document),
    UpdateOne(Document, Document),
    UpdateMany(Document, Document),
    FindOne(Document),
    Find(Document, Box<Option<FindOptions>>),
    ReplaceOne(Document, Document),
    Count(Document),
    Exists(Document),
    CreateIndex(Box<IndexModel>, Box<Option<CreateIndexOptions>>),
}

enum DatabaseRequest {
    Op(DbOp),
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
    ReplaceOne(results::UpdateResult),
    Count(u64),
    Exists(bool),
    CreateIndex(results::CreateIndexResult),
    Error(Error),
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

    fn collection(&self, db_name: &str, collection_name: &str) -> Collection<Document> {
        self.client.database(db_name).collection(collection_name)
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
    name: String,
    state: DatabaseState,
    mut rx: mpsc::UnboundedReceiver<DatabaseRequest>,
    counter: Arc<AtomicI64>,
) {
    while let Some(req) = rx.recv().await {
        match req {
            DatabaseRequest::Op(op) => {
                let coll = state.collection(&op.db_name, &op.collection_name);
                let res = match op.kind {
                    DbOpKind::CreateCollection => {
                        state.client.database(&op.db_name)
                            .create_collection(&op.collection_name)
                            .await
                            .map(|_| DatabaseResponse::CreateCollection)
                    }
                    DbOpKind::InsertOne(doc) => {
                        coll.insert_one(doc).await.map(DatabaseResponse::InsertOne)
                    }
                    DbOpKind::InsertMany(docs) => {
                        coll.insert_many(docs).await.map(DatabaseResponse::InsertMany)
                    }
                    DbOpKind::DeleteOne(filter) => {
                        coll.delete_one(filter).await.map(DatabaseResponse::DeleteOne)
                    }
                    DbOpKind::DeleteMany(filter) => {
                        coll.delete_many(filter).await.map(DatabaseResponse::DeleteMany)
                    }
                    DbOpKind::UpdateOne(filter, update) => {
                        coll.update_one(filter, update).await.map(DatabaseResponse::UpdateOne)
                    }
                    DbOpKind::UpdateMany(filter, update) => {
                        coll.update_many(filter, update).await.map(DatabaseResponse::UpdateMany)
                    }
                    DbOpKind::FindOne(filter) => {
                        coll.find_one(filter).await.map(DatabaseResponse::FindOne)
                    }
                    DbOpKind::Find(filter, options) => {
                        match coll.find(filter).with_options(*options).await {
                            Ok(cur) => cur.try_collect().await.map(DatabaseResponse::Find),
                            Err(err) => Err(err),
                        }
                    }
                    DbOpKind::ReplaceOne(filter, replacement) => {
                        coll.replace_one(filter, replacement).await.map(DatabaseResponse::ReplaceOne)
                    }
                    DbOpKind::Count(filter) => {
                        coll.count_documents(filter).await.map(DatabaseResponse::Count)
                    }
                    DbOpKind::Exists(filter) => {
                        coll.find_one(filter).await.map(|doc| DatabaseResponse::Exists(doc.is_some()))
                    }
                    DbOpKind::CreateIndex(index, options) => {
                        coll.create_index(*index).with_options(*options).await.map(DatabaseResponse::CreateIndex)
                    }
                };
                state.send_result(op.owner, op.session, res);
            }
            DatabaseRequest::Close() => {
                break;
            }
        }
        counter.fetch_sub(1, std::sync::atomic::Ordering::Release);
    }
    DATABASE_CONNECTIONS.remove(&name);
    log::info!("MongoDB connection '{}' closed and removed.", name);
}

fn connect(state: &mut LuaState) -> LuaResult<usize> {
    let database_url = lua_check_str(state, 1)?.to_string();
    let name = lua_check_str(state, 2)?.to_string();

    let actor = ActorRef::from_state(state);
    let owner = actor.id();
    let session = actor.next_session();

    tokio::spawn(async move {
        match DatabaseState::connect(context::PTYPE_MONGODB, database_url).await {
            Ok(db_state) => {
                let (tx, rx) = mpsc::unbounded_channel();
                let counter = Arc::new(AtomicI64::new(0));
                DATABASE_CONNECTIONS.insert(
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

                database_handler(name, db_state, rx, counter).await;
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

    state.push_value(LuaValue::integer(session))?;
    Ok(1)
}

fn extract_find_options(
    state: &mut LuaState,
    options: &LuaValue,
) -> Result<FindOptions, String> {
    let lua_table = options.as_table().ok_or("expected table for find options")?;
    let entries = lua_table.iter_all();
    let mut find_options = FindOptions::default();

    for (key, value) in entries.iter() {
        if let Some(name) = key.as_str() {
            match name {
                "limit" => {
                    if let Some(val) = value.as_integer() {
                        find_options.limit = Some(val);
                    }
                }
                "skip" => {
                    if let Some(val) = value.as_integer() {
                        find_options.skip = Some(val as u64);
                    }
                }
                "sort" => {
                    if value.is_table() {
                        find_options.sort = Some(table_to_doc(state, value)?);
                    } else {
                        return Err("Invalid sort value type".to_string());
                    }
                }
                "projection" => {
                    if value.is_table() {
                        find_options.projection = Some(table_to_doc(state, value)?);
                    } else {
                        return Err("Invalid projection value type".to_string());
                    }
                }
                "max_time" => {
                    if let Some(val) = value.as_integer() {
                        find_options.max_time = Some(Duration::from_millis(val as u64));
                    }
                }
                "batch_size" => {
                    if let Some(val) = value.as_integer() {
                        find_options.batch_size = Some(val as u32);
                    }
                }
                "allow_partial_results" => {
                    if let Some(val) = value.as_boolean() {
                        find_options.allow_partial_results = Some(val);
                    }
                }
                "no_cursor_timeout" => {
                    if let Some(val) = value.as_boolean() {
                        find_options.no_cursor_timeout = Some(val);
                    }
                }
                "cursor_type" => {
                    if let Some(val) = value.as_str() {
                        match val {
                            "NonTailable" => {
                                find_options.cursor_type =
                                    Some(mongodb::options::CursorType::NonTailable);
                            }
                            "Tailable" => {
                                find_options.cursor_type =
                                    Some(mongodb::options::CursorType::Tailable);
                            }
                            "TailableAwait" => {
                                find_options.cursor_type =
                                    Some(mongodb::options::CursorType::TailableAwait);
                            }
                            _ => {
                                return Err(format!("Invalid cursor type: {}", val));
                            }
                        }
                    }
                }
                "read_concern" => {
                    if let Some(val) = value.as_str() {
                        find_options.read_concern = Some(ReadConcern::custom(val));
                    }
                }
                _ => {
                    return Err(format!("Invalid find_options key: '{}'", name));
                }
            }
        }
    }
    Ok(find_options)
}

fn extract_create_index_options(
    options: &LuaValue,
) -> Result<CreateIndexOptions, String> {
    let lua_table = options
        .as_table()
        .ok_or("expected table for create index options")?;
    let entries = lua_table.iter_all();
    let mut create_index_options = CreateIndexOptions::default();

    for (key, value) in entries.iter() {
        if let Some(name) = key.as_str() {
            match name {
                "max_time" => {
                    if let Some(val) = value.as_integer() {
                        create_index_options.max_time = Some(Duration::from_secs(val as u64));
                    }
                }
                _ => {
                    return Err(format!("Invalid key: {}", name));
                }
            }
        }
    }
    Ok(create_index_options)
}

fn extract_index_options(
    state: &mut LuaState,
    options: &LuaValue,
) -> Result<IndexOptions, String> {
    let lua_table = options
        .as_table()
        .ok_or("expected table for index options")?;
    let entries = lua_table.iter_all();
    let mut index_options = IndexOptions::default();

    for (key, value) in entries.iter() {
        if let Some(name) = key.as_str() {
            match name {
                "name" => {
                    if let Some(val) = value.as_str() {
                        index_options.name = Some(val.to_string());
                    }
                }
                "unique" => {
                    if let Some(val) = value.as_boolean() {
                        index_options.unique = Some(val);
                    }
                }
                "background" => {
                    if let Some(val) = value.as_boolean() {
                        index_options.background = Some(val);
                    }
                }
                "sparse" => {
                    if let Some(val) = value.as_boolean() {
                        index_options.sparse = Some(val);
                    }
                }
                "storage_engine" => {
                    if value.is_table() {
                        index_options.storage_engine = Some(table_to_doc(state, value)?);
                    } else {
                        return Err("Invalid storage_engine value type".to_string());
                    }
                }
                "partial_filter_expression" => {
                    if value.is_table() {
                        index_options.partial_filter_expression =
                            Some(table_to_doc(state, value)?);
                    } else {
                        return Err("Invalid partial_filter_expression value type".to_string());
                    }
                }
                "wildcard_projection" => {
                    if value.is_table() {
                        index_options.wildcard_projection = Some(table_to_doc(state, value)?);
                    } else {
                        return Err("Invalid wildcard_projection value type".to_string());
                    }
                }
                "hidden" => {
                    if let Some(val) = value.as_boolean() {
                        index_options.hidden = Some(val);
                    }
                }
                "default_language" => {
                    if let Some(val) = value.as_str() {
                        index_options.default_language = Some(val.to_string());
                    }
                }
                "language_override" => {
                    if let Some(val) = value.as_str() {
                        index_options.language_override = Some(val.to_string());
                    }
                }
                "weights" => {
                    if value.is_table() {
                        index_options.weights = Some(table_to_doc(state, value)?);
                    } else {
                        return Err("Invalid weights value type".to_string());
                    }
                }
                "bits" => {
                    if let Some(val) = value.as_integer() {
                        index_options.bits = Some(val as u32);
                    }
                }
                "max" => {
                    if let Some(val) = value.as_number() {
                        index_options.max = Some(val);
                    }
                }
                "min" => {
                    if let Some(val) = value.as_number() {
                        index_options.min = Some(val);
                    }
                }
                "bucket_size" => {
                    if let Some(val) = value.as_integer() {
                        index_options.bucket_size = Some(val as u32);
                    }
                }
                _ => {
                    return Err(format!("Invalid key: {}", name));
                }
            }
        }
    }
    Ok(index_options)
}

fn lua_to_doc(state: &mut LuaState, value: &LuaValue) -> Result<Document, String> {
    if value.is_table() {
        table_to_doc(state, value)
    } else {
        Err("Invalid type: expected table".to_string())
    }
}

fn table_to_doc(state: &mut LuaState, table: &LuaValue) -> Result<Document, String> {
    let lua_table = table.as_table().ok_or("expected table")?;
    let entries = lua_table.iter_all();
    let mut doc = Document::new();

    for (key, value) in entries.iter() {
        let key_str = if let Some(s) = key.as_str() {
            s.to_string()
        } else if let Some(i) = key.as_integer() {
            i.to_string()
        } else if let Some(n) = key.as_number() {
            n.to_string()
        } else {
            return Err("Invalid key type".to_string());
        };
        let is_object_id = key_str == "_id";
        let bson_value = lua_to_bson(state, value, is_object_id)?;
        doc.insert(key_str, bson_value);
    }

    Ok(doc)
}

fn table_to_bson(state: &mut LuaState, table: &LuaValue) -> Result<Bson, String> {
    let lua_table = table.as_table().ok_or("expected table")?;
    let len = lua_table.len();
    if len > 0 {
        let mut arr = Vec::with_capacity(len);
        for i in 1..=len {
            if let Some(val) = lua_table.raw_geti(i as i64) {
                arr.push(lua_to_bson(state, &val, false)?);
            }
        }
        return Ok(Bson::Array(arr));
    }

    let doc = table_to_doc(state, table)?;
    Ok(Bson::Document(doc))
}

fn lua_to_bson(state: &mut LuaState, value: &LuaValue, is_object_id: bool) -> Result<Bson, String> {
    if value.is_nil() {
        Ok(Bson::Null)
    } else if let Some(b) = value.as_boolean() {
        Ok(Bson::Boolean(b))
    } else if value.is_integer() {
        Ok(Bson::Int64(value.as_integer().unwrap()))
    } else if let Some(n) = value.as_number() {
        Ok(Bson::Double(n))
    } else if let Some(s) = value.as_str() {
        if is_object_id {
            Ok(Bson::ObjectId(
                oid::ObjectId::from_str(s).map_err(|e| e.to_string())?,
            ))
        } else {
            Ok(Bson::String(s.to_string()))
        }
    } else if value.is_table() {
        table_to_bson(state, value)
    } else {
        Err("Invalid type for BSON conversion".to_string())
    }
}

fn bson_to_lua(state: &mut LuaState, value: &Bson) -> Result<LuaValue, String> {
    match value {
        Bson::Double(val) => Ok(LuaValue::float(*val)),
        Bson::String(val) => {
            let s = state
                .create_string(val.as_str())
                .map_err(|_| "mongodb: failed to create lua string".to_string())?;
            Ok(s)
        }
        Bson::Array(bsons) => {
            let table = state
                .create_table(bsons.len(), 0)
                .map_err(|_| "mongodb: failed to create lua table".to_string())?;
            for (i, bson) in bsons.iter().enumerate() {
                let val = bson_to_lua(state, bson)?;
                state.raw_seti(&table, (i + 1) as i64, val);
            }
            Ok(table)
        }
        Bson::Document(document) => {
            let table = state
                .create_table(0, document.len())
                .map_err(|_| "mongodb: failed to create lua table".to_string())?;
            for (key, value) in document {
                let k = state
                    .create_string(key.as_str())
                    .map_err(|_| "mongodb: failed to create lua string".to_string())?;
                let v = bson_to_lua(state, value)?;
                state.raw_set(&table, k, v);
            }
            Ok(table)
        }
        Bson::Boolean(val) => Ok(LuaValue::boolean(*val)),
        Bson::Null => Ok(LuaValue::nil()),
        Bson::Int32(val) => Ok(LuaValue::integer(*val as i64)),
        Bson::Int64(val) => Ok(LuaValue::integer(*val)),
        Bson::Binary(val) => {
            let s = state
                .create_string(&String::from_utf8_lossy(&val.bytes))
                .map_err(|_| "mongodb: failed to create lua string".to_string())?;
            Ok(s)
        }
        Bson::ObjectId(object_id) => {
            let s = state
                .create_string(&object_id.to_string())
                .map_err(|_| "mongodb: failed to create lua string".to_string())?;
            Ok(s)
        }
        Bson::DateTime(date_time) => {
            let s = state
                .create_string(&date_time.to_string())
                .map_err(|_| "mongodb: failed to create lua string".to_string())?;
            Ok(s)
        }
        Bson::Timestamp(timestamp) => {
            let s = state
                .create_string(&timestamp.to_string())
                .map_err(|_| "mongodb: failed to create lua string".to_string())?;
            Ok(s)
        }
        Bson::Decimal128(decimal128) => {
            let s = state
                .create_string(&decimal128.to_string())
                .map_err(|_| "mongodb: failed to create lua string".to_string())?;
            Ok(s)
        }
        _ => Err(format!("mongodb: unsupported bson type: {:?}", value)),
    }
}

fn make_request(
    state: &mut LuaState,
    owner: i64,
    session: i64,
    db_name: String,
    collection_name: String,
    op_name: &str,
    arg_idx: &mut usize,
) -> Result<DatabaseRequest, String> {
    if op_name == "close" {
        return Ok(DatabaseRequest::Close());
    }

    let kind = match op_name {
        "create_coll" => DbOpKind::CreateCollection,
        "insert_one" => {
            let table_val = state.get_arg(*arg_idx).ok_or("expected table for insert_one")?;
            *arg_idx += 1;
            DbOpKind::InsertOne(table_to_doc(state, &table_val)?)
        }
        "insert_many" => {
            let table_val = state.get_arg(*arg_idx).ok_or("expected table for insert_many")?;
            *arg_idx += 1;
            let lua_table = table_val.as_table().ok_or("expected table for insert_many")?;
            let len = lua_table.len();
            let mut docs = Vec::with_capacity(len);
            for i in 1..=len {
                if let Some(val) = lua_table.raw_geti(i as i64) {
                    docs.push(lua_to_doc(state, &val)?);
                }
            }
            DbOpKind::InsertMany(docs)
        }
        "delete_one" => {
            let table_val = state.get_arg(*arg_idx).ok_or("expected table for delete_one")?;
            *arg_idx += 1;
            DbOpKind::DeleteOne(table_to_doc(state, &table_val)?)
        }
        "delete_many" => {
            let table_val = state.get_arg(*arg_idx).ok_or("expected table for delete_many")?;
            *arg_idx += 1;
            DbOpKind::DeleteMany(table_to_doc(state, &table_val)?)
        }
        "update_one" | "update_many" => {
            let filter_val = state.get_arg(*arg_idx).ok_or(format!("expected filter table for {}", op_name))?;
            *arg_idx += 1;
            let update_val = state.get_arg(*arg_idx).ok_or(format!("expected update table for {}", op_name))?;
            *arg_idx += 1;
            let filter = table_to_doc(state, &filter_val)?;
            let update = table_to_doc(state, &update_val)?;
            if op_name == "update_one" {
                DbOpKind::UpdateOne(filter, update)
            } else {
                DbOpKind::UpdateMany(filter, update)
            }
        }
        "find_one" => {
            let table_val = state.get_arg(*arg_idx).ok_or("expected table for find_one")?;
            *arg_idx += 1;
            DbOpKind::FindOne(table_to_doc(state, &table_val)?)
        }
        "find" => {
            let filter_val = state.get_arg(*arg_idx).ok_or("expected filter table for find")?;
            *arg_idx += 1;
            let filter = table_to_doc(state, &filter_val)?;

            let options_val = state.get_arg(*arg_idx).unwrap_or(LuaValue::nil());
            *arg_idx += 1;
            let find_options = if options_val.is_table() {
                Some(extract_find_options(state, &options_val)?)
            } else {
                None
            };
            DbOpKind::Find(filter, Box::new(find_options))
        }
        "replace_one" => {
            let filter_val = state.get_arg(*arg_idx).ok_or("expected filter table for replace_one")?;
            *arg_idx += 1;
            let replacement_val = state.get_arg(*arg_idx).ok_or("expected replacement table for replace_one")?;
            *arg_idx += 1;
            DbOpKind::ReplaceOne(table_to_doc(state, &filter_val)?, table_to_doc(state, &replacement_val)?)
        }
        "count" => {
            let table_val = state.get_arg(*arg_idx).ok_or("expected table for count")?;
            *arg_idx += 1;
            DbOpKind::Count(table_to_doc(state, &table_val)?)
        }
        "exists" => {
            let table_val = state.get_arg(*arg_idx).ok_or("expected table for exists")?;
            *arg_idx += 1;
            DbOpKind::Exists(table_to_doc(state, &table_val)?)
        }
        "create_index" => {
            let keys_val = state.get_arg(*arg_idx).ok_or("expected keys table for create_index")?;
            *arg_idx += 1;
            let keys = table_to_doc(state, &keys_val)?;

            let idx_opts_val = state.get_arg(*arg_idx).unwrap_or(LuaValue::nil());
            *arg_idx += 1;
            let index_options = if idx_opts_val.is_table() {
                Some(extract_index_options(state, &idx_opts_val)?)
            } else {
                None
            };

            let create_opts_val = state.get_arg(*arg_idx).unwrap_or(LuaValue::nil());
            *arg_idx += 1;
            let create_options = if create_opts_val.is_table() {
                Some(extract_create_index_options(&create_opts_val)?)
            } else {
                None
            };

            let index = IndexModel::builder().keys(keys).options(index_options).build();
            DbOpKind::CreateIndex(Box::new(index), Box::new(create_options))
        }
        _ => return Err(format!("Invalid operation: {}", op_name)),
    };

    Ok(DatabaseRequest::Op(DbOp { owner, session, db_name, collection_name, kind }))
}

fn operators(state: &mut LuaState) -> LuaResult<usize> {
    let mut arg_idx = 1;

    let conn = lua_check_typed_lightuserdata_ref::<DatabaseConnection>(state, arg_idx)?;
    arg_idx += 1;

    let session: i64 = lua_opt_integer(state, arg_idx).unwrap_or(0);
    arg_idx += 1;

    let op_name = lua_check_str(state, arg_idx)?.to_string();
    arg_idx += 1;

    let db_name = lua_check_str(state, arg_idx)?.to_string();
    arg_idx += 1;

    let collection_name = lua_check_str(state, arg_idx)?.to_string();
    arg_idx += 1;

    let actor = ActorRef::from_state(state);
    let owner = actor.id();

    let request = match make_request(
        state,
        owner,
        session,
        db_name,
        collection_name,
        &op_name,
        &mut arg_idx,
    ) {
        Ok(request) => request,
        Err(err) => return push_error_table(state, "ERROR", &err),
    };

    match conn.tx.send(request) {
        Ok(_) => {
            conn.counter
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            state.push_value(LuaValue::integer(session))?;
            Ok(1)
        }
        Err(err) => push_error_table(state, "ERROR", &err.to_string()),
    }
}

fn decode(state: &mut LuaState) -> LuaResult<usize> {
    let result = lua_take_typed_lightuserdata::<DatabaseResponse>(state, 1)?;

    match *result {
        DatabaseResponse::Connect | DatabaseResponse::CreateCollection => {
            push_message_table(state, "message", "Ok")
        }
        DatabaseResponse::InsertOne(res) => {
            push_message_table(state, "inserted_id", &res.inserted_id.to_string())
        }
        DatabaseResponse::InsertMany(res) => {
            let table = state.create_table(0, res.inserted_ids.len())?;
            for (i, id) in res.inserted_ids.iter() {
                let val = bson_to_lua(state, id).map_err(|e| state.error(e))?;
                state.raw_set(&table, LuaValue::integer(*i as i64), val);
            }
            state.push_value(table)?;
            Ok(1)
        }
        DatabaseResponse::DeleteOne(res) | DatabaseResponse::DeleteMany(res) => {
            let table = state.create_table(0, 1)?;
            let k = state.create_string("deleted_count")?;
            state.raw_set(&table, k, LuaValue::integer(res.deleted_count as i64));
            state.push_value(table)?;
            Ok(1)
        }
        DatabaseResponse::UpdateOne(res)
        | DatabaseResponse::UpdateMany(res)
        | DatabaseResponse::ReplaceOne(res) => {
            let table = state.create_table(0, 3)?;
            let k = state.create_string("matched_count")?;
            state.raw_set(&table, k, LuaValue::integer(res.matched_count as i64));
            let k = state.create_string("modified_count")?;
            state.raw_set(&table, k, LuaValue::integer(res.modified_count as i64));
            if let Some(id) = res.upserted_id {
                let k = state.create_string("upserted_id")?;
                let val = bson_to_lua(state, &id).map_err(|e| state.error(e))?;
                state.raw_set(&table, k, val);
            }
            state.push_value(table)?;
            Ok(1)
        }
        DatabaseResponse::FindOne(Some(doc)) => {
            let val = bson_to_lua(state, &Bson::Document(doc)).map_err(|e| state.error(e))?;
            state.push_value(val)?;
            Ok(1)
        }
        DatabaseResponse::FindOne(None) => {
            state.push_value(LuaValue::nil())?;
            Ok(1)
        }
        DatabaseResponse::Find(docs) => {
            let table = state.create_table(docs.len(), 0)?;
            for (i, doc) in docs.into_iter().enumerate() {
                let val = bson_to_lua(state, &Bson::Document(doc)).map_err(|e| state.error(e))?;
                state.raw_seti(&table, (i + 1) as i64, val);
            }
            state.push_value(table)?;
            Ok(1)
        }
        DatabaseResponse::CreateIndex(res) => {
            push_message_table(state, "name", &res.index_name)
        }
        DatabaseResponse::Count(count) => {
            let table = state.create_table(0, 1)?;
            let k = state.create_string("count")?;
            state.raw_set(&table, k, LuaValue::integer(count as i64));
            state.push_value(table)?;
            Ok(1)
        }
        DatabaseResponse::Exists(exists) => {
            let table = state.create_table(0, 1)?;
            let k = state.create_string("exists")?;
            state.raw_set(&table, k, LuaValue::boolean(exists));
            state.push_value(table)?;
            Ok(1)
        }
        DatabaseResponse::Error(err) => {
            push_error_table(state, "ERROR", &err.to_string())
        }
    }
}

fn find_connection(state: &mut LuaState) -> LuaResult<usize> {
    let name = lua_check_str(state, 1)?;
    match DATABASE_CONNECTIONS.get(name) {
        Some(pair) => {
            let conn = Box::new(pair.value().clone());
            let ptr = Box::into_raw(conn);
            state.push_value(LuaValue::lightuserdata(ptr as *mut c_void))?;
        }
        None => {
            state.push_value(LuaValue::nil())?;
        }
    }
    Ok(1)
}

pub fn register_mongodb() -> luars::LibraryModule {
    luars::lua_module!("mongodb.core", {
        "connect" => connect,
        "find_connection" => find_connection,
        "decode" => decode,
        "operators" => operators,
    })
}
