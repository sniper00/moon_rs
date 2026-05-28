use crate::lua_json::{JsonOptions, encode_table};
use crate::{
    lua_check_str,
    lua_check_userdata,
    lua_check_userdata_mut,
    lua_newuserdata,
    lua_take_typed_lightuserdata,
    lua_opt_integer,
    push_error_table,
    push_message_table,
};
use crate::lua_actor::ActorRef;
use dashmap::DashMap;
use lazy_static::lazy_static;
use phf::phf_map;
use actor::context::{self, CONTEXT};
use luars::{CFunction, LuaResult, LuaState, LuaValue};
use sqlx::{
    self, Column, ColumnIndex, Database, MySqlPool, PgPool, Row, Sqlite,
    SqlitePool, TypeInfo, ValueRef,
    migrate::MigrateDatabase,
    postgres::PgPoolOptions,
};
use std::{
    sync::{Arc, atomic::AtomicI64},
    time::Duration,
};
use tokio::{sync::mpsc, time::timeout};

lazy_static! {
    static ref DATABASE_CONNECTIONS: DashMap<String, DatabaseConnection> = DashMap::new();
}

enum CellValue {
    Null,
    Bool(bool),
    Integer(i64),
    Float(f64),
    Text(String),
    Bytes(Vec<u8>),
}

struct QueryResult {
    columns: Vec<String>,
    rows: Vec<Vec<CellValue>>,
}

enum DatabasePool {
    MySql(MySqlPool),
    Postgres(PgPool),
    Sqlite(SqlitePool),
}

fn make_query<DB: sqlx::Database>(
    sql: &str,
    binds: &[QueryParams],
) -> Result<sqlx::query::Query<'static, DB, <DB as sqlx::Database>::Arguments>, sqlx::Error>
where
    for<'a> bool: sqlx::Encode<'a, DB> + sqlx::Type<DB>,
    for<'a> i64: sqlx::Encode<'a, DB> + sqlx::Type<DB>,
    for<'a> f64: sqlx::Encode<'a, DB> + sqlx::Type<DB>,
    for<'a> String: sqlx::Encode<'a, DB> + sqlx::Type<DB>,
    for<'a> serde_json::Value: sqlx::Encode<'a, DB> + sqlx::Type<DB>,
    for<'a> Vec<u8>: sqlx::Encode<'a, DB> + sqlx::Type<DB>,
{
    let mut query = sqlx::query(sqlx::AssertSqlSafe(sql.to_owned()));
    for bind in binds {
        query = match bind {
            QueryParams::Bool(value) => query.bind(*value),
            QueryParams::Int(value) => query.bind(*value),
            QueryParams::Float(value) => query.bind(*value),
            QueryParams::Text(value) => query.bind(value),
            QueryParams::Json(value) => query.bind(value),
            QueryParams::Bytes(value) => query.bind(value),
        };
    }
    Ok(query)
}

fn decode_rows<'a, DB>(rows: &'a [<DB as Database>::Row]) -> QueryResult
where
    DB: sqlx::Database,
    usize: ColumnIndex<<DB as Database>::Row>,
    bool: sqlx::Decode<'a, DB>,
    i64: sqlx::Decode<'a, DB>,
    f32: sqlx::Decode<'a, DB>,
    f64: sqlx::Decode<'a, DB>,
    &'a str: sqlx::Decode<'a, DB>,
    &'a [u8]: sqlx::Decode<'a, DB>,
    chrono::NaiveDate: sqlx::Decode<'a, DB> + sqlx::Type<DB>,
    chrono::NaiveTime: sqlx::Decode<'a, DB> + sqlx::Type<DB>,
    chrono::NaiveDateTime: sqlx::Decode<'a, DB> + sqlx::Type<DB>,
    chrono::DateTime<chrono::Utc>: sqlx::Decode<'a, DB> + sqlx::Type<DB>,
    uuid::Uuid: sqlx::Decode<'a, DB> + sqlx::Type<DB>,
    serde_json::Value: sqlx::Decode<'a, DB> + sqlx::Type<DB>,
{
    if rows.is_empty() {
        return QueryResult { columns: Vec::new(), rows: Vec::new() };
    }

    let first = rows.first().unwrap();
    let col_info: Vec<(usize, DbType)> = first
        .columns()
        .iter()
        .enumerate()
        .map(|(i, c)| (i, DbType::from_name(c.type_info().name())))
        .collect();
    let columns: Vec<String> = first.columns().iter().map(|c| c.name().to_string()).collect();

    let decoded_rows = rows.iter().map(|row| {
        col_info.iter().map(|(index, db_type)| {
            match row.try_get_raw(*index) {
                Ok(value) if value.is_null() => CellValue::Null,
                Ok(value) => match db_type {
                    DbType::Null => CellValue::Null,
                    DbType::Bool => CellValue::Bool(
                        sqlx::decode::Decode::decode(value).unwrap_or(false),
                    ),
                    DbType::Integer => CellValue::Integer(
                        sqlx::decode::Decode::decode(value).unwrap_or(0),
                    ),
                    DbType::Float32 => {
                        let v: f32 = sqlx::decode::Decode::decode(value).unwrap_or(0.0);
                        CellValue::Float(v as f64)
                    }
                    DbType::Float64 => CellValue::Float(
                        sqlx::decode::Decode::decode(value).unwrap_or(0.0),
                    ),
                    DbType::Text => {
                        let v: &str = sqlx::decode::Decode::decode(value).unwrap_or("");
                        CellValue::Text(v.to_string())
                    }
                    DbType::Json => {
                        match <serde_json::Value as sqlx::Decode<DB>>::decode(value) {
                            Ok(v) => CellValue::Text(v.to_string()),
                            Err(_) => CellValue::Null,
                        }
                    }
                    DbType::Date => {
                        match <chrono::NaiveDate as sqlx::Decode<DB>>::decode(value) {
                            Ok(v) => CellValue::Text(v.to_string()),
                            Err(_) => CellValue::Null,
                        }
                    }
                    DbType::Time => {
                        match <chrono::NaiveTime as sqlx::Decode<DB>>::decode(value) {
                            Ok(v) => CellValue::Text(v.to_string()),
                            Err(_) => CellValue::Null,
                        }
                    }
                    DbType::Timestamp => {
                        match <chrono::NaiveDateTime as sqlx::Decode<DB>>::decode(value) {
                            Ok(v) => CellValue::Text(v.to_string()),
                            Err(_) => CellValue::Null,
                        }
                    }
                    DbType::TimestampTz => {
                        match <chrono::DateTime<chrono::Utc> as sqlx::Decode<DB>>::decode(value) {
                            Ok(v) => CellValue::Text(v.to_string()),
                            Err(_) => CellValue::Null,
                        }
                    }
                    DbType::Uuid => {
                        match <uuid::Uuid as sqlx::Decode<DB>>::decode(value) {
                            Ok(v) => CellValue::Text(v.to_string()),
                            Err(_) => CellValue::Null,
                        }
                    }
                    DbType::Unknown => {
                        let v: &[u8] = sqlx::decode::Decode::decode(value).unwrap_or(b"");
                        CellValue::Bytes(v.to_vec())
                    }
                },
                Err(_) => CellValue::Null,
            }
        }).collect()
    }).collect();

    QueryResult { columns, rows: decoded_rows }
}

impl DatabasePool {
    async fn connect(database_url: &str, timeout_duration: Duration) -> Result<Self, sqlx::Error> {
        async fn connect_with_timeout<F, T>(
            timeout_duration: Duration,
            connect_future: F,
        ) -> Result<T, sqlx::Error>
        where
            F: std::future::Future<Output = Result<T, sqlx::Error>>,
        {
            timeout(timeout_duration, connect_future)
                .await
                .map_err(|err| {
                    sqlx::Error::Io(std::io::Error::other(format!("sqlx: connection timeout: {}", err)))
                })?
        }

        if database_url.starts_with("mysql://") {
            let pool =
                connect_with_timeout(timeout_duration, MySqlPool::connect(database_url)).await?;
            Ok(DatabasePool::MySql(pool))
        } else if database_url.starts_with("postgres://") {
            let pool = connect_with_timeout(
                timeout_duration,
                PgPoolOptions::new()
                    .max_connections(1)
                    .acquire_timeout(Duration::from_secs(2))
                    .connect(database_url),
            )
            .await?;
            Ok(DatabasePool::Postgres(pool))
        } else if database_url.starts_with("sqlite://") {
            if !Sqlite::database_exists(database_url).await? {
                Sqlite::create_database(database_url).await?;
            }
            let pool =
                connect_with_timeout(timeout_duration, SqlitePool::connect(database_url)).await?;
            Ok(DatabasePool::Sqlite(pool))
        } else {
            Err(sqlx::Error::Configuration(
                "Unsupported database type".into(),
            ))
        }
    }

    async fn query(&self, request: &DatabaseQuery) -> Result<DatabaseResponse, sqlx::Error> {
        macro_rules! do_query {
            ($pool:expr, $db:ty) => {{
                let query = make_query(&request.sql, &request.binds)?;
                let rows = query.fetch_all($pool).await?;
                Ok(DatabaseResponse::Rows(decode_rows::<$db>(&rows)))
            }};
        }
        match self {
            DatabasePool::MySql(pool) => do_query!(pool, sqlx::MySql),
            DatabasePool::Postgres(pool) => do_query!(pool, sqlx::Postgres),
            DatabasePool::Sqlite(pool) => do_query!(pool, sqlx::Sqlite),
        }
    }

    async fn transaction(
        &self,
        requests: &[DatabaseQuery],
    ) -> Result<DatabaseResponse, sqlx::Error> {
        macro_rules! do_transaction {
            ($pool:expr) => {{
                let mut tx = $pool.begin().await?;
                for request in requests {
                    let query = make_query(&request.sql, &request.binds)?;
                    query.execute(&mut *tx).await?;
                }
                tx.commit().await?;
                Ok(DatabaseResponse::Transaction)
            }};
        }
        match self {
            DatabasePool::MySql(pool) => do_transaction!(pool),
            DatabasePool::Postgres(pool) => do_transaction!(pool),
            DatabasePool::Sqlite(pool) => do_transaction!(pool),
        }
    }
}

enum DatabaseRequest {
    Query(i64, i64, DatabaseQuery),
    Transaction(i64, i64, Vec<DatabaseQuery>),
    Close(),
}

#[derive(Clone)]
struct DatabaseConnection {
    tx: mpsc::Sender<DatabaseRequest>,
    counter: Arc<AtomicI64>,
}

enum DatabaseResponse {
    Connect,
    Rows(QueryResult),
    Error(sqlx::Error),
    Timeout(String),
    Transaction,
}

#[derive(Debug, Clone)]
enum QueryParams {
    Bool(bool),
    Int(i64),
    Float(f64),
    Text(String),
    Json(serde_json::Value),
    Bytes(Vec<u8>),
}

#[derive(Debug, Clone)]
struct DatabaseQuery {
    sql: String,
    binds: Vec<QueryParams>,
}

struct TransactionQueries {
    queries: Vec<DatabaseQuery>,
}

async fn handle_result(
    database_url: &str,
    failed_times: &mut i32,
    counter: &Arc<AtomicI64>,
    protocol_type: u8,
    owner: i64,
    session: i64,
    res: Result<DatabaseResponse, sqlx::Error>,
) -> bool {
    match res {
        Ok(rows) => {
            CONTEXT.send_value(protocol_type, owner, session, rows);
            if *failed_times > 0 {
                log::info!(
                    "Database '{}' recover from error. Retry success. ({}:{})",
                    database_url,
                    file!(),
                    line!()
                );
            }
            counter.fetch_sub(1, std::sync::atomic::Ordering::Release);
            false
        }
        Err(err) => {
            if session != 0 {
                CONTEXT.send_value(protocol_type, owner, session, DatabaseResponse::Error(err));
                counter.fetch_sub(1, std::sync::atomic::Ordering::Release);
                false
            } else {
                if *failed_times > 0 {
                    log::error!(
                        "Database '{}' error: '{:?}'. Will retry. ({}:{})",
                        database_url,
                        err.to_string(),
                        file!(),
                        line!()
                    );
                }
                *failed_times += 1;
                tokio::time::sleep(Duration::from_secs(1)).await;
                true
            }
        }
    }
}

async fn database_handler(
    name: String,
    protocol_type: u8,
    pool: &DatabasePool,
    mut rx: mpsc::Receiver<DatabaseRequest>,
    database_url: &str,
    counter: Arc<AtomicI64>,
) {
    while let Some(op) = rx.recv().await {
        let mut failed_times = 0;
        match &op {
            DatabaseRequest::Query(owner, session, query_op) => {
                while handle_result(
                    database_url,
                    &mut failed_times,
                    &counter,
                    protocol_type,
                    *owner,
                    *session,
                    pool.query(query_op).await,
                )
                .await
                {}
            }
            DatabaseRequest::Transaction(owner, session, query_ops) => {
                while handle_result(
                    database_url,
                    &mut failed_times,
                    &counter,
                    protocol_type,
                    *owner,
                    *session,
                    pool.transaction(query_ops).await,
                )
                .await
                {}
            }
            DatabaseRequest::Close() => {
                break;
            }
        }
    }
    DATABASE_CONNECTIONS.remove(&name);
    log::info!("Database connection '{}' closed and removed.", name);
}

fn connect(state: &mut LuaState) -> LuaResult<usize> {
    let database_url = lua_check_str(state, 1)?.to_string();
    let name = lua_check_str(state, 2)?.to_string();
    let connect_timeout: u64 = lua_opt_integer(state, 3).unwrap_or(5000);

    let actor = ActorRef::from_state(state);
    let owner = actor.id();
    let session = actor.next_session();
    let protocol_type = context::PTYPE_SQLX;

    tokio::spawn(async move {
        match DatabasePool::connect(&database_url, Duration::from_millis(connect_timeout)).await {
            Ok(pool) => {
                let (tx, rx) = mpsc::channel(100);
                let counter = Arc::new(AtomicI64::new(0));
                DATABASE_CONNECTIONS.insert(
                    name.to_string(),
                    DatabaseConnection {
                        tx: tx.clone(),
                        counter: counter.clone(),
                    },
                );
                CONTEXT.send_value(protocol_type, owner, session, DatabaseResponse::Connect);
                database_handler(name, protocol_type, &pool, rx, &database_url, counter).await;
            }
            Err(err) => {
                CONTEXT.send_value(
                    protocol_type,
                    owner,
                    session,
                    DatabaseResponse::Timeout(err.to_string()),
                );
            }
        };
    });

    state.push_value(LuaValue::integer(session))?;
    Ok(1)
}

fn get_query_param(state: &mut LuaState, i: usize) -> Result<QueryParams, String> {
    let val = state.get_arg(i).unwrap_or(LuaValue::nil());
    let options = JsonOptions::default();

    if let Some(b) = val.as_boolean() {
        Ok(QueryParams::Bool(b))
    } else if val.is_integer() {
        Ok(QueryParams::Int(val.as_integer().unwrap()))
    } else if let Some(n) = val.as_number() {
        Ok(QueryParams::Float(n))
    } else if let Some(s) = val.as_str() {
        if s.starts_with('{') || s.starts_with('[') {
            if let Ok(value) = serde_json::from_str::<serde_json::Value>(s) {
                Ok(QueryParams::Json(value))
            } else {
                Ok(QueryParams::Text(s.to_string()))
            }
        } else {
            Ok(QueryParams::Text(s.to_string()))
        }
    } else if let Some(t) = val.as_table() {
        let mut buffer = Vec::new();
        encode_table(&mut buffer, state, &t, 0, false, &options)?;
        if !buffer.is_empty() && (buffer[0] == b'{' || buffer[0] == b'[') {
            if let Ok(value) = serde_json::from_slice::<serde_json::Value>(buffer.as_slice()) {
                Ok(QueryParams::Json(value))
            } else {
                Ok(QueryParams::Bytes(buffer))
            }
        } else {
            Ok(QueryParams::Bytes(buffer))
        }
    } else {
        Err("get_query_param: unsupported value type".to_string())
    }
}

fn query(state: &mut LuaState) -> LuaResult<usize> {
    let mut arg_idx = 1;

    let conn = lua_check_userdata::<DatabaseConnection>(state, arg_idx)?;
    arg_idx += 1;

    let session: i64 = lua_opt_integer(state, arg_idx).unwrap_or(0);
    arg_idx += 1;

    let sql = lua_check_str(state, arg_idx)?.to_string();
    arg_idx += 1;

    let mut params = Vec::new();
    let top = state.arg_count();
    for i in arg_idx..=top {
        match get_query_param(state, i) {
            Ok(value) => params.push(value),
            Err(err) => return push_error_table(state, "ERROR", &err),
        }
    }

    let actor = ActorRef::from_state(state);
    let owner = actor.id();

    match conn.tx.try_send(DatabaseRequest::Query(
        owner,
        session,
        DatabaseQuery {
            sql,
            binds: params,
        },
    )) {
        Ok(_) => {
            conn.counter
                .fetch_add(1, std::sync::atomic::Ordering::Release);
            state.push_value(LuaValue::integer(session))?;
            Ok(1)
        }
        Err(err) => push_error_table(state, "ERROR", &err.to_string()),
    }
}

fn push_transaction_query(state: &mut LuaState) -> LuaResult<usize> {
    let queries = lua_check_userdata_mut::<TransactionQueries>(state, 1)?;

    let sql = lua_check_str(state, 2)?.to_string();

    let mut params = Vec::new();
    let top = state.arg_count();
    for i in 3..=top {
        match get_query_param(state, i) {
            Ok(value) => params.push(value),
            Err(err) => return Err(state.error(err)),
        }
    }

    queries.queries.push(DatabaseQuery {
        sql,
        binds: params,
    });

    Ok(0)
}

fn make_transaction(state: &mut LuaState) -> LuaResult<usize> {
    static METHODS: &[(&str, CFunction)] = &[
        ("push", push_transaction_query),
    ];

    let ud = lua_newuserdata(state, TransactionQueries { queries: Vec::new() }, "sqlx_transaction", METHODS)?;
    state.push_value(ud)?;
    Ok(1)
}

fn transaction(state: &mut LuaState) -> LuaResult<usize> {
    let mut arg_idx = 1;

    let conn = lua_check_userdata::<DatabaseConnection>(state, arg_idx)?;
    arg_idx += 1;

    let session: i64 = lua_opt_integer(state, arg_idx).unwrap_or(0);
    arg_idx += 1;

    let queries = lua_check_userdata_mut::<TransactionQueries>(state, arg_idx)?;

    let actor = ActorRef::from_state(state);
    let owner = actor.id();

    match conn.tx.try_send(DatabaseRequest::Transaction(
        owner,
        session,
        std::mem::take(&mut queries.queries),
    )) {
        Ok(_) => {
            conn.counter
                .fetch_add(1, std::sync::atomic::Ordering::Release);
            state.push_value(LuaValue::integer(session))?;
            Ok(1)
        }
        Err(err) => push_error_table(state, "ERROR", &err.to_string()),
    }
}

fn close(state: &mut LuaState) -> LuaResult<usize> {
    let conn = lua_check_userdata::<DatabaseConnection>(state, 1)?;

    match conn.tx.try_send(DatabaseRequest::Close()) {
        Ok(_) => {
            state.push_value(LuaValue::boolean(true))?;
            Ok(1)
        }
        Err(err) => push_error_table(state, "ERROR", &err.to_string()),
    }
}

#[derive(Copy, Clone)]
enum DbType {
    Null,
    Bool,
    Integer,
    Float32,
    Float64,
    Text,
    Json,
    Date,
    Time,
    Timestamp,
    TimestampTz,
    Uuid,
    Unknown,
}

static DB_TYPE_MAP: phf::Map<&'static str, DbType> = phf_map! {
    "NULL" => DbType::Null,
    "BOOL" => DbType::Bool,
    "BOOLEAN" => DbType::Bool,
    // Integer
    "INT2" => DbType::Integer,
    "INT4" => DbType::Integer,
    "INT8" => DbType::Integer,
    "TINYINT" => DbType::Integer,
    "SMALLINT" => DbType::Integer,
    "INT" => DbType::Integer,
    "MEDIUMINT" => DbType::Integer,
    "BIGINT" => DbType::Integer,
    "INTEGER" => DbType::Integer,
    // Float
    "FLOAT4" => DbType::Float32,
    "REAL" => DbType::Float32,
    "FLOAT8" => DbType::Float64,
    "NUMERIC" => DbType::Float64,
    "FLOAT" => DbType::Float64,
    "DOUBLE" => DbType::Float64,
    // Text
    "TEXT" => DbType::Text,
    "VARCHAR" => DbType::Text,
    "CHAR" => DbType::Text,
    "BPCHAR" => DbType::Text,
    "NAME" => DbType::Text,
    // JSON
    "JSON" => DbType::Json,
    "JSONB" => DbType::Json,
    // Date/Time
    "DATE" => DbType::Date,
    "TIME" => DbType::Time,
    "TIMETZ" => DbType::Time,
    "TIMESTAMP" => DbType::Timestamp,
    "DATETIME" => DbType::Timestamp,
    "TIMESTAMPTZ" => DbType::TimestampTz,
    // UUID
    "UUID" => DbType::Uuid,
};

impl DbType {
    fn from_name(name: &str) -> Self {
        DB_TYPE_MAP.get(name).copied().unwrap_or(Self::Unknown)
    }
}

fn push_query_result(state: &mut LuaState, result: QueryResult) -> LuaResult<usize> {
    let table = state.create_table(result.rows.len(), 0)?;
    let col_keys: Vec<LuaValue> = result.columns.iter()
        .map(|name| state.create_string(name))
        .collect::<Result<_, _>>()?;

    for (i, row) in result.rows.into_iter().enumerate() {
        let row_table = state.create_table(0, row.len())?;
        for (col_idx, cell) in row.into_iter().enumerate() {
            let v = match cell {
                CellValue::Null => continue,
                CellValue::Bool(v) => LuaValue::boolean(v),
                CellValue::Integer(v) => LuaValue::integer(v),
                CellValue::Float(v) => LuaValue::float(v),
                CellValue::Text(v) => state.create_string(&v)?,
                CellValue::Bytes(v) => state.create_bytes(&v)?,
            };
            state.raw_set(&row_table, col_keys[col_idx], v);
        }
        state.raw_seti(&table, (i + 1) as i64, row_table);
    }
    state.push_value(table)?;
    Ok(1)
}

fn find_connection(state: &mut LuaState) -> LuaResult<usize> {
    let name = lua_check_str(state, 1)?;
    match DATABASE_CONNECTIONS.get(name) {
        Some(pair) => {
            static METHODS: &[(&str, CFunction)] = &[
                ("query", query),
                ("transaction", transaction),
                ("close", close),
            ];

            let ud = lua_newuserdata(state, pair.value().clone(), "sqlx_connection", METHODS)?;
            state.push_value(ud)?;
        }
        None => {
            state.push_value(LuaValue::nil())?;
        }
    }
    Ok(1)
}

fn decode(state: &mut LuaState) -> LuaResult<usize> {
    let result = lua_take_typed_lightuserdata::<DatabaseResponse>(state, 1)?;

    match *result {
        DatabaseResponse::Rows(qr) => push_query_result(state, qr),
        DatabaseResponse::Transaction => push_message_table(state, "message", "ok"),
        DatabaseResponse::Connect => push_message_table(state, "message", "success"),
        DatabaseResponse::Error(err) => match err.as_database_error() {
            Some(db_err) => push_error_table(state, "DB", db_err.message()),
            None => push_error_table(state, "ERROR", &err.to_string()),
        },
        DatabaseResponse::Timeout(err) => push_error_table(state, "TIMEOUT", &err),
    }
}

fn stats(state: &mut LuaState) -> LuaResult<usize> {
    let table = state.create_table(0, DATABASE_CONNECTIONS.len())?;
    for pair in DATABASE_CONNECTIONS.iter() {
        let k = state.create_string(pair.key().as_str())?;
        let v = LuaValue::integer(
            pair.value()
                .counter
                .load(std::sync::atomic::Ordering::Acquire),
        );
        state.raw_set(&table, k, v);
    }
    state.push_value(table)?;
    Ok(1)
}

pub fn register_sqlx() -> luars::LibraryModule {
    luars::lua_module!("sqlx.core", {
        "connect" => connect,
        "find_connection" => find_connection,
        "decode" => decode,
        "stats" => stats,
        "make_transaction" => make_transaction,
        "push_query" => push_transaction_query,
        "query" => query,
        "transaction" => transaction,
        "close" => close,
    })
}
