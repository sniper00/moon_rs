use crate::lua_json::{JsonOptions, encode_table};
use dashmap::DashMap;
use futures::TryStreamExt;
use lazy_static::lazy_static;
use moon_runtime::actor::LuaActor;
use moon_runtime::context::{self, ActorId, CONTEXT};
use moon_lua::laux::LuaState;
use moon_lua::{
    cstr, ffi, laux,
    laux::{LuaArgs, LuaTable, LuaValue, lua_into_userdata},
    lreg, lreg_null, luaL_newlib, push_lua_table,
};
use phf::phf_map;
use sqlx::{
    self, Column, ColumnIndex, Database, MySql, MySqlPool, PgPool, Postgres, Row, Sqlite,
    SqlitePool, TypeInfo, ValueRef, migrate::MigrateDatabase,
    mysql::{MySqlPoolOptions, MySqlRow}, postgres::{PgPoolOptions, PgRow},
    sqlite::{SqlitePoolOptions, SqliteRow},
};
use std::{
    ffi::c_int,
    sync::{Arc, atomic::AtomicI64},
    time::Duration,
};
use tokio::sync::{mpsc, oneshot};
use tokio::time::timeout;

lazy_static! {
    static ref DATABASE_CONNECTIONS: DashMap<String, DatabaseConnection> = DashMap::new();
}

// ---------------------------------------------------------------------------
// Column type classification
// ---------------------------------------------------------------------------

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
    // Date / Time
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

// ---------------------------------------------------------------------------
// Direct row → Lua pusher (no intermediate clone)
// ---------------------------------------------------------------------------

fn process_rows<'a, DB>(state: LuaState, rows: &'a [<DB as Database>::Row]) -> c_int
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
    let table = LuaTable::new(state, rows.len(), 0);
    if rows.is_empty() {
        return 1;
    }

    let first = &rows[0];
    let col_info: Vec<(usize, &str, DbType)> = first
        .columns()
        .iter()
        .enumerate()
        .map(|(i, c)| (i, c.name(), DbType::from_name(c.type_info().name())))
        .collect();

    for (i, row) in rows.iter().enumerate() {
        let row_table = LuaTable::new(state, 0, col_info.len());
        for &(index, col_name, db_type) in &col_info {
            match row.try_get_raw(index) {
                Ok(value) if value.is_null() => {}
                Ok(value) => match db_type {
                    DbType::Null => {}
                    DbType::Bool => {
                        row_table.insert(
                            col_name,
                            sqlx::decode::Decode::decode(value).unwrap_or(false),
                        );
                    }
                    DbType::Integer => {
                        row_table.insert(
                            col_name,
                            sqlx::decode::Decode::decode(value).unwrap_or(0i64),
                        );
                    }
                    DbType::Float32 => {
                        let v: f32 = sqlx::decode::Decode::decode(value).unwrap_or(0.0);
                        row_table.insert(col_name, v as f64);
                    }
                    DbType::Float64 => {
                        row_table.insert(
                            col_name,
                            sqlx::decode::Decode::decode(value).unwrap_or(0.0f64),
                        );
                    }
                    DbType::Text => {
                        let v: &str = sqlx::decode::Decode::decode(value).unwrap_or("");
                        row_table.insert(col_name, v);
                    }
                    DbType::Json => {
                        if let Ok(v) = <serde_json::Value as sqlx::Decode<DB>>::decode(value) {
                            let s = v.to_string();
                            row_table.insert(col_name, s.as_str());
                        }
                    }
                    DbType::Date => {
                        if let Ok(v) = <chrono::NaiveDate as sqlx::Decode<DB>>::decode(value) {
                            let s = v.to_string();
                            row_table.insert(col_name, s.as_str());
                        }
                    }
                    DbType::Time => {
                        if let Ok(v) = <chrono::NaiveTime as sqlx::Decode<DB>>::decode(value) {
                            let s = v.to_string();
                            row_table.insert(col_name, s.as_str());
                        }
                    }
                    DbType::Timestamp => {
                        if let Ok(v) = <chrono::NaiveDateTime as sqlx::Decode<DB>>::decode(value) {
                            let s = v.to_string();
                            row_table.insert(col_name, s.as_str());
                        }
                    }
                    DbType::TimestampTz => {
                        if let Ok(v) =
                            <chrono::DateTime<chrono::Utc> as sqlx::Decode<DB>>::decode(value)
                        {
                            let s = v.to_string();
                            row_table.insert(col_name, s.as_str());
                        }
                    }
                    DbType::Uuid => {
                        if let Ok(v) = <uuid::Uuid as sqlx::Decode<DB>>::decode(value) {
                            let s = v.to_string();
                            row_table.insert(col_name, s.as_str());
                        }
                    }
                    DbType::Unknown => {
                        let v: &[u8] = sqlx::decode::Decode::decode(value).unwrap_or(b"");
                        row_table.insert(col_name, v);
                    }
                },
                Err(_) => {}
            }
        }
        table.rawseti(i + 1);
    }
    1
}

// ---------------------------------------------------------------------------
// Database pool
// ---------------------------------------------------------------------------

enum DatabasePool {
    MySql(MySqlPool),
    Postgres(PgPool),
    Sqlite(SqlitePool),
}

impl DatabasePool {
    async fn connect(
        database_url: &str,
        timeout_duration: Duration,
        max_connections: u32,
    ) -> Result<Self, sqlx::Error> {
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
                    sqlx::Error::Io(std::io::Error::other(format!(
                        "sqlx: connection timeout: {}",
                        err
                    )))
                })?
        }

        let acquire_timeout = timeout_duration;

        if database_url.starts_with("mysql://") {
            let pool = connect_with_timeout(
                timeout_duration,
                MySqlPoolOptions::new()
                    .max_connections(max_connections)
                    .acquire_timeout(acquire_timeout)
                    .connect(database_url),
            )
            .await?;
            Ok(DatabasePool::MySql(pool))
        } else if database_url.starts_with("postgres://") {
            let pool = connect_with_timeout(
                timeout_duration,
                PgPoolOptions::new()
                    .max_connections(max_connections)
                    .acquire_timeout(acquire_timeout)
                    .connect(database_url),
            )
            .await?;
            Ok(DatabasePool::Postgres(pool))
        } else if database_url.starts_with("sqlite://") {
            if !Sqlite::database_exists(database_url).await? {
                Sqlite::create_database(database_url).await?;
            }
            let pool = connect_with_timeout(
                timeout_duration,
                SqlitePoolOptions::new()
                    .max_connections(max_connections)
                    .acquire_timeout(acquire_timeout)
                    .connect(database_url),
            )
            .await?;
            Ok(DatabasePool::Sqlite(pool))
        } else {
            Err(sqlx::Error::Configuration(
                "Unsupported database type".into(),
            ))
        }
    }

    fn make_query<DB: sqlx::Database>(
        sql: String,
        binds: &[QueryParams],
    ) -> Result<sqlx::query::Query<'static, DB, <DB as sqlx::Database>::Arguments>, sqlx::Error>
    where
        bool: for<'q> sqlx::Encode<'q, DB> + sqlx::Type<DB>,
        i64: for<'q> sqlx::Encode<'q, DB> + sqlx::Type<DB>,
        f64: for<'q> sqlx::Encode<'q, DB> + sqlx::Type<DB>,
        String: for<'q> sqlx::Encode<'q, DB> + sqlx::Type<DB>,
        serde_json::Value: for<'q> sqlx::Encode<'q, DB> + sqlx::Type<DB>,
        Vec<u8>: for<'q> sqlx::Encode<'q, DB> + sqlx::Type<DB>,
    {
        let mut query = sqlx::query(sqlx::AssertSqlSafe(sql));
        for bind in binds {
            query = match bind {
                QueryParams::Bool(value) => query.bind(*value),
                QueryParams::Int(value) => query.bind(*value),
                QueryParams::Float(value) => query.bind(*value),
                QueryParams::Text(value) => query.bind(value.clone()),
                QueryParams::Json(value) => query.bind(value.clone()),
                QueryParams::Bytes(value) => query.bind(value.clone()),
            };
        }
        Ok(query)
    }

    async fn query(&self, request: &DatabaseQuery) -> Result<DatabaseResponse, sqlx::Error> {
        match self {
            DatabasePool::MySql(pool) => {
                let query = Self::make_query(request.sql.clone(), &request.binds)?;
                let rows = query.fetch_all(pool).await?;
                Ok(DatabaseResponse::MysqlRows(rows))
            }
            DatabasePool::Postgres(pool) => {
                let query = Self::make_query(request.sql.clone(), &request.binds)?;
                let rows = query.fetch_all(pool).await?;
                Ok(DatabaseResponse::PgRows(rows))
            }
            DatabasePool::Sqlite(pool) => {
                let query = Self::make_query(request.sql.clone(), &request.binds)?;
                let rows = query.fetch_all(pool).await?;
                Ok(DatabaseResponse::SqliteRows(rows))
            }
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
                    let query = Self::make_query(request.sql.clone(), &request.binds)?;
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

// ---------------------------------------------------------------------------
// Request / Response / Connection types
// ---------------------------------------------------------------------------

enum CursorSignal {
    Next(ActorId, i64),
    Close,
}

struct SqlxCursorHandle(Option<oneshot::Sender<CursorSignal>>);

impl Drop for SqlxCursorHandle {
    fn drop(&mut self) {
        if let Some(tx) = self.0.take() {
            let _ = tx.send(CursorSignal::Close);
        }
    }
}

enum DatabaseRequest {
    Query(ActorId, i64, DatabaseQuery),
    QueryStream(ActorId, i64, DatabaseQuery, usize),
    Transaction(ActorId, i64, Vec<DatabaseQuery>),
    Close(),
}

#[derive(Clone)]
struct DatabaseConnection {
    tx: mpsc::Sender<DatabaseRequest>,
    counter: Arc<AtomicI64>,
}

enum DatabaseResponse {
    Connect,
    PgRows(Vec<PgRow>),
    MysqlRows(Vec<MySqlRow>),
    SqliteRows(Vec<SqliteRow>),
    PgBatch(Vec<PgRow>, Option<oneshot::Sender<CursorSignal>>),
    MysqlBatch(Vec<MySqlRow>, Option<oneshot::Sender<CursorSignal>>),
    SqliteBatch(Vec<SqliteRow>, Option<oneshot::Sender<CursorSignal>>),
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

// ---------------------------------------------------------------------------
// Async handler
// ---------------------------------------------------------------------------

async fn handle_result(
    database_url: &str,
    failed_times: &mut i32,
    counter: &Arc<AtomicI64>,
    protocol_type: u8,
    owner: ActorId,
    session: i64,
    res: Result<DatabaseResponse, sqlx::Error>,
) -> bool {
    match res {
        Ok(rows) => {
            let _ = CONTEXT.send_value(protocol_type, owner, session, rows);
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
                let _ = CONTEXT.send_value(protocol_type, owner, session, DatabaseResponse::Error(err));
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
            DatabaseRequest::QueryStream(owner, session, query_op, batch_size) => {
                counter.fetch_sub(1, std::sync::atomic::Ordering::Release);
                let batch_size = *batch_size;
                let mut current_owner = *owner;
                let mut current_session = *session;

                macro_rules! do_stream {
                    ($pool:expr, $variant:ident) => {{
                        let query_result = DatabasePool::make_query(query_op.sql.clone(), &query_op.binds);
                        match query_result {
                            Ok(q) => {
                                let mut stream = q.fetch($pool);
                                loop {
                                    let mut batch = Vec::with_capacity(batch_size);
                                    let mut errored = false;
                                    for _ in 0..batch_size {
                                        match stream.try_next().await {
                                            Ok(Some(row)) => batch.push(row),
                                            Ok(None) => break,
                                            Err(err) => {
                                                CONTEXT.response_error(0, current_owner, -current_session, err.to_string());
                                                errored = true;
                                                break;
                                            }
                                        }
                                    }
                                    if errored { break; }

                                    let cursor_exhausted = batch.len() < batch_size;
                                    let (next_tx, next_rx) = if !cursor_exhausted {
                                        let (tx, rx) = oneshot::channel();
                                        (Some(tx), Some(rx))
                                    } else {
                                        (None, None)
                                    };

                                    let _ = CONTEXT.send_value(
                                        protocol_type,
                                        current_owner,
                                        current_session,
                                        DatabaseResponse::$variant(batch, next_tx),
                                    );

                                    if cursor_exhausted { break; }

                                    match next_rx.unwrap().await {
                                        Ok(CursorSignal::Next(new_owner, new_session)) => {
                                            current_owner = new_owner;
                                            current_session = new_session;
                                        }
                                        _ => break,
                                    }
                                }
                            }
                            Err(err) => {
                                CONTEXT.response_error(0, current_owner, -current_session, err.to_string());
                            }
                        }
                    }};
                }

                match pool {
                    DatabasePool::MySql(p) => do_stream!(p, MysqlBatch),
                    DatabasePool::Postgres(p) => do_stream!(p, PgBatch),
                    DatabasePool::Sqlite(p) => do_stream!(p, SqliteBatch),
                }
            }
            DatabaseRequest::Close() => {
                break;
            }
        }
    }
    DATABASE_CONNECTIONS.remove(&name);
    log::info!("Database connection '{}' closed and removed.", name);
}

// ---------------------------------------------------------------------------
// Lua-facing functions
// ---------------------------------------------------------------------------

extern "C-unwind" fn connect(state: LuaState) -> c_int {
    let database_url = unsafe { laux::lua_check_str(state, 1) };
    let name = unsafe { laux::lua_check_str(state, 2) };
    let connect_timeout: u64 = laux::lua_opt(state, 3).unwrap_or(5000);
    let max_connections: u32 = laux::lua_opt(state, 4).unwrap_or(5);

    let actor = LuaActor::from_lua_state(state);
    let owner = unsafe { (*actor).id };
    let session = unsafe { (*actor).next_session() };
    let protocol_type = context::PTYPE_SQLX;

    let database_url = database_url.to_string();
    let name = name.to_string();

    CONTEXT.io_runtime().spawn(async move {
        match DatabasePool::connect(
            &database_url,
            Duration::from_millis(connect_timeout),
            max_connections,
        )
        .await
        {
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
                let _ = CONTEXT.send_value(protocol_type, owner, session, DatabaseResponse::Connect);
                database_handler(name, protocol_type, &pool, rx, &database_url, counter).await;
            }
            Err(err) => {
                let _ = CONTEXT.send_value(
                    protocol_type,
                    owner,
                    session,
                    DatabaseResponse::Timeout(err.to_string()),
                );
            }
        };
    });

    laux::lua_push(state, session);
    1
}

const SQLX_JSON_PARAM_META: *const std::ffi::c_char = cstr!("sqlx_json_param");

extern "C-unwind" fn make_json_param(state: LuaState) -> c_int {
    let str = unsafe { laux::lua_check_str(state, 1) };
    let value: serde_json::Value = match serde_json::from_str(str) {
        Ok(v) => v,
        Err(e) => {
            laux::lua_error(state, format!("sqlx.json: invalid JSON: {}", e));
        }
    };
    laux::lua_newuserdata(state, value, SQLX_JSON_PARAM_META, &[lreg_null!()]);
    1
}

fn get_query_param(state: LuaState, i: i32) -> Result<QueryParams, String> {
    let options = JsonOptions::default();

    let ptr = unsafe { ffi::luaL_testudata(state.as_ptr(), i, SQLX_JSON_PARAM_META) };
    if !ptr.is_null() {
        let value = unsafe { &*(ptr as *const serde_json::Value) };
        return Ok(QueryParams::Json(value.clone()));
    }

    let res = match LuaValue::from_stack(state, i) {
        LuaValue::Boolean(val) => QueryParams::Bool(val),
        LuaValue::Number(val) => QueryParams::Float(val),
        LuaValue::Integer(val) => QueryParams::Int(val),
        LuaValue::String(val) => {
            match String::from_utf8(val.to_vec()) {
                Ok(s) => QueryParams::Text(s),
                Err(e) => QueryParams::Bytes(e.into_bytes()),
            }
        }
        LuaValue::Table(val) => {
            let mut buffer = Vec::new();
            if let Err(err) = encode_table(&mut buffer, &val, 0, false, &options) {
                drop(buffer);
                laux::lua_error(state, err);
            }
            if !buffer.is_empty() && (buffer[0] == b'{' || buffer[0] == b'[') {
                if let Ok(value) = serde_json::from_slice::<serde_json::Value>(buffer.as_slice()) {
                    QueryParams::Json(value)
                } else {
                    QueryParams::Bytes(buffer)
                }
            } else {
                QueryParams::Bytes(buffer)
            }
        }
        _ => {
            return Err(format!(
                "get_query_param: unsupported value type: {}",
                laux::type_name(state, i)
            ));
        }
    };
    Ok(res)
}

extern "C-unwind" fn query(state: LuaState) -> c_int { query_impl(state, false) }
extern "C-unwind" fn exec_query(state: LuaState) -> c_int { query_impl(state, true) }

fn query_impl(state: LuaState, forget: bool) -> c_int {
    let mut args = LuaArgs::new(1);
    let conn = laux::lua_touserdata::<DatabaseConnection>(state, args.iter_arg())
        .expect("invalid database connection pointer");

    let sql = unsafe { laux::lua_check_str(state, args.iter_arg()) };
    let mut params = Vec::new();
    let top = laux::lua_top(state);
    for i in args.iter_arg()..=top {
        match get_query_param(state, i) {
            Ok(value) => params.push(value),
            Err(err) => {
                push_lua_table!(state, "kind" => "ERROR", "message" => err);
                return 1;
            }
        }
    }

    let actor = LuaActor::from_lua_state(state);
    let owner = unsafe { (*actor).id };
    let session: i64 = if forget { 0 } else { unsafe { (*actor).next_session() } };

    match conn.tx.try_send(DatabaseRequest::Query(
        owner,
        session,
        DatabaseQuery {
            sql: sql.to_string(),
            binds: params,
        },
    )) {
        Ok(_) => {
            conn.counter
                .fetch_add(1, std::sync::atomic::Ordering::Release);
            if forget { laux::lua_push(state, true); } else { laux::lua_push(state, session); }
            1
        }
        Err(err) => {
            push_lua_table!(state, "kind" => "ERROR", "message" => err.to_string());
            1
        }
    }
}

extern "C-unwind" fn query_stream(state: LuaState) -> c_int {
    let mut args = LuaArgs::new(1);
    let conn = laux::lua_touserdata::<DatabaseConnection>(state, args.iter_arg())
        .expect("invalid database connection pointer");

    let batch_size: usize = laux::lua_opt(state, args.iter_arg()).unwrap_or(100);
    let sql = unsafe { laux::lua_check_str(state, args.iter_arg()) };
    let mut params = Vec::new();
    let top = laux::lua_top(state);
    for i in args.iter_arg()..=top {
        match get_query_param(state, i) {
            Ok(value) => params.push(value),
            Err(err) => {
                push_lua_table!(state, "kind" => "ERROR", "message" => err);
                return 1;
            }
        }
    }

    let actor = LuaActor::from_lua_state(state);
    let owner = unsafe { (*actor).id };
    let session = unsafe { (*actor).next_session() };

    match conn.tx.try_send(DatabaseRequest::QueryStream(
        owner,
        session,
        DatabaseQuery {
            sql: sql.to_string(),
            binds: params,
        },
        batch_size,
    )) {
        Ok(_) => {
            conn.counter
                .fetch_add(1, std::sync::atomic::Ordering::Release);
            laux::lua_push(state, session);
            1
        }
        Err(err) => {
            push_lua_table!(state, "kind" => "ERROR", "message" => err.to_string());
            1
        }
    }
}

extern "C-unwind" fn push_transaction_query(state: LuaState) -> c_int {
    let queries = laux::lua_touserdata::<TransactionQueries>(state, 1)
        .expect("invalid transaction queries pointer");

    let sql = unsafe { laux::lua_check_str(state, 2) };
    let mut params = Vec::new();
    let top = laux::lua_top(state);
    for i in 3..=top {
        match get_query_param(state, i) {
            Ok(value) => params.push(value),
            Err(err) => {
                drop(params);
                laux::lua_error(state, err);
            }
        }
    }

    queries.queries.push(DatabaseQuery {
        sql: sql.to_string(),
        binds: params,
    });

    0
}

extern "C-unwind" fn make_transaction(state: LuaState) -> c_int {
    laux::lua_newuserdata(
        state,
        TransactionQueries {
            queries: Vec::new(),
        },
        cstr!("sqlx_transaction_metatable"),
        &[lreg!("push", push_transaction_query), lreg_null!()],
    );
    1
}

extern "C-unwind" fn transaction(state: LuaState) -> c_int { transaction_impl(state, false) }
extern "C-unwind" fn exec_transaction(state: LuaState) -> c_int { transaction_impl(state, true) }

fn transaction_impl(state: LuaState, forget: bool) -> c_int {
    let mut args = LuaArgs::new(1);
    let conn = laux::lua_touserdata::<DatabaseConnection>(state, args.iter_arg())
        .expect("invalid database connection pointer");

    let queries = laux::lua_touserdata::<TransactionQueries>(state, args.iter_arg())
        .expect("invalid transaction queries pointer");

    let actor = LuaActor::from_lua_state(state);
    let owner = unsafe { (*actor).id };
    let session: i64 = if forget { 0 } else { unsafe { (*actor).next_session() } };

    match conn.tx.try_send(DatabaseRequest::Transaction(
        owner,
        session,
        std::mem::take(&mut queries.queries),
    )) {
        Ok(_) => {
            conn.counter
                .fetch_add(1, std::sync::atomic::Ordering::Release);
            if forget { laux::lua_push(state, true); } else { laux::lua_push(state, session); }
            1
        }
        Err(err) => {
            push_lua_table!(state, "kind" => "ERROR", "message" => err.to_string());
            1
        }
    }
}

extern "C-unwind" fn close(state: LuaState) -> c_int {
    let conn = laux::lua_touserdata::<DatabaseConnection>(state, 1)
        .expect("invalid database connection pointer");

    match conn.tx.try_send(DatabaseRequest::Close()) {
        Ok(_) => {
            laux::lua_push(state, true);
            1
        }
        Err(err) => {
            push_lua_table!(state, "kind" => "ERROR", "message" => err.to_string());
            1
        }
    }
}

extern "C-unwind" fn find_connection(state: LuaState) -> c_int {
    let name = unsafe { laux::lua_check_str(state, 1) };
    match DATABASE_CONNECTIONS.get(name) {
        Some(pair) => {
            let l = [
                lreg!("query", query),
                lreg!("exec_query", exec_query),
                lreg!("query_stream", query_stream),
                lreg!("transaction", transaction),
                lreg!("exec_transaction", exec_transaction),
                lreg!("close", close),
                lreg_null!(),
            ];
            if laux::lua_newuserdata(
                state,
                pair.value().clone(),
                cstr!("sqlx_connection_metatable"),
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

extern "C-unwind" fn sqlx_cursor_next(state: LuaState) -> c_int {
    let handle = laux::lua_touserdata::<SqlxCursorHandle>(state, 1)
        .expect("invalid sqlx cursor handle");
    if let Some(tx) = handle.0.take() {
        let actor = LuaActor::from_lua_state(state);
        let owner = unsafe { (*actor).id };
        let session = unsafe { (*actor).next_session() };
        let _ = tx.send(CursorSignal::Next(owner, session));
        laux::lua_push(state, session);
        1
    } else {
        crate::lua_push_error(state, "sqlx cursor: already consumed or closed")
    }
}

extern "C-unwind" fn sqlx_cursor_close(state: LuaState) -> c_int {
    let handle = laux::lua_touserdata::<SqlxCursorHandle>(state, 1)
        .expect("invalid sqlx cursor handle");
    if let Some(tx) = handle.0.take() {
        let _ = tx.send(CursorSignal::Close);
    }
    0
}

fn push_cursor_handle(state: LuaState, next_tx: Option<oneshot::Sender<CursorSignal>>) {
    if let Some(tx) = next_tx {
        let methods = [
            lreg!("next", sqlx_cursor_next),
            lreg!("close", sqlx_cursor_close),
            lreg_null!(),
        ];
        laux::lua_newuserdata(
            state,
            SqlxCursorHandle(Some(tx)),
            cstr!("sqlx_cursor_handle"),
            &methods,
        );
    } else {
        laux::lua_pushnil(state);
    }
}

extern "C-unwind" fn decode(state: LuaState) -> c_int {
    laux::lua_checkstack(state, 6, std::ptr::null());
    let result = lua_into_userdata::<DatabaseResponse>(state, 1);

    match *result {
        DatabaseResponse::PgRows(rows) => {
            process_rows::<Postgres>(state, &rows)
        }
        DatabaseResponse::MysqlRows(rows) => process_rows::<MySql>(state, &rows),
        DatabaseResponse::SqliteRows(rows) => process_rows::<Sqlite>(state, &rows),
        DatabaseResponse::PgBatch(rows, next_tx) => {
            process_rows::<Postgres>(state, &rows);
            push_cursor_handle(state, next_tx);
            2
        }
        DatabaseResponse::MysqlBatch(rows, next_tx) => {
            process_rows::<MySql>(state, &rows);
            push_cursor_handle(state, next_tx);
            2
        }
        DatabaseResponse::SqliteBatch(rows, next_tx) => {
            process_rows::<Sqlite>(state, &rows);
            push_cursor_handle(state, next_tx);
            2
        }
        DatabaseResponse::Transaction => {
            push_lua_table!(state, "message" => "ok");
            1
        }
        DatabaseResponse::Connect => {
            push_lua_table!(state, "message" => "success");
            1
        }
        DatabaseResponse::Error(err) => match err.as_database_error() {
            Some(db_err) => {
                push_lua_table!(state, "kind" => "DB", "message" => db_err.message());
                1
            }
            None => {
                push_lua_table!(state, "kind" => "ERROR", "message" => err.to_string());
                1
            }
        },
        DatabaseResponse::Timeout(err) => {
            push_lua_table!(state, "kind" => "TIMEOUT", "message" => err.to_string());
            1
        }
    }
}

extern "C-unwind" fn stats(state: LuaState) -> c_int {
    let table = LuaTable::new(state, 0, DATABASE_CONNECTIONS.len());
    DATABASE_CONNECTIONS.iter().for_each(|pair| {
        table.insert(
            pair.key().as_str(),
            pair.value()
                .counter
                .load(std::sync::atomic::Ordering::Acquire),
        );
    });
    1
}

pub extern "C-unwind" fn luaopen_sqlx(state: LuaState) -> c_int {
    let l = [
        lreg!("connect", connect),
        lreg!("find_connection", find_connection),
        lreg!("decode", decode),
        lreg!("stats", stats),
        lreg!("make_transaction", make_transaction),
        lreg!("json_param", make_json_param),
        lreg_null!(),
    ];

    luaL_newlib!(state, l);

    1
}
