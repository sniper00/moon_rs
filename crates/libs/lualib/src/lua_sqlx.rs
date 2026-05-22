use crate::lua_json::{JsonOptions, encode_table};
use crate::{
    lua_check_str,
    lua_check_typed_lightuserdata_mut,
    lua_check_typed_lightuserdata_ref,
    lua_take_typed_lightuserdata,
    lua_opt_integer,
    lua_push_error,
    push_error_table,
    push_message_table,
};
use crate::lua_actor::ActorRef;
use dashmap::DashMap;
use lazy_static::lazy_static;
use phf::phf_map;
use actor::context::{self, CONTEXT};
use luars::{LuaResult, LuaState, LuaValue};
use sqlx::{
    self, Column, ColumnIndex, Database, MySql, MySqlPool, PgPool, Postgres, Row, Sqlite,
    SqlitePool, TypeInfo,
    migrate::MigrateDatabase,
    mysql::MySqlRow,
    postgres::{PgPoolOptions, PgRow},
    sqlite::SqliteRow,
};
use std::{
    ffi::c_void,
    sync::{Arc, atomic::AtomicI64},
    time::Duration,
};
use tokio::{sync::mpsc, time::timeout};

lazy_static! {
    static ref DATABASE_CONNECTIONS: DashMap<String, DatabaseConnection> = DashMap::new();
}

enum DatabasePool {
    MySql(MySqlPool),
    Postgres(PgPool),
    Sqlite(SqlitePool),
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

    fn make_query<'a, DB: sqlx::Database>(
        sql: &'a str,
        binds: &'a [QueryParams],
    ) -> Result<sqlx::query::Query<'a, DB, <DB as sqlx::Database>::Arguments<'a>>, sqlx::Error>
    where
        bool: sqlx::Encode<'a, DB> + sqlx::Type<DB>,
        i64: sqlx::Encode<'a, DB> + sqlx::Type<DB>,
        f64: sqlx::Encode<'a, DB> + sqlx::Type<DB>,
        &'a str: sqlx::Encode<'a, DB> + sqlx::Type<DB>,
        serde_json::Value: sqlx::Encode<'a, DB> + sqlx::Type<DB>,
        &'a Vec<u8>: sqlx::Encode<'a, DB> + sqlx::Type<DB>,
    {
        let mut query = sqlx::query(sql);
        for bind in binds {
            query = match bind {
                QueryParams::Bool(value) => query.bind(*value),
                QueryParams::Int(value) => query.bind(*value),
                QueryParams::Float(value) => query.bind(*value),
                QueryParams::Text(value) => query.bind(value.as_str()),
                QueryParams::Json(value) => query.bind(value),
                QueryParams::Bytes(value) => query.bind(value),
            };
        }
        Ok(query)
    }

    async fn query(&self, request: &DatabaseQuery) -> Result<DatabaseResponse, sqlx::Error> {
        match self {
            DatabasePool::MySql(pool) => {
                let query = Self::make_query(&request.sql, &request.binds)?;
                let rows = query.fetch_all(pool).await?;
                Ok(DatabaseResponse::MysqlRows(rows))
            }
            DatabasePool::Postgres(pool) => {
                let query = Self::make_query(&request.sql, &request.binds)?;
                let rows = query.fetch_all(pool).await?;
                Ok(DatabaseResponse::PgRows(rows))
            }
            DatabasePool::Sqlite(pool) => {
                let query = Self::make_query(&request.sql, &request.binds)?;
                let rows = query.fetch_all(pool).await?;
                Ok(DatabaseResponse::SqliteRows(rows))
            }
        }
    }

    async fn transaction(
        &self,
        requests: &[DatabaseQuery],
    ) -> Result<DatabaseResponse, sqlx::Error> {
        match self {
            DatabasePool::MySql(pool) => {
                let mut transaction = pool.begin().await?;
                for request in requests {
                    let query = Self::make_query(&request.sql, &request.binds)?;
                    query.execute(&mut *transaction).await?;
                }
                transaction.commit().await?;
                Ok(DatabaseResponse::Transaction)
            }
            DatabasePool::Postgres(pool) => {
                let mut transaction = pool.begin().await?;
                for request in requests {
                    let query = Self::make_query(&request.sql, &request.binds)?;
                    query.execute(&mut *transaction).await?;
                }
                transaction.commit().await?;
                Ok(DatabaseResponse::Transaction)
            }
            DatabasePool::Sqlite(pool) => {
                let mut transaction = pool.begin().await?;
                for request in requests {
                    let query = Self::make_query(&request.sql, &request.binds)?;
                    query.execute(&mut *transaction).await?;
                }
                transaction.commit().await?;
                Ok(DatabaseResponse::Transaction)
            }
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
    PgRows(Vec<PgRow>),
    MysqlRows(Vec<MySqlRow>),
    SqliteRows(Vec<SqliteRow>),
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
        let s = s.to_string();
        if s.starts_with('{') || s.starts_with('[') {
            if let Ok(value) = serde_json::from_str::<serde_json::Value>(&s) {
                Ok(QueryParams::Json(value))
            } else {
                Ok(QueryParams::Text(s))
            }
        } else {
            Ok(QueryParams::Text(s))
        }
    } else if val.is_table() {
        let mut buffer = Vec::new();
        encode_table(&mut buffer, state, &val.as_table().unwrap(), 0, false, &options)?;
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

    let conn = lua_check_typed_lightuserdata_ref::<DatabaseConnection>(state, arg_idx)?;
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
    let queries = lua_check_typed_lightuserdata_mut::<TransactionQueries>(state, 1)?;

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
    let tq = Box::new(TransactionQueries { queries: Vec::new() });
    let ptr = Box::into_raw(tq);
    state.push_value(LuaValue::lightuserdata(ptr as *mut c_void))?;
    Ok(1)
}

fn transaction(state: &mut LuaState) -> LuaResult<usize> {
    let mut arg_idx = 1;

    let conn = lua_check_typed_lightuserdata_ref::<DatabaseConnection>(state, arg_idx)?;
    arg_idx += 1;

    let session: i64 = lua_opt_integer(state, arg_idx).unwrap_or(0);
    arg_idx += 1;

    let queries = lua_check_typed_lightuserdata_mut::<TransactionQueries>(state, arg_idx)?;

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
    let conn = lua_check_typed_lightuserdata_ref::<DatabaseConnection>(state, 1)?;

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
    Float,
    Text,
    Unknown,
}

static DB_TYPE_MAP: phf::Map<&'static str, DbType> = phf_map! {
    // Null
    "NULL" => DbType::Null,
    // Bool
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
    "FLOAT4" => DbType::Float,
    "FLOAT8" => DbType::Float,
    "NUMERIC" => DbType::Float,
    "FLOAT" => DbType::Float,
    "DOUBLE" => DbType::Float,
    "REAL" => DbType::Float,
    // Text
    "TEXT" => DbType::Text,
};

impl DbType {
    #[inline]
    fn from_name(name: &str) -> Self {
        DB_TYPE_MAP.get(name).copied().unwrap_or(Self::Unknown)
    }
}

fn process_rows<'a, DB>(
    state: &mut LuaState,
    rows: &'a [<DB as Database>::Row],
) -> LuaResult<usize>
where
    DB: sqlx::Database,
    usize: ColumnIndex<<DB as Database>::Row>,
    bool: sqlx::Decode<'a, DB>,
    i64: sqlx::Decode<'a, DB>,
    f64: sqlx::Decode<'a, DB>,
    &'a str: sqlx::Decode<'a, DB>,
    &'a [u8]: sqlx::Decode<'a, DB>,
{
    let table = state.create_table(rows.len(), 0)?;
    if rows.is_empty() {
        state.push_value(table)?;
        return Ok(1);
    }

    let column_info: Vec<(usize, String, DbType)> = rows
        .first()
        .unwrap()
        .columns()
        .iter()
        .enumerate()
        .map(|(index, column)| {
            (
                index,
                column.name().to_string(),
                DbType::from_name(column.type_info().name()),
            )
        })
        .collect();

    for (i, row) in rows.iter().enumerate() {
        let row_table = state.create_table(0, row.len())?;
        for (index, column_name, db_type) in column_info.iter() {
            let k = state.create_string(column_name)?;
            match row.try_get_raw(*index) {
                Ok(value) => match db_type {
                    DbType::Null => {
                        state.raw_set(&row_table, k, LuaValue::nil());
                    }
                    DbType::Bool => {
                        let v: bool = sqlx::decode::Decode::decode(value).unwrap_or(false);
                        state.raw_set(&row_table, k, LuaValue::boolean(v));
                    }
                    DbType::Integer => {
                        let v: i64 = sqlx::decode::Decode::decode(value).unwrap_or(0);
                        state.raw_set(&row_table, k, LuaValue::integer(v));
                    }
                    DbType::Float => {
                        let v: f64 = sqlx::decode::Decode::decode(value).unwrap_or(0.0);
                        state.raw_set(&row_table, k, LuaValue::float(v));
                    }
                    DbType::Text => {
                        let v: &str = sqlx::decode::Decode::decode(value).unwrap_or("");
                        let sv = state.create_string(v)?;
                        state.raw_set(&row_table, k, sv);
                    }
                    DbType::Unknown => {
                        let column_value: &[u8] =
                            sqlx::decode::Decode::decode(value).unwrap_or(b"");
                        let sv =
                            state.create_string(&String::from_utf8_lossy(column_value))?;
                        state.raw_set(&row_table, k, sv);
                    }
                },
                Err(error) => {
                    return lua_push_error(state, &format!("sqlx: decode error on column '{}': {}", column_name, error));
                }
            }
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

fn decode(state: &mut LuaState) -> LuaResult<usize> {
    let result = lua_take_typed_lightuserdata::<DatabaseResponse>(state, 1)?;

    match *result {
        DatabaseResponse::PgRows(rows) => process_rows::<Postgres>(state, &rows),
        DatabaseResponse::MysqlRows(rows) => process_rows::<MySql>(state, &rows),
        DatabaseResponse::SqliteRows(rows) => process_rows::<Sqlite>(state, &rows),
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
