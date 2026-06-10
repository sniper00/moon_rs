use crate::lua_json::{JsonOptions, encode_table};
use crate::request_pool::{PendingCounter, QueuedRequest, drain_queued_requests};
use dashmap::DashMap;
use futures::TryStreamExt;
use lazy_static::lazy_static;
use moon_lua::laux::LuaState;
use moon_lua::{
    cstr, ffi, laux,
    laux::{LuaArgs, LuaTable, LuaValue},
    lreg, lreg_null, luaL_newlib, push_lua_table,
};
use moon_runtime::actor::LuaActor;
use moon_runtime::context::{self, ActorId, CONTEXT};
use phf::phf_map;
use sqlx::{
    self, Column, ColumnIndex, Database, MySql, MySqlPool, PgPool, Postgres, Row, Sqlite,
    SqlitePool, TypeInfo, ValueRef,
    migrate::MigrateDatabase,
    mysql::{MySqlPoolOptions, MySqlRow},
    postgres::{PgPoolOptions, PgRow},
    sqlite::{SqlitePoolOptions, SqliteRow},
};
use std::{ffi::c_int, time::Duration};
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
        } else if database_url.starts_with("postgres://")
            || database_url.starts_with("postgresql://")
        {
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

    /// Build a parameterized query. **Trust requirement:** the `sql` string is
    /// passed to the driver verbatim via `sqlx::AssertSqlSafe` — only the `binds`
    /// are parameterized. The caller (Lua) is responsible for ensuring `sql` is a
    /// trusted, statically-known statement; never build it by concatenating
    /// untrusted input. Use `$1`/`?` placeholders + binds for all values.
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
                let rows = collect_capped(query.fetch(pool)).await?;
                Ok(DatabaseResponse::MysqlRows(rows))
            }
            DatabasePool::Postgres(pool) => {
                let query = Self::make_query(request.sql.clone(), &request.binds)?;
                let rows = collect_capped(query.fetch(pool)).await?;
                Ok(DatabaseResponse::PgRows(rows))
            }
            DatabasePool::Sqlite(pool) => {
                let query = Self::make_query(request.sql.clone(), &request.binds)?;
                let rows = collect_capped(query.fetch(pool)).await?;
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

/// Drain a row stream into a `Vec`, failing fast once it would exceed
/// `crate::LIMITS.db_query_rows`. This bounds the memory a single non-streaming
/// `query` can buffer; large result sets must use the streaming cursor API.
async fn collect_capped<S, R>(mut stream: S) -> Result<Vec<R>, sqlx::Error>
where
    S: futures::Stream<Item = Result<R, sqlx::Error>> + Unpin,
{
    let mut rows = Vec::new();
    while let Some(row) = stream.try_next().await? {
        if rows.len() >= crate::LIMITS.db_query_rows {
            return Err(sqlx::Error::Protocol(format!(
                "query returned more than {} rows; use query_stream for large result sets",
                crate::LIMITS.db_query_rows
            )));
        }
        rows.push(row);
    }
    Ok(rows)
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

impl DatabaseRequest {
    /// `(owner, session)` for requests that the handler increments the
    /// backpressure counter for and may reply to; `None` for `Close`.
    fn owner_session(&self) -> Option<(ActorId, i64)> {
        match self {
            DatabaseRequest::Query(owner, session, _)
            | DatabaseRequest::QueryStream(owner, session, _, _)
            | DatabaseRequest::Transaction(owner, session, _) => Some((*owner, *session)),
            DatabaseRequest::Close() => None,
        }
    }
}

impl QueuedRequest for DatabaseRequest {
    fn owner_session(&self) -> Option<(ActorId, i64)> {
        DatabaseRequest::owner_session(self)
    }
}

#[derive(Clone)]
struct DatabaseConnection {
    tx: mpsc::Sender<DatabaseRequest>,
    counter: PendingCounter,
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

/// Whether a `sqlx::Error` is a transient connection/IO failure worth retrying,
/// as opposed to a logical error (bad SQL, constraint violation, decode error)
/// that will fail identically on every retry. Used to gate the fire-and-forget
/// (`session == 0`) self-heal retry so a permanent error can't wedge the
/// single-threaded handler in an infinite loop.
fn is_transient_sqlx_error(err: &sqlx::Error) -> bool {
    matches!(
        err,
        sqlx::Error::Io(_)
            | sqlx::Error::Tls(_)
            | sqlx::Error::PoolTimedOut
            | sqlx::Error::PoolClosed
            | sqlx::Error::WorkerCrashed
    )
}

async fn handle_result(
    database_url: &str,
    failed_times: &mut i32,
    counter: &PendingCounter,
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
            counter.dec();
            false
        }
        Err(err) => {
            if session != 0 {
                let _ =
                    CONTEXT.send_value(protocol_type, owner, session, DatabaseResponse::Error(err));
                counter.dec();
                false
            } else {
                // Fire-and-forget (session == 0): there is no caller to receive
                // the error. Only self-heal on transient connection errors; a
                // logical error (bad SQL, constraint violation, ...) fails the
                // same way on every retry, so drop it instead of looping forever
                // and wedging this single-threaded handler.
                if !is_transient_sqlx_error(&err) {
                    log::error!(
                        "Database '{}' permanent error: '{}'. Dropped (fire-and-forget, no retry). ({}:{})",
                        database_url,
                        err,
                        file!(),
                        line!()
                    );
                    counter.dec();
                    return false;
                }
                if *failed_times > 0 {
                    log::error!(
                        "Database '{}' transient error: '{}'. Will retry. ({}:{})",
                        database_url,
                        err,
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
    counter: PendingCounter,
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
                let batch_size = *batch_size;
                let mut current_owner = *owner;
                let mut current_session = *session;

                macro_rules! do_stream {
                    ($pool:expr, $variant:ident) => {{
                        let query_result =
                            DatabasePool::make_query(query_op.sql.clone(), &query_op.binds);
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
                                        protocol_type,
                                        current_owner,
                                        current_session,
                                        DatabaseResponse::$variant(batch, next_tx),
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
                                CONTEXT.response_error(
                                    0,
                                    current_owner,
                                    -current_session,
                                    err.to_string(),
                                );
                            }
                        }
                    }};
                }

                match pool {
                    DatabasePool::MySql(p) => do_stream!(p, MysqlBatch),
                    DatabasePool::Postgres(p) => do_stream!(p, PgBatch),
                    DatabasePool::Sqlite(p) => do_stream!(p, SqliteBatch),
                }
                // The stream is fully drained / errored / cancelled above; the
                // operation only stops being in-flight now, so `stats()` reflects
                // an active stream for its whole lifetime (decrement at the end,
                // not at entry).
                counter.dec();
            }
            DatabaseRequest::Close() => {
                drain_queued_requests(&mut rx, &counter, |owner, session| {
                    CONTEXT.response_error(
                        0,
                        owner,
                        -session,
                        "sqlx connection closed".to_string(),
                    );
                });
                break;
            }
        }
    }
    // Only remove our own registry entry. If this handler is being superseded by
    // a `connect()` of the same name, the new entry is already in place and must
    // not be deleted — match on our own pending counter to identify ourselves.
    DATABASE_CONNECTIONS.remove_if(&name, |_, v| v.counter.ptr_eq(&counter));
    log::info!("Database connection '{}' closed and removed.", name);
}

// ---------------------------------------------------------------------------
// Lua-facing functions
// ---------------------------------------------------------------------------

extern "C-unwind" fn connect(state: LuaState) -> c_int {
    let database_url = unsafe { laux::lua_check_str(state, 1) };
    let name = unsafe { laux::lua_check_str(state, 2) };
    let connect_timeout: u64 = laux::lua_opt(state, 3).unwrap_or(5000);
    let max_connections: u32 = laux::lua_opt(state, 4).unwrap_or(crate::LIMITS.db_pool_size);
    let queue_capacity: usize =
        laux::lua_opt(state, 5).unwrap_or(crate::LIMITS.request_queue_capacity);
    let queue_capacity = queue_capacity.max(1);

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
                let (tx, rx) = mpsc::channel(queue_capacity);
                let counter = PendingCounter::new();
                // Replacing an existing connection of the same name: tell the
                // previous handler to close so its task and pool don't leak. Its
                // exit removal is guarded (see `database_handler`) so it won't
                // delete the entry we insert here.
                if let Some(old) = DATABASE_CONNECTIONS.insert(
                    name.to_string(),
                    DatabaseConnection {
                        tx: tx.clone(),
                        counter: counter.clone(),
                    },
                ) {
                    log::warn!(
                        "sqlx '{}' reconnected with the same name; closing the previous connection",
                        name
                    );
                    let _ = old.tx.send(DatabaseRequest::Close()).await;
                }
                let _ =
                    CONTEXT.send_value(protocol_type, owner, session, DatabaseResponse::Connect);
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
        LuaValue::String(val) => match String::from_utf8(val.to_vec()) {
            Ok(s) => QueryParams::Text(s),
            Err(e) => QueryParams::Bytes(e.into_bytes()),
        },
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

extern "C-unwind" fn query(state: LuaState) -> c_int {
    query_impl(state, false)
}
extern "C-unwind" fn exec_query(state: LuaState) -> c_int {
    query_impl(state, true)
}

fn query_impl(state: LuaState, forget: bool) -> c_int {
    let mut args = LuaArgs::new(1);
    let conn =
        laux::lua_touserdata::<DatabaseConnection>(state, args.iter_arg()).unwrap_or_else(|| {
            laux::lua_error(state, "invalid database connection pointer".to_string())
        });

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
    let session: i64 = if forget {
        0
    } else {
        unsafe { (*actor).next_session() }
    };

    match conn.tx.try_send(DatabaseRequest::Query(
        owner,
        session,
        DatabaseQuery {
            sql: sql.to_string(),
            binds: params,
        },
    )) {
        Ok(_) => {
            conn.counter.inc();
            if forget {
                laux::lua_push(state, true);
            } else {
                laux::lua_push(state, session);
            }
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
    let conn =
        laux::lua_touserdata::<DatabaseConnection>(state, args.iter_arg()).unwrap_or_else(|| {
            laux::lua_error(state, "invalid database connection pointer".to_string())
        });

    // Parse as i64 so a negative Lua integer is rejected rather than wrapping
    // to a huge `usize`. `batch_size == 0` would make the stream handler emit
    // empty batches forever, so require >= 1.
    let batch_size: i64 =
        laux::lua_opt(state, args.iter_arg()).unwrap_or(crate::LIMITS.db_stream_batch_rows);
    if batch_size < 1 || batch_size as u64 > crate::LIMITS.db_query_rows as u64 {
        push_lua_table!(
            state,
            "kind" => "ERROR",
            "message" => format!(
                "query_stream: batch_size must be between 1 and {}",
                crate::LIMITS.db_query_rows
            )
        );
        return 1;
    }
    let batch_size = batch_size as usize;
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
            conn.counter.inc();
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
    let queries = laux::lua_touserdata::<TransactionQueries>(state, 1).unwrap_or_else(|| {
        laux::lua_error(state, "invalid transaction queries pointer".to_string())
    });

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

extern "C-unwind" fn transaction(state: LuaState) -> c_int {
    transaction_impl(state, false)
}
extern "C-unwind" fn exec_transaction(state: LuaState) -> c_int {
    transaction_impl(state, true)
}

fn transaction_impl(state: LuaState, forget: bool) -> c_int {
    let mut args = LuaArgs::new(1);
    let conn =
        laux::lua_touserdata::<DatabaseConnection>(state, args.iter_arg()).unwrap_or_else(|| {
            laux::lua_error(state, "invalid database connection pointer".to_string())
        });

    let queries = laux::lua_touserdata::<TransactionQueries>(state, args.iter_arg())
        .unwrap_or_else(|| {
            laux::lua_error(state, "invalid transaction queries pointer".to_string())
        });

    let actor = LuaActor::from_lua_state(state);
    let owner = unsafe { (*actor).id };
    let session: i64 = if forget {
        0
    } else {
        unsafe { (*actor).next_session() }
    };

    match conn.tx.try_send(DatabaseRequest::Transaction(
        owner,
        session,
        std::mem::take(&mut queries.queries),
    )) {
        Ok(_) => {
            conn.counter.inc();
            if forget {
                laux::lua_push(state, true);
            } else {
                laux::lua_push(state, session);
            }
            1
        }
        Err(err) => {
            push_lua_table!(state, "kind" => "ERROR", "message" => err.to_string());
            1
        }
    }
}

extern "C-unwind" fn close(state: LuaState) -> c_int {
    let conn = laux::lua_touserdata::<DatabaseConnection>(state, 1).unwrap_or_else(|| {
        laux::lua_error(state, "invalid database connection pointer".to_string())
    });

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
        .unwrap_or_else(|| laux::lua_error(state, "invalid sqlx cursor handle".to_string()));
    if let Some(tx) = handle.0.take() {
        let actor = LuaActor::from_lua_state(state);
        let owner = unsafe { (*actor).id };
        let session = unsafe { (*actor).next_session() };
        // If the stream handler has already exited, its receiver is gone and the
        // send fails. Surface that as an error rather than pushing a session that
        // would never be answered (which would hang the awaiting coroutine).
        if tx.send(CursorSignal::Next(owner, session)).is_err() {
            return crate::lua_push_error(state, "sqlx cursor: stream handler is gone");
        }
        laux::lua_push(state, session);
        1
    } else {
        crate::lua_push_error(state, "sqlx cursor: already consumed or closed")
    }
}

extern "C-unwind" fn sqlx_cursor_close(state: LuaState) -> c_int {
    let handle = laux::lua_touserdata::<SqlxCursorHandle>(state, 1)
        .unwrap_or_else(|| laux::lua_error(state, "invalid sqlx cursor handle".to_string()));
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

fn push_sqlx_response(state: LuaState, result: DatabaseResponse) -> c_int {
    match result {
        DatabaseResponse::PgRows(rows) => process_rows::<Postgres>(state, &rows),
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
        table.insert(pair.key().as_str(), pair.value().counter.load());
    });
    1
}

pub unsafe extern "C-unwind" fn decode_sqlx_message(
    state: LuaState,
    m: *mut moon_runtime::context::Message,
) -> c_int {
    match unsafe { crate::message_decode::take_boxed::<DatabaseResponse>(m) } {
        Ok(response) => push_sqlx_response(state, response),
        Err(e) => crate::lua_push_error(state, &e),
    }
}

pub extern "C-unwind" fn luaopen_sqlx(state: LuaState) -> c_int {
    let l = [
        lreg!("connect", connect),
        lreg!("find_connection", find_connection),
        lreg!("stats", stats),
        lreg!("make_transaction", make_transaction),
        lreg!("json_param", make_json_param),
        lreg_null!(),
    ];

    luaL_newlib!(state, l);

    1
}
