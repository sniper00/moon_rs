use crate::lua_json::{encode_table, JsonOptions};
use dashmap::DashMap;
use lazy_static::lazy_static;
use lib_core::actor::LuaActor;
use lib_core::context::{self, CONTEXT};
use lib_lua::{
    self, cstr, ffi,
    ffi::luaL_Reg,
    laux,
    laux::{lua_into_userdata, LuaArgs, LuaNil, LuaTable, LuaValue},
    lreg, lreg_null, luaL_newlib, push_lua_table,
};
use sqlx::{
    self,
    migrate::MigrateDatabase,
    mysql::MySqlRow,
    postgres::{PgPoolOptions, PgRow},
    sqlite::SqliteRow,
    Column, ColumnIndex, Database, MySql, MySqlPool, PgPool, Postgres, Row, Sqlite, SqlitePool,
    TypeInfo, ValueRef,
};
use std::{
    ffi::c_int,
    sync::{atomic::AtomicI64, Arc},
    time::Duration,
};
use tokio::{sync::mpsc, time::timeout};

lazy_static! {
    static ref DATABASE_CONNECTIONSS: DashMap<String, DatabaseConnection> = DashMap::new();
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
                    sqlx::Error::Io(std::io::Error::new(
                        std::io::ErrorKind::Other,
                        format!("Connection error: {}", err),
                    ))
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
    Query(i64, i64, DatabaseQuery), //owner, session, QueryBuilder
    Transaction(i64, i64, Vec<DatabaseQuery>), //owner, session, Vec<QueryBuilder>
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
}

extern "C-unwind" fn connect(state: *mut ffi::lua_State) -> c_int {
    let database_url: &str = laux::lua_get(state, 1);
    let name: &str = laux::lua_get(state, 2);
    let connect_timeout: u64 = laux::lua_opt(state, 3).unwrap_or(5000);

    let actor = LuaActor::from_lua_state(state);
    let owner = actor.id;
    let session = actor.next_session();
    let protocol_type = context::PTYPE_SQLX;

    tokio::spawn(async move {
        match DatabasePool::connect(database_url, Duration::from_millis(connect_timeout)).await {
            Ok(pool) => {
                let (tx, rx) = mpsc::channel(100);
                let counter = Arc::new(AtomicI64::new(0));
                DATABASE_CONNECTIONSS.insert(
                    name.to_string(),
                    DatabaseConnection {
                        tx: tx.clone(),
                        counter: counter.clone(),
                    },
                );
                CONTEXT.send_value(protocol_type, owner, session, DatabaseResponse::Connect);
                database_handler(protocol_type, &pool, rx, database_url, counter).await;
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

    laux::lua_push(state, session);
    1
}

fn get_query_param(state: *mut ffi::lua_State, i: i32) -> Result<QueryParams, String> {
    let options = JsonOptions::default();

    let res = match LuaValue::from_stack(state, i) {
        LuaValue::Boolean(val) => QueryParams::Bool(val),
        LuaValue::Number(val) => QueryParams::Float(val),
        LuaValue::Integer(val) => QueryParams::Int(val),
        LuaValue::String(val) => {
            if val.starts_with(b"{") || val.starts_with(b"[") {
                if let Ok(value) = serde_json::from_slice::<serde_json::Value>(val) {
                    QueryParams::Json(value)
                } else {
                    QueryParams::Text(unsafe { String::from_utf8_unchecked(val.to_vec()) })
                }
            } else {
                QueryParams::Text(unsafe { String::from_utf8_unchecked(val.to_vec()) })
            }
        }
        LuaValue::Table(val) => {
            let mut buffer = Vec::new();
            if let Err(err) = encode_table(&mut buffer, &val, 0, false, &options) {
                drop(buffer);
                laux::lua_error(state, &err);
            }
            if buffer[0] == b'{' || buffer[0] == b'[' {
                if let Ok(value) = serde_json::from_slice::<serde_json::Value>(buffer.as_slice()) {
                    QueryParams::Json(value)
                } else {
                    QueryParams::Bytes(buffer)
                }
            } else {
                QueryParams::Bytes(buffer)
            }
        }
        _t => {
            return Err(format!(
                "get_query_param: unsupport value type :{}",
                laux::type_name(state, i)
            ));
        }
    };
    Ok(res)
}

extern "C-unwind" fn query(state: *mut ffi::lua_State) -> c_int {
    let mut args = LuaArgs::new(1);
    let conn = laux::lua_touserdata::<DatabaseConnection>(state, args.iter_arg())
        .expect("Invalid database connect pointer");

    let session = laux::lua_get(state, args.iter_arg());

    let sql = laux::lua_get::<&str>(state, args.iter_arg());
    let mut params = Vec::new();
    let top = laux::lua_top(state);
    for i in args.iter_arg()..=top {
        let param = get_query_param(state, i);
        match param {
            Ok(value) => {
                params.push(value);
            }
            Err(err) => {
                push_lua_table!(
                    state,
                    "kind" => "ERROR",
                    "message" => err
                );
                return 1;
            }
        }
    }

    let owner = LuaActor::from_lua_state(state).id;

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

struct TransactionQuerys {
    querys: Vec<DatabaseQuery>,
}

extern "C-unwind" fn push_transaction_query(state: *mut ffi::lua_State) -> c_int {
    let querys = laux::lua_touserdata::<TransactionQuerys>(state, 1)
        .expect("Invalid transaction query pointer");

    let sql = laux::lua_get::<&str>(state, 2);
    let mut params = Vec::new();
    let top = laux::lua_top(state);
    for i in 3..=top {
        let param = get_query_param(state, i);
        match param {
            Ok(value) => {
                params.push(value);
            }
            Err(err) => {
                drop(params);
                laux::lua_error(state, err.as_ref());
            }
        }
    }

    querys.querys.push(DatabaseQuery {
        sql: sql.to_string(),
        binds: params,
    });

    0
}

extern "C-unwind" fn make_transaction(state: *mut ffi::lua_State) -> c_int {
    laux::lua_newuserdata(
        state,
        TransactionQuerys { querys: Vec::new() },
        cstr!("sqlx_transaction_metatable"),
        &[lreg!("push", push_transaction_query), lreg_null!()],
    );
    1
}

extern "C-unwind" fn transaction(state: *mut ffi::lua_State) -> c_int {
    let mut args = LuaArgs::new(1);
    let conn = laux::lua_touserdata::<DatabaseConnection>(state, args.iter_arg())
        .expect("Invalid database connect pointer");

    let session = laux::lua_get(state, args.iter_arg());

    let querys = laux::lua_touserdata::<TransactionQuerys>(state, args.iter_arg())
        .expect("Invalid transaction query pointer");

    let owner = LuaActor::from_lua_state(state).id;

    match conn.tx.try_send(DatabaseRequest::Transaction(
        owner,
        session,
        std::mem::take(&mut querys.querys),
    )) {
        Ok(_) => {
            conn.counter
                .fetch_add(1, std::sync::atomic::Ordering::Release);
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

extern "C-unwind" fn close(state: *mut ffi::lua_State) -> c_int {
    let conn = laux::lua_touserdata::<DatabaseConnection>(state, 1)
        .expect("Invalid database connect pointer");

    match conn.tx.try_send(DatabaseRequest::Close()) {
        Ok(_) => {
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
}

fn process_rows<'a, DB>(
    state: *mut ffi::lua_State,
    rows: &'a [<DB as Database>::Row],
) -> Result<i32, String>
where
    DB: sqlx::Database,
    usize: ColumnIndex<<DB as Database>::Row>,
    bool: sqlx::Decode<'a, DB>,
    i64: sqlx::Decode<'a, DB>,
    f64: sqlx::Decode<'a, DB>,
    &'a str: sqlx::Decode<'a, DB>,
    &'a [u8]: sqlx::Decode<'a, DB>,
{
    let table = LuaTable::new(state, rows.len(), 0);
    if rows.is_empty() {
        return Ok(1);
    }

    let mut column_info = Vec::new();
    if column_info.is_empty() {
        rows.iter()
            .next()
            .unwrap()
            .columns()
            .iter()
            .enumerate()
            .for_each(|(index, column)| {
                column_info.push((index, column.name()));
            });
    }

    let mut i = 0;
    for row in rows.iter() {
        let row_table = LuaTable::new(state, 0, row.len());
        for (index, column_name) in column_info.iter() {
            match row.try_get_raw(*index) {
                Ok(value) => match value.type_info().name() {
                    "NULL" => {
                        row_table.rawset(*column_name, LuaNil {});
                    }
                    "BOOL" | "BOOLEAN" => {
                        row_table.rawset(
                            *column_name,
                            sqlx::decode::Decode::decode(value).unwrap_or(false),
                        );
                    }
                    "INT2" | "INT4" | "INT8" | "TINYINT" | "SMALLINT" | "INT" | "MEDIUMINT"
                    | "BIGINT" | "INTEGER" => {
                        row_table.rawset(
                            *column_name,
                            sqlx::decode::Decode::decode(value).unwrap_or(0),
                        );
                    }
                    "FLOAT4" | "FLOAT8" | "NUMERIC" | "FLOAT" | "DOUBLE" | "REAL" => {
                        row_table.rawset(
                            *column_name,
                            sqlx::decode::Decode::decode(value).unwrap_or(0.0),
                        );
                    }
                    "TEXT" => {
                        row_table.rawset(
                            *column_name,
                            sqlx::decode::Decode::decode(value).unwrap_or(""),
                        );
                    }
                    _ => {
                        let column_value: &[u8] =
                            sqlx::decode::Decode::decode(value).unwrap_or(b"");
                        row_table.rawset(*column_name, column_value);
                    }
                },
                Err(error) => {
                    laux::lua_push(state, false);
                    laux::lua_push(
                        state,
                        format!("{:?} decode error: {:?}", column_name, error),
                    );
                    return Ok(2);
                }
            }
        }
        i += 1;
        table.seti(i);
    }
    Ok(1)
}

extern "C-unwind" fn find_connection(state: *mut ffi::lua_State) -> c_int {
    let name = laux::lua_get::<&str>(state, 1);
    match DATABASE_CONNECTIONSS.get(name) {
        Some(pair) => {
            let l = [
                lreg!("query", query),
                lreg!("transaction", transaction),
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

extern "C-unwind" fn decode(state: *mut ffi::lua_State) -> c_int {
    laux::luaL_checkstack(state, 6, std::ptr::null());
    let result = lua_into_userdata::<DatabaseResponse>(state, 1);

    match *result {
        DatabaseResponse::PgRows(rows) => {
            return process_rows::<Postgres>(state, &rows)
                .map_err(|e| {
                    push_lua_table!(
                        state,
                        "kind" => "ERROR",
                        "message" => e
                    );
                })
                .unwrap_or(1);
        }
        DatabaseResponse::MysqlRows(rows) => {
            return process_rows::<MySql>(state, &rows)
                .map_err(|e| {
                    push_lua_table!(
                        state,
                        "kind" => "ERROR",
                        "message" => e
                    );
                })
                .unwrap_or(1);
        }
        DatabaseResponse::SqliteRows(rows) => {
            return process_rows::<Sqlite>(state, &rows)
                .map_err(|e| {
                    push_lua_table!(
                        state,
                        "kind" => "ERROR",
                        "message" => e
                    );
                })
                .unwrap_or(1);
        }
        DatabaseResponse::Transaction => {
            push_lua_table!(
                state,
                "message" => "ok"
            );
            return 1;
        }
        DatabaseResponse::Connect => {
            push_lua_table!(
                state,
                "message" => "success"
            );
            return 1;
        }
        DatabaseResponse::Error(err) => match err.as_database_error() {
            Some(db_err) => {
                push_lua_table!(
                    state,
                    "kind" => "DB",
                    "message" => db_err.message()
                );
            }
            None => {
                push_lua_table!(
                    state,
                    "kind" => "ERROR",
                    "message" => err.to_string()
                );
            }
        },
        DatabaseResponse::Timeout(err) => {
            push_lua_table!(
                state,
                "kind" => "TIMEOUT",
                "message" => err.to_string()
            );
        }
    }

    1
}

extern "C-unwind" fn stats(state: *mut ffi::lua_State) -> c_int {
    let table = LuaTable::new(state, 0, DATABASE_CONNECTIONSS.len());
    DATABASE_CONNECTIONSS.iter().for_each(|pair| {
        table.rawset(
            pair.key().as_str(),
            pair.value()
                .counter
                .load(std::sync::atomic::Ordering::Acquire),
        );
    });
    1
}

#[unsafe(no_mangle)]
#[allow(clippy::not_unsafe_ptr_arg_deref)]
pub extern "C-unwind" fn luaopen_sqlx(state: *mut ffi::lua_State) -> c_int {
    let l = [
        lreg!("connect", connect),
        lreg!("find_connection", find_connection),
        lreg!("decode", decode),
        lreg!("stats", stats),
        lreg!("make_transaction", make_transaction),
        lreg_null!(),
    ];

    luaL_newlib!(state, l);

    1
}
