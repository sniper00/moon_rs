# SQLx Module (`lua_sqlx`)

Multi-database driver using the `sqlx` crate, supporting PostgreSQL, MySQL, and SQLite.

## Architecture

```
┌─────────────────────────────────────────────────────────────┐
│ Lua Actor Thread                                            │
│                                                             │
│  db:query(sql, params...)                                   │
│    → encode query + params via sqlx                         │
│    → send to worker via mpsc channel                        │
│    → moon.wait(session) — coroutine yields                  │
│                                                             │
│  PTYPE_SQLX message arrives → moon.core.decode_message(m)   │
│    → parse rows into Lua tables                             │
│    → coroutine resumes with result                          │
└─────────────┬───────────────────────────────────────────────┘
              │ mpsc channel
              ▼
┌─────────────────────────────────────────────────────────────┐
│ IO Runtime (tokio) — Worker Pool (sqlx connection pool)      │
│                                                             │
│  sqlx::PgPool / MySqlPool / SqlitePool                      │
│  Executes queries, returns rows to actor via PTYPE_SQLX     │
└─────────────────────────────────────────────────────────────┘
```

## Supported Databases

| Database | URL Prefix | Pool Type |
|----------|-----------|-----------|
| PostgreSQL | `postgres://` or `postgresql://` | `PgPool` |
| MySQL | `mysql://` | `MySqlPool` |
| SQLite | `sqlite://` | `SqlitePool` |

## Lua API

```lua
local sqlx = require("moon.db.sqlx")

-- Connect (creates named pool, async)
local db = sqlx.connect("postgres://user:pass@host:5432/db", "main", 5000, 10)

-- Simple query with parameters
local rows = db:query("SELECT * FROM users WHERE age > ?", 18)

-- Fire-and-forget
db:execute("DELETE FROM logs WHERE created_at < ?", old_date)

-- Transaction (multiple statements atomically)
local result = db:transaction({
    {"INSERT INTO users (name) VALUES (?)", "Alice"},
    {"UPDATE counters SET n = n + 1"},
})

-- Fire-and-forget transaction
db:execute_transaction({
    {"INSERT INTO audit (msg) VALUES (?)", "batch op"},
    {"DELETE FROM temp"},
})

-- Streaming query (memory-efficient for large result sets)
for row in db:query_stream("SELECT * FROM big_table", 100) do
    process(row)
end

-- JSON parameter (explicit type hint for json/jsonb columns)
local rows = db:query(
    "SELECT * FROM t WHERE data @> ?",
    sqlx.json({key = "value"})
)

-- Pool stats
local total = sqlx.stats()

-- Close
db:close()
```

## Result Format

Success (array of row tables):
```lua
{
    { id = 1, name = "Alice", age = 30 },
    { id = 2, name = "Bob",   age = 25 },
}
```

Error:
```lua
{ kind = "Database", message = "duplicate key..." }
```

## Type Mapping

| SQL Type | Lua Type |
|----------|----------|
| INT / BIGINT / SMALLINT | number (integer) |
| FLOAT / DOUBLE / REAL | number (float) |
| DECIMAL / NUMERIC | number (float) |
| BOOLEAN | boolean |
| TEXT / VARCHAR / CHAR | string |
| JSON / JSONB | string (raw JSON) |
| DATE / TIME / TIMESTAMP | string (ISO format) |
| UUID | string |
| BLOB / BYTEA | string (raw bytes) |
| NULL | nil (field absent) |

## Streaming Queries

`query_stream` returns a Lua 5.4+ iterator with to-be-closed semantics:
- Fetches rows in batches (default 100) from the server.
- Each iteration yields one row.
- Server-side cursor is automatically closed when the loop ends (normal, break, or error).

```lua
for row in db:query_stream("SELECT * FROM large_table WHERE active", 200) do
    -- process row one at a time; only 200 rows in memory
end
```

## Configuration

| Parameter | Default | Description |
|-----------|---------|-------------|
| `timeout` | 5000ms | Connect timeout |
| `max_connections` | 5 | Maximum pool size |
| `queue_capacity` | 1024 | Bounded request queue capacity |

## Files

| Path | Role |
|------|------|
| `crates/moon-modules/src/lua_sqlx.rs` | Rust implementation (~1000 lines) |
| `lualib/moon/db/sqlx.lua` | Lua wrapper with async/await |
| `assets/example/example_sqlx.lua` | Usage examples |
| `assets/test/test_sqlx_postgres_types.lua` | PostgreSQL type tests |
| `assets/test/test_sqlx_mysql_types.lua` | MySQL type tests |
| `assets/test/test_sqlx_stream.lua` | Streaming query tests |
