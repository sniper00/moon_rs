# PostgreSQL Native Driver (`lua_pg`)

High-performance native PostgreSQL driver implementing the v3 wire protocol in Rust.

## Architecture

```
┌─────────────────────────────────────────────────────────────┐
│ Lua Actor Thread                                            │
│                                                             │
│  pg:query(sql)                                              │
│    → encode wire bytes (Parse/Bind/Describe/Execute/Sync)   │
│    → select worker (round-robin)                            │
│    → send request via mpsc channel                          │
│    → moon.wait(session) — coroutine yields                  │
│                                                             │
│  PTYPE_PG message arrives → moon.core.decode_message(m)     │
│    → parse DataRow/CommandComplete into Lua tables           │
│    → coroutine resumes with result                          │
└─────────────┬───────────────────────────────────────────────┘
              │ mpsc channel (pre-built wire buffer)
              ▼
┌─────────────────────────────────────────────────────────────┐
│ IO Runtime (tokio) — Worker Tasks (one per connection)       │
│                                                             │
│  Worker #0: TcpStream → write request → read response       │
│  Worker #1: TcpStream → write request → read response       │
│  ...                                                        │
│  Worker #N: TcpStream → write request → read response       │
│                                                             │
│  Each worker owns its TCP connection and reconnects on error │
└─────────────────────────────────────────────────────────────┘
```

## Design Principles

1. **No external crate dependency** — hand-written PG v3 wire protocol for full control.
2. **Minimal copies** — request wire bytes are built once on the Lua thread, then moved to the worker. Response bytes are parsed directly into Lua values.
3. **Connection pool** — named pools with configurable `max_connections` workers.
4. **Auto-reconnect** — workers reconnect transparently on socket errors.
5. **SCRAM-SHA-256 authentication** — full implementation including channel binding.

## Lua API

```lua
local pg = require("moon.db.pg")

-- Connect (creates a named pool, async). All settings live in the URL; pool
-- options are supplied as ?param=value query parameters (name is required).
local db = pg.connect("postgres://user:pass@host:5432/dbname?name=main&max_connections=5")

-- Or find an existing pool by name
local db = pg.find_connection("main")

-- Simple query (may contain multiple statements separated by ;)
local res = db:query("SELECT * FROM users WHERE id = 1")

-- Extended query with parameters ($1, $2, ...)
local res = db:query_params("SELECT * FROM users WHERE id = $1", 42)

-- Pipeline multiple statements in a transaction
local res = db:pipe({
    {"INSERT INTO log (msg) VALUES ($1)", "hello"},
    {"UPDATE counters SET n = n + 1 WHERE id = $1", 1},
})

-- Bulk insert (auto-chunked, transactional)
local res = db:insert_many("users", {"name", "age"}, {
    {"Alice", 30},
    {"Bob", 25},
})

-- Bulk upsert
local res = db:insert_many("users", {"id", "name"}, {
    {1, "Alice Updated"},
}, "ON CONFLICT (id) DO UPDATE SET name = EXCLUDED.name")

-- Bulk update
local res = db:update_many("users", "id", {"name", "age"}, {
    {1, "New Name", 31},
    {2, "Bob2", 26},
}, "bigint")

-- Fire-and-forget variants (no response awaited)
db:execute("NOTIFY channel, 'payload'")
db:execute_params("INSERT INTO log (msg) VALUES ($1)", "fire-and-forget")
db:execute_pipe({...})
db:execute_insert_many(...)
db:execute_update_many(...)

-- Pool stats
local pending = db:len()         -- per-worker queue lengths
local total = pg.stats()         -- total pending across all pools

-- Close
db:close()
```

## Result Format

Successful query:
```lua
{
    { id = 1, name = "Alice", age = 30 },
    { id = 2, name = "Bob",   age = 25 },
}
```

Error:
```lua
{
    code = "23505",           -- PostgreSQL SQLSTATE or "SOCKET"/"CONFIG"/"ENCODE"
    message = "duplicate key value violates unique constraint..."
}
```

Pipeline result (array of results per statement):
```lua
{
    { {affected_rows = 1} },    -- INSERT result
    { {affected_rows = 1} },    -- UPDATE result
}
```

## Type Mapping

| PostgreSQL Type | Lua Type |
|----------------|----------|
| `int2/int4/int8` | number (integer) |
| `float4/float8/numeric` | number (float) |
| `bool` | boolean |
| `text/varchar/char` | string |
| `json/jsonb` | decoded Lua table |
| `bytea` | string (raw bytes) |
| `timestamp/date/time` | string (ISO format) |
| `uuid` | string |
| `array types` | Lua table (array) |
| `NULL` | nil (field absent) |

## Connection URL Format

```
postgresql://username:password@host:port/database?param=value&...
```

All connect settings are carried in the single URL. The pool settings are
supplied as `?param=value` query parameters:

| Query param | Default | Description |
|-------------|---------|-------------|
| `name` | *(required)* | Pool name for `find_connection` |
| `application_name` | `moon` | PostgreSQL `application_name` |
| `connect_timeout` | 5000ms | Connect timeout |
| `max_connections` / `pool_size` | 5 | Pool size (worker count) |
| `read_timeout` | 10000ms | Response read timeout |
| `queue_capacity` | 1024 | Per-worker bounded request queue capacity |

Example:

```
postgres://postgres:123456@127.0.0.1:5432/postgres?name=main&max_connections=8&read_timeout=20000
```

## Wire Protocol Details

- **Simple Query**: Single `'Q'` message, returns complete result set.
- **Extended Query**: Parse → Bind → Describe → Execute → Sync pipeline for parameterized queries.
- **Pipeline Mode**: Multiple Parse/Bind/Execute sequences batched in one write, wrapped in implicit `BEGIN`/`COMMIT`.
- **Bulk Operations**: Multi-row `VALUES` clauses auto-chunked to stay under the 65535 parameter limit.

## Files

| Path | Role |
|------|------|
| `crates/moon-runtime/src/modules/lua_pg.rs` | Rust implementation (~2100 lines) |
| `lualib/moon/db/pg.lua` | Lua wrapper with async/await |
| `assets/example/example_pg.lua` | Usage examples |
| `assets/test/test_pg.lua` | Comprehensive type/feature tests |
