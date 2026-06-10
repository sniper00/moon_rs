# Redis Native Driver (`lua_redis`)

High-performance native Redis driver with RESP protocol implementation in Rust.

## Architecture

```
┌─────────────────────────────────────────────────────────────┐
│ Lua Actor Thread                                            │
│                                                             │
│  redis:get("key")                                           │
│    → encode RESP command: "*2\r\n$3\r\nGET\r\n$3\r\nkey\r\n" │
│    → select worker (round-robin)                            │
│    → send via mpsc channel                                  │
│    → moon.wait(session) — coroutine yields                  │
│                                                             │
│  PTYPE_REDIS message arrives → moon.core.decode_message(m)  │
│    → parse RESP into Lua values                             │
│    → coroutine resumes with result                          │
└─────────────┬───────────────────────────────────────────────┘
              │ mpsc channel (pre-built RESP buffer)
              ▼
┌─────────────────────────────────────────────────────────────┐
│ IO Runtime (tokio) — Worker Tasks (one per connection)       │
│                                                             │
│  Worker #0: TcpStream → write RESP → read RESP response     │
│  Worker #1: TcpStream → write RESP → read RESP response     │
│  ...                                                        │
│                                                             │
│  Each worker owns its TCP connection and reconnects on error │
└─────────────────────────────────────────────────────────────┘
```

## Design Principles

1. **RESP protocol in Rust** — direct byte-level encoding/decoding, no third-party Redis crate.
2. **Minimal copies** — RESP command bytes built once, moved to worker. Response bytes parsed directly into Lua values.
3. **Named connection pools** — multiple pools (e.g. "cache", "session") accessible by name.
4. **Auto-reconnect** — workers reconnect transparently on socket errors with AUTH/SELECT replay.
5. **Dynamic command dispatch** — any Redis command usable as a method via Lua `__index` metamethod.

## Lua API

```lua
local redis = require("moon.db.redis")

-- Connect (creates a named pool, async)
local rdb, err = redis.connect({
    host = "127.0.0.1",
    port = 6379,
    auth = "password",   -- optional
    db = 0,              -- optional, default 0
}, "default", 5000, 4)  -- name, timeout_ms, pool_size, queue_capacity?

-- Or find existing pool
local rdb = redis.find_connection("default")

-- Any Redis command as a method (auto uppercased)
local val = rdb:get("mykey")
rdb:set("mykey", "myvalue")
rdb:hset("myhash", "field", "value")
local all = rdb:hgetall("myhash")
rdb:expire("mykey", 60)
local n = rdb:incr("counter")
rdb:del("key1", "key2")
rdb:zadd("myset", 1, "a", 2, "b")
local members = rdb:zrange("myset", 0, -1, "WITHSCORES")

-- Pipeline (batch multiple commands, single round-trip)
local results = rdb:pipeline({
    {"SET", "k1", "v1"},
    {"SET", "k2", "v2"},
    {"GET", "k1"},
    {"GET", "k2"},
})
-- results = { "OK", "OK", "v1", "v2" }

-- Fire-and-forget
rdb:execute("SET", "key", "value")
rdb:execute_pipeline({
    {"SET", "a", "1"},
    {"SET", "b", "2"},
})

-- Pool stats
local pending = rdb:len()        -- per-worker queue lengths
local total = redis.stats()      -- total pending across all pools

-- Close
rdb:close()

-- Pub/Sub watch (dedicated socket, not pooled)
local watch = redis.watch({ host = "127.0.0.1", port = 6379, timeout = 5000 })
watch:subscribe("my-channel")
moon.async(function()
    local message, channel = watch:message()
    print(channel, message)
end)
rdb:publish("my-channel", "hello")
watch:disconnect()
```

## Result Format

Commands return Lua-native values:

| Redis Response | Lua Value |
|---------------|-----------|
| Simple String (`+OK`) | `"OK"` |
| Bulk String | `string` |
| Integer | `number` (integer) |
| Array | `table` (array) |
| Nil | `nil` (via `cjson.null` or absent) |
| Error | `table: { code = "REDIS", message = "ERR ..." }` |

### HGETALL Result

```lua
-- Redis: HGETALL myhash → ["field1", "val1", "field2", "val2"]
-- Lua: flat array (standard Redis behavior)
local r = rdb:hgetall("myhash")
-- r = { "field1", "val1", "field2", "val2" }
```

### Pipeline Result

```lua
-- Array of results, one per command
local r = rdb:pipeline({
    {"SET", "k", "v"},
    {"GET", "k"},
    {"INCR", "counter"},
})
-- r = { "OK", "v", 42 }
```

### Error Result

```lua
-- On socket/connection error:
{ code = "SOCKET", message = "connection refused" }

-- On Redis command error:
{ code = "REDIS", message = "WRONGTYPE Operation against a key holding..." }
```

## Connection Options

```lua
{
    host = "127.0.0.1",    -- Redis server host
    port = 6379,           -- Redis server port
    auth = "",             -- AUTH password (empty = no auth)
    db = 0,               -- SELECT database number
}
```

| Parameter | Default | Description |
|-----------|---------|-------------|
| `name` | `"default"` | Pool name for lookup |
| `timeout` | 5000ms | Connect timeout |
| `pool_size` | 1 | Number of worker connections |
| `queue_capacity` | 1024 | Per-worker bounded request queue capacity |

## Worker Lifecycle

1. Connect to Redis server (with timeout).
2. Send `AUTH` if password is set.
3. Send `SELECT` if db != 0.
4. Enter request loop: read from channel → write RESP → read response → deliver to actor.
5. On socket error: log, wait 1s, reconnect, replay AUTH/SELECT.

## RESP Encoding

Commands are encoded in RESP (Redis Serialization Protocol) format:
```
*<argc>\r\n
$<len>\r\n<arg>\r\n
$<len>\r\n<arg>\r\n
...
```

Lua values are converted to RESP bulk strings:
- `string` → as-is
- `number` → `tostring()`
- `boolean` → `"1"` / `"0"`
- `nil` → empty bulk string

## Files

| Path | Role |
|------|------|
| `crates/moon-modules/src/lua_redis.rs` | Rust implementation (pool + pub/sub watch) |
| `lualib/moon/db/redis.lua` | Lua wrapper with dynamic dispatch |
| `assets/example/example_redis.lua` | Usage examples |
| `assets/test/test_redis.lua` | Native driver + pub/sub watch + stream tests |
| `assets/test/redis_stream.lua` | Stream helper (XADD/XREADGROUP/...) |
| `assets/benchmark/benchmark_redis.lua` | Native driver performance benchmark |
