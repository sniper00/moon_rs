# MongoDB Module (`lua_mongodb`)

Native MongoDB driver using the official `mongodb` Rust crate.

## Architecture

```
┌─────────────────────────────────────────────────────────────┐
│ Lua Actor Thread                                            │
│                                                             │
│  col("find", query, opts)                                   │
│    → convert Lua table to BSON Document                     │
│    → send DatabaseRequest to worker via mpsc                │
│    → moon.wait(session) — coroutine yields                  │
│                                                             │
│  PTYPE_MONGODB message arrives → moon.core.decode_message(m)│
│    → parse BSON documents into Lua tables                   │
│    → coroutine resumes with result                          │
└─────────────┬───────────────────────────────────────────────┘
              │ mpsc channel (DatabaseRequest enum)
              ▼
┌─────────────────────────────────────────────────────────────┐
│ IO Runtime (tokio) — MongoDB Client Worker                   │
│                                                             │
│  mongodb::Client (connection pool managed internally)        │
│  Executes operations, returns BSON results to actor         │
└─────────────────────────────────────────────────────────────┘
```

## Lua API

```lua
local mongodb = require("moon.db.mongodb")

-- Connect (async)
local db = mongodb.connect("mongodb://127.0.0.1:27017", "main")
-- Optional third argument: queue_capacity (default 1024)

-- Or find existing connection
local db = mongodb.find_connection("main")

-- Get collection handle
local users = db:collection("mydb", "users")

-- Insert
local res = users("insert_one", { name = "Alice", age = 30 })
local res = users("insert_many", {
    { name = "Bob", age = 25 },
    { name = "Carol", age = 28 },
})

-- Find
local doc = users("find_one", { name = "Alice" })
local docs = users("find", { age = { ["$gte"] = 25 } }, { limit = 10 })

-- Update
local res = users("update_one",
    { name = "Alice" },                    -- filter
    { ["$set"] = { age = 31 } }            -- update
)
local res = users("update_many",
    { age = { ["$lt"] = 18 } },
    { ["$set"] = { status = "minor" } }
)

-- Delete
local res = users("delete_one", { name = "Bob" })
local res = users("delete_many", { status = "inactive" })

-- Count
local n = users("count", { age = { ["$gte"] = 18 } })

-- Streaming find (memory-efficient cursor iteration)
for doc in db:find_stream("mydb", "users", { active = true }, nil, 100) do
    process(doc)
end

-- Pool stats
local total = mongodb.stats()

-- Close
db:close()
```

## Supported Operations

| Operation | Description |
|-----------|-------------|
| `find_one` | Find a single document matching filter |
| `find` | Find all documents matching filter (with options) |
| `insert_one` | Insert a single document |
| `insert_many` | Insert multiple documents |
| `update_one` | Update first document matching filter |
| `update_many` | Update all documents matching filter |
| `delete_one` | Delete first document matching filter |
| `delete_many` | Delete all documents matching filter |
| `count` | Count documents matching filter |
| `find_stream` | Streaming cursor iteration |

## Find Options

```lua
local opts = {
    limit = 100,           -- max documents to return
    skip = 20,             -- skip first N documents
    sort = { age = -1 },   -- sort order (1=asc, -1=desc)
    projection = { name = 1, age = 1, _id = 0 },  -- field selection
}
local docs = users("find", {}, opts)
```

## Streaming Queries

`find_stream` returns a Lua 5.4+ iterator with to-be-closed semantics:
- Fetches documents in batches (default 100) using a server-side cursor.
- Each iteration yields one document.
- Cursor is automatically closed when the loop exits (normal, break, or error).

```lua
for doc in db:find_stream("mydb", "large_collection", {}, nil, 200) do
    -- process one doc at a time
end
```

## BSON Type Mapping

| BSON Type | Lua Type |
|-----------|----------|
| String | string |
| Int32 / Int64 | number (integer) |
| Double | number (float) |
| Boolean | boolean |
| Document | table (map) |
| Array | table (array) |
| ObjectId | string (24-char hex) |
| DateTime | number (unix timestamp ms) |
| Null | nil |
| Binary | string (raw bytes) |

## Result Format

Insert result:
```lua
{ inserted_id = "60f7..." }
```

Update result:
```lua
{ matched_count = 1, modified_count = 1 }
```

Delete result:
```lua
{ deleted_count = 5 }
```

Error:
```lua
{ kind = "ServerError", message = "..." }
```

## Files

| Path | Role |
|------|------|
| `crates/moon-modules/src/lua_mongodb.rs` | Rust implementation (~1200 lines) |
| `lualib/moon/db/mongodb.lua` | Lua wrapper with collection proxy |
| `assets/example/example_mongodb.lua` | Usage examples |
| `assets/test/test_mongodb.lua` | Functional tests |
| `assets/test/test_mongodb_stream.lua` | Streaming cursor tests |
