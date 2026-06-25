# Statistics (Stats)

moon_rs exposes two categories of runtime statistics:

- **Server / runtime stats**: `moon.server_stats([key])` — process-level counters (service count, memory, message volume, logs, timers, CPU, etc.) plus per-actor details.
- **Connection pool stats**: `<driver>.stats()` on each native DB driver — in-flight / cumulative / peak / worker counts per named connection pool; pooled drivers also provide `db:len()` to inspect per-worker queue lengths.

---

## 1. Server / Runtime Stats: `moon.server_stats`

Implementation: `server_stats` in `crates/moon-runtime/src/modules/lua_actor.rs` (registered as `moon.core.server_stats`; since `moon` is `moon.core`, it can be called directly as `moon.server_stats`).

Two calling conventions:

- **Scalar query**: `moon.server_stats(key)` returns a single integer (returns `0` for unknown keys). Backward-compatible.
- **Full snapshot**: `moon.server_stats()` (no argument) returns a **JSON string** whose keys match the table below, plus an additional `services` array.

```lua
local json = require("json")

-- Single item
local n = moon.server_stats("service.count")

-- Full snapshot
local snapshot = json.decode(moon.server_stats())
print(snapshot["memory.total"], #snapshot.services)
```

### 1.1 Counters

| key | Meaning | Unit |
|---|---|---|
| `service.count` | Number of live actors | count |
| `service.registered` | Route table entries (including pseudo-actors) | count |
| `service.unique` | Number of unique / named services | count |
| `service.created` | Cumulative actors created since startup | count |
| `log.error_count` | Cumulative error-level log lines | count |
| `log.queue` | **Log lines enqueued but not yet flushed to disk by the logger thread** | count |
| `timer.count` | Scheduled but not yet fired timers | count |
| `env.count` | Runtime environment variables | count |
| `time.offset` | Simulated clock offset | ms |
| `time.now` | Server timestamp | ms |
| `uptime` | Process uptime | s |
| `memory.total` | Total Lua memory across all actors | bytes |
| `message.total` | Cumulative messages dispatched across all actors | count |
| `cpu.total_ms` | Cumulative dispatch time across all actors | ms |

> `log.queue` reflects asynchronous log backlog (grows when production outpaces disk writes); useful for backpressure observation and pre-shutdown drain checks. The counter is maintained in `crates/moon-runtime/src/log.rs`: +1 on enqueue in `write()`, -1 after the consumer thread writes each line.

### 1.2 `services` Array (Full Snapshot Only)

The JSON snapshot additionally includes a `services` array, one entry per live actor (cluster pseudo-actors are filtered out):

| Field | Meaning |
|---|---|
| `id` | Actor id |
| `name` | Service name |
| `memory` | Lua memory for this actor (bytes) |
| `messages` | Cumulative messages processed by this actor |
| `cpu_ms` | Cumulative dispatch time for this actor (ms) |

Per-actor details are tracked by each actor's own watchdog.

---

## 2. Connection Pool Stats: `<driver>.stats()`

Available for native DB drivers: **redis, pg, sqlx, mongodb**. Returns a Lua table keyed by **connection name**, with values being `pool_stats` tables (global view covering all named connections/pools for that driver).

```lua
local pg = require("moon.db.pg")
local all = pg.stats()
for name, s in pairs(all) do
    print(name, s.pending, s.total, s.peak, s.workers)
end
```

### 2.1 `pool_stats` Fields

| Field | Meaning |
|---|---|
| `pending` | Requests dispatched but not yet answered (current backpressure / in-flight count) |
| `total` | Cumulative requests dispatched (monotonically increasing) |
| `peak` | Peak concurrent in-flight requests (high-water mark) |
| `workers` | Number of worker tasks backing this connection |

Semantics:

- **Pooled drivers (redis / pg)**: `pending`/`total`/`peak` are sums across all workers; `peak` is the **sum of per-worker peaks**, which is an upper bound (approximation) of the true concurrent peak. `workers` is the pool worker count (i.e., `pool_size`).
- **Single-connection drivers (sqlx / mongodb)**: one counter per named connection; `peak` is exact; `workers` is always `1` (sqlx's internal connection pool is managed by the sqlx crate and appears to the upper layer as a single request handler).

> Implementation: stats are centrally maintained by `PendingCounter` in `crates/moon-runtime/src/request_pool.rs` — `inc()` (on dispatch) updates `pending`/`total`/`peak` simultaneously, `dec()` (on response/drain) only decrements `pending`. `WorkerSet` provides cross-worker summation. All drivers produce a homogeneous result table via the shared `push_pool_stats` helper.

### 2.2 Per-Driver Entry Points

| Driver | Call | Notes |
|---|---|---|
| Redis | `redis.stats()` | `require("moon.db.redis")` |
| PostgreSQL | `pg.stats()` | `require("moon.db.pg")` |
| SQLx | `sqlx.stats()` | `require("moon.db.sqlx")` |
| MongoDB | `mongodb.stats()` | `require("moon.db.mongodb")` |

---

## 3. Per-Pool Queue Length: `db:len()` (redis / pg)

Pooled driver connection handles expose `db:len()`, which returns an **array of each worker's current pending count** `{q1, q2, ...}` (length equals `pool_size`). Useful for fine-grained checks such as "drain before shutdown."

```lua
local db = pg.find_connection("game")
local lengths = db:len()   -- e.g. { 0, 2, 1 }
```

- `db:len()`: per-worker breakdown for a **single** pool (array).
- `<driver>.stats()`: summary for **all** named pools (`pool_stats` grouped by name).

sqlx / mongodb use a single-connection model and do not provide `len()`; check in-flight volume via `stats()[name].pending`.

---

## 4. Choosing the Right Stat

- For overall process health (memory, message volume, log backlog, CPU) → `moon.server_stats()`.
- For resource usage of a specific actor → the `services` array in `moon.server_stats()`.
- For DB backpressure / throughput (in-flight, cumulative, peak) → `<driver>.stats()`.
- For confirming a pool is fully drained before shutdown → pooled drivers: `db:len()`; single-connection drivers: `stats()[name].pending == 0`.
