# Writing IO-Related Native Extension Libraries (Design Guide)

This document is for writing **IO-related native extension libraries** in moon_rs — database drivers, networking/RPC, custom socket protocols, etc. It distills two real-world cases (the native PostgreSQL driver `pg.core` and cluster RPC) into **general patterns** and **correctness essentials** to reference when designing a new IO module.

For implementation-level code conventions, FFI longjmp safety rules, input validation, etc., see the companion skill `moon-async-module`; this document focuses on **architecture and design trade-offs**.

The document has three parts:

- **Part I — General Patterns**: the shared skeleton, data flow, and correctness invariants for all IO extension libraries.
- **Part II — Case A: Native PG Driver** (`pg.core`): wire protocol port + low-copy codec + batch writes.
- **Part III — Case B: Cluster RPC**: a deep dive into pending-wait release correctness for cross-node RPC.

---

# Part I — General Patterns

## 1. Core Mental Model: session-based async bridge

Each actor owns one `lua_State`, **single-threaded, must never block**. Native async IO runs on a separate `CONTEXT.io_runtime()`. The two sides communicate via **session-keyed messages**, not shared memory:

1. Lua calls a C function (`extern "C-unwind"`).
2. The C function grabs `(owner, session)`, hands the work to the IO runtime / worker task, **returns `session` immediately**, and Lua suspends the current coroutine on `moon.wait(session)`.
3. When the work finishes, the worker calls `CONTEXT.send_value(PTYPE_X, owner, session, response)`; the runtime routes the message into the actor's mailbox, and a **registered decoder** converts it into Lua values and resumes the coroutine.

Conventions:

- `session == 0` means **fire-and-forget** (no reply expected, no `moon.wait`, but the in-flight counter must still be maintained).
- Errors flow back the same way: typed error responses for positive sessions, or `CONTEXT.response_error(from, owner, -session, msg)` for the generic error path.
- The Lua wrapper is always a thin layer: `c.request(...)` returns a session, `moon.wait` collects the decoded reply.

## 2. Architecture Skeleton

- **Global named registry**: `DashMap<String, Pool>` (e.g. PG connection pools, sqlx's `DATABASE_CONNECTIONS`). Register/lookup by `name`; `find_connection(name)` returns a userdata handle with methods.
- **Connection pool / workers**: `N` worker tasks run on `CONTEXT.io_runtime()`, each holding a connection and its own `mpsc` request channel; round-robin (or optional `hash`) dispatch. Each worker loop: `recv` → execute on its connection → `send_value(...)` deliver back, with retry/reconnect inside Rust on socket errors.
- **Custom message types**: add `PTYPE_X` in `context.rs`; call `register_message_decoder(PTYPE_X, decode_x)` at module init; `moon.wait` / `_dispatch` decode via `moon.core.decode_message(m)`.

## 3. Low-Copy Data Flow

Put "encoding" on the Lua thread and "decoding" on the actor thread, so raw bytes are **moved** through the middle rather than copied element-by-element:

- **Request path (one-shot encode on Lua thread)**: encode the complete protocol message directly from Lua stack values into a **single owned byte buffer** — numbers formatted in place, strings copied only once, **no intermediate enum/struct** introduced. That buffer is then **moved** to the async worker for socket write.
- **Response path (keep raw bytes, deferred parse)**: the worker preserves **raw bytes** in the boxed response (plus parsed field descriptors), **without** building intermediate Rust row structs. The registered `decode_x` parses the raw bytes **directly into Lua tables** on the actor thread. The entire path involves only one copy: wire bytes → Lua strings/numbers.

## 4. Connection Pool Stats and Draining

Each worker tracks **in-flight queue depth** with an `AtomicI64`: +1 on enqueue (Lua thread), -1 after the worker processes and replies. The counting rule is **exactly once** — decrement on the reply path **or** the drain path, never both, never neither (a leaked increment pollutes the whole process's `stats()`).

- `core.stats()` → a global view keyed by connection `name`, with values being total in-flight requests for that pool.
- `handle:len()` → per-worker in-flight count array for a single pool, used for "drain before exit" checks.

## 5. Correctness Invariant: suspended coroutines must be released exactly once

> This is the **most error-prone** aspect of IO extension libraries. Part III uses cluster as a deep case study.

**Core invariant**: every `session` handed back to Lua and `moon.wait`-ed on must receive **exactly one** reply on **every** path — success or error. No reply = coroutine suspended forever (leak); two replies = waking a stale continuation.

Boundaries to handle explicitly:

- **Enqueue fails** (channel closed / workers gone): return `lua_push_error`; **never** push a session.
- **Requests queued behind `Shutdown`/`Close`**: drain `rx.try_recv()`, reply error for each `session != 0` and decrement the counter.
- **`connect()` fails inside the spawned task**: reply an error to the **connect session**.
- **Reconnect reuses the same pool name**: send `Shutdown` to old workers so they drain and fail their queue, rather than dropping the old pool directly (which would orphan its queued sessions).
- **Reply arrives after owner actor died**: `send_value` returns an undelivered `Message`; ignore it, never panic.

For more complex release semantics — precise failure across connections, identity/generation-guarded removal, late-response deduplication — see Part III.

---

# Part II — Case A: Native PostgreSQL Driver (`pg.core`)

## A.1 Background and Design Intent

The pure-Lua `pg.lua` exists fundamentally to **have stronger control at the protocol layer and reduce data copies** (e.g. assembling Lua tables directly into PG protocol bytes). Hence the native approach:

- **The protocol layer depends on no existing Postgres crate** (no `tokio-postgres`, no `sqlx`); the wire protocol is fully hand-written, controlling on-wire bytes and avoiding the copies of general-purpose clients.
- **Auxiliary crates are allowed** (e.g. `url` for connection string parsing), but only the protocol/codec core stays hand-written.
- Preserve the direct "Lua data → wire protocol bytes" assembly, minimizing copies as much as possible.

Goal: port the PG v3 wire protocol into a Rust extension module (**Strategy B — raw TCP, text-format parameters, OID-based type conversion**), expose a session-based modern async API (aligned with `sqlx.lua`), and push connection pooling/reconnect down to Rust.

## A.2 Overall Architecture

Follows the actor/session model of `lua_sqlx.rs`, but with a **hand-written PG v3 wire protocol** over raw `tokio::net::TcpStream`:

- **Named connection pool registry**: global `DashMap<String, PgPool>`.
- **Connection pool**: `max_connections` workers, each holding one `TcpStream` and its own `mpsc` channel; round-robin dispatch. Worker loop: `recv` → execute → `CONTEXT.send_value(PTYPE_PG, owner, session, result)`, with retry/reconnect inside Rust on socket errors.
- **New protocol type**: add `PTYPE_PG = 19` in `context.rs`; call `register_message_decoder(PTYPE_PG, decode_pg_message)` at init.

## A.3 Wire Protocol to Port (from `pg.lua`)

1. **Connect + startup**: TCP connect (with `connect_timeout`), `StartupMessage` (protocol number 196608, `user`/`database`/`application_name=moon`).
2. **Authentication**: AuthenticationOk(0), CleartextPassword(3), MD5Password(5, `md-5` crate), SASL SCRAM-SHA-256(10, reuse the extracted `ScramSha256Client`). Reject `SCRAM-SHA-256-PLUS`.
3. **Message framing**: `type(1) + len(4) + body`; read until `ReadyForQuery (Z)`.
4. **Simple query (`Q`)**: multi-statement; collect `RowDescription (T)`, `DataRow (D)`, `CommandComplete (C)`, `NotificationResponse (A)`, `ErrorResponse (E)`.
5. **Extended query**: `Parse/Bind/Describe/Execute/Sync`, parameters in **text format** (format code 0, OID unspecified).
6. **Pipeline**: implicit `BEGIN` … N statements … `COMMIT`.
7. **Result types (OID-based)**: bool(16), int2/4/8(21/23/20), float4/8(700/701), numeric(1700) → Lua bool/number; everything else string — aligned with `pg.lua`'s `PG_TYPES`.
8. **Error parsing**: `ErrorResponse` fields (`S/C/M/P/D/s/t/n`) → `severity/code/message/...`.

## A.4 Low-Copy Data Flow (core, corresponds to Part I §3)

**Request path** (one-shot encode on Lua thread):

- `query(sql)`: directly assemble the `Q` message (`'Q'` + len + sql + NUL) into a single contiguous buffer.
- `query_params(sql, ...)`: encode the `Parse/Bind/Describe/Execute/Sync` sequence **directly from the Lua stack** into a **single owned buffer** — parameters in text format, numbers formatted in place, strings copied only once, **no intermediate `Vec<QueryParams>` enum** introduced.
- `pipe({{sql,p1,..},...})`: the same encoder wrapped with `BEGIN`…`COMMIT`.

The buffer is then **moved** to the worker for socket write.

**Response path** (keep raw bytes): the worker preserves raw message bytes + field descriptors; `decode_pg_message` (called via `moon.core.decode_message(m)`) parses the raw bytes directly into Lua tables on the actor thread, performing OID-based type conversion.

## A.5 Lua-Side API (aligned with `sqlx.lua`)

`pg.core` (native module):

- `connect(database_url, name, timeout, max_connections)` → returns `session`; success/failure delivered via `PTYPE_PG`; pool registered under `name`.
- `find_connection(name)` → userdata handle with methods.
- `handle:query(session, sql)` / `:query_params(session, sql, ...)` / `:pipe(session, queries)`.
- `handle:len()` → array of per-worker pending queue depths.
- `handle:close()`, `stats()`.

Connection string format (sqlx standard):

```
postgres://user:password@host:port/database?application_name=moon&...
```

- Accepts `postgresql://` alias; parsed with the `url` crate (gated behind the `pg` feature), default port `5432`, missing `user`/`database` → configuration error delivered via `PTYPE_PG`.
- Plaintext only (`sslmode` ignored for now); `connect_timeout` is a separate `timeout` (milliseconds) parameter.

`lualib/moon/db/pg.lua` (thin wrapper): modeled after `sqlx.lua`, `moon.wait(c.connect(...))` → `find_connection`, methods via `moon.wait(self.obj:method(moon.next_session(), ...))`, results preserve `pg_result` structure (`data`/`num_queries`/`notifications`, errors as `code/message/...`).

## A.6 Stats / Queue Depth (corresponds to Part I §4)

Each worker tracks in-flight queue depth with an `AtomicI64`: +1 on enqueue, -1 on reply. `pg.core.stats()` returns a global view; `handle:len()` returns per-worker count arrays for a single pool (used for drain-before-exit). `pg.lua` exposes these as `pg.stats()` and `db:len()`.

## A.7 File Change Checklist

1. **`Cargo.toml` (workspace)** — bare protocol only uses `tokio`/`bytes`/`md-5`/`sha2`/`base64`/`rand` (all already present); reuse `url = "2.5.0"` for connection strings.
2. **`crates/moon-runtime/Cargo.toml`** — add `pg` feature (gates `lua_pg`, depends on `url`), include in `default`.
3. **`lua_scram.rs`** — make `ScramSha256Client` and helpers `pub(crate)` for reuse.
4. **`lua_pg.rs` (new)** — connection pool, workers, wire protocol, auth, low-copy encode, `decode_pg_message` registration, `stats`/`len`.
5. **`lib.rs`** — `mod lua_pg;` + `lua_require!(state, "pg.core", lua_pg::luaopen_pg)` (`#[cfg(feature = "pg")]`).
6. **`context.rs`** — `pub const PTYPE_PG: u8 = 19;`.
7. **`lualib/moon.lua`** — `moon.PTYPE_PG = 19`.
8. **`lualib/moon/db/pg.lua`** — rewrite as a thin wrapper over `pg.core`.
9. **`lua_json.rs`** — optionally remove now-redundant `json.pq_query` / `json.pq_pipe` (the encoder has moved into `pg.core`).
10. **`assets/example/example_pg.lua`** and benchmarks — update to connection-string usage.

## A.8 Key Decisions and Risks

- **Strategy B (bare wire protocol port)**: full control over on-wire bytes, avoids general-purpose client copies.
- **NULL representation**: SQL NULL is represented in result rows as "absent field (nil)" (consistent with `lua_sqlx.rs`), no `"\0"` sentinel, no `json.null` introduced.
- **Connection affinity**: remove the old `hash` routing; each request is atomic on a single connection (pipe/transaction safety), cross-request ordering affinity dropped (optional `hash` parameter can be kept if needed).
- **LISTEN/NOTIFY**: keep `notifications` collected within each query; async push on idle connections is out of scope for now.

## A.9 Batch Writes: `insert_many` / `update_many`

Under write-heavy loads, the `query_params`/`pipe` bottleneck is "one RTT per row + one commit fsync per row + repeated Parse/Execute per row." Provide **set-based single-statement** batch writes that compress N rows into one multi-row statement (one Parse/Bind/Execute/plan).

- **`db:insert_many(table, columns, rows, conflict?)`**: assemble multi-row `INSERT INTO t (cols) VALUES (...),(...),...[conflict]`. PG's single Bind parameter limit is 65535 (u16); auto-chunk by `floor(65535/column_count)`; **>1 chunk** wrapped in `BEGIN`/`COMMIT`. The `conflict` clause is spliced verbatim for UPSERT (note: multi-row single-statement UPSERT cannot hit the same conflict key twice in one statement; deduplicate by key before calling).
- **`db:update_many(table, key_column, set_columns, rows, key_type?)`**: `UPDATE t AS _t SET c=_d.c,... FROM (VALUES ...) AS _d(_k,c,...) WHERE _t.key <cmp> _d._k`. `VALUES` bindings default to `text`; the join key needs explicit handling: provide `key_type` (e.g. `"bigint"`) → `_d._k::bigint` (preserves index); omit it → `_t.key::text = _d._k` (works for any type but skips the index). Same auto-chunk + cross-chunk transaction wrap.
- **General**: still one-shot encoded on the Lua thread; shares `encode_many` + `build_insert_sql`/`build_update_sql`; no Describe on the write path. Result shape matches `pipe`.
- **Measured**: local PG, 10000-row jsonb upsert, `pipe` ≈ 0.805s, `insert_many` ≈ 0.046s, **~17× speedup**. For batch DELETE, use `WHERE id = ANY($1::bigint[])` as a single statement.

---

# Part III — Case B: Cluster RPC Pending-Wait Correctness

> This part is a deep case study of Part I §5 "suspended coroutines must be released exactly once." Reference objects: moon_rs's `lua_cluster.rs` (Rust native) and the original Moon's `service/cluster.lua` (pure Lua). Core question: across the three paths of "connection close / timeout / normal response", how is the caller's suspended coroutine released, and how do we ensure **no missed release, no double release**.

## B.1 What is "pending wait"

After `cluster.call(node, sname, ...)`, the Lua side's `moon.wait(session)` hangs the coroutine on `session_id_coroutine[session]`, **with no timeout of its own**. It can only be woken by two things: receiving the peer's response (RESP), or receiving a `PTYPE_ERROR` carrying that `session`. If neither happens, the coroutine is suspended forever. Therefore we must guarantee: **every call that has been sent ultimately receives a response or error, exactly once.**

Two categories of state must be distinguished:

- **Outbound call (true pending wait)**: initiated by this node, awaiting a remote result.
- **Inbound call**: a remote request received by this node; the local service is the "responder" and has **no** locally suspended coroutine.

## B.2 Data Model

| | Outbound call (pending wait) | Inbound call |
|---|---|---|
| **Reference (Lua)** | `call_watcher[sender][sessionid] = {time, fd}`, two-level table = composite key `(from_addr, session)`, records the **concrete connection fd** | No table built; carried by a **coroutine** via `moon.async` + `moon.wait(local_session)` |
| **moon_rs (Rust)** | `outbound_calls: DashMap<(u32,i64), OutboundCallInfo>`, composite key `(from_addr, session)`, records `to_node` + connection generation `cgen` | `pending_calls: DashMap<i64, PendingCallInfo>`, keyed by globally unique `local_session` |

**Why outbound must use a composite key**: `session` comes from a **per-actor** counter (`LuaActor::next_session`, i.e. `self.uuid += 1`) and will collide across actors. If the global map were keyed by session alone, calls from different actors would overwrite/mis-delete each other; the overwritten one would be missed during close/timeout cleanup → coroutine permanently hung. The composite key `(from_addr, session)` matches the reference's two-level table semantics.

**Why inbound needs its own table**: moon_rs's cluster endpoint is a **pseudo-actor (not a Lua coroutine)**, so it cannot carry "inbound call in progress" state with coroutines. Hence `pending_calls` on the Rust side does the bookkeeping.

> **General takeaway**: if sessions come from a "global counter", you can key by session alone; if from a "per-actor counter", you **must** use an `(owner, session)` composite key, or cross-actor collisions will cause missed releases.

## B.3 The Three Release Paths

**Normal response (RESP)**: remove from `outbound_calls` by `(from_addr, session)`, then deliver the response to the originating actor (delivery is routed by the wire header's `from_addr + session`, not dependent on the map). **RESP dedup**: only deliver if `remove(...)` hits; otherwise discard the late response, preventing a double wake.

**Connection close**: an outbound call records the **connection generation `cgen`** it was on when sent (not yet sent, still in connection → recorded as `u64::MAX`, only the timeout can cover it). `on_connection_closed`:

- Teardown of `connections`/`conn_gen`/`pending_calls` is guarded by `is_current` (generation guard); it does **not touch** old connection state that has been replaced by a new connection.
- Outbound calls are **always** failed immediately when `to_node == node_id && cgen == closed_generation`.

Thus calls sent on the closed connection generation fail **immediately** rather than waiting ~10s for timeout; matching by `cgen` avoids accidentally killing calls on a new connection. Corresponds to the reference's `info.fd == fd`.

**Timeout**: `spawn_call_timeout_checker` periodically walks `outbound_calls`; calls exceeding `CALL_TIMEOUT_S` (10s) get an error reply and are removed. This is the **final safety net**: even if both of the above paths miss, a pending wait is released within ~10s.

**"Exactly once" guarantee**: all error paths go through a unified `fail_outbound_call` — only if `remove(...)` hits do we reply with an error. Combined with RESP dedup, each `(from_addr, session)` is released exactly once.

## B.4 Inbound Call Close/Timeout Handling

Inbound cleanup only `retain`-discards `pending_calls` entries; it does **not reply an error to anyone**. Reason: if an error were delivered to `info.from_addr` (a **remote** actor id), the local side would not find that actor and would discard it — dead code. The reference implementation likewise sends no error back, only letting the carrying coroutine end naturally — the initiator is released by the peer's own outbound cleanup.

## B.5 Key Design Trade-offs

**fail-fast vs. wait-for-timeout**: immediately failing calls on the connection generation that closed aligns with the reference's fail-fast approach. The cost is a rare race:

> The request and response travel over the **same bidirectional socket**, but the response return path reads `connections[to_node]` (the current connection). In the rare race where "the old connection is replaced and the peer has already processed the request," the peer may send the response over the **new connection**; but at that point the call has already been failed immediately by the old connection's close.

That response is discarded by RESP dedup; the caller sees the call as failed (retryable). If "avoid false positives, prefer waiting for timeout" matters more, revert to an overall early-return; the current choice matches the original Moon.

**duplicate assertion**: `debug_assert!(!contains_key((from_addr, session)))` before `outbound_calls.insert` catches session reuse bugs early in development; zero cost in release.

## B.6 Test Coverage

- **Unit tests**: `lua_cluster.rs` `#[cfg(test)] mod tests` contains 7 cases — composite key isolation, precise failure by generation, RESP normal delivery + dedup, late RESP after close does not double-wake, inbound `pending_calls` silent cleanup, `fail_outbound_call` idempotency, and one end-to-end case where a **real loopback TCP socket** close triggers `read_task` → `on_connection_closed` → releases the pending wait.
- **Manual end-to-end**: `assets/test/test_cluster_selfconnect.lua` (single-process **self-connect**: ships its own discovery HTTP server, maps both self/peer nodes to the local listen address, `cluster.call(PEER)` goes over real TCP to itself). Covers call/accum, 20 concurrent calls, unknown service error returns `false`, `send` fire-and-forget, and verifies the process exits cleanly after `moon.exit`. Not in CI; run manually: `cargo run assets/test/test_cluster_selfconnect.lua`.

---

## Appendix: General Checklist and Key Code Locations

### New IO Module Checklist (see skill `moon-async-module` for details)

```
- [ ] crates/moon-runtime/src/modules/lua_<name>.rs  (C functions + worker/pool)
- [ ] Cargo.toml feature flag (if it has optional deps)
- [ ] lua_require!(state, "<name>.core", ...) in lib.rs (mod + #[cfg])
- [ ] pub const PTYPE_<NAME>: u8 = N; in context.rs
- [ ] moon.PTYPE_<NAME> = N in lualib/moon.lua + register_message_decoder
- [ ] lualib/moon/<name>.lua thin wrapper (returns session, moon.wait)
- [ ] lualib/meta/<name>/core.lua EmmyLua annotations
- [ ] docs/<name>.md
- [ ] cargo build && cargo clippy --all-targets
```

### Design Self-Check (correctness)

- Does every `session` handed back to Lua get exactly one reply on **all paths**? (enqueue failure, connect failure, close drain, reconnect, actor already dead)
- Does the session come from a global counter or per-actor? The latter **must** use an `(owner, session)` composite key.
- Is the in-flight counter incremented and decremented exactly once? Drain old queues on close/reconnect.
- Can a late response cause a double wake? Use "only deliver if remove hits" for dedup.

### Key Code Locations

- PG reference paradigm: `lua_sqlx.rs` (actor/session pool), `lua_pg.rs` (wire protocol + low-copy).
- Cluster outbound initiation: `lua_cluster.rs` `lua_cluster_request`.
- Cluster RESP handling: `lua_cluster.rs` `dispatch_frame` → `"RESP"` branch.
- Cluster close cleanup: `lua_cluster.rs` `on_connection_closed`.
- Cluster timeout safety net: `lua_cluster.rs` `spawn_call_timeout_checker`.
- Reference implementation: `moon/service/cluster.lua` `command.Request` / `add_call_watch` / `socket.on("close")`.
