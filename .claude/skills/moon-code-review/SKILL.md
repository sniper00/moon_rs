---
name: moon-code-review
description: >-
  Review code changes in moon_rs (Rust↔Lua game server framework) against
  project-specific correctness and safety rules: unsafe Lua FFI patterns,
  longjmp safety, actor concurrency, session lifecycle invariants, low-copy
  IO design, skiplist arena soundness, and the accepted patterns that should
  NOT be flagged. Use when reviewing any PR, commit, or diff in this repo.
---

# moon_rs Code Review Guide

Project-specific review dimensions and accepted patterns for the moon_rs
codebase. This guide encodes the rules that are easy to get wrong and the
patterns that are deliberately accepted (don't flag these).

## Project Context

moon_rs is a Lua-scripted actor runtime on Tokio. Each actor owns one
`lua_State`, runs single-threaded, and communicates via typed messages. Native
Rust modules (socket, HTTP, SQL, Redis, PG, etc.) are exposed through C FFI;
async I/O completions are delivered back as actor messages.

Key files:
- `crates/moon-base/` — Lua 5.5 C sources + FFI bindings (`laux.rs`), macros, `Buffer`
- `crates/moon-runtime/src/modules/lua_*.rs` — all native modules
- `crates/moon-runtime/src/context.rs` — global `CONTEXT`, actor registry, `PTYPE_*`
- `crates/moon-app/` — binary entry, Tokio setup, signal handling

## Review Dimensions

### 1. Memory Safety & Unsafe Code

**Lua FFI correctness:**
- Lua stack push/pop balance — every push must have a corresponding pop (or
  be returned as the function result count).
- Type checks (`lua_is*`, `luaL_check*`) before reading values from the stack.
- Raw pointer lifetimes across the Rust↔Lua boundary — userdata pointers must
  remain valid as long as Lua holds the userdata.

**Skiplist arena (`lua_zset.rs`):**
- Nodes live in a flat `Vec<u64>` arena, addressed by `u32` word offsets with
  a `NIL` sentinel.
- Verify offset arithmetic, `level_len`/`backward` packing, and
  `get_unchecked` hot paths cannot read out of bounds or alias incorrectly
  after arena growth/reallocation.
- Check rank/span bookkeeping for off-by-one or overflow.
- **i64 negation overflow:** any code that negates a score/value for reverse
  ordering (`-score`, `-raw`) panics on `i64::MIN` in debug builds and wraps
  in release. Use `checked_neg().unwrap_or(i64::MAX)` or equivalent. This
  applies to skiplist `ZSet::update`/`ZSet::score`, but also to any sort
  key, leaderboard, or priority inversion elsewhere.

**Arena allocation caps that aren't real caps:**
- `i32::MAX` as a "limit" on allocation size (range query result count,
  buffer capacity) is ~2.1 billion entries — effectively no limit, a single
  crafted input can OOM the process. Cap at a practical value (e.g. 100k)
  that matches real use cases.
- `debug_assert` alone is not a safety guard — it compiles away in release.
  When a cast (e.g. `o as u32`) would silently truncate on overflow, add an
  explicit `<= u32::MAX as usize` check that fires in release too.

**`AtomicPtr` schema/descriptor swap (`lua_schema.rs`, `lua_protobuf.rs`):**
- Global state is published via `AtomicPtr` swap; the previous value is
  intentionally leaked so other actor threads keep a valid `&'static` view.
- Confirm memory orderings are correct and readers never observe a torn/freed
  pointer.

**Recursion depth:**
- `MAX_RECURSION_DEPTH` in `lua_protobuf.rs` and schema trace walking must be
  enforced on every nested-message/nested-table path; cannot be bypassed by
  crafted input.

**longjmp safety — what to flag vs what to skip:**

**IN SCOPE (flag these):** Only call sites where the reviewed code contains a
**literal, textual** call to `laux::lua_error` / `luaL_error` /
`ffi::lua_error`. These longjmp out without running Rust `Drop`. Look for:
- `Box::from_raw`, `Buffer` pointers, connection/cursor handles,
  `ResponseHandle`, `CursorHandle` — acquired but not dropped before a literal
  `lua_error`.
- Ownership transferred to Lua (`into_raw`) then a literal `lua_error`
  leaks it.
- Literal `lua_error` after partial decode/consume of `Message` payload may
  double-free or use-after-free.
- **`lua_newuserdata` failure + longjmp:** when `ZSet::new()` / `Pool::new()` /
  similar allocates `Vec`s or handles before calling `lua_newuserdata`, and
  `lua_newuserdata` returns `None` (OOM), a subsequent `lua_error` longjmps
  without running the value's `Drop`. In an OOM scenario this is usually
  acceptable (process is dying), but it should have a comment acknowledging
  the leak.

**OUT OF SCOPE (do NOT report):** Longjmps that originate implicitly or
transitively inside helpers or the Lua VM, including:
- `laux::lua_checkstack` returning false / raising
- Allocation/OOM inside `lua_push*` / `lua_createtable` / `luaL_*`
- `luaL_check*`, `lua_check_*`, `lua_arg_*` argument/type checks
- `push_value`, `pack_one`, `throw_error`, nested `seri`/`json` encode/decode
- Assume the codebase has already accepted or handled these paths.

**Preferred error pattern:** return via `lua_push_error(state, msg)` →
pushes `(false, errmsg)`, returns `2`, lets Lua `pcall` handle failure
**after** Rust destructors run.

**`.expect()` on userdata retrieval — do NOT report:** `lua_touserdata::<T>(state, 1).expect(...)` in pool/connection/cursor method
bindings is a deliberate, accepted pattern. These methods are only called
through Lua-layer wrappers that guarantee the `self` argument type. Genuine
entry points taking untrusted Lua args should still validate.

### 2. Concurrency & Thread Safety

- Actor message passing via `mpsc` channels — race conditions?
- `DashMap` usage in `CONTEXT` — **AB-BA deadlock risk**: never hold a shard
  guard from one `DashMap` while locking another. Snapshot first, then act.
  The classic trap in this codebase: `self.actors.iter().map(|e| { self.unique_actors.iter().find(...) })`.
  Fix with two-phase collection — collect entries from the first map (releasing
  the lock), then look up names in the second:
  ```rust
  // Phase 1: snapshot under actors lock
  let entries: Vec<(ActorId, ...)> = self.actors.iter()
      .map(|e| (*e.key(), e.value().watchdog.clone())).collect();
  // Phase 2: look up names under unique_actors lock (actors lock released)
  entries.into_iter().map(|(id, wd)| { ... self.unique_actors.iter()... }).collect()
  ```
- `Send`/`Sync` correctness when passing data between Tokio tasks and Lua
  states.
- Tokio task lifecycle — are tasks properly cancelled on shutdown?
- Background tasks must terminate on shutdown; poll `CONTEXT.exit_code()`.
  **Removing or commenting out a cooperative shutdown check** (e.g.
  `if CONTEXT.exit_code() != i32::MAX { break; }` in the timer task) means
  the task never exits its loop until the IO runtime is forcibly dropped.
  This wastes CPU during shutdown and may prevent clean drain. If the check
  is intentionally removed, ensure there is an alternative termination path
  (sender drop, explicit `AbortHandle`, or drain-all-timers-on-shutdown).

### 3. Error Handling & Session Lifecycle

**Core invariant:** every `session` handed back to Lua (that the caller will
`moon.wait` on) must receive **exactly one** reply on **every** path.

Check each boundary:
| Boundary | Required handling |
|---|---|
| Enqueue fails (`tx.send` Err) | return `lua_push_error`; never push a session |
| Requests queued behind `Shutdown`/`Close` | drain `rx.try_recv()`, reply error for each `session != 0` |
| `connect()` fails in spawned task | reply error to the connect session |
| Reconnect under same name | `Shutdown` old workers → drain + fail queue |
| Reply arrives after owner died | `send_value` returns undelivered `Message`; ignore, don't panic |
| `close()` with stale handle | identity-guarded removal (`remove_if` + `Arc::ptr_eq`) |

In-flight counter: increment once at dispatch; decrement exactly once on the
reply path or drain path, never both, never neither.

**Atomic counter balance (generalized):** any `fetch_add`/`fetch_sub` pair
(timers, pool connections, request queue depth) must be symmetric across ALL
exit paths, not just the happy path. Trace every `return`/`break`/`continue`
and every error branch — if `fetch_add` fires but the corresponding
`fetch_sub` is behind a conditional that can be skipped, the counter drifts
permanently.

### 4. Implementation Swap Impact

When a core function implementation is replaced (e.g. switching JSON decoders,
changing HTTP backend, swapping allocator), audit every existing test that
calls through the affected code path. Tests written for the old implementation
may encode wrong expectations:

- **Depth/recursion limits** — different decoders have different max nesting.
- **Numeric type classification** — `2^63` might be `integer` in one decoder
  and `float` in another.
- **Function registration** — if old tests call `mod.old_func` and the new
  registration only exposes `new_func`, tests silently fail at runtime.
- **Error message format** — Lua tests that match error strings by substring
  (e.g. `assert(err:find("expected"))`) may break on wording changes.
- **C/Rust dual-path consistency** — when both a C path (yyjson) and a Rust
  path (serde_json) coexist behind the same Lua API, they must agree on edge
  cases: big-int handling, escape sequence behavior, trailing garbage
  rejection, depth limits. Test both paths explicitly.

### 5. Performance

- Hot path allocations in message dispatch.
- Excessive copying — prefer the low-copy pattern: encode on Lua thread,
  move buffer to IO worker, keep raw bytes, decode on actor thread.
- `lua_checkstack` before recursive Lua stack pushes.
- Enum size bloat — `Box` infrequently-used large variants.
- Blocking calls on async runtime.

### 6. Security

- SQL injection in sqlx bindings / dynamic SQL construction.
- HTTP: header injection, path traversal, request/body size limits.
- WebSocket: origin validation, message size limits, bound
  `max_write_buffer_size` (tungstenite defaults to `usize::MAX`).
- User-controlled Lua input reaching unsafe Rust without validation.
- Bounded reads: enforce `MAX_NETWORK_READ_SIZE` incrementally.
- Frame length casts: check bounds before `as u32`/`as u16`.
- Filesystem: don't follow symlinks when recursing; lexically clean paths.
- Reject negative integers used as lengths (wraps to huge `usize`).

### 7. Code Quality

- Feature-gate consistency: `#[cfg(feature = "...")]` on module registration.
- Naming: `lua_<feature>.rs`, `PTYPE_*`, `<name>.core` for native modules.
- Layering: C core → `lualib/moon/<name>.lua` → user scripts.
- EmmyLua annotations in `lualib/meta/`.
- Dead code, unused dependencies, missing docs on unsafe invariants.
- **Config field renames:** when renaming a struct field (e.g. `network_read_bytes`
  → `max_network_read_bytes`), check both code references AND doc comments /
  inline comments — stale comment references won't cause compile errors but
  will mislead future readers.
- **Metatable / API key stability:** metatable marker keys (e.g. `__json_object`)
  and public Lua API function names are part of the user-facing contract.
  Renaming them breaks any external service code that inspects metatables or
  calls the old function name. Treat these as breaking changes.
- **Init-ordering for pseudo-actors:** when a pseudo-actor (cluster endpoint,
  monitor) uses a fixed `ActorId` in the normal allocation range (e.g. `2`),
  a user actor created before the pseudo-actor initializes can claim that ID,
  causing silent overwrite. Prefer reserved ranges (e.g. `0xFFFF_FF00`) or
  document the required init order with an assertion in `register_pseudo_actor`.

## Deliverable Format

1. **Critical** — bugs, UB, security vulnerabilities (file:line)
2. **High Priority** — significant design/performance/robustness issues
3. **Medium Priority** — code quality, maintainability, API design
4. **Low Priority** — style, docs, minor suggestions
5. **Positive Highlights** — well-designed aspects worth preserving

Each issue: location, description, impact, recommendation.

## Project Design Patterns (evaluate, don't re-litigate)

These are deliberate architectural choices. Review whether they are
well-executed; don't suggest removing them:

- **Zero-dependency wire protocols** (Redis RESP, PG v3) — full control over
  on-wire bytes, minimal copies, compact code.
- **Pool-and-worker architecture** — global `DashMap` of named pools,
  round-robin dispatch, per-worker `mpsc` channels, `AtomicI64` in-flight
  counters. Shared across Redis, PG, sqlx.
- **Pre-encode on actor thread, decode on actor thread** — IO workers only
  do `write_all` + `read`; encoding/decoding stays on the Lua actor thread.
- **Feature-gated module registration** — `lua_require!` macro +
  `#[cfg(feature)]` guards, `not_null_wrapper!` for safe C function wrapping.
- **`ShortBytes<N>`** — stack-allocated const-generic inline buffer for
  socket delimiters; `Copy`, single cache line.
- **PG SCRAM-SHA-256** — inline implementation (RFC 5802/7677) on `sha2`
  alone; no extra auth crates.
- **PG bulk operations** — auto-chunking to 65535-param limit, multi-chunk
  `BEGIN`/`COMMIT`, `quote_ident()` with SQL-standard escaping.
- **HTTP static file server** — streaming for large files, `If-Modified-Since`
  caching, LRU-bounded metadata cache.
- **Flat-arena skiplist** — nodes packed in `Vec<u64>`, `u32` word offsets
  instead of pointers, single cache line per hop.
- **Pure-Rust protobuf** — `FileDescriptorSet`-based, `AtomicPtr`-published
  descriptor table, depth-limited.
- **Session-aware auto-reconnect** — `session != 0` fails fast,
  `session == 0` retries with backoff.
