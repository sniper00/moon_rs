---
name: moon-async-module
description: >-
  Write or modify async Rust↔Lua native modules in moon_rs (lua_<feature>.rs +
  lualib wrapper): the session-based request/response bridge between a
  synchronous Lua actor and the Tokio IO runtime, connection/worker pools,
  graceful shutdown/draining, and the FFI longjmp-safety rules. Use when adding
  a new native module, adding a Lua-facing C function, wiring DB/network drivers,
  or touching anything under crates/moon-runtime/src/modules/lua_*.rs.
---

# moon_rs: Async Rust↔Lua Modules

Build native modules that let a **synchronous, single-threaded Lua actor** drive
**async Tokio work** without blocking, then receive the result as a normal actor
message. This skill encodes the conventions and the safety rules that are easy
to get wrong (and that recurring code reviews keep flagging).

## Mental model

- Each actor owns one `lua_State` and runs single-threaded. It must **never block**.
- Native async work runs on `CONTEXT.io_runtime()` (a separate Tokio runtime).
- The two sides communicate by **session-keyed messages**, not shared memory:
  1. Lua calls a C function (`extern "C-unwind"`).
  2. The C function grabs `(owner, session)`, hands the work to the IO runtime / a
     worker task, **returns the `session` immediately**, and Lua suspends on
     `moon.wait(session)`.
  3. When the work finishes, the worker calls
     `CONTEXT.send_value(PTYPE_X, owner, session, response)`. The runtime routes
     that message to the actor's mailbox; a **registered decoder** turns it into
     Lua values and resumes the coroutine.
- `session == 0` means **fire-and-forget** (no reply expected, no `moon.wait`).
- Errors flow back the same way: positive-session typed error responses, or
  `CONTEXT.response_error(from, owner, -session, msg)` for the generic error path.

The Lua wrapper is always a thin layer: `c.request(...)` returns a session,
`moon.wait` collects the decoded reply.

```lua
-- lualib/moon/db/<name>.lua (thin wrapper)
local c = require("<name>.core")
function pool:command(...)
    return moon.wait(c.command(self.obj, ...))  -- c.command returns a session
end
```

```rust
// Lua-facing C function: capture owner/session, dispatch, return session.
fn dispatch_async(state: LuaState, pool: &Pool, data: Vec<u8>) -> c_int {
    let actor = LuaActor::from_lua_state(state);
    let owner = unsafe { (*actor).id };
    let session = unsafe { (*actor).next_session() };
    match pool.dispatch(owner, session, data) {
        Ok(_) => { laux::lua_push(state, session); 1 }
        Err(e) => crate::lua_push_error(state, &e),  // (false, msg)
    }
}
```

## Edge cases: never strand a waiting coroutine

**Core invariant:** every `session` you hand back to Lua (that the caller will
`moon.wait` on) must receive **exactly one** reply — success or error — on
**every** path. A session with no reply is a coroutine suspended forever (a
permanent leak); two replies resume a stale continuation. `session == 0` is the
only no-reply case (fire-and-forget), and it still must have its in-flight
counter decremented.

Handle each boundary explicitly:

| Boundary | Wrong (hangs / leaks) | Right |
| --- | --- | --- |
| Enqueue fails (`tx.send` Err — pool closed / workers gone) | push the session anyway → caller waits forever | return `lua_push_error`; **never** push a session |
| Requests queued behind `Shutdown`/`Close` | drop them with the channel | drain `rx.try_recv()`, reply error for each `session != 0`, decrement counter |
| In-flight request when close arrives | — | already safe: a worker reads its channel **serially**, so the current request replies *before* `Shutdown` is dequeued — don't double-handle it |
| `connect()` fails inside the spawned task | return silently | reply an error to the **connect session** |
| Cursor/stream `next` send fails (task gone) | push a session | return `lua_push_error` |
| Reconnect under same name | drop old pool (orphans its queued sessions) | send old workers `Shutdown` so they drain + fail their queue |
| Reply arrives after the owner actor died | `unwrap()` / assume delivery | `send_value` to a dead actor returns the undelivered `Message`; ignore it, never panic |

**Counter rule:** increment once at dispatch; decrement **exactly once** — on the
reply path *or* the drain path, never both, never neither. A leaked increment
corrupts `stats()`/`pending()` for the lifetime of the process.

## The cardinal rule: longjmp safety

`laux::lua_error` / `luaL_error` raise a Lua error via **longjmp**, which
**skips every Rust destructor** between the call and the enclosing `pcall`. Any
owned resource still live at that point (sockets, `File`, `String`, `Box`,
`Vec`, a `TcpListener`) **leaks**.

Rules, in priority order:

1. **Prefer `crate::lua_push_error(state, msg)`** (returns `(false, msg)` and
   `2`) for anything recoverable. It returns normally, so destructors run.
2. If you must call `lua_error`, **`drop()` every owned resource first**, or
   confine owned resources to a closure returning `Result<_, String>` and raise
   the error only *after* the closure returns (see `lua_json::decode_file`).
3. When decoding a `Message` body whose push path can longjmp, **borrow the
   buffer, never take it** (`message_decode::borrow_buffer`) so the `Message`'s
   own `Drop` frees it on both paths.
4. Call `laux::lua_checkstack` before pushing many values or recursing.
5. `.expect()`/`.unwrap()` on `laux::lua_touserdata::<T>(state, 1)` in pool/
   handle methods is an **accepted invariant** (the Lua wrapper guarantees the
   `self` userdata type). Do not "fix" these. Genuine entry points taking
   untrusted args must still validate via `lua_push_error`.

## Validate untrusted Lua input

Anything a user script controls must be bounded before use:

- Cap incoming/outgoing byte sizes against `crate::MAX_NETWORK_READ_SIZE`
  (request/response bodies, accumulated reads, frames).
- Reject negative integers used as lengths (`i64 -> usize` wraps to huge).
- Cap rows/documents for non-streaming queries; validate `batch_size >= 1`.
- Never silently truncate on `as u16` / `as u32` casts — check the bound and
  error instead.

## Build & verify

Build with **default features** (the `--all-features` `luau` path is currently
broken and unrelated to your change):

```bash
cargo build
cargo clippy --all-targets
```

## Adding a new module — checklist

```
- [ ] crates/moon-runtime/src/modules/lua_<name>.rs  (C functions + worker/pool)
- [ ] Cargo.toml feature flag (if it has optional deps)
- [ ] lua_require!(state, "<name>.core", lua_<name>::luaopen_<name>) in lib.rs (mod + #[cfg])
- [ ] pub const PTYPE_<NAME>: u8 = N; in moon-runtime/src/context.rs
- [ ] moon.PTYPE_<NAME> = N in lualib/moon.lua  + register a message decoder
- [ ] lualib/moon/<name>.lua thin wrapper (returns sessions, moon.wait)
- [ ] lualib/meta/<name>/core.lua EmmyLua annotations
- [ ] docs/<name>.md
- [ ] cargo build && cargo clippy --all-targets
```

## Reference files

- Detailed patterns + the full review-derived pitfalls list (pools, draining,
  identity-guarded removal, deadlocks, decoders, security caps): see
  [reference.md](reference.md).
- Copy-paste skeletons (Rust module, worker pool, Lua wrapper, registration):
  see [templates.md](templates.md).
- IO extension design guide (architecture decisions, low-copy data flow,
  PG wire protocol case study, cluster RPC pending-wait correctness):
  see [io_extension_design.md](io_extension_design.md).
