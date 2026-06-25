# Reference: async Rust↔Lua module patterns

Detailed patterns and the recurring pitfalls that code reviews flag. Read this
when implementing a connection-pooled driver, a streaming cursor, or any module
with background tasks.

## 1. Request/response round-trip (end to end)

1. **Lua-facing C fn** runs on the actor thread. It must be cheap and
   non-blocking: read/validate args, encode the request, get
   `owner = (*actor).id` and `session = (*actor).next_session()` via
   `LuaActor::from_lua_state(state)`, send the work to the IO runtime / a worker
   channel, then `lua_push(state, session); return 1`.
2. **Worker / spawned task** does the await-heavy work on `CONTEXT.io_runtime()`.
3. On completion it delivers the result:
   `CONTEXT.send_value(PTYPE_X, owner, session, ResponseEnum)` — `T: Send`,
   boxed into the message.
4. **Decoder** (registered for `PTYPE_X`) converts the message body into Lua
   stack values and returns the count; the runtime resumes the coroutine blocked
   in `moon.wait(session)`.

`session == 0` ⇒ no reply: skip `send_value`, just log errors. The dispatch path
still increments the in-flight counter, so a fire-and-forget request must still
be accounted for and decremented on completion/drain.

## 2. Message decoders

A reply only resumes Lua if a decoder is registered for its `PTYPE`. Decoders
have type `unsafe extern "C-unwind" fn(LuaState, *mut Message) -> i32`.

- For `Boxed` payloads, `message_decode::take_boxed::<T>(m)` downcasts.
- For `Buffer` payloads whose push path can longjmp (Lua OOM), use
  `message_decode::borrow_buffer(m)` — **borrow, don't take** — so the
  `Message`'s `Drop` frees the buffer on both success and error paths. Taking a
  `Box<Buffer>` and then longjmp-ing leaks it.
- Generic helpers already exist: `decode_error_message`, `decode_integer_message`,
  `decode_buffer_as_string_message`.

## 3. Connection / worker pool lifecycle

Global registry: `static ref X_CONNECTIONS: DashMap<String, Pool>` (via
`lazy_static`). A `Pool` wraps `inner: Arc<PoolInner>` holding the worker
channels and a name. Each worker carries an `Arc<AtomicI64>` in-flight counter.

### connect

- Spawn N workers, each owning an `mpsc::UnboundedReceiver<Message>`.
- Insert into the registry. **If a pool already exists under that name, shut the
  old one down gracefully** (don't just drop it): send each old worker a
  `Shutdown`/`Close` so its socket closes and its queued requests are failed.

```rust
if let Some(old) = X_CONNECTIONS.insert(name, pool) {
    log::warn!("X '{}' reconnected with same name; shutting down previous pool", old.inner.name);
    for w in old.inner.workers.iter() { let _ = w.tx.send(Message::Shutdown); }
}
```

### dispatch

Round-robin worker selection; `counter.fetch_add(1, Release)` on enqueue.

### graceful shutdown — drain queued requests (critical)

When a worker receives `Shutdown`/`Close`, requests already queued **behind** it
must not be silently dropped — their callers are blocked on `moon.wait` forever
and their counter increments leak. Drain and fail them:

```rust
Message::Shutdown => {
    while let Ok(queued) = rx.try_recv() {
        if let Message::Request(req) = queued {
            if req.session != 0 {
                let _ = CONTEXT.send_value(PTYPE_X, req.owner, req.session,
                    Response::Error("connection closed".to_string()));
            }
            counter.fetch_sub(1, Ordering::Release);
        }
    }
    break;
}
```

For a single-task handler (mongodb/sqlx style) the same applies on `Close()`:
`while let Ok(pending) = rx.try_recv() { ... fail session, decrement ... }`.

### session lifecycle — every path replies exactly once

A `session > 0` returned to Lua is a suspended coroutine. It only resumes when a
message carrying that session reaches the actor. Walk every way a session can be
created and make sure each terminates in **one** reply (typed success/error via
`send_value`, or `response_error`):

| Stage | What can go wrong | Required handling |
| --- | --- | --- |
| Lua-facing fn returns session | enqueue (`tx.send`) failed because the pool was closed / workers exited | return `lua_push_error` and **do not** push the session; the caller gets `(false, msg)` synchronously |
| Worker processing the request | the op errors / the socket dies | reply the error for `session != 0` on that same path, then `fetch_sub` the counter |
| Worker about to process when `Shutdown` is next | nothing — see below | the current request already replied; only the *not-yet-started* queued ones need the drain |
| Requests still queued at close | dropped silently with the receiver | drain + reply error + decrement (the drain loop above) |
| `connect()` spawned task fails | the connecting coroutine waits forever | reply error to the connect session (and don't insert a half-built pool) |
| streaming cursor `next` | the stream task already exited, channel send fails | return `lua_push_error`; never push a session no one will answer |
| reconnect replaces the pool | old pool's queued sessions orphaned | `Shutdown` old workers → they drain + fail their queue |
| owner actor exited before reply | `send_value` returns the undelivered `Message` | ignore the return value; the coroutine is already gone — do not panic or retry |

**Why in-flight requests are safe without extra handling:** a worker loop is a
single task that calls `rx.recv().await` once per iteration. It fully `await`s
the current request (and sends its reply) *before* it loops back and dequeues the
`Shutdown`. So `Shutdown`/`Close` only ever observes requests that have **not yet
started** — exactly the ones the drain loop fails. Do not also try to "cancel"
the in-flight one; that would double-reply.

**Counter accounting:** the dispatch-time `fetch_add` must be matched by exactly
one `fetch_sub`, on whichever single path ends the request (normal reply, error
reply, or drain). A missing decrement makes `stats()` report phantom in-flight
work forever; a double decrement can make it go negative.

**Two-reply hazards to avoid:** replying in the worker *and* again in the drain
for the same request; or treating a `connect` failure as both a pushed error and
a spawned-task error reply. Pick one owner of the reply for each session.

### close() — identity-guarded removal

A stale Lua handle calling `close()` must not evict a **newer** pool that
reconnected under the same name. Use `remove_if` keyed on `Arc::ptr_eq`:

```rust
X_CONNECTIONS.remove_if(&pool.inner.name, |_, v| Arc::ptr_eq(&v.inner, &pool.inner));
for w in &pool.inner.workers { let _ = w.tx.send(Message::Shutdown); }
```

The same guard belongs in a handler's own exit path
(`remove_if(&name, |_, v| Arc::ptr_eq(&v.counter, &counter))`).

### streaming cursors

If a Lua-facing `cursor_next` sends a signal to a stream task over a channel,
**check the send result**. If it fails the task is gone — return
`lua_push_error` instead of pushing a session the caller would wait on forever.

## 4. Concurrency pitfalls

- **DashMap AB-BA deadlock**: never hold a shard guard from one `DashMap` while
  calling code that locks another (or the same) map. Iterating
  `map_a.iter()` and calling `self.send()` (which locks `actors`) inside the
  closure can deadlock against a path that locks them in the other order.
  **Snapshot first**, then act:

  ```rust
  let targets: Vec<ActorId> = self.unique_actors.iter().map(|v| *v.value()).collect();
  for to in targets { let _ = self.send(/* ... */); }
  ```

- **Background tasks must terminate on shutdown**. A task whose only exit is a
  channel close will run forever if the sender lives in the global `CONTEXT`.
  Poll the shutdown sentinel:

  ```rust
  loop {
      if CONTEXT.exit_code() != i32::MAX { break; } // shutdown initiated
      // ... bounded select/timeout so the check runs periodically ...
  }
  ```

## 5. Security / robustness caps (don't regress these)

- **Bounded reads**: accumulate-until-delimiter and body reads must enforce
  `MAX_NETWORK_READ_SIZE` incrementally (e.g. `fill_buf`/`consume` loops), not
  rely on a withheld delimiter.
- **Frame length casts**: a 4-byte length prefix means `total as u32` truncates
  >4 GiB frames and desyncs the stream. Cap to the protocol max (which is
  `< u32::MAX`) and drop oversize frames instead.
- **Bind-parameter / column counts**: check `len > u16::MAX` before `as u16`.
- **SQL identifiers**: build `ON CONFLICT`/dynamic SQL from structured tables and
  quote identifiers; treat raw string forms as trusted SQL with validation.
- **WebSocket**: bound `max_write_buffer_size` by default (tungstenite defaults
  to `usize::MAX`); apply the bound even when no options table is passed.
- **JSON**: escape object **keys** like values; reject NaN/±Inf on encode; on
  decode keep `i64` exact and represent out-of-range `u64` as `f64` rather than
  silently `0`.
- **Filesystem**: don't follow symlinks when recursing (`is_dir() &&
  !is_symlink()`); lexically clean joined paths (resolve `.`/`..`).
- **`seri.unpack` / raw pointers**: the `(lightuserdata, len)` pair is a trusted
  contract with the C dispatch layer, but still reject a negative length (wraps
  to a huge `usize` → OOB read).

## 6. Naming & layout conventions

- Source files: `lua_<feature>.rs`; protocol constants `PTYPE_*`.
- Native modules register as `<name>.core` (e.g. `redis.core`, `net.core`).
- Feature-gate optional modules with `#[cfg(feature = "...")]` in `lib.rs` and a
  feature in `crates/moon-runtime/Cargo.toml`.
- Global singletons `CONTEXT` and `LOGGER` are `lazy_static`.
- Layering: C core → `lualib/moon/<name>.lua` wrapper → user scripts; EmmyLua
  annotations live in `lualib/meta/`.
