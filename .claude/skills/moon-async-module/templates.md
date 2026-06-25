# Templates: new async module skeleton

Copy and adapt. Replace `name`/`Name`/`NAME` and `PTYPE_NAME`. These mirror the
existing `lua_redis.rs` / `lua_pg.rs` pool drivers.

## 1. Rust module — `crates/moon-runtime/src/modules/lua_name.rs`

```rust
use dashmap::DashMap;
use lazy_static::lazy_static;
use moon_base::{
    cstr,
    laux::{self, LuaState, LuaTable},
    lreg, lreg_null, luaL_newlib,
};
use moon_runtime::{
    actor::LuaActor,
    context::{self, ActorId, CONTEXT},
};
use std::{
    ffi::c_int,
    sync::{
        atomic::{AtomicI64, AtomicUsize, Ordering},
        Arc,
    },
};
use tokio::sync::mpsc;

lazy_static! {
    static ref NAME_CONNECTIONS: DashMap<String, Pool> = DashMap::new();
}

enum NameMessage {
    Request(NameRequest),
    Shutdown,
}

struct NameRequest {
    owner: ActorId,
    session: i64,
    data: Vec<u8>,
}

enum NameResponse {
    Ok(Vec<u8>),
    Error(String),
}

struct Worker {
    tx: mpsc::UnboundedSender<NameMessage>,
    counter: Arc<AtomicI64>,
}

struct PoolInner {
    name: String,
    workers: Vec<Worker>,
    next: AtomicUsize,
}

#[derive(Clone)]
struct Pool {
    inner: Arc<PoolInner>,
}

impl Pool {
    fn dispatch(&self, owner: ActorId, session: i64, data: Vec<u8>) -> Result<(), String> {
        let n = self.inner.workers.len();
        let idx = self.inner.next.fetch_add(1, Ordering::Relaxed) % n;
        let w = &self.inner.workers[idx];
        w.tx.send(NameMessage::Request(NameRequest { owner, session, data }))
            .map_err(|e| e.to_string())?;
        w.counter.fetch_add(1, Ordering::Release);
        Ok(())
    }

    fn pending(&self) -> i64 {
        self.inner.workers.iter().map(|w| w.counter.load(Ordering::Acquire)).sum()
    }
}

async fn worker_loop(
    name: String,
    mut rx: mpsc::UnboundedReceiver<NameMessage>,
    counter: Arc<AtomicI64>,
) {
    while let Some(msg) = rx.recv().await {
        let req = match msg {
            NameMessage::Request(req) => req,
            NameMessage::Shutdown => {
                // Drain queued requests so their callers aren't stuck on moon.wait.
                while let Ok(queued) = rx.try_recv() {
                    if let NameMessage::Request(req) = queued {
                        if req.session != 0 {
                            let _ = CONTEXT.send_value(
                                context::PTYPE_NAME, req.owner, req.session,
                                NameResponse::Error("connection closed".to_string()),
                            );
                        }
                        counter.fetch_sub(1, Ordering::Release);
                    }
                }
                break;
            }
        };

        // ... do the real async work for `req.data` ...
        let result = NameResponse::Ok(Vec::new());

        if req.session != 0 {
            let _ = CONTEXT.send_value(context::PTYPE_NAME, req.owner, req.session, result);
        }
        counter.fetch_sub(1, Ordering::Release);
        let _ = &name;
    }
}

extern "C-unwind" fn connect(state: LuaState) -> c_int {
    // parse + validate args here (use lua_push_error for recoverable errors) ...
    let name: String = laux::lua_opt(state, 2).unwrap_or_else(|| "default".to_string());
    let pool_size: usize = laux::lua_opt(state, 3).unwrap_or(4);

    let actor = LuaActor::from_lua_state(state);
    let owner = unsafe { (*actor).id };
    let session = unsafe { (*actor).next_session() };

    CONTEXT.io_runtime().spawn(async move {
        let mut workers = Vec::with_capacity(pool_size);
        for _ in 0..pool_size {
            let (tx, rx) = mpsc::unbounded_channel();
            let counter = Arc::new(AtomicI64::new(0));
            CONTEXT.io_runtime().spawn(worker_loop(name.clone(), rx, counter.clone()));
            workers.push(Worker { tx, counter });
        }
        let pool = Pool {
            inner: Arc::new(PoolInner { name: name.clone(), workers, next: AtomicUsize::new(0) }),
        };
        if let Some(old) = NAME_CONNECTIONS.insert(name.clone(), pool) {
            log::warn!("name '{}' reconnected; shutting down previous pool", old.inner.name);
            for w in old.inner.workers.iter() { let _ = w.tx.send(NameMessage::Shutdown); }
        }
        // reply to the connect() session via send_value(PTYPE_NAME, owner, session, ...)
        let _ = (owner, session);
    });

    laux::lua_push(state, session);
    1
}

extern "C-unwind" fn close(state: LuaState) -> c_int {
    let pool = laux::lua_touserdata::<Pool>(state, 1).expect("invalid name pool pointer");
    NAME_CONNECTIONS.remove_if(&pool.inner.name, |_, v| Arc::ptr_eq(&v.inner, &pool.inner));
    for w in &pool.inner.workers { let _ = w.tx.send(NameMessage::Shutdown); }
    laux::lua_push(state, true);
    1
}

extern "C-unwind" fn stats(state: LuaState) -> c_int {
    let table = LuaTable::new(state, 0, NAME_CONNECTIONS.len());
    NAME_CONNECTIONS.iter().for_each(|p| table.insert(p.key().as_str(), p.value().pending()));
    1
}

pub extern "C-unwind" fn luaopen_name(state: LuaState) -> c_int {
    let l = [
        lreg!("connect", connect),
        lreg!("close", close),
        lreg!("stats", stats),
        lreg_null!(),
    ];
    luaL_newlib!(state, l);
    1
}
```

## 2. Register in `crates/moon-runtime/src/lib.rs`

```rust
// Module sources live under src/modules/; keep them as crate-root modules via #[path].
#[cfg(feature = "name")]
#[path = "modules/lua_name.rs"]
mod lua_name;

// inside luaopen_custom_libs(state):
#[cfg(feature = "name")]
lua_require!(state, "name.core", lua_name::luaopen_name);
```

## 3. Protocol type + decoder

`crates/moon-runtime/src/context.rs`:
```rust
pub const PTYPE_NAME: u8 = 21; // next free id
```

`lualib/moon.lua`:
```lua
moon.PTYPE_NAME = 21
```

Register a decoder (see `message_decode.rs` helpers; borrow buffers, don't take).
Reuse `decode_buffer_as_string_message` / `decode_error_message` where they fit.

## 4. Cargo feature — `crates/moon-runtime/Cargo.toml`

```toml
[features]
name = ["dep:some-optional-crate"]
```

## 5. Lua wrapper — `lualib/moon/name.lua`

```lua
local moon = require("moon")
local c = require("name.core")

local M = {}
local meta = { __index = {} }

function M.connect(opts)
    local obj, err = moon.wait(c.connect(opts))
    if not obj then return nil, err end
    return setmetatable({ obj = obj }, meta)
end

function meta.__index:command(...)
    return moon.wait(c.command(self.obj, ...)) -- c.command returns a session
end

function meta.__index:close()
    return c.close(self.obj)
end

return M
```
