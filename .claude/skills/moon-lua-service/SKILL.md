---
name: moon-lua-service
description: >-
  Write or modify Lua services (actors) in moon_rs: service skeletons, message
  dispatch/call/response/send, coroutine flow (moon.async/wait/sleep), the
  per-service session/PTYPE conventions, service lifecycle and the unique-service
  shutdown gotcha, and cluster.call/send usage. Use when authoring or editing
  Lua under lualib/ or assets/ (service scripts, moon.dispatch handlers, cluster
  scripts), or when a moon_rs process hangs on exit / a coroutine never returns.
---

# moon_rs: Writing Lua Services

A **service** is an actor: one `lua_State`, single-threaded, driven by a message
loop. Services never share memory — they communicate by typed messages. All
blocking-looking calls (`moon.call`, `moon.wait`, `moon.sleep`) are coroutine
yields, so they must run **inside a coroutine** (`moon.async`), and the actor
itself must **never truly block**.

Worked example to copy from: `assets/test/test_cluster_selfconnect.lua`.
Copy-paste skeletons: [templates.md](templates.md).

## Service skeleton: one file, multiple roles

A service script receives its creation params as varargs. The idiomatic pattern
puts several roles in one file and branches on the param table:

```lua
local moon = require "moon"
local conf = ...   -- nil for the entry/bootstrap script; the params table otherwise

if conf and conf.name == "worker" then
    -- sub-service role: register handlers, then return
    moon.dispatch("lua", function(sender, session, cmd, ...) ... end)
    return
end

-- bootstrap role: spin up sub-services and drive the program
moon.async(function()
    local id = moon.new_service { name = "worker", source = "this_file.lua", unique = true }
    assert(id > 0)
    -- ... do work ...
    moon.exit(0)
end)
```

- `moon.new_service{ name, source, unique?, memlimit? }` is **async** → call it
  inside `moon.async` (it returns the new service id, `0` on failure).
- `source` is the script path; the new service re-runs that file with the params
  table as `...`.

## Working directory and `source` path resolution

At startup, the process **changes the working directory** to the bootstrap
script's parent directory (`crates/moon-app/src/main.rs`):

```rust
let cwd = path.parent().unwrap_or(Path::new("./"));
env::set_current_dir(cwd)?;
```

This means:

- If you run `cargo run assets/test/my_test.lua`, the CWD becomes `assets/test/`.
- **`source` in `moon.new_service` is passed to `luaL_loadfile`** — a raw
  filesystem `fopen`, NOT a `require`-style package.path search.
- Therefore `source` must be resolvable relative to the **bootstrap script's
  directory** (or be an absolute path).

Practical consequences:

| Bootstrap location | Child service location | Correct `source` value |
| --- | --- | --- |
| `assets/test/test_foo.lua` | same dir: `assets/test/test_foo.lua` (same file, different role) | `"test_foo.lua"` |
| `assets/test/test_foo.lua` | `lualib/moon/sharetable.lua` | `"../../lualib/moon/sharetable.lua"` |
| `assets/example/main.lua` | `assets/example/worker.lua` | `"worker.lua"` |

`require("moon.sharetable")` inside a running service uses `package.path` and
works normally — this note only concerns the `source` field of `new_service`.

## Messages: dispatch / call / response / send

Register a handler per protocol; `PTYPE_LUA` ("lua") is the default for Lua args:

```lua
moon.dispatch("lua", function(sender, session, cmd, ...)
    if cmd == "echo" then
        moon.response("lua", sender, session, ...)   -- reply to a call
    elseif cmd == "notify" then
        -- session == 0 here: fire-and-forget, do NOT respond
    end
end)
```

- `moon.call(PTYPE, receiver, ...)` — **async** request/response. Returns the
  responder's values, or `false, errmsg` if it failed (responder died, or it
  replied an error). Always callable only inside `moon.async`.
- `moon.response(PTYPE, receiver, session, ...)` — reply to a call. Pass back the
  **same `session` you received**; the core flips its sign so the caller's
  coroutine resumes. `session == 0` makes it a no-op.
- `moon.send(PTYPE, receiver, ...)` — fire-and-forget (no session, no reply).
- Session sign convention (rarely needed directly): an inbound **request**
  arrives with `session < 0` (dispatched to your handler); an inbound
  **response** arrives with `session > 0` (resumes the waiting coroutine). You
  normally just echo `session` back via `moon.response`.

## Coroutines and timing

- `moon.async(fn, ...)` — start a coroutine. Anything that yields (`call`,
  `wait`, `sleep`, `new_service`) must be inside one.
- `moon.wait(session)` — low-level suspend until `session` is answered; most
  native modules return a session you wrap with `moon.wait`.
- `moon.sleep(ms)` — yield for at least `ms`; returns `false` if woken early.
- `moon.timeout(ms, fn)` — non-yielding timer callback.
- Run many concurrent requests by launching multiple `moon.async` closures and
  joining on a counter (see the concurrency block in the example script).

## Lifecycle and the unique-service shutdown gotcha

This is the single most common way to hang the process.

- `moon.exit(code)` — begin shutdown. `code >= 0` is graceful: it broadcasts
  `PTYPE_SHUTDOWN` and the process only ends once **every** service has quit
  (i.e. the actor counter reaches 0).
- The **default** `PTYPE_SHUTDOWN` handler only quits the bootstrap and
  **non-unique** services. A **`unique = true`** service is left running unless
  it registers its own handler:

```lua
-- REQUIRED in every unique service, or moon.exit hangs forever
moon.shutdown(function()
    -- optional graceful cleanup here (close sockets, flush, etc.)
    moon.quit()
end)
```

- `moon.quit()` — quit the current service (closes its coroutines, then kills it).
- `moon.kill(addr)` — force-kill another service by id (no graceful hook).

If a `cargo run ... .lua` process prints your "done" log but never exits and
keeps emitting background logs, suspect a `unique` service with no
`moon.shutdown` handler.

## Cluster calls (cross-node messaging)

```lua
local cluster = require "moon.cluster"
cluster.init(node_id, "http://host:port/cluster?node={}")  -- discovery URL, {} = node id
cluster.listen()                                            -- bind this node's address

local r = cluster.call(to_node, "svc_name", cmd, ...)       -- async; returns responder values
cluster.send(to_node, "svc_name", cmd, ...)                 -- fire-and-forget
```

- Same-node target (`to_node == NODE`) short-circuits to a local
  `moon.call`/`moon.send` by name (no TCP).
- A missing remote service comes back as a **returned `false, errmsg`**, not a
  Lua error — check the first return value, don't `pcall`.
- For a single-process end-to-end test, map two node ids to the same listen
  address so a call to the "other" node connects over real TCP to yourself
  (see `assets/test/test_cluster_selfconnect.lua`).

## Pitfalls

| Symptom | Cause | Fix |
| --- | --- | --- |
| Process hangs after `moon.exit` | `unique` service never quits | add `moon.shutdown(moon.quit)` to it |
| `attempt to yield ... outside a coroutine` | `call`/`wait`/`sleep`/`new_service` at top level | wrap in `moon.async` |
| Caller coroutine never resumes | handler didn't `moon.response`, or responded to the wrong `session` | echo back the received `session` exactly once |
| `cluster.call` "succeeds" but returns `false` | remote service not found / replied error | inspect the `false, msg` return; don't `pcall` |
| Double-handling a request | called `moon.response` twice for one `session` | respond exactly once per session |

## Verify

```bash
cargo run assets/test/<your_script>.lua   # cluster is a default feature
```

A clean run ends with `system end with code N` and exits on its own. If it
lingers, re-check the shutdown gotcha above.
