# CLAUDE.md

Project-level instructions for AI coding assistants working on **moon_rs**.

## Project Overview

moon_rs is a Rust reimplementation of [Moon](https://github.com/sniper00/moon) — a lightweight, high-performance game server framework. It provides a **Lua-scripted actor runtime on Tokio** with native Rust bindings for networking, HTTP, databases, and more.

Architecture: Lua actors (services) communicate via typed message passing, with Rust handling async I/O on a separate Tokio runtime. Each actor has its own `lua_State` and runs as a Tokio task (or dedicated OS thread for unique actors).

## Workspace Structure

```
crates/
  moon-base/      # Foundation: Lua 5.5 C sources + Rust FFI (laux.rs), macros, shared Buffer, yyjson JSON decoder
  moon-runtime/   # Actor server runtime (CONTEXT, message types, timer, logger) + ALL Rust→Lua native bindings (lua_socket, lua_httpc, lua_httpd, etc.)
  moon-app/       # Binary entry point (moon_rs), Tokio setup, signal handling, bootstrap
  moon-thrift/    # Standalone Lua C extension (cdylib), excluded from the workspace; built separately
lualib/           # Lua runtime library (user-facing API: moon.lua, socket, http, db wrappers)
assets/           # Example scripts, benchmarks, integration test Lua scripts
docs/             # Per-module architecture and API docs
```

## Build & Run

```bash
# Build (requires Rust nightly)
cargo build --release

# Run with a Lua bootstrap script
cargo run --release assets/example/example.lua

# Run tests
cargo test

# Run Lua integration tests (manual)
cargo run --release assets/test/test_socket.lua
```

## Key Conventions

### Rust

- **Edition:** 2024 for all crates
- **Allocator:** `mimalloc` (set in `moon-app`)
- **Async runtime:** Tokio multi-thread (main) + dedicated IO runtime in `CONTEXT` (up to 4 workers)
- **Error handling:** `moon_runtime::Error` enum with `derive_more::From`; Lua FFI uses `lua_push_error()` for `(false, errmsg)` returns, `laux::lua_error()` for hard errors
- **Unsafe code:** extensive around Lua C API — `#[allow(clippy::not_unsafe_ptr_arg_deref)]` etc. Wrap Rust fns as `extern "C-unwind"` via `not_null_wrapper!` macro
- **Naming:** crates are `moon-*`; Lua binding source files are `lua_<feature>.rs`; protocol constants are `PTYPE_*`
- **Global singletons:** `CONTEXT` (actor registry, runtimes), `LOGGER` (async logger) — both `lazy_static`
- **Feature flags (`moon-runtime`):** each native module is feature-gated (`excel`, `httpc`, `httpd`, `sqlx`, `mongodb`, `websocket`, `pg`, `redis`, `cluster`, `protobuf`). Use `#[cfg(feature = "...")]` for conditional compilation

### Lua

- Native modules register as `moon.core`, `net.core`, `httpc.core`, `httpd.core`, etc.
- User-facing Lua API layered: C core → `lualib/moon/*.lua` wrappers → user scripts
- EmmyLua annotations in `lualib/moon/api/*.lua` for IDE support

### Testing

- **Rust unit tests:** inline `#[cfg(test)] mod tests` in `moon-runtime/src/lib.rs`, `lua_redis.rs`, `lua_pg.rs`, `moon-base/src/buffer.rs`
- **Test helpers:** `new_lua_vm()`, `run_lua()`, `run_lua_expr()` in `moon-runtime/src/lib.rs`
- **Lua integration tests:** under `assets/test/` — run manually via `cargo run --release`
- **No automated tests in CI** — CI only builds release binaries

### Module Pattern

Adding a new native module follows this pattern:
1. Create `crates/moon-runtime/src/modules/lua_<name>.rs`
2. Add feature flag in `crates/moon-runtime/Cargo.toml` if it has optional deps
3. Register via `lua_require!` macro in `crates/moon-runtime/src/lib.rs` (with `#[cfg(feature)]` guard)
4. Create Lua wrapper in `lualib/moon/<name>.lua`
5. Add tests and docs

## Architecture Notes

- **Unique vs non-unique actors:** unique actors (`unique = true`) run on dedicated OS threads with blocking receive; non-unique actors run as Tokio tasks
- **Message types:** typed via `PTYPE_*` constants (`PTYPE_LUA`, `PTYPE_SOCKET_TCP`, `PTYPE_HTTPC`, etc.)
- **Per-actor memory:** custom Lua allocator tracks memory per actor with configurable limits
- **Startup flow:** `main()` → Tokio runtime → `async_main()` → signal setup → logger → monitor/timer tasks → bootstrap Lua actor → event loop until shutdown

## Important Caveats

- The project uses Lua 5.5 C sources compiled via `cc` in `build.rs` (`links = "lua54"` is a legacy artifact)
- Extensive `unsafe` code is expected and necessary for Lua C API interop
- When modifying Lua FFI code, always check `lua_checkstack` for recursive operations
- Watch for enum size bloat from infrequently-used variants (see `todo.txt`)
- `.gitattributes` maps `*.c` to Rust for GitHub language stats (intentional)
