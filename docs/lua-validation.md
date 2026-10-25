# Lua library validation

Run from the repository root with Rust, Python 3, and a working Docker Engine:

```sh
cargo test --workspace --locked
python3 assets/test/run_all.py
```

The Python runner builds the workspace with all features, runs each Lua regression in its own
`target/debug/moon_rs` process, and starts disposable database containers.
It uses the repository's bundled Lua 5.5.1 and all runtime module features. The Moon
process runs on the host; database services run in Docker.

```sh
# No Docker required: native helpers, actor lifecycle, files, networking, gRPC.
python3 assets/test/run_all.py --only local

# Only selected database groups; reuse an already-built binary.
python3 assets/test/run_all.py --skip-build --only postgres mysql

# If Docker group membership was just granted in another terminal:
sg docker -c 'python3 assets/test/run_all.py'
```

Logs and `results.json` default to `/tmp/moon-rs-validation`. Use `--output PATH`
to keep independent runs. A nonzero exit status indicates a failed script,
timeout, or unavailable service. Each Lua process has a deadline; timed-out
process groups are killed. Run one suite at a time, because some existing
network tests use fixed loopback ports.

## Verified result

The completed run passed **34 Lua regression scripts** and **496 Rust tests**.
Six Rust performance/profiling tests remained ignored by their existing test
attributes. All database services started successfully and were removed after
validation. This includes a full rerun after the fixes listed below.

Final logs for this session are in `/tmp/moon-rs-validation/final/` (Lua),
`/tmp/moon-rs-validation/final.log` (runner summary), and
`/tmp/moon-rs-validation/rust-final.log` (Rust).

## Coverage

| Area | Functional coverage |
| --- | --- |
| Lua / actor core | Service creation, uniqueness, calls/responses, service exit cleanup, timers, to-be-closed values, nested coroutines, watchdog interruption |
| Buffer / serialization | Buffer reads/writes, binary data, seri round trips, JSON options and parsing, schema validation |
| Helpers | Random, UUID, sorted sets, hashes/base64 (also covered by Rust tests), filesystem traversal and removal |
| Pure Lua | Base string/table/math/os/io/class helpers, queue FIFO, reentrant mutex and concurrent task completion; hotfix identity matching, state preservation, additions and rollback |
| Shared tables / cloning | Cross-service shared tables; `hotfix.core` exercised by hotfix |
| Excel | Generated XLSX with Unicode, number and boolean cells; CSV header/data, row limit, missing-file error. XLSX booleans use the existing integer 0/1 mapping |
| Socket | TCP reads/writes and framing, accept/connect/error paths |
| HTTP | Lua server and native server, requests/forms, headers/status codes, binary streaming, early stream close, request timeout, mixed LF/CRLF SSE |
| WebSocket | Local client/server, text/binary, ping/pong, close and configured limits |
| Cluster | Same-node calls and real TCP self-connect through a local discovery server |
| Protobuf / gRPC | Rust protobuf encode/decode regressions; gRPC connection failure, unary and streaming RPCs, cancellation and close |
| Native PostgreSQL | Connection/auth errors, bound parameters, transactions/pipelines, bulk writes, types, pool behavior, arrays and NULL elements |
| SQLx PostgreSQL | Types including JSON/JSONB, timestamp formatting, decimal precision/range, transactions and errors |
| SQLx MySQL | Types, decimal precision, all unsigned integer widths through max u64, transactions, streamed batches and early close |
| SQLx SQLite | In-memory queries, runtime storage types, queue saturation, streaming, statistics and same-name replacement |
| MongoDB | CRUD, BSON/options, indexes, result conversion, streaming and early cursor release |
| Redis | Commands, pipelines, pub/sub and stream operations |
| Name search | PostgreSQL prefix/substring/trigram search, literal wildcard escaping, update/delete |
| etcd | Native gRPC/protobuf transport, key put/range/delete, Watch, lease grant/revoke, maintenance status |

The suite uses `postgres:17`, `mysql:8.4`, `mongo:8.0`, `redis:7-alpine`, and
`quay.io/coreos/etcd:v3.5.17`. Database ports are chosen dynamically and published
only on `127.0.0.1`. Credentials in the runner are disposable test credentials;
no developer database URL or example-file credentials are used. Containers and
anonymous volumes are removed in `finally` blocks; downloaded images remain
cached. The databases do not mount repository or application data.

Existing database tests also accept these environment overrides for individual
runs:

| Variable | Consumer |
| --- | --- |
| `MOON_PG_TEST_URL` | Native PG, PG arrays, SQLx PostgreSQL, name search |
| `MOON_MYSQL_TEST_URL` | SQLx MySQL types and streaming |
| `MOON_MONGO_TEST_URL` | MongoDB CRUD and streaming |
| `MOON_REDIS_TEST_PORT` | Redis loopback test port |
| `MOON_ETCD_TEST_URL` | etcd gRPC endpoint |
| `MOON_TEST_FIXTURES` | Generated XLSX and temporary files for helper tests |

Some database tests create/drop named tables or collections: use a disposable
database when running them independently. The integrated runner provides one.
`MOON_PG_TEST_URL` for the native PG suite should not include query parameters;
the tests append their own pool settings.

## Findings fixed during validation

- CSV row-count preallocation exhausted the reader before decoding rows. CSV
  data now loads in one pass, including when a row limit is supplied.
- SSE delimiter selection could skip an earlier CRLF event when a later LF event
  appeared in the same buffer. The parser now chooses the earliest delimiter.
- SQLx did not classify/decode MySQL unsigned integers. Values fitting Lua's i64
  are integers; larger values remain exact decimal strings.
- The local cluster test left its unique echo service running after assertions
  passed. It now registers an explicit shutdown handler.
- SQLx type tests expected the old timestamp strings. Assertions now match the
  ISO-style naive timestamp and RFC3339 UTC timestamp formats.

## Limits

This is functional regression coverage, not exhaustive coverage of every option,
error interleaving, database type, or production deployment. TLS database modes,
WSS, replicas, multi-node etcd/Redis/MongoDB, authentication administration, and
sustained load are outside this run. The existing HTTP smoke test also contacts
`https://bing.com`, so the local group currently requires outbound HTTPS access.
Cluster TCP coverage uses a self-connection, not two independent processes.

The old `test_endless_loop.lua`, `test_watchdog_interrupt.lua`, and
`test_watchdog_coroutine.lua` are manual diagnostics without reliable termination
criteria. The automated `test_watchdog.lua` replaces the latter two in the suite
and verifies both interrupt paths with a deadline. Six ignored Rust performance
benchmarks/profiling tests are intentionally not part of the functional run.

## Lua 5.5.1 refresh

The bundled sources are synchronized with `moon/third/lua`, including its
Lua 5.5.1 release update and shared-table GC/raw-write protections (source Lua
tree last changed by commit `014255f89c9e5affe952734ea5be3affde4430d1`).
This repository keeps its own `lhotfix.c` extension and closure-redirection
patch instead of restoring the source repository's older `clonefunc.c` include.
`hotfix.patch` is refreshed against the new core. The Rust `luaL_Buffer`
declaration also accounts for the release's increased native alignment.

Validation passed the all-feature workspace build, 548 Rust tests (6 ignored),
all 25 local Lua scripts, and focused shared-table overwrite/GC regressions.
ASan/UBSan checks passed all 85 hotfix cases, five stubbed actual-business
samples, and loading from GC-triggering text/binary readers. Leak detection was
disabled for these sanitizer checks; they do not establish leak freedom.

## Hotfix contract

`hotfix.require(name)` loads a table-returning Lua module and records its loader.
After editing the same source, call `hotfix.update(name)`; an optional second
argument selects another source module. Updates return `true, module` or
`false, message`. Custom searchers remain supported; the default searcher reads
fresh source through `package.path`, bypassing Moon's bytecode cache. To register
an already loaded module, supply the original loader and its actual return table
to `hotfix.register(name, loader, module)`.

Existing functions match by their child-prototype paths. Declaration order is
an application contract: do not reorder or delete existing functions, or insert
new siblings before them. New declarations append at the end of their own
lexical parent; nested functions obey the same rule independently. Runtime
checks reject removed prototypes but cannot establish declaration identity
from counts alone. Use the offline check below before applying a patch.

Module initialization is not rerun. Existing tables, fields, selected branches,
computed keys and captured state remain in place. Existing function bodies can
update regardless of whether their closures were created by loops, conditional
branches, computed table keys or factory calls. Captured tables and their actual
metatables are traversed without invoking metamethods, so discovered `__index`
and `__gc` callbacks also update. Each live closure instance gets its own bound
replacement. New captures must resolve to existing state or a statically
declared module helper. Function-scope module cells survive temporary removal
of a capture; loop and block-local cells are not permanently anchored.

Binding information is recovered separately from standard Lua instructions to
locate newly added exported fields and module-level helpers. The analyzer reads
constants, register operations and debug scopes; it never evaluates module code,
calls functions or reads environment values. It needs no source attachment or
modified compiler, including for custom searchers that return `load(...)` results.
Static method names, arbitrary literal string keys,
table literals, aliases and enum member keys are supported for additions.
Bindings for already existing declarations are not reapplied: changing an
initializer's key or condition updates its known closure body, while preserving
the existing table layout. Adding an alias to an existing function is supported.
New parent tables and data initializers are not constructed by hotfix.

Existing function values keep their identity; cached references execute the
latest body on their next call:

```lua
local module = hotfix.require("my_module")
-- Alternatively, for an already loaded module with its original loader:
hotfix.register("my_module", loader, module)
local cached = module.run
assert(hotfix.update("my_module"))
assert(module.run == cached) -- existing function values keep their identity
cached()                    -- executes the latest implementation
```

Redirection is the only update strategy; there is no mode option.
All generations of each known closure instance share one target cell, so a
reference to an intermediate version also follows subsequent updates without a
chain of redirects. State binding inspects the current target, rather than the
original closure's upvalues. Different factory instances keep separate target
cells and captured state. Registration and updates discover functions through
the module table, captured state and table metatables. Previously discovered
instances held only by external references remain updatable through weak
tracking. Message, timer,
HTTP, gRPC and socket callback registrars have no hotfix hooks; callbacks never
discovered through the module or its captured state are outside this tracking.

Redirection happens before stack sizing in both normal calls and tail calls.
An already-running or suspended frame continues its original bytecode; its
subsequent calls may enter updated helpers. This is not a snapshot of an entire
call chain. Inspection through the ordinary debug API, prototype inspection,
cloning and dumping still refer to the function value's original body;
`require("hotfix.core").resolve(f)` returns its current target for inspection. Redirection
cells are runtime state and are not serialized in bytecode. The low-level
`require("hotfix.core").redirect(old, new)` shares a target cell and rejects merging different
instances; it does not perform hotfix state validation or binding by itself.

Preparation binds only new closures. Commit updates target cells and adds new
exported fields; existing function references and captured cells are not
rewritten. Target changes and new fields share one rollback plan; the current
loader advances only after success. Field writes use raw table access and do
not invoke module metamethods. This is intended for ordinary module tables,
not virtual table proxies.

Adding an enum-key field requires its actual key to remain readable through
existing captures or their recorded aliases; otherwise the update fails with
`missing enum key state`. Conflicting captured key origins are rejected. These
reads use raw table access. Existing handler body updates do not resolve keys,
even if the enum was used only during initialization. Arbitrary function calls
and loops are not executed to create new entries: a newly appended module-level
function without a static export/helper binding is rejected before commit.

Target cells use closed upvalues, with explicit initialization, GC traversal and
write barriers. On 64-bit builds every Lua closure has an additional 8-byte
pointer. A target cell is allocated only when an instance is first redirected
(40 bytes on this build). A stable original function value continues to retain
its original code and captured cells, as well as its latest target; abandoned
intermediate targets and unreachable instances
remain collectible. Function-call entry also performs an additional pointer
check. Binding analysis allocates temporary Lua tables during registration and
updates, with no additional fields in `Proto` or `Upvaldesc`.

The compiler, dumper and undumper are unchanged by hotfix. Both `string.dump(f)`
and `string.dump(f, true)` emit official Lua 5.5 format 0. Unstripped chunks
support automatic additions without source files; stripped chunks execute
normally but cannot be hotfixed without upvalue names and debug scopes.

`lualib/hotfix.lua` contains binding analysis, export resolution, declaration-order
checks and the update implementation; there are no hotfix Lua submodules.
Its native helpers are loaded with `require "hotfix.core"`; the runtime registers
that module through `luaopen_hotfix_core` from `lua/lhotfix.c`.
Binding analysis conservatively reads Lua 5.5 instructions.
Branch-dependent keys, conflicting aliases and shadowed binding names cannot
establish new fields. Existing bodies still match by prototype path. Constant
folding may remove a source-level branch, so the analyzer describes compiler
output rather than reconstructing every lexical source construct. The native
reader exposes opcode names/modes from Lua's own headers; operand layouts and
instruction semantics remain version-dependent and need review on a Lua upgrade.

The build compiles `lua/onelua.c`, which includes `lua/lhotfix.c` after the core.
The native helpers share its translation unit and can use static internal symbols.
The remaining hotfix core patch touches `lobject.h`, `lfunc.c`, `ldo.c`, `lgc.c`
and `ltests.c`. It is recorded in `crates/moon-lua-sys/patches/hotfix.patch`; see the
adjacent README for upgrade checks. The patch also records the `onelua.c` include;
keep `lhotfix.c` when refreshing the vendor sources. It does not include Moon's preexisting
shared-code-cache changes. Rebuild the runtime after changing native sources.

Prototype inspection walks checked paths without creating temporary template
closures. Registration and successful updates retain compact capture descriptors,
discarding the expanded binding analysis. Cached old functions keep their own code without
retaining the old loader or its sibling functions. The registry weakly tracks
only current targets, with every live generation reaching that target through
the shared cell. Module-level state cells remain available for future updates.
Successful updates retain prototype paths and export identities without
retaining expanded key-resolution paths.

To measure registration overhead and retention across repeated updates, run:

```sh
target/debug/moon_rs assets/benchmark/benchmark_hotfix.lua 400 12
target/debug/moon_rs assets/benchmark/benchmark_hotfix.lua 400 100
```

The benchmark retains one old function per update, then releases those references.
It reports the current Lua state's heap after full collection, rather than
process RSS; Moon's shared bytecode cache is outside this measurement.

Run the focused regressions with:

```sh
cargo build -p moon-app --locked
target/debug/moon_rs assets/test/test_hotfix.lua
```

Check declaration order without executing either version's initialization with
`hotfix.check_order(oldloader, newloader)`, returning `valid, errors, uncertain`.
The command-line script only reads files and reports these results:

```sh
target/debug/moon_rs assets/tools/check_hotfix_order.lua old.lua new.lua
```

Exit 0 means the available identities passed; exit 1 indicates changed identities,
insertions, removals or invalid source; exit 2 requires manual review because an
anonymous or repeated binding cannot establish identity. This is a conservative
review aid, not a proof for arbitrary anonymous-code edits. It is independent of
the update path. Byte-identical input files pass after syntax checking.

The focused suite covers context loaders, copied command functions, independent
loop/factory instances, computed and literal keys, automatic additions,
metatable callbacks, GC retention, rollback and the offline checker. Isolated
checks with actual `xserver` sources (`UserSettingMgr`, `HeroMgr`, `CounterMgr`,
`SearchModule` and `sdk_handler`) stub external dependencies; they verify state
and updated calls without exercising a complete running game service. A static
self-comparison of all 252 logic/handler modules also passes; this does not prove
that an arbitrary old/new business patch obeys declaration-order requirements.
