# Lua library validation

Run from the repository root with Rust, Python 3, and a working Docker Engine:

```sh
cargo test --workspace --locked
python3 assets/test/run_all.py
```

The Python runner builds `moon-app`, runs each Lua regression in its own
`target/debug/moon_rs` process, and starts disposable database containers.
It uses the repository's bundled Lua 5.5.1 and default module features. The Moon
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
| Pure Lua | Base string/table/math/os/io/class helpers, queue FIFO, reentrant mutex and concurrent task completion, hotfix preserving upvalues |
| Shared tables / cloning | Cross-service shared tables; clonefunc exercised by hotfix |
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
