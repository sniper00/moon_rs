# Cluster Module (`lua_cluster`)

Inter-node RPC and message routing for the `moon_rs` actor system. Lets a Lua
service on one process address a service on another process by `(node_id,
service_name)`, with the same `send` (fire-and-forget) and `call`
(request/response) semantics as local messaging.

- **Rust core:** `crates/moon-runtime/src/modules/lua_cluster.rs` (registered as `cluster.core`)
- **Lua wrapper:** `lualib/moon/cluster.lua` (`require("moon.cluster")`)
- **Example:** `assets/example/cluster/`
- **Tests:** `assets/test/test_cluster.lua`, `assets/test/test_cluster_selfconnect.lua`

## Architecture

```
┌─────────────────────────────────────────────────────────┐
│ Node 1 (moon_rs process)                                │
│                                                         │
│  Lua Actor A ──cluster.call()──► lua_cluster.rs ──┐    │
│  Lua Actor B ──cluster.send()──► (cluster.core)   │    │
│                                                    │    │
│  ┌────────────────────────────────────────────┐   │    │
│  │ CLUSTER: ClusterState (global singleton)   │   │    │
│  │  • connections:    DashMap<node_id, conn>  │   │    │
│  │  • outbound_calls: DashMap<(from_addr,     │   │    │
│  │                     session), info>        │   │    │
│  │  • pending_calls:  DashMap<local_session,  │   │    │
│  │                     info>                  │   │    │
│  └────────────────────────────────────────────┘   │    │
│                                                    │    │
│  write_task ◄─── ClusterFrame (header + body) ────┘    │
│       │                                                 │
│       ▼ TCP                                             │
└───────┼─────────────────────────────────────────────────┘
        │  4-byte big-endian len + payload
        ▼
┌───────┼─────────────────────────────────────────────────┐
│ Node 2│                                                  │
│       ▼                                                  │
│  read_task ──► dispatch_frame() ──► Local Actor          │
└──────────────────────────────────────────────────────────┘
```

Each TCP connection has a dedicated `read_task` and `write_task` running on the
shared **IO runtime** (Tokio). Lua actors never touch the socket directly: they
enqueue a `ClusterFrame` onto the connection's bounded mpsc channel, and the
`write_task` drains it.

## Lua API

```lua
local cluster = require("moon.cluster")

-- Initialize once per process. node_id is this node's id; discovery_url is an
-- HTTP template with `{}` as the placeholder for a target node id.
cluster.init(node_id, "http://127.0.0.1:9090/cluster?node={}")

-- Start accepting inbound cluster connections (resolves own address via discovery).
cluster.listen()

-- Fire-and-forget message to a remote service. No response, no waiting.
cluster.send(to_node, "service_name", arg1, arg2, ...)

-- RPC call. Yields the current coroutine until the response (or an error)
-- arrives. Returns whatever the remote handler returns.
local ok, result = cluster.call(to_node, "service_name", arg1, arg2, ...)

-- React to a connection loss (delivered to all unique services).
cluster.on_close(function(node_id, reason)
    moon.error(string.format("cluster connection to node %d lost: %s", node_id, reason))
end)

-- Tear down all connections and fail every in-flight call.
cluster.shutdown()
```

| Function | Description |
|----------|-------------|
| `cluster.init(node_id, discovery_url)` | Initialize the cluster singleton. Must be called once before any other API. Errors if called twice. |
| `cluster.listen()` | Resolve this node's own address via discovery and start the accept loop. |
| `cluster.send(to_node, sname, ...)` | Fire-and-forget. Auto-connects if no connection exists yet. |
| `cluster.call(to_node, sname, ...)` | RPC; yields the coroutine. Returns the remote handler's results, or `false, err` on failure. |
| `cluster.on_close(handler)` | Register a `fun(node_id, reason)` callback for connection-loss events. |
| `cluster.shutdown()` | Close all connections, fail pending calls, stop background tasks. |

### Local optimization

`cluster.send` / `cluster.call` are implemented in `lualib/moon/cluster.lua`. If
`to_node == self`, they bypass the network entirely and route through
`moon.send` / `moon.call` after a local `moon.query(sname)` lookup. A missing
local service raises `"local service not found: <sname>"`.

### Argument serialization

The Lua wrapper packs varargs with `seri.pack(...)` before handing the buffer to
`cluster.core`. The remote side receives the body as a `PTYPE_LUA` message, so a
remote `moon.dispatch("lua", ...)` handler sees the original arguments unchanged.

## Node Discovery

Nodes locate each other through an HTTP endpoint instead of hard-coded
addresses. The `discovery_url` passed to `cluster.init` contains `{}` as a
placeholder for the target node id:

```
http://127.0.0.1:9090/cluster?node={}
```

When a connection (or `listen()`) needs an address for node `N`, the cluster
core issues `GET` on the URL with `{}` replaced by `N` and expects the response
body to be the plain-text `host:port` of that node's cluster listener.

The example ships a tiny discovery server, `assets/example/cluster/cluster_etc.lua`,
backed by `node.json`:

```json
[
    { "node": 1, "cluster": "127.0.0.1:42345" },
    { "node": 2, "cluster": "127.0.0.1:42346" }
]
```

## Connection Model

- **Single bidirectional connection per node pair.** Requests and responses for
  a call travel over the same socket, which preserves message ordering.
- **Handshake:** the initiator sends `HELLO <node_id>` first; the acceptor reads
  it as the first frame and registers the connection under that id.
- **Connection generation (`cgen`):** every connection is tagged with a
  monotonically increasing generation. Close handlers only tear down a
  connection whose generation still matches, so a stale close cannot remove a
  newer (reconnected) connection.
- **`connecting: DashSet<u32>` guard:** prevents two tasks from dialing the same
  node concurrently; the loser waits briefly for the winner's connection.
- **Auto-connect:** `send`/`call` to a node with no live connection trigger an
  async connect, then enqueue the frame once connected.

## Wire Protocol

Each TCP frame:

```
[4 bytes: u32 big-endian payload length] [payload]
```

Payload = a text header line terminated by `\n`, optionally followed by a binary
body (seri-encoded Lua values).

Max frame size: **512 MB** (`LIMITS.max_network_read_bytes`). Frames larger than
this are rejected (inbound) or dropped (outbound).

### Message types (verbs)

| Verb | Format | Body | Description |
|------|--------|------|-------------|
| `HELLO` | `HELLO <node_id>\n` | no | Handshake; first frame from the initiator |
| `PING` | `PING\n` | no | Keepalive probe |
| `PONG` | `PONG\n` | no | Keepalive response |
| `SEND` | `SEND <service> <from_node> <from_addr>\n` | yes | Fire-and-forget message |
| `CALL` | `CALL <service> <from_node> <from_addr> <session>\n` | yes | RPC request |
| `RESP` | `RESP <from_addr> <session>\n` | yes | RPC response |

The header and body are kept as separate buffers end-to-end (see *Zero-Copy
Design*).

## Call Lifecycle & Bookkeeping

Two `DashMap`s track in-flight work; they intentionally use different keys
because they track opposite ends of a call:

| Map | Key | Tracks |
|-----|-----|--------|
| `outbound_calls` | `(from_addr, session)` | Calls *this* node made and is waiting on. The composite key is required because `session` comes from a **per-actor** counter and is not unique across actors. |
| `pending_calls` | `local_session` (globally unique) | Inbound calls being serviced *for* a peer. The local handler is the responder, not a waiter — there is no local coroutine suspended on these. |

### Outbound call (`cluster.call`)

1. Lua yields via `moon.wait(core.request(...))`.
2. `lua_cluster_request` inserts an `outbound_calls` entry up-front with
   `cgen = u64::MAX` ("not yet sent"), so the timeout checker tracks it even
   while still connecting.
3. When the frame is actually enqueued on a connection, its `cgen` is set to
   that connection's generation.
4. On `RESP`, the entry is removed and the body is routed back to the waiting
   actor by `(from_addr, session)`. A `RESP` for an already-removed entry is
   **dropped** (dedup), so the coroutine is never woken twice.

### Inbound call (remote `CALL`)

1. `dispatch_frame` allocates a fresh global `local_session`, records a
   `pending_calls` entry, and delivers the body to the target actor with a
   negative session (`-local_session`) so its `moon.response` is routed back.
2. The actor's response arrives on the cluster pseudo-actor's mailbox;
   `spawn_response_reader` looks up the `pending_calls` entry and writes a `RESP`
   frame back over the originating connection.
3. If the service is not found, the core immediately sends back a `RESP` whose
   body is a seri-encoded `(false, "node X, service 'Y' not found")`.

## Error Handling & Release Paths

A suspended `cluster.call` coroutine is released by **exactly one** of three
paths, and each path is idempotent (`fail_outbound_call` only responds if it
actually removed the entry):

| Scenario | Behavior |
|----------|----------|
| Normal response | `RESP` removes the entry and resumes the coroutine with the result. |
| Target service not found | Coroutine resumes with `false, "node X, service 'Y' not found"`. |
| Connection closed mid-call | Every outbound call **sent on that connection generation** fails immediately with `"cluster connection to node X closed (<reason>)"`. Calls on a newer generation are untouched; a late `RESP` is deduped. |
| Call timeout (10s) | Coroutine resumes with `"cluster call to node X timeout"`. This is the final backstop — even "still connecting" calls (`cgen = MAX`) are released here. |
| Connect failure | Coroutine resumes with `"cluster call: connect to node X failed: ..."`. |
| Write-queue full / closed | The connection is torn down and matching outbound calls fail with a backpressure/queue-closed reason. |
| `cluster.shutdown()` | All connections close and **all** remaining outbound calls fail with `"cluster shutdown (SHUTDOWN)"`. |

Inbound `pending_calls` are **not** failed back to anyone on close/timeout — the
remote caller owns the suspended coroutine and releases it via its own outbound
cleanup. The local side only clears the bookkeeping so a late response is
discarded and the map does not grow unbounded.

### Close reasons

`ClusterCloseReason` is surfaced both to logs and to `cluster.on_close`:
`EOF`, `SOCKET_ERROR`, `PROTOCOL_ERROR`, `BACKPRESSURE`, `QUEUE_CLOSED`,
`SHUTDOWN`.

### Connection-loss notification

On a genuine connection loss (no newer connection has replaced it), the core
broadcasts a `PTYPE_SYSTEM` message `"_cluster_close,<node_id>,<reason>"` to all
**unique** actors. The Lua wrapper turns this into the `cluster.on_close(node_id,
reason)` callback.

## Background Tasks

Spawned on the IO runtime by `cluster.init`; they stop when `initialized`
becomes false (i.e. on `cluster.shutdown`).

| Task | Interval | Purpose |
|------|----------|---------|
| `spawn_keepalive` | 5s | `PING` all connections; log connection/pending-call stats. A failed enqueue tears the connection down. |
| `spawn_call_timeout_checker` | 10s | Expire stale inbound and outbound pending calls. |
| `spawn_response_reader` | event-driven | Drain the cluster pseudo-actor mailbox and route `RESP` frames back to callers. |

## Zero-Copy Design

- **Send path:** `ClusterFrame` keeps the header (`Vec<u8>`) and body
  (`Box<Buffer>`) separate. `write_task` emits the 4-byte length prefix, header,
  and body via a single vectored write (`write_vectored`) — no concatenation.
- **Receive path:** `read_one_frame` reads directly into a `Box<Buffer>`. After
  the text header is parsed, `Buffer::consume()` advances the read position past
  the header, and the *same* buffer (now representing only the body) is handed to
  the target actor.

## Configuration Constants

| Constant | Value | Description |
|----------|-------|-------------|
| `CONNECT_TIMEOUT_MS` | 5000 | TCP connect timeout |
| `PING_INTERVAL_MS` | 5000 | Keepalive interval |
| `CALL_TIMEOUT_S` | 10 | RPC call timeout (and timeout-checker interval) |
| `MAX_FRAME_SIZE` | 512 MB | `LIMITS.max_network_read_bytes` |
| `CLUSTER_WRITE_QUEUE_CAPACITY` | 64 Ki | `LIMITS.network_write_queue_capacity`; per-connection write backpressure bound |
| `CLUSTER_ACTOR_ID` | `0xFFFF_FF00` | Reserved pseudo-actor that receives call responses |

## Running the Example

Three processes (discovery server + two nodes):

```bash
# 1. Discovery server (serves node.json over HTTP on :9090)
cargo run --release assets/example/cluster/cluster_etc.lua assets/example/cluster/node.json

# 2. Receiver node (listens, handles ACCUM/COUNTER commands)
cargo run --release assets/example/cluster/node.lua 2

# 3. Sender node (calls + sends to node 2)
cargo run --release assets/example/cluster/node.lua 1
```

For a single-process smoke test (self-connect over real TCP via a bundled
discovery server):

```bash
cargo run --release assets/test/test_cluster_selfconnect.lua
```

## Files

| Path | Role |
|------|------|
| `crates/moon-runtime/src/modules/lua_cluster.rs` | Core Rust implementation + unit tests |
| `lualib/moon/cluster.lua` | Lua wrapper (local routing, seri pack/unpack, `on_close`) |
| `assets/example/cluster/node.lua` | Example sender/receiver node |
| `assets/example/cluster/cluster_etc.lua` | Example discovery HTTP server |
| `assets/example/cluster/node.json` | Example node→address table |
| `assets/test/test_cluster.lua` | Same-node functional test |
| `assets/test/test_cluster_selfconnect.lua` | Self-connect end-to-end test |
| `docs/cluster_pending_wait_analysis.md` | Deep-dive on pending-wait release correctness |
