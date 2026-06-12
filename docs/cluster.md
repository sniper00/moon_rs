# Cluster Module

Inter-node RPC and message routing for `moon_rs` actor system.

## Architecture

```
┌─────────────────────────────────────────────────────────┐
│ Node 1 (moon_rs process)                                │
│                                                         │
│  Lua Actor A ──cluster.call()──► lua_cluster.rs ──┐    │
│  Lua Actor B ──cluster.send()──► (Rust module)    │    │
│                                                    │    │
│  ┌────────────────────────────────────────────┐   │    │
│  │ ClusterState (global singleton)            │   │    │
│  │  • connections: DashMap<node_id, tx>       │   │    │
│  │  • outbound_calls: DashMap<session, info>  │   │    │
│  │  • pending_calls: DashMap<session, info>   │   │    │
│  └────────────────────────────────────────────┘   │    │
│                                                    │    │
│  write_task ◄─── ClusterFrame (header + body) ────┘    │
│       │                                                 │
│       ▼ TCP                                             │
└───────┼─────────────────────────────────────────────────┘
        │  4-byte len + payload
        ▼
┌───────┼─────────────────────────────────────────────────┐
│ Node 2│                                                  │
│       ▼                                                  │
│  read_task ──► dispatch_frame() ──► Local Actor          │
└──────────────────────────────────────────────────────────┘
```

## Wire Protocol

Each TCP frame:
```
[4 bytes: u32 big-endian payload length] [payload]
```

Payload = NATS-style text header + `\n` + binary body (seri-encoded Lua values).

Max frame size: **256 MB**.

### Message Types (verbs)

| Verb | Format | Description |
|------|--------|-------------|
| `HELLO` | `HELLO <node_id>\n` | Handshake on connect (no body) |
| `PING` | `PING\n` | Keepalive probe (no body) |
| `PONG` | `PONG\n` | Keepalive response (no body) |
| `SEND` | `SEND <service> <from_node> <from_addr>\n<body>` | Fire-and-forget message |
| `CALL` | `CALL <service> <from_node> <from_addr> <session>\n<body>` | RPC request |
| `RESP` | `RESP <from_addr> <session>\n<body>` | RPC response |

## Node Discovery

Nodes discover each other via an HTTP endpoint. The `discovery_url` contains `{}` as a placeholder for the target node ID:
```
http://127.0.0.1:9090/cluster?node={}
```
The endpoint returns the plain-text `host:port` of that node's cluster listener.

## Connection Model

- **Single bidirectional connection** per node pair (guarantees message ordering).
- The initiator sends `HELLO` first; the acceptor registers the connection upon receiving it.
- **Connection generation counter** (`conn_gen`) prevents race conditions during reconnection — stale close handlers won't remove newer connections.
- **`DashSet<u32> connecting`** guard prevents concurrent connection attempts to the same node.

## Lua API

```lua
local cluster = require("moon.cluster")

-- Initialize (once per process)
cluster.init(node_id, "http://etc-server:9090/cluster?node={}")
cluster.listen()

-- Fire-and-forget send
cluster.send(target_node, "service_name", arg1, arg2, ...)

-- RPC call (yields current coroutine)
local ok, result = pcall(cluster.call, target_node, "service_name", arg1, arg2, ...)
```

### Local Optimization

If `target_node == self`, calls are routed locally via `moon.send` / `moon.call` without network IO.

## Error Handling

| Scenario | Behavior |
|----------|----------|
| Target service not found | CALL returns error: `"node X, service 'Y' not found"` |
| Connection closed mid-call | All pending outbound calls receive error immediately |
| Call timeout (10s default) | Coroutine resumes with error: `"cluster call to node X timeout"` |
| Connect failure | Error returned to caller: `"connect to node X (addr) failed: ..."` |
| Channel send failure | Immediate async error response to waiting coroutine |

## Background Tasks

| Task | Interval | Purpose |
|------|----------|---------|
| `spawn_keepalive` | 5s | PING all connections, log stats |
| `spawn_call_timeout_checker` | 10s | Expire inbound + outbound pending calls |
| `spawn_response_reader` | — | Reads pseudo-actor mailbox, routes RESP frames back to callers |

## Zero-Copy Design

- **Send path**: Header (`Vec<u8>`) and body (`Box<Buffer>`) are kept separate in `ClusterFrame`. The `write_task` writes length prefix, header, and body as 3 sequential `write_all` calls — no intermediate concatenation.
- **Receive path**: `read_one_frame` reads directly into `Box<Buffer>`. After parsing the text header, `Buffer::consume()` advances `rpos` past the header — the same `Box<Buffer>` (now representing only the body) is passed to the target actor.

## Configuration Constants

| Constant | Value | Description |
|----------|-------|-------------|
| `CONNECT_TIMEOUT_MS` | 5000 | TCP connect timeout |
| `PING_INTERVAL_MS` | 5000 | Keepalive interval |
| `CALL_TIMEOUT_S` | 10 | RPC call timeout |
| `MAX_FRAME_SIZE` | 256 MB | Maximum single message size |
| `CLUSTER_ACTOR_ID` | `0xFFFF_FF00` | Reserved pseudo-actor for response routing |

## Files

| Path | Role |
|------|------|
| `crates/moon-runtime/src/modules/lua_cluster.rs` | Core Rust implementation |
| `lualib/moon/cluster.lua` | Thin Lua wrapper (local routing + seri pack/unpack) |
| `assets/example/cluster/` | Example: multi-node cluster with discovery server |
| `assets/test/test_cluster.lua` | Same-node functional test |
