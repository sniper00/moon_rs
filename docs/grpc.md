# gRPC Native Client + Server (`lua_grpc`)

A native gRPC **client and server** for moon_rs built on
[`tonic`](https://github.com/hyperium/tonic)'s HTTP/2 framing. The client uses a
tonic `Channel`; the server is a [`hyper`](https://github.com/hyperium/hyper)
HTTP/2 (h2c) listener whose framing/trailers are driven by `tonic::server::Grpc`.
Inspired by [`lua-resty-ffi-grpc`](https://github.com/kingluo/lua-resty-ffi-grpc)
(OpenResty + tonic), adapted to moon_rs's session-based actor model.

**Protobuf is delegated to the `protobuf` module** (`lua_protobuf.rs`). The native
layer carries only *raw protobuf bytes* over the wire; the Lua wrapper encodes the
request with `protobuf.encode` and decodes the reply with `protobuf.decode`. This
keeps a single schema source of truth and avoids a second protobuf implementation.

## Architecture

```
┌──────────────────────────────────────────────────────────────┐
│ Lua Actor Thread                                              │
│                                                              │
│  conn:unary(path, req_type, req, resp_type)                  │
│    → protobuf.encode(req_type, req)  → request bytes         │
│    → grpc.core unary(path, bytes)    → returns a session     │
│    → moon.wait(session) — coroutine yields                   │
│                                                              │
│  PTYPE_GRPC reply → decode_grpc_message                      │
│    → { status, message, body }                               │
│    → protobuf.decode(resp_type, body) → table                │
└──────────────┬───────────────────────────────────────────────┘
               │ (owner, session) + raw bytes
               ▼
┌──────────────────────────────────────────────────────────────┐
│ IO Runtime (tokio) — tonic Channel (one HTTP/2 connection)    │
│                                                              │
│  BytesCodec: identity passthrough (no protobuf parsing)      │
│  Grpc::unary / server_streaming / streaming                  │
│  TLS via rustls (aws-lc-rs) + webpki roots                   │
│  send_value(PTYPE_GRPC, owner, session, response)            │
└──────────────────────────────────────────────────────────────┘
```

A tonic `Channel` multiplexes concurrent requests over a single HTTP/2 connection
and reconnects on its own, so there is **one channel per connection name** (no
worker pool). Streaming RPCs get a small handle (`grpc.core.find_stream(fd)`).

## Build

Gated behind the `grpc` Cargo feature (enabled by default):

```toml
# crates/moon-runtime/Cargo.toml
grpc = [
    "dep:tonic", "dep:tokio-stream", "dep:http", "dep:futures-util",
    "dep:hyper", "dep:hyper-util", "dep:http-body-util", "dep:tokio-util",
]
```

## Lua API (`require("moon.grpc")`)

```lua
local protobuf = require("protobuf")
local grpc     = require("moon.grpc")

-- Load a serialized FileDescriptorSet once (protoc --descriptor_set_out=...).
local f = assert(io.open("helloworld.pb", "rb"))
protobuf.load(f:read("*a")); f:close()

-- Connect. http:// is plaintext h2c; https:// auto-enables TLS.
local conn, err = grpc.connect({
    endpoint        = "http://127.0.0.1:50051",
    name            = "greeter",   -- registry key (default "default")
    connect_timeout = 5000,        -- ms
    -- tls = { domain = "example.com", ca = <pem>, cert = <pem>, key = <pem> },
})
assert(conn, err)
```

### Unary

```lua
local reply, status = conn:unary(
    "/helloworld.Greeter/SayHello",  -- method path
    "helloworld.HelloRequest",       -- request message type
    { name = "moon_rs" },            -- request table
    "helloworld.HelloReply",         -- response message type
    { timeout = 3000, metadata = { authorization = "Bearer x" } } -- optional
)
if reply then
    print(reply.message)
else
    print("error", status.code, status.message) -- gRPC status code (0 = OK)
end
```

### Server streaming (one request → many responses)

```lua
local stream = conn:server_stream(
    "/routeguide.RouteGuide/ListFeatures",
    "routeguide.Rectangle", { lo = {...}, hi = {...} },
    "routeguide.Feature")

while true do
    local feature, serr = stream:recv()
    if not feature then
        if serr then print("stream error:", serr) end
        break  -- nil + no error => clean end of stream
    end
    print(feature.name)
end
stream:close()
```

### Client / bidirectional streaming

```lua
local chat = conn:bidi_stream(
    "/routeguide.RouteGuide/RouteChat",
    "routeguide.RouteNote",  -- request type for :send
    "routeguide.RouteNote")  -- response type for :recv

chat:send({ message = "hi" })
local note = chat:recv()
chat:close_send()  -- half-close the request side
chat:close()
```

Streams support Lua **to-be-closed** variables (`<close>`), so they auto-release
the native task/registry slot on scope exit, `break`, or error — no explicit
`close()` needed (and it stays idempotent if you call `close()` anyway):

```lua
do
    local stream <close> = conn:server_stream(path, req_type, req, resp_type)
    for msg in function() return stream:recv() end do
        print(msg)
    end
end -- stream:close() runs here automatically
```

## Server

The server is a plain HTTP/2 cleartext (h2c) listener. Each inbound RPC — **any**
of the four kinds — is delivered to the listening actor's `grpc.dispatch` handler
as a `grpc.ServerStream`. The transport treats every call as a bidirectional
stream (the wire framing is identical for all kinds); your handler reads/writes as
many messages as the method needs. As on the client, the handle moves **raw
protobuf bytes** — you decode/encode with the `protobuf` module.

```
┌──────────────────────────────────────────────────────────────┐
│ IO Runtime (tokio) — hyper http2 accept loop                 │
│   per connection: tonic::server::Grpc::new(BytesCodec)       │
│     .streaming(svc, req)  → framing + grpc-status trailers   │
│   send_value(PTYPE_GRPC, owner, session=0, ServerRpc{path})  │
└──────────────┬───────────────────────────────────────────────┘
               │ (path, server-stream handle)  [session 0 → dispatch]
               ▼
┌──────────────────────────────────────────────────────────────┐
│ Lua Actor — grpc.dispatch(fn) runs fn(stream, path) in a co  │
│   stream:recv() → moon.wait → next request bytes (nil = end) │
│   stream:send(bytes) ; stream:finish(code?, msg?)            │
└──────────────────────────────────────────────────────────────┘
```

```lua
local protobuf = require("protobuf")
local grpc     = require("moon.grpc")

local f = assert(io.open("helloworld.pb", "rb"))
protobuf.load(f:read("a")); f:close()

grpc.dispatch(function(stream, path)
    if path == "/helloworld.Greeter/SayHello" then            -- unary
        local req = protobuf.decode("helloworld.HelloRequest", stream:recv())
        stream:send(protobuf.encode("helloworld.HelloReply", { message = "hello " .. req.name }))
        -- finish() is auto-called with OK when the handler returns

    elseif path == "/helloworld.Greeter/SayHelloStream" then  -- server streaming
        local req = protobuf.decode("helloworld.HelloRequest", stream:recv())
        for i = 1, 3 do
            stream:send(protobuf.encode("helloworld.HelloReply", { message = req.name .. " #" .. i }))
        end

    elseif path == "/helloworld.Greeter/SayHelloChat" then    -- client/bidi streaming
        while true do
            local bytes = stream:recv()
            if bytes == nil then break end                    -- request stream half-closed
            local req = protobuf.decode("helloworld.HelloRequest", bytes)
            stream:send(protobuf.encode("helloworld.HelloReply", { message = "echo " .. req.name }))
        end

    else
        stream:finish(12, "unimplemented: " .. path)          -- 12 = UNIMPLEMENTED
    end
end)

local fd = grpc.listen("0.0.0.0:50051")   -- { max_connections = N } optional
-- grpc.stop(fd)  -- to shut the listener down
```

The handler runs in its own coroutine and may `moon.wait`/`moon.sleep` freely. If
it returns without calling `finish`, the stream is auto-finished with **OK**; if it
raises an error, the stream is finished with **INTERNAL (13)** — so a client never
hangs on a crashed handler. TLS termination is not built in; front the listener
with a TLS-terminating proxy (or a load balancer) if you need it.

## API reference

### Module `moon.grpc`

| Function | Description |
| --- | --- |
| `grpc.connect(opts)` | Async connect; registers the channel under `opts.name`. Returns `conn` or `nil, err`. |
| `grpc.find_connection(name)` | Wrap an already-connected channel (sync). |
| `grpc.close(name)` | Unregister and drop a named channel. |
| `grpc.stats()` | `{ connections, streams, servers }` counters. |
| `grpc.listen(addr, opts?)` | Start a gRPC (h2c) server → listener `fd`. `opts.max_connections`. |
| `grpc.stop(fd)` | Stop a listener by fd. |
| `grpc.dispatch(fn)` | Register the inbound RPC handler `fn(stream, path)`. |

### `ServerStream` (server side)

| Method | Description |
| --- | --- |
| `stream:recv()` | Next request message (raw bytes). `nil` = end of request stream; `nil, err` = error. |
| `stream:send(bytes)` | Send one response message (raw protobuf bytes). |
| `stream:finish(code?, message?)` | End the response with a gRPC status (default OK). Idempotent. |
| `<close>` | Has a `__close` metamethod → `finish(OK)` on scope exit (idempotent). |

### `Connection`

| Method | Description |
| --- | --- |
| `conn:unary(path, req_type, req, resp_type, opts?)` | Unary RPC → `reply` or `nil, status`. |
| `conn:server_stream(path, req_type, req, resp_type, opts?)` | Open a server-streaming RPC → `Stream` or `nil, err`. |
| `conn:bidi_stream(path, req_type, resp_type, opts?)` | Open a bidi/client-streaming RPC → `Stream` or `nil, err`. |

`opts`: `{ timeout?: integer (ms), metadata?: table<string,string> }`.

### `Stream`

| Method | Description |
| --- | --- |
| `stream:recv()` | Decode the next response. `nil` (no err) = end-of-stream; `nil, err` = error. |
| `stream:send(req)` | Encode + send one request message (client/bidi only). |
| `stream:close_send()` | Half-close the request stream. |
| `stream:close()` | Close and release native resources. |
| `<close>` | Has a `__close` metamethod → usable as a to-be-closed variable (auto-`close`). |

## Notes & limits

- **Status codes** follow the gRPC spec (`0` = OK, `4` = DEADLINE_EXCEEDED, etc.).
  A client-side `timeout` maps to `4`.
- **Message size** is capped by `LIMITS.max_network_read_bytes` for both encode and
  decode.
- **Concurrency**: many unary calls can be in flight over one channel. A stream
  permits at most one outstanding `recv` at a time (a second returns an error).
- **Generating descriptors**:
  `protoc --include_imports --descriptor_set_out=out.pb -I proto your.proto`.
  The service/method names are *not* read from the descriptor — you pass the
  method path and message type names explicitly.

## Source

- Native module: `crates/moon-runtime/src/modules/lua_grpc.rs`
- Lua wrapper: `lualib/moon/grpc.lua`
- Annotations: `lualib/meta/grpc/core.lua`
- Protocol type: `PTYPE_GRPC = 21` (`context.rs`, `moon.lua`)
- Server e2e test (client↔server, all four RPC kinds): `assets/test/grpc/test_grpc_server.lua`
