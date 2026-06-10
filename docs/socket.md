# Socket Module (`lua_socket`)

Low-level async TCP socket API for custom protocol implementations.

## Architecture

```
┌─────────────────────────────────────────────────────────────┐
│ Lua Actor Thread                                            │
│                                                             │
│  socket.connect("host:port")                                │
│  socket.read(fd, "\r\n")  — coroutine yields               │
│  socket.write(fd, data)   — non-blocking                    │
│  socket.listen(addr, on_accept)                             │
│                                                             │
│  Dispatched via PTYPE_SOCKET_TCP / PTYPE_SOCKET_EVENT       │
│    → moon.core.decode_message(m) on actor thread              │
└─────────────┬───────────────────────────────────────────────┘
              │ NetOp commands via mpsc
              ▼
┌─────────────────────────────────────────────────────────────┐
│ IO Runtime (tokio)                                           │
│                                                             │
│  Per-connection pair:                                        │
│    read_task:  BufReader → read delimiter/bytes/frame        │
│    write_task: receives Write ops, writes to OwnedWriteHalf │
│                                                             │
│  Event delivery: PTYPE_SOCKET_EVENT (accept/message/close)  │
└─────────────────────────────────────────────────────────────┘
```

## Lua API

```lua
local socket = require("moon.socket")

-- TCP Client
local fd, err = socket.connect("127.0.0.1:6379", 5000)  -- addr, timeout_ms

-- Read modes
local line = socket.read(fd, "\r\n")          -- read until delimiter
local data = socket.read(fd, 1024)            -- read exact N bytes
local line = socket.read(fd, "\n", 4096)      -- read until delimiter, max bytes
local data = socket.read(fd, 100, 5000)       -- read N bytes, timeout_ms

-- Frame protocol (2-byte length prefix with chunking)
local buf = socket.read_frame(fd, 5000)       -- read one framed message

-- Write
socket.write(fd, "PING\r\n")                  -- write raw bytes
socket.write(fd, data, nil, true)             -- write + close after
socket.write_frame(fd, data)                  -- write with frame header

-- Close
socket.close(fd)

-- TCP Server
local listen_fd = socket.listen("0.0.0.0:8080", function(conn_fd, remote_addr)
    -- handle new connection
    local line = socket.read(conn_fd, "\r\n")
    socket.write(conn_fd, "OK\r\n")
    socket.close(conn_fd)
end)

-- Callback-based frame reading (high throughput mode)
socket.start_read_frame(fd, 5000)  -- timeout_ms
socket.on("message", function(fd, buffer_ptr)
    -- process framed message
    -- return true to take ownership of buffer_ptr
    return true
end)
socket.on("close", function(fd, remote_addr, err)
    -- connection closed
end)

-- Utility
local ip = socket.host()           -- get local IP
socket.unlink(fd)                   -- release fd from tracking (ownership transfer)
```

## Read Modes

| Mode | Call | Description |
|------|------|-------------|
| Delimiter | `read(fd, "\r\n")` | Read until delimiter found |
| Delimiter + max | `read(fd, "\r\n", 4096)` | Read until delimiter, max N bytes |
| Exact bytes | `read(fd, 1024)` | Read exactly N bytes |
| Exact + timeout | `read(fd, 1024, 5000)` | Read N bytes with timeout |
| Frame | `read_frame(fd, 5000)` | Read one length-prefixed frame |

## Frame Protocol

The built-in frame protocol uses a 2-byte big-endian length prefix with chunking:
- `length < 0xFFFF`: final chunk of `length` bytes.
- `length == 0xFFFF`: continuation chunk (max 65534 bytes), more data follows.
- Multiple chunks concatenated form one logical message.
- `write_frame(fd, data)` handles framing automatically.

## Event System

Two modes of operation:

### Coroutine Mode (pull)
```lua
local data = socket.read(fd, "\r\n")  -- yields coroutine
```

### Callback Mode (push)
```lua
socket.start_read_frame(fd)
socket.on("message", function(fd, buf_ptr)
    -- high-throughput, no coroutine overhead
    return true  -- take ownership of buffer
end)
```

## Connection Lifecycle

1. `socket.connect(addr)` → returns `fd` (tracked in pool).
2. Read/Write operations via `fd`.
3. `socket.close(fd)` → closes and removes from tracking.
4. On GC: all tracked fds are auto-closed.

## Error Handling

All async operations return `false, error_message` on failure:
```lua
local fd, err = socket.connect("bad:host")
if not fd then
    print("connect failed:", err)
end

local data, err = socket.read(fd, 1024, 5000)
if not data then
    print("read failed:", err)  -- timeout or connection closed
end
```

## Files

| Path | Role |
|------|------|
| `crates/moon-modules/src/lua_socket.rs` | Rust implementation (~820 lines) |
| `lualib/moon/socket.lua` | Lua wrapper with event dispatch |
| `assets/test/test_socket.lua` | Socket tests |
| `assets/test/test_socket_frame.lua` | Frame protocol tests |
| `assets/benchmark/benchmark_socket.lua` | Socket benchmark |
| `assets/benchmark/benchmark_socket_frame.lua` | Frame protocol benchmark |
