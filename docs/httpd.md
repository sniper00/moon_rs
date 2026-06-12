# HTTP Server Module (`lua_httpd`)

High-performance HTTP/1.1 server built on `hyper`, with static file serving and Lua request handlers.

## Architecture

```
┌─────────────────────────────────────────────────────────────┐
│ IO Runtime (tokio + hyper)                                   │
│                                                             │
│  TcpListener → accept connections                           │
│    → hyper http1::Connection                                │
│    → parse HTTP request                                     │
│    → route to Lua handler OR serve static file              │
│                                                             │
│  Lua handler path:                                          │
│    → send request to actor via oneshot channel              │
│    → actor processes, returns response                      │
│    → hyper sends response to client                         │
│                                                             │
│  Static file path (direct, no Lua):                         │
│    → file < 1MB: cached in memory (5s TTL)                  │
│    → file > 1MB: streamed from disk                         │
└─────────────────────────────────────────────────────────────┘
```

## Lua API

```lua
local httpserver = require("moon.http.server")

-- Route handlers
httpserver.on("/api/hello", function(request, response)
    local name = request:query().name or "world"
    response:write_header("Content-Type", "application/json")
    response:write('{"msg":"hello ' .. name .. '"}')
end)

httpserver.on("/api/users", function(request, response)
    if request.method == "POST" then
        local body = request.body
        -- process body...
        response.status_code = 201
        response:write("created")
    else
        response:write("user list")
    end
end)

-- Wildcard catch-all
httpserver.on("*", function(request, response)
    response.status_code = 404
    response:write("not found")
end)

-- Static file serving (loads directory into memory)
httpserver.static("./public", true)  -- dir, show_debug

-- Start listening
httpserver.listen("0.0.0.0:8080", 5000)  -- addr, read_timeout_ms

-- Configuration
httpserver.keepalive = true           -- enable HTTP keep-alive (default true)
httpserver.header_max_len = 8192      -- max header size
httpserver.content_max_len = false    -- max body size (false = no limit)
```

## Request Object

```lua
---@class HttpRequest
request.method       -- "GET", "POST", etc.
request.path         -- "/api/hello"
request.body         -- request body string (POST/PUT)
request.headers      -- table of headers (lowercase keys)
request:query()      -- parsed query parameters table
```

## Response Object

```lua
---@class HttpResponse
response.status_code = 200                    -- HTTP status code
response:write_header("key", "value")         -- add response header
response:write("body content")                -- set response body
response:collect()                            -- internal: gather parts for writing
```

## Static File Serving

```lua
httpserver.static("./public")
```

- Recursively scans directory, loads files into memory.
- Serves based on path match (e.g. `/css/style.css` → `./public/css/style.css`).
- Auto-detects MIME types from file extension.
- `index.html` served for directory paths.

### Supported MIME Types

`.txt`, `.html`, `.css`, `.js`, `.json`, `.xml`, `.jpg`, `.png`, `.gif`, `.svg`, `.mp3`, `.mp4`, `.pdf`, `.zip`, `.woff`, `.woff2`, `.ttf`, `.otf`

## Rust-Level Features (lua_httpd.rs)

The Rust-level `httpd` module (used internally) provides:

- **hyper-based HTTP/1.1** server with connection-level concurrency.
- **Semaphore-based connection limiting** (default 10,000 max connections).
- **Body size limiting** (default 10 MB).
- **File streaming** for large files (>1 MB threshold).
- **In-memory file cache** with 5-second TTL and LRU eviction (max 10,000 entries).
- **Range request support** for streaming responses.

## Error Handling

- Handler exceptions: caught by `xpcall`, logged, returns 500.
- No matching route: returns 404.
- Invalid request: returns 400 and closes connection.
- Custom error handler: `httpserver.error = function(fd, err) ... end`

## Files

| Path | Role |
|------|------|
| `crates/moon-runtime/src/modules/lua_httpd.rs` | Rust hyper server (~700 lines) |
| `lualib/moon/http/server.lua` | Lua HTTP server with routing |
| `lualib/moon/http/internal.lua` | HTTP parsing helpers |
| `assets/test/httpd_fixtures.lua` | Shared demo/test routes for `moon.httpd` |
| `assets/example/example_httpd.lua` | Usage examples |
| `assets/test/test_httpd.lua` | HTTP server tests |
| `assets/benchmark/benchmark_httpd.lua` | HTTP benchmark |
