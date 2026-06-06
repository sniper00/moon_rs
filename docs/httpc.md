# HTTP Client Module (`lua_httpc`)

Async HTTP client built on `reqwest` with connection pooling and proxy support.

## Architecture

```
┌─────────────────────────────────────────────────────────────┐
│ Lua Actor Thread                                            │
│                                                             │
│  httpc.get(url)                                             │
│    → core.request(opts) returns session                     │
│    → moon.wait(session) — coroutine yields                  │
│                                                             │
│  PTYPE_HTTPC message arrives → core.decode(raw)             │
│    → parse status, headers, body into Lua table             │
│    → coroutine resumes with HttpResponse                    │
└─────────────┬───────────────────────────────────────────────┘
              │ Spawned on io_runtime
              ▼
┌─────────────────────────────────────────────────────────────┐
│ IO Runtime (tokio + reqwest)                                 │
│                                                             │
│  reqwest::Client (pooled by timeout+proxy key)              │
│    → TLS via rustls                                         │
│    → TCP_NODELAY enabled                                    │
│    → Connection pool reuse                                  │
│    → Response delivered via PTYPE_HTTPC                     │
└─────────────────────────────────────────────────────────────┘
```

## Lua API

```lua
local httpc = require("moon.http.client")

-- GET request
local res = httpc.get("https://api.example.com/data")
print(res.status_code)  -- 200
print(res.body)         -- response body string

-- GET with options
local res = httpc.get("https://api.example.com/data", {
    headers = { ["Authorization"] = "Bearer token123" },
    timeout = 10000,    -- 10 seconds
    proxy = "http://proxy:8080",
})

-- POST with raw body
local res = httpc.post("https://api.example.com/submit", "raw body data", {
    headers = { ["Content-Type"] = "text/plain" },
})

-- POST JSON (auto-encodes table, auto-decodes response)
local res = httpc.post_json("https://api.example.com/users", {
    name = "Alice",
    age = 30,
})
-- res.body is already a decoded Lua table (if status 200)

-- POST form (url-encoded)
local res = httpc.post_form("https://api.example.com/login", {
    username = "alice",
    password = "secret",
})
```

## Response Object

```lua
---@class HttpResponse
res.status_code    -- integer (200, 404, 500, etc.)
res.body           -- string (response body)
res.headers        -- table<string, string>
res.version        -- "HTTP/1.1", "HTTP/2.0", etc.
```

## Request Options

```lua
---@class HttpRequestOptions
{
    headers = {},       -- table<string, string>
    timeout = 5000,     -- request timeout in milliseconds
    proxy = "",         -- proxy URL (e.g. "http://proxy:8080")
}
```

## Connection Pooling

HTTP clients are cached by `(timeout, proxy)` key:
- Same timeout + proxy combination reuses the same `reqwest::Client`.
- Underlying TCP connections are pooled and reused (HTTP keep-alive).
- TLS sessions cached for HTTPS.

## Features

- **TLS**: via `rustls` (no OpenSSL dependency).
- **HTTP/1.1 and HTTP/2**: automatic protocol negotiation.
- **Proxy support**: HTTP/HTTPS/SOCKS5 via `reqwest::Proxy`.
- **TCP_NODELAY**: enabled for all connections.
- **URL encoding/decoding**: `core.form_urlencode()` / `core.form_urldecode()`.

## Low-Level Core API

```lua
local core = require("httpc.core")

-- Raw request (returns session for moon.wait)
local session = core.request({
    method = "GET",
    url = "http://example.com",
    headers = {},
    body = "",
    timeout = 5000,
    proxy = "",
})
local response = moon.wait(session)

-- Decode raw response (used by protocol handler)
local decoded = core.decode(raw_response)

-- URL encoding utilities
local encoded = core.form_urlencode({ key = "hello world" })
-- "key=hello+world"
local decoded = core.form_urldecode("key=hello+world")
-- { key = "hello world" }

-- HTTP parsing (for low-level use)
local req = core.parse_request(raw_http_request_string)
local res = core.parse_response(raw_http_response_string)
```

## Error Handling

On network/timeout errors, the response still returns but with error status:
```lua
local res = httpc.get("http://unreachable:9999", { timeout = 2000 })
-- res may have status_code = 0 or connection error in body
```

For `post_json`, response body is only auto-decoded when `status_code == 200`.

## Files

| Path | Role |
|------|------|
| `crates/moon-modules/src/lua_httpc.rs` | Rust implementation (~350 lines) |
| `lualib/moon/http/client.lua` | Lua wrapper with convenience methods |
| `assets/test/test_http.lua` | HTTP client tests |
