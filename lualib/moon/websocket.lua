local moon = require "moon"
local c = require "ws.core"

local protocol_type = moon.PTYPE_WEBSOCKET

moon.register_protocol {
    name = "websocket",
    PTYPE = protocol_type,
    pack = function(...) return ... end,
}

---@class websocket.Options
---@field max_message_size? integer Max WebSocket message size in bytes (default 64MB)
---@field max_frame_size? integer Max WebSocket frame size in bytes (default 16MB)
---@field write_buffer_size? integer Target outbound write buffer size in bytes (default 128KB)
---@field max_write_buffer_size? integer Hard cap on the outbound write buffer in bytes; bounds memory when a peer reads slowly (default: max_message_size + write_buffer_size, instead of unbounded)
---@field max_connections? integer (listen only) Max concurrently accepted connections (default 100000)
---@field origins? string[] (listen only) Allow-list of exact `Origin` header values; when set, handshakes with a missing or unlisted Origin are rejected (prevents cross-site WebSocket hijacking). Omit to disable the check (non-browser/trusted clients).

---@class websocket.ConnectResponse
---@field fd integer Connection file descriptor
---@field version string HTTP version, e.g. "HTTP/1.1"
---@field status_code integer HTTP status code
---@field headers table<string, string> Response headers
---@field body? string Response body

---@class Websocket
---@field obj any
---@field response? websocket.ConnectResponse
---@field id integer
---@field addr? string
local M = {}

---@async
---@nodiscard
---@param url string WebSocket url e.g. "wss://example.com/socket"
---@param timeout? integer Connect timeout in milliseconds. Default 5000ms
---@param opts? websocket.Options
---@return Websocket
function M.connect(url, timeout, opts)
    local t = {
        url = url,
    }
    if timeout ~= nil then
        t.connect_timeout = timeout
    end
    if opts then
        t.max_message_size = opts.max_message_size
        t.max_frame_size = opts.max_frame_size
        t.write_buffer_size = opts.write_buffer_size
        t.max_write_buffer_size = opts.max_write_buffer_size
    end

    local response, err = moon.wait(c.connect(t))
    if not response then
        error(string.format("websocket connect failed: %s", err))
    end

    local o = {
        obj = c.find_connection(response.fd),
        id = response.fd,
        response = response
    }
    return setmetatable(o, { __index = M })
end

---@param addr string Listen address e.g. "0.0.0.0:8080"
---@param opts? websocket.Options
---@return integer listener_fd
function M.listen(addr, opts)
    return c.listen(addr, opts)
end

---@async
---@nodiscard
---@param listener_fd integer The listener fd from listen()
---@return Websocket?
---@return string? errmsg
function M.accept(listener_fd)
    local response, err = moon.wait(c.accept(listener_fd))
    if not response then
        return nil, err
    end
    local o = {
        obj = c.find_connection(response.fd),
        id = response.fd,
        addr = response.addr,
    }
    return setmetatable(o, { __index = M })
end

---@nodiscard
---@param id integer Connection id
---@return Websocket
function M.find_connection(id)
    local o = {
        obj = c.find_connection(id),
        id = id,
    }
    return setmetatable(o, { __index = M })
end

---@nodiscard
---@async
---@param timeout? integer Timeout in milliseconds. Default 5000ms
---@return string data, string kind  kind: "t"=text, "b"=binary, "p"=ping, "o"=pong, "c"=close
function M:read(timeout)
    return moon.wait(self.obj:read(timeout))
end

---@param data string|lightuserdata Binary data
function M:write(data)
    return self.obj:write("b", data)
end

---@param data string|lightuserdata Text data
function M:write_text(data)
    return self.obj:write("t", data)
end

---@param data string|lightuserdata Ping data
function M:write_ping(data)
    return self.obj:write("p", data)
end

function M:close()
    self.obj:close()
end

return M
