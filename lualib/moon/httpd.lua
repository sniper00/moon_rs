local moon = require "moon"
local c = require "httpd.core"

local protocol_type = moon.PTYPE_HTTPD

moon.register_protocol {
    name = "httpd",
    PTYPE = protocol_type,
    pack = function(...) return ... end,
}

---@class httpd
local M = {}

---@class httpd.Request
---@field method string HTTP method, e.g. "GET", "POST"
---@field path string Request path, e.g. "/api/users"
---@field query_string string Raw query string without '?', e.g. "foo=bar&a=1"
---@field headers table<string, string> Request headers (lowercase keys)
---@field body string Request body (raw bytes)

---@class httpd.ListenOptions
---@field max_body_size? integer Max request body in bytes (default 10MB)
---@field max_connections? integer Max concurrent connections (default 100000)
---@field static_dir? string Directory path for serving static files. GET/HEAD requests matching files under this directory are served directly without dispatching to the Lua handler. Supports index.html for directory requests. Path traversal is blocked.

---@param addr string Listen address e.g. "0.0.0.0:8080"
---@param opts? httpd.ListenOptions
---@return integer listener_fd
function M.listen(addr, opts)
    return c.listen(addr, opts)
end

---Register a request handler. The handler receives an `httpd.Request` and
---returns an optional status code, headers table, and body string.
---@param fn fun(req: httpd.Request): integer?, table<string,string>?, string?
function M.dispatch(fn)
    moon.dispatch("httpd", function(sender, session, req, handle)
        local ok, status, headers, body = pcall(fn, req)
        if ok then
            ---@cast status integer?
            c.response(handle, status, headers, body)
        else
            c.response(handle, 500, {}, tostring(status))
        end
    end)
end

---Close a listener by its fd.
---@param fd integer The listener fd returned by `listen`
---@return boolean success
function M.close(fd)
    return c.close(fd)
end

return M
