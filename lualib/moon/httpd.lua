local moon = require "moon"
local c = require "httpd.core"

local protocol_type = moon.PTYPE_HTTP_SRV

moon.register_protocol {
    name = "httpsrv",
    PTYPE = protocol_type,
    pack = function(...) return ... end,
    unpack = function(val)
        return c.decode(val)
    end
}

local M = {}

---@param addr string Listen address e.g. "0.0.0.0:8080"
---@return integer listener_fd
function M.listen(addr)
    return c.listen(addr)
end

---@param fn fun(req: table): integer?, table?, string?
function M.dispatch(fn)
    moon.dispatch("httpsrv", function(sender, session, req, handle)
        local ok, status, headers, body = pcall(fn, req)
        if ok then
            c.response(handle, status or 200, headers or {}, body or "")
        else
            c.response(handle, 500, {}, tostring(status))
        end
    end)
end

---@param fd integer
---@return boolean
function M.close(fd)
    return c.close(fd)
end

return M
