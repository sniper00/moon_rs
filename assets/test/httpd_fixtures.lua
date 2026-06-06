--- Shared demo/test routes for moon.httpd.
--- Used by example, test, and benchmark scripts to avoid duplicating handlers.

local M = {}

M.addr = "127.0.0.1:19878"

---@param req httpd.Request
---@return integer?, table<string,string>?, string?
function M.handler(req)
    if req.path == "/hello" then
        return 200, {["content-type"] = "text/plain"}, "Hello World!"
    elseif req.path == "/echo" then
        return 200, {["content-type"] = "application/octet-stream"}, req.body
    elseif req.path == "/method" then
        return 200, {["content-type"] = "text/plain"}, req.method
    elseif req.path == "/headers" then
        local val = req.headers["x-custom"] or ""
        return 200, {["x-echo"] = val, ["content-type"] = "text/plain"}, val
    elseif req.path == "/query" then
        return 200, {["content-type"] = "text/plain"}, req.query_string
    elseif req.path == "/status/201" then
        return 201, {["content-type"] = "text/plain"}, "Created"
    elseif req.path == "/status/404" then
        return 404, {["content-type"] = "text/plain"}, "Not Found"
    elseif req.path == "/large" then
        return 200, {["content-type"] = "text/plain"}, string.rep("X", 100000)
    elseif req.path == "/empty" then
        return 204, {}, ""
    else
        return 404, {["content-type"] = "text/plain"}, "Unknown"
    end
end

return M
