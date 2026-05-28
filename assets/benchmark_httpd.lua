local moon = require "moon"
local httpserver = require "moon.httpd"

local test_addr = "127.0.0.1:19878"

-------------------- Server Setup --------------------

local listener_fd = httpserver.listen(test_addr)
print(string.format("HTTP server listening on %s (fd=%d)", test_addr, listener_fd))

httpserver.dispatch(function(req)
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
end)


local http_server = require("moon.http.server")

-- http_server.content_max_len = 8192

http_server.error = function(fd, err)
    print("http server fd", fd, " disconnected:", err)
end

http_server.on("/hello", function(request, response)
    response:write_header("Content-Type", "text/plain")
    response:write("GET:Hello World")
end)

http_server.listen("127.0.0.1:19879")



