---
--- benchmark_httpd.lua — HTTP server demo for moon.httpd and moon.http.server.
---
--- Run: moon_rs assets/benchmark/benchmark_httpd.lua
---

local httpserver = require "moon.httpd"
local fixtures = dofile("../test/httpd_fixtures.lua")

local listener_fd = httpserver.listen(fixtures.addr)
print(string.format("HTTP server listening on %s (fd=%d)", fixtures.addr, listener_fd))

httpserver.dispatch(fixtures.handler)

local http_server = require("moon.http.server")

http_server.error = function(fd, err)
    print("http server fd", fd, " disconnected:", err)
end

http_server.on("/hello", function(request, response)
    response:write_header("Content-Type", "text/plain")
    response:write("GET:Hello World")
end)

http_server.listen("127.0.0.1:19879")
