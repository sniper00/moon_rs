---
--- example_httpd.lua — moon.httpd usage demo with shared fixture routes.
---
--- Run: moon_rs assets/example/example_httpd.lua
---

local httpserver = require "moon.httpd"
local fixtures = dofile("../test/httpd_fixtures.lua")

local listener_fd = httpserver.listen(fixtures.addr, { static_dir = "./" })
print(string.format("HTTP server listening on %s (fd=%d)", fixtures.addr, listener_fd))

httpserver.dispatch(fixtures.handler)
