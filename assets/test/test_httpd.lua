---
--- test_httpd.lua — Functional tests for moon.httpd.
---
--- Run: moon_rs assets/test/test_httpd.lua
---

local moon = require "moon"
local httpserver = require "moon.httpd"
local httpc = require "moon.http.client"
local fixtures = dofile("httpd_fixtures.lua")

local test_addr = fixtures.addr
local base_url = "http://" .. test_addr

local pass_count = 0
local fail_count = 0

local function assert_eq(name, expected, actual)
    if expected == actual then
        pass_count = pass_count + 1
    else
        fail_count = fail_count + 1
        print(string.format("FAIL [%s]: expected=%s(%s), actual=%s(%s)",
            name, tostring(expected), type(expected), tostring(actual), type(actual)))
    end
end

local function assert_true(name, value)
    if value then
        pass_count = pass_count + 1
    else
        fail_count = fail_count + 1
        print(string.format("FAIL [%s]: expected true, got %s", name, tostring(value)))
    end
end

-------------------- Server Setup --------------------

local listener_fd = httpserver.listen(test_addr)
print(string.format("HTTP server listening on %s (fd=%d)", test_addr, listener_fd))

httpserver.dispatch(fixtures.handler)

-------------------- Client Tests --------------------

moon.async(function()
    moon.sleep(100)

    ---- Test 1: Simple GET ----
    print("\n--- Test 1: Simple GET ---")
    local resp = httpc.get(base_url .. "/hello")
    assert_eq("GET /hello status", 200, resp.status_code)
    assert_eq("GET /hello body", "Hello World!", resp.body)
    assert_eq("GET /hello content-type", "text/plain", resp.headers["content-type"])

    ---- Test 2: POST with body echo ----
    print("\n--- Test 2: POST body echo ---")
    local resp2 = httpc.post(base_url .. "/echo", "test body data")
    assert_eq("POST /echo status", 200, resp2.status_code)
    assert_eq("POST /echo body", "test body data", resp2.body)

    ---- Test 3: Method detection ----
    print("\n--- Test 3: Method detection ---")
    local resp3 = httpc.get(base_url .. "/method")
    assert_eq("GET method", "GET", resp3.body)

    local resp3b = httpc.post(base_url .. "/method", "")
    assert_eq("POST method", "POST", resp3b.body)

    ---- Test 4: Custom headers ----
    print("\n--- Test 4: Custom headers ---")
    local resp4 = httpc.get(base_url .. "/headers", {
        headers = {["x-custom"] = "my-value"}
    })
    assert_eq("custom header echo body", "my-value", resp4.body)
    assert_eq("custom header echo header", "my-value", resp4.headers["x-echo"])

    ---- Test 5: Query string ----
    print("\n--- Test 5: Query string ---")
    local resp5 = httpc.get(base_url .. "/query?foo=bar&a=1")
    assert_eq("query string", "foo=bar&a=1", resp5.body)

    ---- Test 6: Status codes ----
    print("\n--- Test 6: Status codes ---")
    local resp6a = httpc.get(base_url .. "/status/201")
    assert_eq("status 201", 201, resp6a.status_code)
    assert_eq("status 201 body", "Created", resp6a.body)

    local resp6b = httpc.get(base_url .. "/status/404")
    assert_eq("status 404", 404, resp6b.status_code)
    assert_eq("status 404 body", "Not Found", resp6b.body)

    ---- Test 7: Unknown route ----
    print("\n--- Test 7: Unknown route ---")
    local resp7 = httpc.get(base_url .. "/nonexistent")
    assert_eq("unknown route 404", 404, resp7.status_code)
    assert_eq("unknown route body", "Unknown", resp7.body)

    ---- Test 8: Large response ----
    print("\n--- Test 8: Large response (100KB) ---")
    local resp8 = httpc.get(base_url .. "/large")
    assert_eq("large status", 200, resp8.status_code)
    assert_eq("large body length", 100000, #resp8.body)

    ---- Test 9: Empty body with 204 ----
    print("\n--- Test 9: Empty body 204 ---")
    local resp9 = httpc.get(base_url .. "/empty")
    assert_eq("204 status", 204, resp9.status_code)

    ---- Test 10: Multiple concurrent requests ----
    print("\n--- Test 10: Multiple concurrent requests ---")
    local done = 0
    local expected = 5
    for i = 1, expected do
        moon.async(function()
            local r = httpc.get(base_url .. "/hello")
            assert_eq("concurrent #" .. i .. " status", 200, r.status_code)
            done = done + 1
        end)
    end
    while done < expected do
        moon.sleep(50)
    end
    assert_eq("all concurrent done", expected, done)

    ---- Test 11: Close listener ----
    print("\n--- Test 11: Close listener ---")
    local ok = httpserver.close(listener_fd)
    assert_true("close returns true", ok)
    moon.sleep(100)

    ---- Summary ----
    print(string.format("\n========================================"))
    print(string.format("  HTTP Server Tests: %d passed, %d failed", pass_count, fail_count))
    print(string.format("========================================"))
    if fail_count > 0 then
        print("SOME TESTS FAILED!")
    else
        print("ALL TESTS PASSED!")
    end

    moon.exit(fail_count)
end)
