local moon = require "moon"
local ws = require "moon.websocket"

local test_addr = "127.0.0.1:19877"

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

local function assert_not_nil(name, value)
    if value ~= nil then
        pass_count = pass_count + 1
    else
        fail_count = fail_count + 1
        print(string.format("FAIL [%s]: expected non-nil", name))
    end
end

local function assert_nil(name, value)
    if value == nil then
        pass_count = pass_count + 1
    else
        fail_count = fail_count + 1
        print(string.format("FAIL [%s]: expected nil, got %s", name, tostring(value)))
    end
end

local server_ready = false
local server_conn_count = 0
local expected_clients = 4

-------------------- Server --------------------

moon.async(function()
    local listener_fd = ws.listen(test_addr)
    assert_not_nil("server listen returns fd", listener_fd)
    print(string.format("Server listening on %s (fd=%d)", test_addr, listener_fd))
    server_ready = true

    for i = 1, expected_clients do
        local conn, err = ws.accept(listener_fd)
        if not conn then
            print(string.format("Server: accept #%d failed: %s", i, tostring(err)))
            goto continue
        end
        server_conn_count = server_conn_count + 1
        print(string.format("Server: accepted #%d (fd=%d, addr=%s)", i, conn.id, conn.addr or "?"))

        moon.async(function()
            while true do
                local data, kind = conn:read(5000)
                if not data then
                    break
                end
                if kind == "b" then
                    conn:write(data)
                else
                    conn:write_text("echo:" .. data)
                end
            end
        end)
        ::continue::
    end
    print("Server: accept loop finished")
end)

-------------------- Client Tests --------------------

moon.async(function()
    while not server_ready do
        moon.sleep(10)
    end
    moon.sleep(50)

    ---- Test 1: Connect and response ----
    print("\n--- Test 1: Connect and handshake response ---")
    local c1 = ws.connect("ws://" .. test_addr, 3000)
    assert_not_nil("connect returns object", c1)
    assert_not_nil("connect has id", c1.id)
    assert_not_nil("connect has response", c1.response)
    assert_eq("handshake status 101", 101, c1.response.status_code)
    assert_not_nil("handshake has headers", c1.response.headers)

    ---- Test 2: Text message echo ----
    print("\n--- Test 2: Text message echo ---")
    c1:write_text("hello")
    local data, kind = c1:read(3000)
    assert_eq("text echo content", "echo:hello", data)
    assert_eq("text echo kind=t", "t", kind)

    ---- Test 3: Binary message echo ----
    print("\n--- Test 3: Binary message echo ---")
    local bin = string.char(0, 1, 2, 128, 255)
    c1:write(bin)
    local data2, kind2 = c1:read(3000)
    assert_eq("binary echo length", #bin, #data2)
    assert_eq("binary echo content", bin, data2)
    assert_eq("binary echo kind=b", "b", kind2)

    ---- Test 4: Multiple rapid messages ----
    print("\n--- Test 4: Multiple rapid messages ---")
    for i = 1, 10 do
        c1:write_text("seq" .. i)
        local d = c1:read(3000)
        assert_eq("rapid msg #" .. i, "echo:seq" .. i, d)
    end

    ---- Test 5: Empty text ----
    print("\n--- Test 5: Empty text ---")
    c1:write_text("")
    local data3 = c1:read(3000)
    assert_eq("empty text echo", "echo:", data3)

    ---- Test 6: Large payload ----
    print("\n--- Test 6: Large payload (64KB) ---")
    local large = string.rep("A", 65536)
    c1:write_text(large)
    local data4 = c1:read(5000)
    assert_eq("large payload echo", "echo:" .. large, data4)

    ---- Test 7: UTF-8 text ----
    print("\n--- Test 7: UTF-8 text ---")
    local utf8_msg = "你好世界🌍émojis"
    c1:write_text(utf8_msg)
    local data5 = c1:read(3000)
    assert_eq("utf8 echo", "echo:" .. utf8_msg, data5)

    ---- Test 8: Close and read error ----
    print("\n--- Test 8: Close and read error ---")
    c1:close()
    moon.sleep(200)

    ---- Test 9: Second client after first closed ----
    print("\n--- Test 9: Second independent client ---")
    local c2 = ws.connect("ws://" .. test_addr, 3000)
    assert_not_nil("c2 connect", c2)
    c2:write_text("from_c2")
    local data6 = c2:read(3000)
    assert_eq("c2 echo", "echo:from_c2", data6)
    c2:close()
    moon.sleep(100)

    ---- Test 10: find_connection ----
    print("\n--- Test 10: find_connection ---")
    local c3 = ws.connect("ws://" .. test_addr, 3000)
    assert_not_nil("c3 connect", c3)
    local found = ws.find_connection(c3.id)
    assert_not_nil("find_connection ok", found)
    assert_eq("find_connection same id", c3.id, found.id)
    found:write_text("via_find")
    local data7 = found:read(3000)
    assert_eq("find_connection echo", "echo:via_find", data7)
    c3:close()
    moon.sleep(100)

    ---- Test 11: find_connection with invalid fd ----
    print("\n--- Test 11: find_connection invalid fd ---")
    local bad = ws.find_connection(999999)
    assert_nil("find invalid fd returns nil", bad.obj)

    ---- Test 12: Read timeout ----
    print("\n--- Test 12: Read timeout ---")
    local c4 = ws.connect("ws://" .. test_addr, 3000)
    assert_not_nil("c4 connect", c4)
    local t_start = moon.clock()
    local data8, err8 = c4:read(500)
    local elapsed = moon.clock() - t_start
    assert_eq("timeout returns false", false, data8)
    assert_true("timeout elapsed >= 0.4s", elapsed >= 0.4)
    assert_true("timeout error message", type(err8) == "string" and err8:find("timeout"))
    c4:close()
    moon.sleep(100)

    ---- Summary ----
    assert_eq("server accepted all clients", expected_clients, server_conn_count)

    print(string.format("\n========================================"))
    print(string.format("  WebSocket Tests: %d passed, %d failed", pass_count, fail_count))
    print(string.format("========================================"))
    if fail_count > 0 then
        print("SOME TESTS FAILED!")
    else
        print("ALL TESTS PASSED!")
    end

    moon.exit(fail_count)
end)
