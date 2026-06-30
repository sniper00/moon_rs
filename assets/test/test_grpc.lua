---
--- test_grpc.lua — smoke test for the native gRPC client plumbing.
---
--- Does NOT require a running gRPC server: it exercises the connect-failure
--- path (session round-trip + PTYPE_GRPC decoder) and the module surface.
--- For a full end-to-end run see assets/example/example_grpc.lua.
---
--- Run:  moon_rs assets/test/test_grpc.lua
---

local moon = require("moon")
local grpc = require("moon.grpc")

local function assert_eq(a, b, msg)
    if a ~= b then
        error(string.format("assert failed: %s (%s ~= %s)", msg or "", tostring(a), tostring(b)))
    end
end

moon.async(function()
    print("=== grpc smoke test ===")

    -- 1. Connecting to an unreachable endpoint must fail gracefully (no hang,
    --    no crash) and surface an error string.
    local conn, err = grpc.connect({
        endpoint        = "http://127.0.0.1:1",  -- nothing listening
        name            = "unreachable",
        connect_timeout = 500,
    })
    assert_eq(conn, nil, "connect to dead endpoint should return nil")
    assert(type(err) == "string", "error should be a string")
    print("connect failure handled:", err)

    -- 2. find_connection on a missing name returns nil.
    assert_eq(grpc.find_connection("nope"), nil, "missing connection -> nil")

    -- 3. stats() is shaped correctly.
    local s = grpc.stats()
    assert(type(s.connections) == "number", "stats.connections is a number")
    assert(type(s.streams) == "number", "stats.streams is a number")
    print(string.format("stats: connections=%d streams=%d", s.connections, s.streams))

    -- 4. close() on an unknown name is a no-op (returns true).
    assert_eq(grpc.close("nope"), true, "close unknown -> true")

    print("=== grpc smoke test passed ===")
    moon.exit(0)
end)

moon.shutdown(function()
    moon.quit()
end)
