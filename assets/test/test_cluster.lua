---
--- Test cluster module - same-node shortcut test.
--- This test verifies that cluster.call/cluster.send work correctly
--- when targeting services on the same node (local shortcut path).
---
--- For cross-node testing, use assets/example/cluster/ with separate processes.
---
--- Usage: moon_rs assets/test/test_cluster.lua
---

local moon = require "moon"
local cluster = require "moon.cluster"

local conf = ...

if not conf or not conf.name then
    moon.async(function()
        local id = moon.new_service {
            name = "echo",
            source = "test_cluster.lua",
            unique = true,
        }
        assert(id > 0, "failed to create echo service")
        print("echo service started, id:", string.format("0x%08X", id))

        moon.sleep(100)

        -- Initialize cluster (node 1, dummy URL since we only test local)
        cluster.init(1, "http://127.0.0.1:9999/cluster?node={}")

        -- Test cluster.send (same node shortcut)
        print("=== Test cluster.send (same node) ===")
        cluster.send(1, "echo", "ping_send", 123)
        moon.sleep(100)
        print("cluster.send OK (fire-and-forget)")

        -- Test cluster.call (same node shortcut)
        print("=== Test cluster.call (same node) ===")
        local r1, r2, r3 = cluster.call(1, "echo", "echo", "hello", 42)
        print("cluster.call result:", r1, r2, r3)
        assert(r1 == "echo_reply", "expected echo_reply, got: " .. tostring(r1))
        assert(r2 == "hello", "expected hello, got: " .. tostring(r2))
        assert(r3 == 42, "expected 42, got: " .. tostring(r3))
        print("cluster.call OK")

        -- Test cluster.call with math
        print("=== Test cluster.call ACCUM ===")
        local sum = cluster.call(1, "echo", "accum", 1, 2, 3, 4, 5)
        print("cluster.call accum(1..5) =", sum)
        assert(sum == 15, "expected 15, got: " .. tostring(sum))
        print("cluster.call ACCUM OK")

        -- Test error case: unknown service
        print("=== Test cluster.call unknown service ===")
        local ok, err = pcall(cluster.call, 1, "nonexistent", "hello")
        assert(not ok, "expected error for nonexistent service")
        print("Error (expected):", err)
        print("cluster.call unknown service error OK")

        print("\n=============================")
        print("All cluster tests PASSED!")
        print("=============================")

        moon.exit(0)
    end)

elseif conf.name == "echo" then
    moon.dispatch("lua", function(sender, session, cmd, ...)
        if cmd == "echo" then
            moon.response("lua", sender, session, "echo_reply", ...)
        elseif cmd == "accum" then
            local nums = { ... }
            local total = 0
            for _, v in ipairs(nums) do
                total = total + v
            end
            moon.response("lua", sender, session, total)
        elseif cmd == "ping_send" then
            print("echo received ping_send:", ...)
        else
            moon.response("lua", sender, session, false, "unknown: " .. tostring(cmd))
        end
    end)
end
