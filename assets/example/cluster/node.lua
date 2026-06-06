---
--- Cluster node example.
--- Run two instances of this script with different node IDs:
---   Process 1 (etc server): moon_rs assets/example/cluster/cluster_etc.lua
---   Process 2 (node 1, sender): moon_rs assets/example/cluster/node.lua 1
---   Process 3 (node 2, receiver): moon_rs assets/example/cluster/node.lua 2
---
--- Node 2 listens for cluster connections and handles RPC.
--- Node 1 sends messages and calls to node 2.
---

local moon = require("moon")
local cluster = require("moon.cluster")

local args = moon.args()
local NODE_ID = math.tointeger(tonumber(args[1]))

if not NODE_ID then
    print("Usage: moon_rs assets/example/cluster/node.lua <node_id>")
    print("  node_id=1: sender node")
    print("  node_id=2: receiver node")
    moon.exit(-1)
    return
end

local CLUSTER_ETC_URL = "http://127.0.0.1:9090/cluster?node={}"

local function run_sender()
    local counter = 0

    moon.async(function()
        while true do
            moon.sleep(1000)
            if counter > 0 then
                print(string.format("calls/sec: %d", counter))
                counter = 0
            end
        end
    end)

    moon.async(function()
        moon.sleep(500)

        print("=== Testing cluster.call to node 2 ===")
        local result = cluster.call(2, "bootstrap", "ACCUM", 1, 2, 3, 4, 5)
        print("cluster.call ACCUM result:", result)

        print("=== Sending 1000 messages to node 2 ===")
        for i = 1, 1000 do
            cluster.send(2, "bootstrap", "COUNTER", i)
        end
        print("=== 1000 messages sent ===")

        print("=== Starting continuous call benchmark ===")

        moon.async(function()
            local i = 0
            while true do
                local ret, err = cluster.call(2, "bootstrap", "ACCUM", 1, 2, 3, 4, 5, 6, 7, 8)
                if not ret then
                    print("call failed1:", err)
                    moon.sleep(1000)
                end
                counter = counter + 1
                i = i + 1
                if i % 10000 == 0 then
                    print("1 xxxx")
                end
            end
        end)

        moon.async(function()
        local i = 0
        while true do
            local ret, err = cluster.call(2, "bootstrap", "ACCUM", 1, 2, 3, 4, 5, 6, 7, 8)
            if not ret then
                print("call failed2:", err)
                moon.sleep(1000)
            end
            counter = counter + 1
            i = i + 1
            if i % 10000 == 0 then
                print("2 xxxx")
            end
        end
        end)

        while true do
            local i = 0
            while true do
                local ret, err = cluster.call(2, "bootstrap", "ACCUM", 1, 2, 3, 4, 5, 6, 7, 8)
                if not ret then
                    print("call failed3:", err)
                    moon.sleep(1000)
                end
                counter = counter + 1
                i = i + 1
                if i % 10000 == 0 then
                    print("3 xxxx")
                end
            end
        end
    end)
end

local function run_receiver()
    local command = {}

    command.ACCUM = function(...)
        local numbers = { ... }
        local total = 0
        for _, v in pairs(numbers) do
            total = total + v
        end
        return total
    end

    local count = 0
    command.COUNTER = function(...)
        count = count + 1
        if count % 1000 == 0 then
            print(string.format("received %d messages", count))
        end
    end

    moon.dispatch("lua", function(sender, session, CMD, ...)
        local f = command[CMD]
        if f then
            if session ~= 0 then
                moon.response("lua", sender, session, f(...))
            else
                f(...)
            end
        else
            moon.error(string.format("Unknown command: %s", tostring(CMD)))
            if session ~= 0 then
                moon.response("lua", sender, session, false, "unknown command")
            end
        end
    end)

    print("Receiver ready, waiting for messages...")
end

moon.async(function()
    cluster.init(NODE_ID, CLUSTER_ETC_URL)

    if NODE_ID == 2 then
        cluster.listen()
        print(string.format("Node %d: cluster listening", NODE_ID))
        run_receiver()
    elseif NODE_ID == 1 then
        print(string.format("Node %d: sender mode", NODE_ID))
        run_sender()
    else
        print("Unknown node role for node_id=" .. NODE_ID)
    end
end)

moon.shutdown(function()
    print("Node " .. NODE_ID .. " shutting down")
    moon.quit()
end)
