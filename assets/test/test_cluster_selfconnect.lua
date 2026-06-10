---
--- Single-process cluster test that exercises the REAL TCP path (not the
--- same-node shortcut in test_cluster.lua).
---
--- Trick: the node runs its own discovery HTTP server and maps BOTH its own
--- node id (SELF_NODE) and a "peer" id (PEER_NODE) to the SAME listen address.
--- Calling PEER_NODE then satisfies `to_node ~= NODE`, so cluster.call/send go
--- through `core.request`/`core.send` and the node connects to ITSELF over a
--- real TCP socket: initiator side -> accept side -> local service -> RESP back
--- over the same bidirectional connection. This drives read_task / write_task /
--- dispatch_frame / on_connection_closed for real, in one process, no second
--- node and no external discovery service.
---
--- This is a MANUAL test (spins up real sockets, ~1s); it is not part of CI.
---
--- Usage: cargo run --release assets/test/test_cluster_selfconnect.lua
---

local moon = require "moon"
local cluster = require "moon.cluster"

local conf = ...

local SELF_NODE = 1
local PEER_NODE = 2 -- resolves to our own cluster address -> self-connect
local DISCOVERY_PORT = 19090
local CLUSTER_PORT = 19091
local CLUSTER_ADDR = "127.0.0.1:" .. CLUSTER_PORT

-- ---------------------------------------------------------------------------
-- echo service: handles cluster CALL/SEND dispatched to it locally.
-- ---------------------------------------------------------------------------
if conf and conf.name == "echo" then
    -- A unique service is NOT auto-quit by the default PTYPE_SHUTDOWN handler
    -- (see lualib/moon.lua), so register one explicitly; otherwise the process
    -- never reaches actor_counter == 0 and hangs after moon.exit.
    moon.shutdown(function()
        moon.quit()
    end)
    moon.dispatch("lua", function(sender, session, cmd, ...)
        if cmd == "echo" then
            moon.response("lua", sender, session, "echo_reply", ...)
        elseif cmd == "accum" then
            local total = 0
            for _, v in ipairs({ ... }) do
                total = total + v
            end
            moon.response("lua", sender, session, total)
        elseif cmd == "ping_send" then
            print("[echo] got ping_send:", ...)
        else
            moon.response("lua", sender, session, false, "unknown cmd: " .. tostring(cmd))
        end
    end)
    return
end

-- ---------------------------------------------------------------------------
-- bootstrap: discovery server + cluster setup + the actual test sequence.
-- ---------------------------------------------------------------------------
local httpserver = require "moon.http.server"

local node_addr = {
    [SELF_NODE] = CLUSTER_ADDR,
    [PEER_NODE] = CLUSTER_ADDR, -- the self-connect mapping
}

httpserver.on("/cluster", function(request, response)
    local query = request:query()
    local node = tonumber(query.node)
    local addr = node_addr[node]
    if not addr then
        response.status_code = 404
        response:write_header("Content-Type", "text/plain")
        response:write("cluster node not found " .. tostring(query.node))
        return
    end
    response.status_code = 200
    response:write_header("Content-Type", "text/plain")
    response:write(addr)
end)

local function expect(cond, msg)
    if not cond then
        print("FAILED: " .. msg)
        moon.exit(1)
        error(msg)
    end
end

moon.async(function()
    httpserver.listen("127.0.0.1:" .. DISCOVERY_PORT)
    print("[discovery] listening on 127.0.0.1:" .. DISCOVERY_PORT)

    local echo_id = moon.new_service {
        name = "echo",
        source = "test_cluster_selfconnect.lua",
        unique = true,
    }
    expect(echo_id > 0, "failed to create echo service")
    print("[echo] started id:", string.format("0x%08X", echo_id))

    cluster.init(SELF_NODE, "http://127.0.0.1:" .. DISCOVERY_PORT .. "/cluster?node={}")
    cluster.listen()
    moon.sleep(500) -- let the listener bind (discovery + bind are async)

    print("=== cluster.call over real TCP (self-connect via PEER_NODE) ===")
    local r1, r2, r3 = cluster.call(PEER_NODE, "echo", "echo", "hello", 42)
    expect(r1 == "echo_reply" and r2 == "hello" and r3 == 42,
        "echo mismatch: " .. tostring(r1) .. "," .. tostring(r2) .. "," .. tostring(r3))
    print("cluster.call echo OK:", r1, r2, r3)

    local sum = cluster.call(PEER_NODE, "echo", "accum", 1, 2, 3, 4, 5)
    expect(sum == 15, "accum mismatch: " .. tostring(sum))
    print("cluster.call accum OK:", sum)

    -- Many concurrent in-flight calls: stresses the (from_addr, session)
    -- outbound bookkeeping and RESP dispatch over the single connection.
    print("=== concurrent cluster.call (20 in flight) ===")
    local n = 20
    local done = 0
    local results = {}
    for i = 1, n do
        moon.async(function()
            results[i] = cluster.call(PEER_NODE, "echo", "accum", i, i)
            done = done + 1
        end)
    end
    while done < n do
        moon.sleep(10)
    end
    for i = 1, n do
        expect(results[i] == i * 2, "concurrent mismatch at " .. i .. ": " .. tostring(results[i]))
    end
    print("concurrent calls OK (", n, "responses )")

    -- Error path: remote service does not exist. The remote replies an error
    -- frame seri-packed as `false, msg`, so the call RETURNS (false, msg) rather
    -- than raising; the waiting coroutine is still released exactly once.
    print("=== cluster.call unknown remote service ===")
    local okflag, errmsg = cluster.call(PEER_NODE, "nonexistent", "hello")
    expect(okflag == false, "expected false for nonexistent remote service, got: " .. tostring(okflag))
    expect(type(errmsg) == "string" and errmsg:find("not found"),
        "expected 'not found' error message, got: " .. tostring(errmsg))
    print("expected error returned:", okflag, errmsg)

    -- Fire-and-forget over real TCP.
    print("=== cluster.send over real TCP ===")
    cluster.send(PEER_NODE, "echo", "ping_send", 777)
    moon.sleep(200)

    print("\n=================================")
    print("All SELF-CONNECT cluster tests PASSED!")
    print("=================================")

    moon.exit(0)
end)
