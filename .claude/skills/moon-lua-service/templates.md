# moon_rs Lua service templates

Copy-paste skeletons. Replace `worker`/`svc_name`/`cmd` names as needed.

## 1. Single-role service (its own file)

```lua
local moon = require "moon"

-- If this service is created with `unique = true`, keep this handler.
moon.shutdown(function()
    moon.quit()
end)

moon.dispatch("lua", function(sender, session, cmd, ...)
    if cmd == "ping" then
        moon.response("lua", sender, session, "pong")
    elseif cmd == "add" then
        local a, b = ...
        moon.response("lua", sender, session, a + b)
    else
        moon.response("lua", sender, session, false, "unknown cmd: " .. tostring(cmd))
    end
end)
```

## 2. Multi-role file (bootstrap + sub-service)

```lua
local moon = require "moon"
local conf = ...

if conf and conf.name == "worker" then
    moon.shutdown(function() moon.quit() end)   -- only needed if unique
    moon.dispatch("lua", function(sender, session, cmd, ...)
        if cmd == "work" then
            moon.response("lua", sender, session, "done", ...)
        end
    end)
    return
end

-- bootstrap role
moon.async(function()
    local worker = moon.new_service {
        name = "worker",
        source = "this_file.lua",   -- the same file path
        unique = true,
    }
    assert(worker > 0, "failed to start worker")

    local r1, r2 = moon.call("lua", worker, "work", 42)
    print("result:", r1, r2)

    moon.exit(0)
end)
```

## 3. Concurrent requests, joined on a counter

```lua
moon.async(function()
    local n, done = 20, 0
    local results = {}
    for i = 1, n do
        moon.async(function()
            results[i] = moon.call("lua", target, "compute", i)
            done = done + 1
        end)
    end
    while done < n do
        moon.sleep(10)
    end
    -- all results ready here
end)
```

## 4. Periodic timer (non-yielding callback)

```lua
local function tick()
    -- ... periodic work ...
    moon.timeout(1000, tick)   -- reschedule
end
moon.timeout(1000, tick)
```

For yielding work on an interval, use a coroutine + sleep loop instead:

```lua
moon.async(function()
    while running do
        do_async_work()       -- may moon.call / moon.wait
        moon.sleep(1000)
    end
end)
```

## 5. Cluster node (request + respond across nodes)

```lua
local moon = require "moon"
local cluster = require "moon.cluster"
local conf = ...

if conf and conf.name == "svc" then
    moon.shutdown(function() moon.quit() end)
    moon.dispatch("lua", function(sender, session, cmd, ...)
        moon.response("lua", sender, session, "reply:" .. tostring(cmd), ...)
    end)
    return
end

moon.async(function()
    local NODE = 1
    cluster.init(NODE, "http://127.0.0.1:9090/cluster?node={}")
    cluster.listen()

    moon.new_service { name = "svc", source = "this_file.lua", unique = true }
    moon.sleep(200)

    local r = cluster.call(2, "svc", "hello")   -- to remote node 2
    if r == false then
        moon.error("cluster call failed")
    end

    cluster.send(2, "svc", "notify", 123)       -- fire-and-forget
end)
```

## 6. Custom system command (control-plane message)

```lua
moon.system("reload", function(sender, ...)
    -- handle out-of-band control message addressed to this service
end)
```
