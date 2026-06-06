---
--- example_redis.lua — Showcase the native Redis driver (redis.core).
---
--- Run:  moon_rs assets/example/example_redis.lua
---

local moon = require("moon")
local redis = require("moon.db.redis")

local HOST = "127.0.0.1"
local PORT = 6379

moon.async(function()
    print("=== Redis Native Driver Example ===\n")

    -----------------------------------------------------------
    -- 1. Connect
    -----------------------------------------------------------
    local db, err = redis.connect({ host = HOST, port = PORT }, "example", 5000, 1)
    if not db then
        print("connect failed:", err)
        moon.exit(-1)
        return
    end
    print("Connected to " .. HOST .. ":" .. PORT)

    db:flushdb()

    -----------------------------------------------------------
    -- 2. Basic SET / GET
    -----------------------------------------------------------
    print("\n--- SET / GET ---")
    print("SET A:", db:set("A", "hello"))
    print("SET B:", db:set("B", "world"))
    print("GET A:", db:get("A"))
    print("GET B:", db:get("B"))

    -----------------------------------------------------------
    -- 3. Integer commands
    -----------------------------------------------------------
    print("\n--- INCR / DECR ---")
    db:set("counter", 0)
    print("INCR:", db:incr("counter"))
    print("INCR:", db:incr("counter"))
    print("INCRBY 10:", db:incrby("counter", 10))
    print("DECR:", db:decr("counter"))
    print("GET counter:", db:get("counter"))

    -----------------------------------------------------------
    -- 4. Hash operations
    -----------------------------------------------------------
    print("\n--- HASH ---")
    db:hset("user:1", "name", "Alice")
    db:hset("user:1", "age", "30")
    db:hmset("user:1", "city", "Beijing", "role", "admin")
    print("HGET name:", db:hget("user:1", "name"))
    print("HGET age:", db:hget("user:1", "age"))
    local vals = db:hvals("user:1")
    io.write("HVALS:")
    for _, v in ipairs(vals) do io.write(" " .. tostring(v)) end
    print()

    -----------------------------------------------------------
    -- 5. List operations
    -----------------------------------------------------------
    print("\n--- LIST ---")
    db:rpush("mylist", "a", "b", "c")
    db:lpush("mylist", "z")
    print("LRANGE:", table.concat(db:lrange("mylist", 0, -1), ", "))
    print("LLEN:", db:llen("mylist"))
    print("LPOP:", db:lpop("mylist"))
    print("RPOP:", db:rpop("mylist"))

    -----------------------------------------------------------
    -- 6. Set operations
    -----------------------------------------------------------
    print("\n--- SET ---")
    db:sadd("myset", "one", "two", "three")
    print("SCARD:", db:scard("myset"))
    print("SISMEMBER one:", db:sismember("myset", "one"))
    print("SISMEMBER four:", db:sismember("myset", "four"))
    local members = db:smembers("myset")
    io.write("SMEMBERS:")
    for _, v in ipairs(members) do io.write(" " .. tostring(v)) end
    print()

    -----------------------------------------------------------
    -- 7. EXISTS / DEL / TTL
    -----------------------------------------------------------
    print("\n--- EXISTS / DEL / TTL ---")
    print("EXISTS A:", db:exists("A"))
    print("EXISTS nosuch:", db:exists("nosuch"))
    db:set("temp", "will expire")
    db:expire("temp", 60)
    print("TTL temp:", db:ttl("temp"))
    db:del("temp")
    print("EXISTS temp after DEL:", db:exists("temp"))

    -----------------------------------------------------------
    -- 8. MULTI / EXEC transaction
    -----------------------------------------------------------
    print("\n--- MULTI / EXEC ---")
    db:multi()
    db:set("tx_a", "1")
    db:set("tx_b", "2")
    db:get("tx_a")
    db:get("tx_b")
    local tx_result = db:exec()
    for i, v in ipairs(tx_result) do
        print(string.format("  tx[%d] = %s", i, tostring(v)))
    end

    -----------------------------------------------------------
    -- 9. Pipeline
    -----------------------------------------------------------
    print("\n--- PIPELINE ---")
    local results = db:pipeline({
        { "SET", "cat",   "Marry" },
        { "SET", "horse", "Bob" },
        { "GET", "cat" },
        { "GET", "horse" },
        { "DEL", "cat",   "horse" },
    })
    for i, v in ipairs(results) do
        print(string.format("  pipe[%d] = %s", i, tostring(v)))
    end

    -----------------------------------------------------------
    -- 10. Fire-and-forget
    -----------------------------------------------------------
    print("\n--- Fire & Forget ---")
    db:execute("SET", "ff_key", "ff_value")
    db:execute_pipeline({
        { "SET", "ff_a", "1" },
        { "SET", "ff_b", "2" },
    })
    print("GET ff_key:", db:get("ff_key"))
    print("GET ff_a:", db:get("ff_a"))
    print("GET ff_b:", db:get("ff_b"))

    -----------------------------------------------------------
    -- 11. Performance: rapid SET
    -----------------------------------------------------------
    print("\n--- Performance ---")
    local N = 10000

    local t0,elapsed
    for i = 1, 10 do
        t0 = moon.clock()
        for i = 1, N do
            db:set("bench:" .. i, "value_" .. i)
        end
        elapsed = moon.clock() - t0
        print(string.format("  %d SET: %.2f ms (%.0f ops/s)", N, elapsed * 1000, N / elapsed))

        t0 = moon.clock()
        for i = 1, N do
            db:get("bench:" .. i)
        end
        elapsed = moon.clock() - t0
        print(string.format("  %d GET: %.2f ms (%.0f ops/s)", N, elapsed * 1000, N / elapsed))
    end


    -- Pipeline performance
    local cmds = {}
    for i = 1, N do
        cmds[i] = { "SET", "pbench:" .. i, "val_" .. i }
    end
    t0 = moon.clock()
    db:pipeline(cmds)
    elapsed = moon.clock() - t0
    print(string.format("  %d PIPELINE SET: %.2f ms (%.0f ops/s)", N, elapsed * 1000, N / elapsed))

    -----------------------------------------------------------
    -- Cleanup
    -----------------------------------------------------------
    db:flushdb()
    db:close()

    print("\n=== example_redis done ===")
    moon.exit(0)
end)

moon.shutdown(function()
    moon.quit()
end)
