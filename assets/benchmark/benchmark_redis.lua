---
--- benchmark_redis.lua — Native Redis driver performance benchmark.
---
--- Run:  moon_rs assets/benchmark/benchmark_redis.lua
---

local moon = require("moon")
local redis = require("moon.db.redis")

local HOST = "127.0.0.1"
local PORT = 6379

local N_SINGLE = 10000
local N_PIPE   = 10000

local STREAM_KEY = "bench:stream"
local STREAM_GROUP = "bench_group"

---------------------------------------------------------------------------
-- Results collection
---------------------------------------------------------------------------

local results = {}

local function record(test, ops, elapsed)
    results[test] = { ops = ops, elapsed = elapsed }
end

---------------------------------------------------------------------------
-- Text table output
---------------------------------------------------------------------------

local function print_final_table()
    local test_order = {
        { key = "single_set",        label = "Single SET" },
        { key = "single_get",        label = "Single GET" },
        { key = "pipe_set",          label = "Pipeline SET" },
        { key = "pipe_get",          label = "Pipeline GET" },
        { key = "pipe_mixed",        label = "Pipeline Mixed" },
        { key = "incr",              label = "INCR" },
        { key = "hset",              label = "HSET" },
        { key = "lpush",             label = "LPUSH" },
        { key = "stream_xadd",       label = "Stream XADD" },
        { key = "stream_pipe_xadd",  label = "Stream Pipe XADD" },
        { key = "stream_consume",    label = "Stream XREADGROUP+XACK" },
    }

    local W = { 22, 7, 10, 10, 8 }
    local H = { "Test", "Ops", "ms", "ops/s", "us/op" }

    local function mksep()
        local p = {}
        for i = 1, 5 do p[i] = string.rep("-", W[i] + 2) end
        return "+" .. table.concat(p, "+") .. "+"
    end

    local function mkhdr()
        local p = {}
        for i = 1, 5 do
            local a = i == 1 and "-" or ""
            p[i] = string.format(" %" .. a .. W[i] .. "s ", H[i])
        end
        return "|" .. table.concat(p, "|") .. "|"
    end

    local function mkrow(vals)
        local p = {}
        for i = 1, 5 do
            local a = i == 1 and "-" or ""
            p[i] = string.format(" %" .. a .. W[i] .. "s ", tostring(vals[i] or ""))
        end
        return "|" .. table.concat(p, "|") .. "|"
    end

    local function data(label, ops, elapsed)
        local ops_s = string.format("%.0f", ops / elapsed)
        local us = string.format("%.1f", elapsed / ops * 1e6)
        local ms = string.format("%.2f", elapsed * 1000)
        return mkrow({ label, ops, ms, ops_s, us })
    end

    local sep = mksep()

    print("\n================================================================")
    print("  Redis Native Driver Performance")
    print(string.format("  N_single=%d  N_pipe=%d", N_SINGLE, N_PIPE))
    print("  Stream: XADD / Pipeline XADD / XREADGROUP+XACK")
    print("================================================================\n")

    print(sep)
    print(mkhdr())
    print(sep)

    for _, t in ipairs(test_order) do
        local e = results[t.key]
        if e then
            print(data(t.label, e.ops, e.elapsed))
        end
    end
    print(sep)
end

---------------------------------------------------------------------------
-- Main
---------------------------------------------------------------------------

moon.async(function()
    print("Connecting...")

    local db, err = redis.connect({ host = HOST, port = PORT }, "bench_redis", 5000, 1)
    if not db then
        print("connect failed:", err)
        moon.exit(-1)
        return
    end
    print("connected")

    db:flushdb()

    io.write("\nRunning benchmarks")

    -----------------------------------------------------------
    -- 1. Single SET
    -----------------------------------------------------------
    do
        local bt = moon.clock()
        for i = 1, N_SINGLE do
            db:set("k:" .. i, "value_" .. i)
        end
        record("single_set", N_SINGLE, moon.clock() - bt)
        io.write("."); io.flush()
    end

    -----------------------------------------------------------
    -- 2. Single GET
    -----------------------------------------------------------
    do
        local bt = moon.clock()
        for i = 1, N_SINGLE do
            db:get("k:" .. ((i % N_SINGLE) + 1))
        end
        record("single_get", N_SINGLE, moon.clock() - bt)
        io.write("."); io.flush()
    end

    -----------------------------------------------------------
    -- 3. Pipeline SET
    -----------------------------------------------------------
    do
        local cmds = {}
        for i = 1, N_PIPE do
            cmds[i] = { "SET", "p:" .. i, "val_" .. i }
        end

        local bt = moon.clock()
        db:pipeline(cmds)
        record("pipe_set", N_PIPE, moon.clock() - bt)
        io.write("."); io.flush()
    end

    -----------------------------------------------------------
    -- 4. Pipeline GET
    -----------------------------------------------------------
    do
        local cmds = {}
        for i = 1, N_PIPE do
            cmds[i] = { "GET", "p:" .. i }
        end

        local bt = moon.clock()
        db:pipeline(cmds)
        record("pipe_get", N_PIPE, moon.clock() - bt)
        io.write("."); io.flush()
    end

    -----------------------------------------------------------
    -- 5. Pipeline Mixed (SET + GET interleaved)
    -----------------------------------------------------------
    do
        local cmds = {}
        for i = 1, N_PIPE do
            if i % 2 == 1 then
                cmds[i] = { "SET", "m:" .. i, "v" .. i }
            else
                cmds[i] = { "GET", "m:" .. (i - 1) }
            end
        end

        local bt = moon.clock()
        db:pipeline(cmds)
        record("pipe_mixed", N_PIPE, moon.clock() - bt)
        io.write("."); io.flush()
    end

    -----------------------------------------------------------
    -- 6. INCR
    -----------------------------------------------------------
    do
        db:set("ctr", 0)

        local bt = moon.clock()
        for i = 1, N_SINGLE do
            db:incr("ctr")
        end
        record("incr", N_SINGLE, moon.clock() - bt)
        io.write("."); io.flush()
    end

    -----------------------------------------------------------
    -- 7. HSET
    -----------------------------------------------------------
    do
        local bt = moon.clock()
        for i = 1, N_SINGLE do
            db:hset("h", "f" .. i, "v" .. i)
        end
        record("hset", N_SINGLE, moon.clock() - bt)
        io.write("."); io.flush()
    end

    -----------------------------------------------------------
    -- 8. LPUSH
    -----------------------------------------------------------
    do
        local bt = moon.clock()
        for i = 1, N_SINGLE do
            db:lpush("list", "item_" .. i)
        end
        record("lpush", N_SINGLE, moon.clock() - bt)
        io.write("."); io.flush()
    end

    -----------------------------------------------------------
    -- 9. Stream XADD
    -----------------------------------------------------------
    do
        db:xgroup("CREATE", STREAM_KEY, STREAM_GROUP, "0", "MKSTREAM")

        local bt = moon.clock()
        for i = 1, N_SINGLE do
            db:xadd(STREAM_KEY, "*", "k", tostring(i))
        end
        record("stream_xadd", N_SINGLE, moon.clock() - bt)
        io.write("."); io.flush()
    end

    -----------------------------------------------------------
    -- 10. Stream Pipeline XADD
    -----------------------------------------------------------
    do
        local cmds = {}
        for i = 1, N_PIPE do
            cmds[i] = { "XADD", STREAM_KEY, "*", "p", tostring(i) }
        end

        local bt = moon.clock()
        db:pipeline(cmds)
        record("stream_pipe_xadd", N_PIPE, moon.clock() - bt)
        io.write("."); io.flush()
    end

    -----------------------------------------------------------
    -- 11. Stream XREADGROUP + XACK
    -----------------------------------------------------------
    do
        local consume_stream = STREAM_KEY .. ":consume"
        local consume_group = STREAM_GROUP .. "_c"
        local consumer = "bench_consumer"

        db:xgroup("CREATE", consume_stream, consume_group, "0", "MKSTREAM")
        for i = 1, N_SINGLE do
            db:xadd(consume_stream, "*", "c", tostring(i))
        end

        local bt = moon.clock()
        for i = 1, N_SINGLE do
            local r = db:xreadgroup("GROUP", consume_group, consumer,
                "COUNT", "1", "STREAMS", consume_stream, ">")
            assert(r and r[1] and r[1][2] and r[1][2][1],
                "stream_consume: expected message at index " .. i)
            local id = r[1][2][1][1]
            db:xack(consume_stream, consume_group, id)
        end
        record("stream_consume", N_SINGLE, moon.clock() - bt)
        io.write("."); io.flush()
    end

    print(" done\n")

    print_final_table()

    db:flushdb()
    db:close()

    moon.exit(0)
end)

moon.shutdown(function()
    moon.quit()
end)
