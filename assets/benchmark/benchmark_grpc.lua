---
--- benchmark_grpc.lua — Throughput/latency benchmark for the native gRPC
--- client + server (`moon.grpc`).
---
--- Self-contained: a dedicated `server` service runs a schema-agnostic byte
--- echo (so the server adds ~zero work and we measure transport + the client's
--- protobuf encode/decode + the full tonic/h2 path). The bootstrap actor is the
--- client and drives:
---   1. unary latency        — sequential round trips (concurrency 1)
---   2. unary throughput      — N calls with C in-flight (h2 multiplexing)
---   3. server-stream tput    — one request -> a batch of responses, drained tight
---   4. bidi ping-pong        — sequential send/recv round trips on one stream
---   5. bidi pipelined        — keep W messages in flight (max stream throughput
---                              without the "send-all-then-recv" h2 flow-control stall)
---   6. large messages        — unary + pipelined bidi at LARGE payload, in MB/s
---
--- Run:  moon_rs assets/benchmark/benchmark_grpc.lua [OPS] [CONCURRENCY] [PAYLOAD_BYTES] [WINDOW] [LARGE_BYTES]
---       moon_rs assets/benchmark/benchmark_grpc.lua 100000 128 32 64 65536
---

local moon     = require("moon")
local protobuf = require("protobuf")
local grpc     = require("moon.grpc")

moon.loglevel("INFO") -- silence per-frame h2 DEBUG logging

local ADDR         = "127.0.0.1:50081"
local REQ          = "helloworld.HelloRequest"
local REPLY        = "helloworld.HelloReply"
local UNARY        = "/bench.Echo/Unary"
local STREAM       = "/bench.Echo/Stream"
local CHAT         = "/bench.Echo/Chat"
local STREAM_BATCH = 1000 -- responses per server-stream call (shared constant)

-- HelloRequest{name} and HelloReply{message} are both "field 1: string", so the
-- server can blindly echo request bytes back as a valid reply.

-- =========================================================================
-- Role: server service (created via moon.new_service below).
-- =========================================================================
local params = ...
if params and params.role == "server" then
    grpc.dispatch(function(stream, path)
        if path == UNARY then
            stream:send(stream:recv())
        elseif path == STREAM then
            local b = stream:recv() -- the single request
            for _ = 1, STREAM_BATCH do
                stream:send(b)
            end
        elseif path == CHAT then
            while true do
                local b = stream:recv()
                if b == nil then break end
                stream:send(b)
            end
        else
            stream:finish(12, "unimplemented: " .. path)
        end
    end)
    grpc.listen(ADDR)
    moon.info("grpc benchmark server listening on " .. ADDR)
    return
end

-- =========================================================================
-- Role: client / main.
-- =========================================================================
local args    = moon.args()
local OPS      = tonumber(args[1]) or 100000 -- unary calls (throughput run)
local CONC     = tonumber(args[2]) or 128    -- in-flight unary calls
local PAYLOAD  = tonumber(args[3]) or 32      -- request "name" length in bytes
local WINDOW   = tonumber(args[4]) or 64      -- in-flight messages for pipelined bidi
local LARGE    = tonumber(args[5]) or 65536   -- large-message payload in bytes
local PINGPONG = math.max(1, math.floor(OPS / 10))  -- sequential round trips
local STREAM_REPEAT = math.max(1, math.floor(OPS / STREAM_BATCH)) -- server-stream calls
local LARGE_OPS = math.max(1, math.floor(OPS / 20)) -- fewer iters for big payloads
local LARGE_WINDOW = 16                              -- big msgs fill h2 windows fast

-- The client encodes/decodes; the server is schema-agnostic, so only the client
-- loads the descriptor. cwd is this script's dir (assets/benchmark).
local function readfile(path)
    local f = assert(io.open(path, "rb"))
    local data = f:read("a")
    f:close()
    return data
end
protobuf.load(readfile("../test/grpc/helloworld.pb"))

local clock = moon.clock
local name_payload = string.rep("x", PAYLOAD)
local large_payload = string.rep("x", LARGE)
local rows = {}

-- `bytes_per_op` (optional): application payload moved per op (one direction);
-- when given, also report MB/s.
local function bench(label, count, fn, bytes_per_op)
    local bt = clock()
    fn(count)
    local elapsed = clock() - bt
    local mbps = bytes_per_op and (count * bytes_per_op / elapsed / 1e6) or nil
    rows[#rows + 1] = {
        label  = label,
        count  = count,
        ms     = elapsed * 1000,
        ops    = count / elapsed,
        avg_us = (elapsed / count) * 1e6,
        mbps   = mbps,
    }
    moon.info(string.format("  [%-22s] %10d ops in %9.1f ms  (%12.0f ops/s, %8.2f us/op%s)",
        label, count, elapsed * 1000, count / elapsed, (elapsed / count) * 1e6,
        mbps and string.format(", %8.1f MB/s", mbps) or ""))
end

local function print_results()
    print("\n=================================================================================")
    print(string.format("  gRPC benchmark  (OPS=%d, concurrency=%d, payload=%dB, window=%d, large=%dB)",
        OPS, CONC, PAYLOAD, WINDOW, LARGE))
    print("=================================================================================")
    local hdr = string.format("%-24s %12s %12s %14s %10s %10s", "op", "count", "total ms", "ops/sec", "avg us", "MB/s")
    print(hdr)
    print(string.rep("-", #hdr))
    for _, r in ipairs(rows) do
        print(string.format("%-24s %12d %12.1f %14.0f %10.2f %10s",
            r.label, r.count, r.ms, r.ops, r.avg_us, r.mbps and string.format("%.1f", r.mbps) or "-"))
    end
    print(string.rep("-", #hdr))
end

moon.async(function()
    -- 1) Start the echo server in its own (non-unique) service, then connect
    --    with retry. Non-unique => the default shutdown handler quits it on exit.
    local sid = moon.new_service({ name = "grpc_bench_server", source = "benchmark_grpc.lua", role = "server" })
    assert(sid and sid > 0, "failed to create benchmark server service")

    local conn
    for _ = 1, 20 do
        moon.sleep(50)
        local c = grpc.connect({ endpoint = "http://" .. ADDR, name = "bench" })
        if c then conn = c; break end
    end
    assert(conn, "failed to connect to benchmark server")

    -- Run `total` unary calls keeping `conc` in flight (h2 multiplexes them over
    -- the one connection). Joins via a counter + wakeup.
    local function run_unary_concurrent(total, conc, payload)
        local issued, done = 0, 0
        local main_co = coroutine.running()
        local waiting = false
        local function worker()
            while issued < total do
                issued = issued + 1
                local r = conn:unary(UNARY, REQ, { name = payload }, REPLY)
                if not r then error("unary failed") end
                done = done + 1
                if done == total and waiting then
                    moon.wakeup(main_co)
                end
            end
        end
        for _ = 1, math.min(conc, total) do moon.async(worker) end
        if done < total then
            waiting = true
            moon.wait()
        end
    end

    -- Pipelined bidi: keep at most `window` requests in flight. Sending all then
    -- receiving would stall once the h2 flow-control window fills (the server
    -- can't drain requests while blocked sending responses we never read), so we
    -- send one more only as each reply arrives.
    local function run_bidi_pipelined(total, window, payload)
        local s <close> = conn:bidi_stream(CHAT, REQ, REPLY)
        local sent, recvd = 0, 0
        while sent < total and sent < window do
            assert(s:send({ name = payload })); sent = sent + 1
        end
        while recvd < total do
            local r = s:recv()
            if not r then error("bidi pipelined recv failed") end
            recvd = recvd + 1
            if sent < total then
                assert(s:send({ name = payload })); sent = sent + 1
            end
        end
        s:close_send()
    end

    -- 2) Warm up (establish the h2 connection / tonic buffers).
    for _ = 1, 200 do
        assert(conn:unary(UNARY, REQ, { name = name_payload }, REPLY))
    end

    -- ---- unary latency: sequential round trips (concurrency 1) ----
    bench("unary latency (c=1)", PINGPONG, function(count)
        for _ = 1, count do
            local r = conn:unary(UNARY, REQ, { name = name_payload }, REPLY)
            if not r then error("unary failed") end
        end
    end)

    -- ---- unary throughput: C in-flight calls, h2 multiplexed ----
    bench(string.format("unary tput (c=%d)", CONC), OPS, function()
        run_unary_concurrent(OPS, CONC, name_payload)
    end)

    -- ---- server-streaming throughput: one request -> STREAM_BATCH responses ----
    bench("server-stream recv", STREAM_BATCH * STREAM_REPEAT, function()
        for _ = 1, STREAM_REPEAT do
            local s <close> = conn:server_stream(STREAM, REQ, { name = name_payload }, REPLY)
            local n = 0
            while true do
                local m = s:recv()
                if m == nil then break end
                n = n + 1
            end
            if n ~= STREAM_BATCH then error("server-stream short read: " .. n) end
        end
    end)

    -- ---- bidi ping-pong: sequential send/recv round trips on one stream ----
    bench("bidi ping-pong", PINGPONG, function(count)
        local s <close> = conn:bidi_stream(CHAT, REQ, REPLY)
        for _ = 1, count do
            assert(s:send({ name = name_payload }))
            local r = s:recv()
            if not r then error("bidi recv failed") end
        end
        s:close_send()
    end)

    -- ---- bidi pipelined throughput: W in-flight on one stream ----
    bench(string.format("bidi pipelined (w=%d)", WINDOW), OPS, function(count)
        run_bidi_pipelined(count, WINDOW, name_payload)
    end)

    -- ---- large messages: report MB/s (one-way application payload) ----
    bench(string.format("unary large (c=%d)", CONC), LARGE_OPS, function()
        run_unary_concurrent(LARGE_OPS, CONC, large_payload)
    end, LARGE)

    bench(string.format("bidi pipelined large (w=%d)", LARGE_WINDOW), LARGE_OPS, function(count)
        run_bidi_pipelined(count, LARGE_WINDOW, large_payload)
    end, LARGE)

    print_results()
    moon.exit(0)
end)

moon.shutdown(function()
    moon.quit()
end)
