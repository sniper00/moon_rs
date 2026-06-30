---
--- test_grpc_server.lua — Pure-Lua functional tests for the native gRPC
--- client + server (`moon.grpc`).
---
--- Self-contained: one actor starts a gRPC server implementing all four RPC
--- kinds, then the gRPC client calls each method over h2c loopback and asserts
--- the results. No external tooling (protoc/python/grpcio) is needed at run
--- time; only the pre-generated descriptor `helloworld.pb` (committed alongside
--- this file) is loaded.
---
--- Run from the repo root:
---   cargo run assets/test/grpc/test_grpc_server.lua
---

local moon     = require("moon")
local protobuf = require("protobuf")
local grpc     = require("moon.grpc")

local ADDR  = "127.0.0.1:50071"
local REQ   = "helloworld.HelloRequest"
local REPLY = "helloworld.HelloReply"

-- moon sets the cwd to this script's directory, so the descriptor is alongside.
local function readfile(path)
    local f = assert(io.open(path, "rb"))
    local data = f:read("a")
    f:close()
    return data
end
protobuf.load(readfile("helloworld.pb"))

-- --------------------------------------------------------------------------
-- assertion helpers (project convention: count pass/fail, exit fail_count)
-- --------------------------------------------------------------------------
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

local function assert_true(name, value, extra)
    if value then
        pass_count = pass_count + 1
    else
        fail_count = fail_count + 1
        print(string.format("FAIL [%s]: expected truthy, got %s %s",
            name, tostring(value), extra and ("(" .. tostring(extra) .. ")") or ""))
    end
end

-- --------------------------------------------------------------------------
-- Server: dispatch by method path; decode/encode with protobuf ourselves.
-- --------------------------------------------------------------------------
grpc.dispatch(function(stream, path)
    if path == "/helloworld.Greeter/SayHello" then
        -- unary: one request -> one reply (or an explicit error status)
        local req = protobuf.decode(REQ, stream:recv())
        if req.name == "deny" then
            stream:finish(7, "permission denied") -- 7 = PERMISSION_DENIED
            return
        end
        stream:send(protobuf.encode(REPLY, { message = "hello " .. req.name }))
        -- finish(OK) auto-called by the wrapper on return
    elseif path == "/helloworld.Greeter/SayHelloStream" then
        -- server streaming: one request -> N replies
        local req = protobuf.decode(REQ, stream:recv())
        for i = 1, 3 do
            stream:send(protobuf.encode(REPLY, { message = string.format("%s #%d", req.name, i) }))
        end
    elseif path == "/helloworld.Greeter/SayHelloCollect" then
        -- client streaming: N requests -> one reply
        local names = {}
        while true do
            local bytes = stream:recv()
            if bytes == nil then break end
            names[#names + 1] = protobuf.decode(REQ, bytes).name
        end
        stream:send(protobuf.encode(REPLY, { message = "collected: " .. table.concat(names, ",") }))
    elseif path == "/helloworld.Greeter/SayHelloChat" then
        -- bidi: echo each request as a reply
        while true do
            local bytes = stream:recv()
            if bytes == nil then break end
            local req = protobuf.decode(REQ, bytes)
            stream:send(protobuf.encode(REPLY, { message = "echo " .. req.name }))
        end
    elseif path == "/helloworld.Greeter/Boom" then
        -- handler error must not hang the client: wrapper finishes INTERNAL(13)
        error("intentional handler failure")
    else
        stream:finish(12, "unimplemented: " .. path) -- 12 = UNIMPLEMENTED
    end
end)

local listener = grpc.listen(ADDR)
print(string.format("grpc server listening on %s (fd=%d)", ADDR, listener))

-- --------------------------------------------------------------------------
-- Client: exercise every method shape + edge cases.
-- --------------------------------------------------------------------------
moon.async(function()
    moon.sleep(200) -- give the listener a beat

    ---- Group A: client-only surface (no server interaction) ----
    print("\n--- A. client surface ---")
    assert_eq("find_connection missing -> nil", nil, grpc.find_connection("nope"))
    assert_eq("close unknown -> true", true, grpc.close("nope"))
    local dead = grpc.connect({ endpoint = "http://127.0.0.1:1", connect_timeout = 300, name = "dead" })
    assert_eq("connect to dead endpoint -> nil", nil, dead)

    ---- Connect to our own server ----
    local conn, err = grpc.connect({ endpoint = "http://" .. ADDR, name = "greeter" })
    assert_true("connect", conn ~= nil, err)
    if not conn then
        moon.exit(1); return
    end
    assert_true("find_connection existing", grpc.find_connection("greeter") ~= nil)

    ---- Group B: unary ----
    print("\n--- B. unary ---")
    local reply, status = conn:unary("/helloworld.Greeter/SayHello", REQ, { name = "moon" }, REPLY)
    assert_true("unary reply present", reply ~= nil, status and status.message)
    assert_eq("unary reply body", "hello moon", reply and reply.message)

    -- unary with metadata (just confirms the call still succeeds end-to-end)
    local reply_md = conn:unary("/helloworld.Greeter/SayHello", REQ, { name = "md" }, REPLY,
        { timeout = 3000, metadata = { authorization = "Bearer x", ["x-trace"] = "1" } })
    assert_eq("unary with metadata", "hello md", reply_md and reply_md.message)

    -- unary error status from the handler
    local rdeny, sdeny = conn:unary("/helloworld.Greeter/SayHello", REQ, { name = "deny" }, REPLY)
    assert_eq("unary denied reply nil", nil, rdeny)
    assert_eq("unary denied code", 7, sdeny and sdeny.code)

    ---- Group C: server streaming ----
    print("\n--- C. server streaming ---")
    local ss = conn:server_stream("/helloworld.Greeter/SayHelloStream", REQ, { name = "moon" }, REPLY)
    assert_true("server_stream open", ss ~= nil)
    local got = {}
    if ss then
        while true do
            local msg = ss:recv()
            if msg == nil then break end
            got[#got + 1] = msg.message
        end
        ss:close()
    end
    assert_eq("server_stream count", 3, #got)
    assert_eq("server_stream first", "moon #1", got[1])
    assert_eq("server_stream last", "moon #3", got[3])

    ---- Group D: client streaming (bidi handle: send N, half-close, recv 1) ----
    print("\n--- D. client streaming ---")
    local cs = conn:bidi_stream("/helloworld.Greeter/SayHelloCollect", REQ, REPLY)
    assert_true("client_stream open", cs ~= nil)
    if cs then
        cs:send({ name = "a" })
        cs:send({ name = "b" })
        cs:send({ name = "c" })
        cs:close_send()
        local r = cs:recv()
        assert_eq("client_stream reply", "collected: a,b,c", r and r.message)
        assert_eq("client_stream end", nil, cs:recv())
        cs:close()
    end

    ---- Group E: bidirectional streaming ----
    print("\n--- E. bidi streaming ---")
    local bs = conn:bidi_stream("/helloworld.Greeter/SayHelloChat", REQ, REPLY)
    assert_true("bidi open", bs ~= nil)
    if bs then
        for _, n in ipairs({ "x", "y", "z" }) do
            bs:send({ name = n })
            local r = bs:recv()
            assert_eq("bidi echo " .. n, "echo " .. n, r and r.message)
        end
        bs:close_send()
        assert_eq("bidi end", nil, bs:recv())
        bs:close()
    end

    ---- Group F: to-be-closed auto cleanup ----
    print("\n--- F. to-be-closed ---")
    do
        local sc <close> = conn:bidi_stream("/helloworld.Greeter/SayHelloChat", REQ, REPLY)
        sc:send({ name = "scoped" })
        local r = sc:recv()
        assert_eq("to_be_closed recv", "echo scoped", r and r.message)
    end -- sc:close() invoked here via __close
    moon.sleep(50)
    assert_eq("to_be_closed cleaned (streams==0)", 0, grpc.stats().streams)

    ---- Group G: error paths ----
    print("\n--- G. error paths ---")
    local rnf, snf = conn:unary("/helloworld.Greeter/Nope", REQ, { name = "x" }, REPLY)
    assert_eq("unimplemented reply nil", nil, rnf)
    assert_eq("unimplemented code", 12, snf and snf.code)

    local rb, sb = conn:unary("/helloworld.Greeter/Boom", REQ, { name = "x" }, REPLY)
    assert_eq("handler crash reply nil", nil, rb)
    assert_eq("handler crash code (INTERNAL)", 13, sb and sb.code)

    ---- Group H: concurrency over one channel ----
    print("\n--- H. concurrency ---")
    local done, expected = 0, 8
    for i = 1, expected do
        moon.async(function()
            local r = conn:unary("/helloworld.Greeter/SayHello", REQ, { name = "c" .. i }, REPLY)
            assert_eq("concurrent #" .. i, "hello c" .. i, r and r.message)
            done = done + 1
        end)
    end
    while done < expected do moon.sleep(20) end
    assert_eq("all concurrent done", expected, done)

    ---- Group I: stats + listener shutdown ----
    print("\n--- I. stats + shutdown ---")
    local st = grpc.stats()
    assert_true("stats.connections >= 1", st.connections >= 1, st.connections)
    assert_eq("stats.streams leaked", 0, st.streams)
    assert_eq("stats.servers", 1, st.servers)
    assert_eq("stop listener -> true", true, grpc.stop(listener))
    assert_eq("stop unknown -> false", false, grpc.stop(999999))
    moon.sleep(50)
    assert_eq("stats.servers after stop", 0, grpc.stats().servers)

    ---- Summary ----
    print("\n========================================")
    print(string.format("  gRPC Tests: %d passed, %d failed", pass_count, fail_count))
    print("========================================")
    print(fail_count == 0 and "ALL TESTS PASSED!" or "SOME TESTS FAILED!")
    moon.exit(fail_count)
end)

moon.shutdown(function()
    moon.quit()
end)
