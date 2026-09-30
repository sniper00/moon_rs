-- Run: moon_rs assets/test/grpc/test_grpc_close.lua (loopback only).
local moon = require "moon"
local grpc = require "moon.grpc"
local protobuf = require "protobuf"
local file = assert(io.open("helloworld.pb", "rb"))
assert(protobuf.load(file:read("a")))
file:close()
local request_type, reply_type = "helloworld.HelloRequest", "helloworld.HelloReply"

moon.timeout(10000, function()
    moon.error("gRPC close regression timed out")
    moon.exit(1)
end)

grpc.dispatch(function(stream, path)
    if path == "/test/error" then
        stream:finish(13, "remote failure")
    elseif path == "/test/empty" then
        stream:send(protobuf.encode(reply_type, {}))
    else
        assert(stream:recv())
        assert(stream:send(protobuf.encode(reply_type, {message = "ready"})))
        moon.sleep(60000) -- Keep the response stream open without more messages.
    end
end)

moon.async(function()
    local ok, err = xpcall(function()
        local addr, listener
        for _ = 1, 10 do
            addr = "127.0.0.1:" .. math.random(30000, 60000)
            local bound, fd = pcall(grpc.listen, addr)
            if bound then listener = fd; break end
        end
        assert(listener, "could not bind a loopback listener")
        local conn = assert(grpc.connect({name = "close_regression", endpoint = "http://" .. addr}))

        -- Cancel before a bidi response is established, then during an active
        -- read with another reader queued behind it.
        for _, established in ipairs({false, true}) do
            local stream
            if established then
                stream = assert(conn:server_stream("/test/hold", request_type, {}, reply_type))
                assert(stream:recv().message == "ready")
            else
                stream = assert(conn:bidi_stream("/test/hold", request_type, reply_type))
            end
            local count = established and 2 or 1
            local released = 0
            for _ = 1, count do
                moon.async(function()
                    local value, recv_err = stream:recv()
                    assert(value == nil and recv_err:find("closed", 1, true), tostring(recv_err))
                    released = released + 1
                end)
                moon.sleep(20)
            end
            assert(released == 0, "recv should still be pending")
            assert(stream:close())
            while released < count do moon.sleep(1) end
            assert(stream:close(), "close must be idempotent")
            local value, recv_err = stream:recv()
            assert(value == nil and recv_err:find("closed", 1, true))
        end

        local failed = assert(conn:bidi_stream("/test/error", request_type, reply_type))
        moon.sleep(20) -- The opening error must survive until the first recv.
        local value, recv_err = failed:recv()
        assert(value == nil and recv_err:find("remote failure", 1, true))
        failed:close()

        -- An empty protobuf payload is a valid message; only nil/false is EOF/error.
        local empty = assert(conn:bidi_stream("/test/empty", request_type, reply_type))
        assert(empty:recv().message == "")
        local eof, eof_err = empty:recv()
        assert(eof == nil and eof_err == nil)
        empty:close()
        assert(grpc.stats().streams == 0)
        grpc.close("close_regression")
        grpc.stop(listener)
    end, debug.traceback)
    if ok then print("gRPC close regression passed") else moon.error(err) end
    moon.exit(ok and 0 or 1)
end)
