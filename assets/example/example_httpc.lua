--- HTTP client streaming example.
---
--- Run: moon_rs assets/example/example_httpc.lua

local moon = require "moon"
local json = require "json"
local httpc = require "moon.http.client"
local httpserver = require "moon.httpd"

local addr = "127.0.0.1:19879"
local listener_fd = httpserver.listen(addr)

httpserver.dispatch(function(req)
    if req.path == "/stream" then
        return 200, { ["content-type"] = "text/plain" }, "first|second|third"
    end

    if req.path == "/sse" then
        local body = table.concat {
            "data: ", json.encode({ delta = "hello" }), "\n\n",
            "data: ", json.encode({ delta = " world" }), "\n\n",
            "data: [DONE]\n\n",
        }
        return 200, { ["content-type"] = "text/event-stream" }, body
    end

    return 404, {}, "not found"
end)

moon.async(function()
    local response, stream = httpc.stream("http://" .. addr .. "/stream", {
        read_timeout = 5000,
    })
    assert(response.status_code == 200)

    local chunks = {}
    while true do
        local chunk, err = stream:next()
        assert(not err, err)
        if not chunk then
            break
        end
        chunks[#chunks + 1] = chunk
    end
    assert(table.concat(chunks) == "first|second|third")

    -- A to-be-closed stream is released automatically when this scope ends.
    do
        local scoped_response, scoped_stream = httpc.stream("http://" .. addr .. "/stream")
        assert(scoped_response.status_code == 200)
        local stream <close> = scoped_stream
        assert(stream:next())
    end

    local sse_response, events = httpc.stream_sse("http://" .. addr .. "/sse")
    assert(sse_response.status_code == 200)

    local event, err = events:next()
    assert(not err, err)
    assert(event.delta == "hello")

    event, err = events:next()
    assert(not err, err)
    assert(event.delta == " world")

    event, err = events:next()
    assert(not err, err)
    assert(event == nil)

    events:close()
    httpserver.close(listener_fd)
    print("HTTP streaming example passed")
    moon.quit()
end)
