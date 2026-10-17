local moon = require "moon"
moon.loglevel("INFO")
local httpc = require "moon.http.client"
local httpd = require "moon.httpd"
local bytes = "first\0second\255"
httpd.dispatch(function(req)
    if req.path == "/sse" then
        -- CRLF delimiter precedes a later LF delimiter in the same body.
        return 200, {["content-type"]="text/event-stream"},
            'data: {"n":1}\r\n\r\ndata: {"n":2}\n\ndata: [DONE]\n\n'
    elseif req.path == "/slow" then
        moon.sleep(200)
    end
    return 200, {}, bytes
end)
local address, listener
for _=1,10 do
    address = "127.0.0.1:" .. math.random(30000,60000)
    local ok, fd = pcall(httpd.listen,address)
    if ok then listener=fd; break end
end
assert(listener)
moon.async(function()
    local ok, err = xpcall(function()
        local response, stream = httpc.stream("http://"..address.."/body")
        assert(response and response.status_code == 200)
        local handle <close> = stream
        local parts = {}
        while true do
            local chunk, failure = stream:next()
            assert(not failure, failure)
            if chunk == nil then break end
            parts[#parts+1] = chunk
        end
        assert(table.concat(parts) == bytes)
        local headers, events = httpc.stream_sse("http://"..address.."/sse")
        assert(headers and headers.status_code == 200)
        local sse <close> = events
        assert(events:next().n == 1)
        assert(events:next().n == 2)
        assert(events:next() == nil)
        local early_headers, early = httpc.stream("http://"..address.."/body")
        assert(early_headers.status_code == 200)
        early:close()
        assert(early:next() == nil)
        local slow = httpc.get("http://"..address.."/slow",{timeout=20})
        assert(slow.status_code ~= 200)
        print("HTTP STREAM PASSED")
    end,debug.traceback)
    httpd.close(listener)
    if not ok then moon.error(err) end
    moon.exit(ok and 0 or 1)
end)
