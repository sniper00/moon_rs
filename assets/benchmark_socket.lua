local moon = require("moon")
local socket = require("moon.socket")
local conf = ...

collectgarbage("incremental", 200, 200, 13)

conf.host = conf.host or "127.0.0.1"
conf.port = conf.port or 6770
conf.count = conf.count or 1

local function handle_connection(fd)
    moon.async(function()
    local i = 0
        while true do
            local n, err = socket.read(fd,"\r\n")
            if not n then
                --moon.error(fd,"closed", err)
                return
            end

            local t = {a = 1, b = 2, c = 3, d = 4, e = 5, f = 6, g = 7, h = 8, i = 9, j = 10}
            -- print("read",  n, n == "appendonly")
            if n == "PING" then
                socket.write(fd, "+PONG\r\n")
            elseif n== "save" then
                socket.write(fd, "*2\r\n$4\r\nsave\r\n$23\r\n3600 1 300 100 60 10000\r\n")
            elseif n== "appendonly" then
                socket.write(fd, "*2\r\n$10\r\nappendonly\r\n$3\r\nyes\r\n")
            end

            i = i + 1

            if i % 1000 == 0 then
                collectgarbage("collect")
            end
        end
    end)
end

moon.async(function()

    local listenfd  = assert(socket.listen(conf.host..":"..conf.port))

    print(string.format([[

        network text benchmark run at %s %d with %d slaves.
        run benchmark use: redis-benchmark -t ping -p %d -c 100 -n 100000
    ]], conf.host, conf.port, conf.count, conf.port))

    while true do
        local fd,err = socket.accept(listenfd)
        if not fd then
            print("accept failed", err)
            return
        end
        --print("accept", fd)
        handle_connection(fd)
    end
end)
