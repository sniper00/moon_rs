local moon = require("moon")
local socket = require("moon.socket")

local conf = ...

local host = "127.0.0.1"
local port = 6770

if conf and conf.agent then
    local fd = conf.fd

    moon.async(function()
        while true do
            local data, err = socket.read(fd, "\r\n")
            if not data then
                return
            end

            local n = 0
            for i=1, 1000 do
                n = n + 1
            end

            if data == "PING" then
                socket.write(fd, "+PONG\r\n")
            elseif data == "save" then
                socket.write(fd, "*2\r\n$4\r\nsave\r\n$23\r\n3600 1 300 100 60 10000\r\n")
            elseif data == "appendonly" then
                socket.write(fd, "*2\r\n$10\r\nappendonly\r\n$3\r\nyes\r\n")
            end
        end
    end)
else
    moon.async(function()
        local listenfd = assert(socket.listen(host .. ":" .. port, function(fd, addr)
            moon.new_service({
                name = "benchmark_socket_agent",
                source = "benchmark_socket_multi.lua",
                agent = true,
                fd = fd,
            })
        end))

        print(string.format([[

        network text benchmark run at %s:%d
        run benchmark use: redis-benchmark -t ping -p %d -c 100 -n 1000000
    ]], host, port, port))
    end)
end
