local socket = require "moon.socket"
local moon   = require "moon"

moon.async(function(...)
    -- Test the listen function
    do
        local ok, listenfd = assert(pcall(socket.listen, "localhost:8089"))
        assert(ok, "listen function failed")
        assert(listenfd, "listen function did not return a file descriptor")
        socket.close(listenfd)
    end

    -- Test the connect and accept functions
    do
        local listenfd = assert(socket.listen("localhost:9089"))
        local ok, clientfd = pcall(socket.connect, "localhost:9089")
        assert(ok, "connect function failed" .. clientfd)
        assert(clientfd, "connect function did not return a file descriptor")

        local ok, serverfd = pcall(socket.accept, listenfd)
        assert(ok, "accept function failed")
        assert(serverfd, "accept function did not return a file descriptor")

        socket.close(clientfd)
        socket.close(serverfd)
        socket.close(listenfd)
    end

    -- Test the write and read functions
    do
        local listenfd = assert(socket.listen("localhost:18089"))

        local clientfd = socket.connect("localhost:18089")

        local serverfd = socket.accept(listenfd)

        local ok, err = pcall(socket.write, clientfd, "Hello, World!\n")
        assert(ok, "write function failed: " .. tostring(err or ""))

        local ok, data = pcall(socket.read, serverfd, "\n")

        assert(ok, "read function failed: " .. (data or ""))
        assert(data == "Hello, World!", "read function did not return the correct data")

        socket.close(clientfd)
        socket.close(serverfd)
        socket.close(listenfd)
    end

    print("done")

    moon.quit()
end)
