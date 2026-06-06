local moon = require("moon")
local socket = require("moon.socket")
local buffer = require("buffer")

local conf = ...

if conf.server then
    socket.on("message", function(fd, buf_ptr)
        local data = buffer.unpack(buf_ptr, "Z")
        socket.write_frame(fd, data)
    end)

    socket.on("close", function(fd, remote_addr, err)
    end)

    moon.async(function()
        local addr = conf.addr or "127.0.0.1:19888"
        local listenfd = assert(socket.listen(addr, function(fd, remote_addr)
            socket.start_read_frame(fd)
        end))
        moon.send("lua", conf.starter, "READY", addr)
    end)

    moon.shutdown(function()
        moon.quit()
    end)

    return
end

-- benchmark client (bootstrap)
local num_clients = conf.num_clients or 1000
local num_messages = conf.num_messages or 500
local msg_size = conf.msg_size or 256
local max_inflight = conf.max_inflight or 50000

local recv_count = 0
local start_time = 0
local total_expected = 0
local done = false

socket.on("message", function(fd, buf_ptr)
    recv_count = recv_count + 1
    if not done and recv_count == total_expected then
        done = true
        local elapsed = moon.clock() - start_time
        local ops = total_expected / elapsed
        print(string.format("\n===== Benchmark Result ====="))
        print(string.format("  Clients:         %d", num_clients))
        print(string.format("  Messages/client: %d", num_messages))
        print(string.format("  Message size:    %d bytes", msg_size))
        print(string.format("  Total messages:  %d", total_expected))
        print(string.format("  Elapsed:         %.3f s", elapsed))
        print(string.format("  Throughput:      %d msg/s", math.floor(ops)))
        print(string.format("  Bandwidth:       %.2f MB/s", ops * msg_size / 1024 / 1024))
        print(string.format("============================\n"))
        moon.quit()
    end
end)

socket.on("close", function(fd, remote_addr, err)
end)

local server_ready = false
local server_addr = ""

moon.dispatch("lua", function(sender, session, cmd, ...)
    if cmd == "READY" then
        server_addr = ...
        server_ready = true
    end
end)

moon.async(function()
    local addr = "127.0.0.1:19888"

    local server_id = moon.new_service({
        name = "moon_socket_server",
        source = "benchmark_socket_frame.lua",
        server = true,
        addr = addr,
        starter = moon.id,
    })
    assert(server_id > 0, "failed to create server service")

    while not server_ready do
        moon.sleep(10)
    end

    print(string.format("[bench] server ready at %s", server_addr))
    print(string.format("[bench] connecting %d clients...", num_clients))

    local fds = {}
    for i = 1, num_clients do
        local fd, err = socket.connect(server_addr, 5000)
        assert(fd, "connect failed for client " .. i.. " err:" .. tostring(err))
        socket.start_read_frame(fd)
        table.insert(fds, fd)
    end

    moon.sleep(100)
    print(string.format("[bench] all %d clients connected", #fds))

    local payload = string.rep("A", msg_size)
    total_expected = num_clients * num_messages

    print(string.format("[bench] sending %d messages (%d bytes each), max_inflight=%d...",
        total_expected, msg_size, max_inflight))

    start_time = moon.clock()

    local sent = 0
    for _, fd in ipairs(fds) do
        for _ = 1, num_messages do
            socket.write_frame(fd, payload)
            sent = sent + 1
            if sent - recv_count >= max_inflight then
                while sent - recv_count >= max_inflight do
                    moon.sleep(0)
                end
            end
        end
    end

    print(string.format("[bench] all messages sent (recv_count=%d), waiting for echoes...", recv_count))
end)
