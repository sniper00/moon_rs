---
--- test_socket.lua — Socket API and close/read regression tests.
---
--- Run: moon_rs assets/test/test_socket.lua
---

local socket = require "moon.socket"
local moon   = require "moon"

local accepted_fds = {}
local close_events = {}

local function on_accept(fd, addr)
    table.insert(accepted_fds, fd)
end

socket.on("close", function(fd, remote_addr, err)
    print(string.format("[close] fd=%d addr=%s err=%s", fd, remote_addr, err))
    table.insert(close_events, { fd = fd, addr = remote_addr, err = err })
end)

socket.on("message", function(fd, buf_ptr)
    return true
end)

moon.async(function()
    -----------------------------------------------------------------
    -- Basic API: listen, connect, accept, write, read
    -----------------------------------------------------------------
    print("--- Basic API tests ---")

    do
        local ok, listenfd = assert(pcall(socket.listen, "localhost:28089", on_accept))
        assert(ok, "listen function failed")
        assert(listenfd, "listen function did not return a file descriptor")
        socket.close(listenfd)
    end

    do
        local listenfd = assert(socket.listen("localhost:29089", on_accept))
        local ok, clientfd = pcall(socket.connect, "localhost:29089")
        assert(ok, "connect function failed" .. clientfd)
        assert(clientfd, "connect function did not return a file descriptor")

        moon.sleep(50)
        assert(#accepted_fds >= 1, "accept callback not called")
        local serverfd = table.remove(accepted_fds, 1)
        assert(serverfd, "accept did not return a file descriptor")

        socket.close(clientfd)
        socket.close(serverfd)
        socket.close(listenfd)
    end

    do
        local listenfd = assert(socket.listen("localhost:38089", on_accept))

        local clientfd = socket.connect("localhost:38089")

        moon.sleep(50)
        local serverfd = table.remove(accepted_fds, 1)

        local ok, err = pcall(socket.write, clientfd, "Hello, World!\n")
        assert(ok, "write function failed: " .. tostring(err or ""))

        local ok, data = pcall(socket.read, serverfd, "\n")

        assert(ok, "read function failed: " .. (data or ""))
        assert(data == "Hello, World!", "read function did not return the correct data")

        socket.close(clientfd)
        socket.close(serverfd)
        socket.close(listenfd)
    end

    print("PASS: basic API tests")

    -----------------------------------------------------------------
    -- Regression: close event and pending read behavior
    -----------------------------------------------------------------
    local base_port = 17700 + math.random(0, 200)

    -- Test 1: BUG-3 — single close event with correct reason
    print("\n--- Test 1: single close event with correct reason (BUG-3) ---")
    do
        local port = base_port
        local server_fd
        local listenfd = assert(socket.listen("127.0.0.1:" .. port, function(fd, addr)
            server_fd = fd
            socket.start_read_frame(fd)
        end))

        local client_fd = socket.connect("127.0.0.1:" .. port, 5000)
        assert(client_fd, "connect failed")
        moon.sleep(50)
        assert(server_fd, "accept callback not called")

        close_events = {}

        socket.close(client_fd)
        moon.sleep(200)

        local count = 0
        local reason
        for _, ev in ipairs(close_events) do
            if ev.fd == server_fd then
                count = count + 1
                reason = ev.err
            end
        end

        assert(count == 1,
            string.format("expected exactly 1 close event for server fd, got %d", count))
        assert(reason == "eof",
            string.format("expected close reason 'eof', got '%s'", tostring(reason)))
        print("PASS: single close event with reason 'eof'")

        socket.close(listenfd)
    end

    -- Test 2: BUG-4 — pending read unblocks on close
    print("\n--- Test 2: pending read unblocks on close (BUG-4) ---")
    do
        local port = base_port + 1
        local server_fd
        local listenfd = assert(socket.listen("127.0.0.1:" .. port, function(fd, addr)
            server_fd = fd
        end))

        local client_fd = socket.connect("127.0.0.1:" .. port, 5000)
        assert(client_fd, "connect failed")
        moon.sleep(50)
        assert(server_fd, "accept callback not called")

        local read_done = false
        local read_ok, read_err
        moon.async(function()
            read_ok, read_err = socket.read(server_fd, "\n")
            read_done = true
        end)

        moon.sleep(50)
        assert(not read_done, "read should still be pending")

        socket.close(client_fd)
        moon.sleep(200)

        assert(read_done, "read coroutine should have unblocked")
        assert(not read_ok, "read should have returned false, got: " .. tostring(read_ok))
        print(string.format("PASS: pending read unblocked with error: %s", tostring(read_err)))

        socket.close(server_fd)
        socket.close(listenfd)
    end

    -- Test 3: BUG-4 — pending read_frame unblocks on close
    print("\n--- Test 3: pending read_frame unblocks on close (BUG-4) ---")
    do
        local port = base_port + 2
        local server_fd
        local listenfd = assert(socket.listen("127.0.0.1:" .. port, function(fd, addr)
            server_fd = fd
        end))

        local client_fd = socket.connect("127.0.0.1:" .. port, 5000)
        assert(client_fd, "connect failed")
        moon.sleep(50)
        assert(server_fd, "accept callback not called")

        local read_done = false
        local read_ok, read_err
        moon.async(function()
            read_ok, read_err = socket.read_frame(server_fd)
            read_done = true
        end)

        moon.sleep(50)
        assert(not read_done, "read_frame should still be pending")

        socket.close(client_fd)
        moon.sleep(200)

        assert(read_done, "read_frame coroutine should have unblocked")
        assert(not read_ok, "read_frame should have returned false, got: " .. tostring(read_ok))
        print(string.format("PASS: pending read_frame unblocked with error: %s", tostring(read_err)))

        socket.close(server_fd)
        socket.close(listenfd)
    end

    -- Test 4: BUG-3 — server-initiated close gives single event
    print("\n--- Test 4: server-initiated close gives single event (BUG-3) ---")
    do
        local port = base_port + 3
        local server_fd
        local listenfd = assert(socket.listen("127.0.0.1:" .. port, function(fd, addr)
            server_fd = fd
        end))

        local client_fd = socket.connect("127.0.0.1:" .. port, 5000)
        assert(client_fd, "connect failed")
        socket.start_read_frame(client_fd)
        moon.sleep(50)
        assert(server_fd, "accept callback not called")

        close_events = {}

        socket.close(server_fd)
        moon.sleep(200)

        local count = 0
        for _, ev in ipairs(close_events) do
            if ev.fd == client_fd then
                count = count + 1
            end
        end

        assert(count == 1,
            string.format("expected exactly 1 close event for client fd, got %d", count))
        print("PASS: single close event on server-initiated close")

        socket.close(listenfd)
    end

    -- Test 5: BUG-4 — write-side death unblocks pending raw read
    print("\n--- Test 5: write-side death unblocks pending raw read (BUG-4) ---")
    do
        local port = base_port + 4
        local server_fd
        local listenfd = assert(socket.listen("127.0.0.1:" .. port, function(fd, addr)
            server_fd = fd
        end))

        local client_fd = socket.connect("127.0.0.1:" .. port, 5000)
        assert(client_fd, "connect failed")
        moon.sleep(50)
        assert(server_fd, "accept callback not called")

        local read_done = false
        local read_ok
        moon.async(function()
            read_ok = socket.read(server_fd, 999999)
            read_done = true
        end)

        moon.sleep(50)
        assert(not read_done, "large read should still be pending")

        socket.close(client_fd)
        moon.sleep(300)

        assert(read_done, "large read coroutine should have unblocked")
        assert(not read_ok, "large read should have returned false")
        print("PASS: write-side death unblocked pending raw read")

        socket.close(server_fd)
        socket.close(listenfd)
    end

    print("\n=== All socket tests passed! ===")
    moon.quit()
end)
