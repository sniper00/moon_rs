local socket = require "moon.socket"
local moon   = require "moon"
local buffer = require "buffer"

local server_fds = {}
local client_fd = nil

local received_server = {}
local received_client = {}
local accepted_fds = {}
local closed_info = {}

socket.on("message", function(fd, buf_ptr)
    local data = buffer.unpack(buf_ptr, "Z")
    if server_fds[fd] then
        print(string.format("[server] message fd=%d len=%d", fd, #data))
        table.insert(received_server, { fd = fd, data = data })
        socket.write_frame(fd, data)
    else
        print(string.format("[client] message fd=%d len=%d", fd, #data))
        table.insert(received_client, { fd = fd, data = data })
    end
end)

socket.on("close", function(fd, remote_addr, err)
    print(string.format("[close] fd=%d addr=%s err=%s", fd, remote_addr, err))
    table.insert(closed_info, { fd = fd, addr = remote_addr, err = err })
    server_fds[fd] = nil
end)

moon.async(function()
    print("--- Test 1: listen + connect ---")

    local listenfd = assert(socket.listen("127.0.0.1:19877", function(fd, remote_addr)
        print(string.format("[server] accept fd=%d addr=%s", fd, remote_addr))
        server_fds[fd] = true
        table.insert(accepted_fds, fd)
        socket.start_read_frame(fd)
    end))
    print("listen fd:", listenfd)

    client_fd = socket.connect("127.0.0.1:19877", 5000)
    assert(client_fd, "connect failed")
    print("client fd:", client_fd)

    -- Start auto-read on client side too
    socket.start_read_frame(client_fd)

    moon.sleep(100)

    assert(#accepted_fds == 1, "expected 1 accepted fd, got " .. #accepted_fds)
    print("PASS: accept and connect events")

    -- Test 2: client -> server message
    print("\n--- Test 2: client -> server message ---")
    socket.write_frame(client_fd, "Hello Moon!")

    moon.sleep(100)

    assert(#received_server == 1, "expected 1 server msg, got " .. #received_server)
    assert(received_server[1].data == "Hello Moon!",
        "data mismatch: " .. tostring(received_server[1].data))
    print("PASS: server received message")

    -- Test 3: server echo -> client
    print("\n--- Test 3: server echo -> client ---")
    assert(#received_client == 1, "expected 1 client msg, got " .. #received_client)
    assert(received_client[1].data == "Hello Moon!",
        "echo mismatch: " .. tostring(received_client[1].data))
    print("PASS: client received echo")

    -- Test 4: multiple messages
    print("\n--- Test 4: multiple messages ---")
    local srv_before = #received_server
    local cli_before = #received_client
    for i = 1, 5 do
        socket.write_frame(client_fd, "msg_" .. i)
    end

    moon.sleep(200)

    assert(#received_server - srv_before == 5,
        "expected 5 server msgs, got " .. (#received_server - srv_before))
    assert(#received_client - cli_before == 5,
        "expected 5 client echoes, got " .. (#received_client - cli_before))
    print("PASS: multiple messages and echoes")

    -- Test 5: large message (chunked, >65535 bytes)
    print("\n--- Test 5: large chunked message (100KB) ---")
    local large_data = string.rep("X", 100000)
    srv_before = #received_server
    cli_before = #received_client
    socket.write_frame(client_fd, large_data)

    moon.sleep(500)

    assert(#received_server - srv_before == 1,
        "expected 1 large server msg, got " .. (#received_server - srv_before))
    assert(received_server[#received_server].data == large_data,
        "large message data mismatch on server, len=" .. #received_server[#received_server].data)
    assert(#received_client - cli_before == 1,
        "expected 1 large client echo, got " .. (#received_client - cli_before))
    assert(received_client[#received_client].data == large_data,
        "large echo data mismatch on client, len=" .. #received_client[#received_client].data)
    print("PASS: large chunked message (100KB)")

    -- Test 6: close connection
    print("\n--- Test 6: close connection ---")
    local close_before = #closed_info
    socket.close(client_fd)

    moon.sleep(200)

    assert(#closed_info > close_before, "close event not received")
    print("PASS: close event received")

    -- Cleanup
    socket.close(listenfd)

    print("\n=== All moon socket tests passed! ===")
    moon.quit()
end)
