local moon = require "moon"
local core = require "net.core"
local buffer = require "buffer"

---@alias socket_event
---| 'message' # Data message received (frame protocol auto-read).
---| 'close'   # Socket closed

---@type table<string, integer>
local socket_data_type = {
    message = 3,
    close = 4,
}

---@type table<integer, fun(fd: integer, ...)?>
local event_callbacks = {}

---@type table<integer, fun(fd: integer, addr: string)?>
local accept_callbacks = {}

local ACCEPT_EVENT = 2
local MESSAGE_EVENT = 3
local CLOSE_EVENT = 4

local socket_pool = setmetatable({}, {
    __gc = function(p)
        for fd in pairs(p) do
            core.close(fd)
        end
    end
})

moon.register_protocol {
    name = "tcp",
    PTYPE = moon.PTYPE_SOCKET_TCP,
    pack = function(...) return ... end,
    dispatch = function() end
}

moon.register_protocol {
    name = "socket_event",
    PTYPE = moon.PTYPE_SOCKET_EVENT,
    pack = function(...) return ... end,
    dispatch = function(_, _, first, event_type, val, ...)
        if event_type == ACCEPT_EVENT then
            -- first = listen_fd, val = conn_fd, ... = remote_addr
            socket_pool[val] = true
            local cb = accept_callbacks[first]
            if cb then
                cb(val, ...)
            end
            return
        end

        if event_type == CLOSE_EVENT then
            socket_pool[first] = nil
        end

        local fn = event_callbacks[event_type]
        if fn then
            local ok, ret = pcall(fn, first, val, ...)
            if not ok then
                moon.error(ret)
            elseif ret then
                return
            end
        end

        if event_type == MESSAGE_EVENT then
            buffer.drop(val)
        end
    end
}

---@class socket
local socket = {
    ---@type fun(fd: integer, data: string|buffer_ptr, max_write_capacity?: integer, close?: boolean) @ Writes data to the socket.
    write = core.write,
    ---@type fun(fd: integer, data: string|buffer_ptr, max_write_capacity?: integer, close?: boolean) @ Writes data with frame protocol header.
    write_frame = core.write_frame,
    ---@type fun(query_addr?:string):string @ This function is used to connect to a host `query_addr` and return the local IP address. query_addr default is "1.1.1.1:80".
    host = core.host,
    ---@type fun(fd: integer, read_timeout?: integer):boolean @ Start auto-read frame protocol mode (callback-based via socket.on("message")).
    start_read_frame = core.start_read_frame,
}

--- Closes the socket and removes it from the tracking pool.
---@param fd integer @ The file descriptor to close.
function socket.close(fd)
    socket_pool[fd] = nil
    core.close(fd)
end

--- Removes the fd from the tracking pool without closing it.
--- Use this when transferring ownership of a fd to another actor/service.
---@param fd integer @ The file descriptor to unlink.
function socket.unlink(fd)
    socket_pool[fd] = nil
end

--- Listens on the specified address with auto-accept.
--- Each accepted connection invokes `on_accept(conn_fd, remote_addr)`.
--- Multiple listeners can coexist, each with its own callback.
--- @param addr string @ The address to listen on (e.g. "0.0.0.0:8080").
--- @param on_accept fun(fd: integer, addr: string) @ Callback invoked for each accepted connection.
--- @param opts? table @ `{ max_connections? }` cap on concurrently accepted connections (default 100000).
---@return integer|false, string? @ Returns the listen fd if successful, or `false` and an error message.
function socket.listen(addr, on_accept, opts)
    local fd, err = core.listen(addr, opts)
    if not fd then
        return fd, err
    end
    socket_pool[fd] = true
    accept_callbacks[fd] = on_accept
    return fd
end

--- Connects to a remote address.
--- @async
--- @param addr string @ The remote address in the format of "host:port".
--- @param timeout? integer @ Optional. The connect timeout in milliseconds. Default is 5000ms.
---@return integer|false, string? @ Returns the file descriptor of the new connection if successful, or `false` and an error message if failed.
function socket.connect(addr, timeout)
    local fd, err = moon.wait(core.connect(addr, timeout))
    if fd then
        socket_pool[fd] = true
    end
    return fd, err
end

--- Reads data from a socket (TCP raw protocol).
--- @async
--- @param fd integer @ The file descriptor of the socket.
--- @param delim string|integer @ The delimiter string, or number of bytes to read.
--- @param maxcount? integer @ Optional. The maximum number of bytes to read (only for string delim).
--- @param timeout? integer @ Optional. Read timeout in milliseconds. 0 means no timeout.
--- @overload fun(fd: integer, count: integer, timeout?: integer): string|false, string?
---@return string|false, string? @ Returns the read data if successful, or `false` and an error message if failed.
function socket.read(fd, delim, maxcount, timeout)
    if type(delim) == "number" then
        return moon.wait(core.read(fd, delim, maxcount))
    else
        return moon.wait(core.read(fd, delim, maxcount, timeout))
    end
end

--- Reads one framed message from fd (coroutine mode).
--- Frame protocol: [2-byte BE length][payload]. length=0xFFFF means continuation chunk;
--- length<0xFFFF is the final chunk. Multiple chunks are concatenated into one message.
--- @async
--- @param fd integer @ The file descriptor of the socket.
--- @param timeout? integer @ Optional. Read timeout in milliseconds. 0 means no timeout.
---@return buffer_ptr|false, string? @ Returns buffer pointer if successful, or `false` and an error message if failed.
function socket.read_frame(fd, timeout)
    return moon.wait(core.read_frame(fd, timeout))
end

---Register a callback for socket events.
---@param name socket_event The socket event type to register for
---@param cb fun(fd: integer, ...) The callback function to handle the event
---  - on("message", function(fd, buffer_ptr) end)  -- frame protocol auto-read, Callback must return true if it takes ownership of buffer_ptr, to prevent double-free.
---  - on("close",   function(fd, remote_addr, err) end)
function socket.on(name, cb)
    local n = socket_data_type[name]
    if n then
        event_callbacks[n] = cb
    else
        error("register unsupported socket event type: " .. name)
    end
end

return socket
