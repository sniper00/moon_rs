local moon = require "moon"
local core = require "net.core"

---@class socket
local socket = {
    ---@type fun(addr: string):integer @ Listens on the specified address. return listenfd
    listen = core.listen,
    ---@type fun(fd: integer, data: string|buffer_ptr, close?: boolean) @ Writes data to the socket.
    write = core.write,
    ---@type fun(fd: integer) Closes the socket.
    close = core.close,
    ---@type fun(query_addr?:string):string @ This function is used to connect to a host `query_addr` and return the local IP address. query_addr default is "1.1.1.1:80".
    host = core.host,
}

--- Accepts a new connection on a socket.
--- @async
--- @param listenfd integer @ The file descriptor of the listening socket.
---@return integer|false,string? @ Returns the file descriptor of the new connection if successful, or `false` and an error message if failed.
function socket.accept(listenfd)
    return moon.wait(core.accept(listenfd))
end

--- Connects to a remote address.
--- @async
--- @param addr string @ The remote address in the format of "host:port".
--- @param timeout? integer @ Optional. The connect timeout in milliseconds. Default is 5000ms.
---@return integer|false, string? @ Returns the file descriptor of the new connection if successful, or `false` and an error message if failed.
function socket.connect(addr, timeout)
    return moon.wait(core.connect(addr, timeout))
end

--- Reads data from a socket.
--- @async
--- @param fd integer @ The file descriptor of the socket.
--- @param delim string @ The delimiter. The function reads until it reaches the specified delimiter.
--- @param maxcount? integer @ Optional. The maximum number of bytes to read.
--- @param timeout? integer @ Optional. The read timeout in milliseconds.c Default is 0, which means no timeout.
--- @overload fun(fd: integer, count: integer, timeout?:integer) @ Reads a specified number of bytes from the socket.
---@return string|false, string? @ Returns the read data if successful, or `false` and an error message if failed.
function socket.read(fd, delim, maxcount, timeout)
    return moon.wait(core.read(fd, delim, maxcount, timeout))
end

return socket
