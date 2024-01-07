local moon = require "moon"
local core = require "net.core"

local socket = {}

---@type fun(addr: string):integer @ listen on the specified addr, return listenfd
socket.listen = core.listen
---@type fun(fd: integer, data: string, close?: boolean) @ write data to the socket
socket.write = core.write
---@type fun(fd: integer, timeout: integer) @ set read timeout in milliseconds
socket.settimeout = core.settimeout
---@type fun(fd: integer)
socket.close = core.close

---@async
---@param listenfd integer
function socket.accept(listenfd)
    local session = moon.make_session()
    core.accept(listenfd, session)
    local fd, err = moon.wait(session)
    if not fd then
        return nil, err
    end
    return fd
end

---@async
---@param addr string # host:port
---@param timeout? integer # connect timeout in milliseconds, default 5000ms
---@return integer|nil,string?
function socket.connect(addr, timeout)
    local session = moon.make_session()
    core.connect(session, addr)
    local fd, err = moon.wait(session)
    if not fd then
        return nil, err
    end
    return fd
end

---@async
---@param delim string @read until reach the specified delim string from the socket
---@param maxcount? integer
---@param timeout? integer @ read timeout in milliseconds, default 0 means no timeout
---@overload fun(fd: integer, count: integer, timeout?:integer) @ read a specified number of bytes from the socket.
function socket.read(fd, delim, maxcount, timeout)
    local session = moon.make_session()
    core.read(fd, session, delim, maxcount, timeout)
    return moon.wait(session)
end

return socket
