---@meta
-- IDE annotation file only. Do not require this file at runtime.

--- Native TCP socket module (`require("net.core")`).
---@class net.core
local net = {}

--- Listen on an address. Returns a listener fd.
---@param addr string @ e.g. `"127.0.0.1:8080"`
---@param opts? table @ `{ max_connections? }` cap on concurrently accepted connections (default 100000)
---@return integer fd
---@return false? err
---@return string? errmsg
function net.listen(addr, opts) end

--- Connect to a remote address asynchronously.
---@param addr string
---@param timeout? integer @ Connect timeout in ms (default 5000)
---@return integer session
function net.connect(addr, timeout) end

--- Read bytes or until delimiter asynchronously.
---@param fd integer
---@param size integer @ Read exactly `size` bytes
---@param read_timeout? integer @ Read timeout in ms (0 = no timeout)
---@return integer session
---@overload fun(fd: integer, delim: string, max_size?: integer, read_timeout?: integer): integer
function net.read(fd, size, read_timeout) end

--- Read one length-prefixed frame asynchronously.
---@param fd integer
---@param read_timeout? integer
---@return integer session
function net.read_frame(fd, read_timeout) end

--- Start auto frame-read mode (session=0, callback-based).
---@param fd integer
---@param read_timeout? integer
---@return boolean success
---@return false? err
---@return string? errmsg
function net.start_read_frame(fd, read_timeout) end

--- Write data to a socket. Accepts `buffer_arc_ptr` or string/buffer.
---@param fd integer
---@param data buffer_arc_ptr|string|buffer_ptr
---@param max_write_capacity? integer @ Backpressure limit (default `65535`)
---@param close? boolean @ Close after write
---@return boolean success
---@return false? err
---@return string? errmsg
function net.write(fd, data, max_write_capacity, close) end

--- Write length-prefixed frame data.
---@param fd integer
---@param data buffer_arc_ptr|string|buffer_ptr
---@param max_write_capacity? integer
---@param close? boolean
---@return boolean success
---@return false? err
---@return string? errmsg
function net.write_frame(fd, data, max_write_capacity, close) end

--- Close a socket fd.
---@param fd integer
---@return boolean
function net.close(fd) end

--- Resolve local IP by connecting to a remote address (default `"1.1.1.1:80"`).
---@param addr? string
---@return string? ip
function net.host(addr) end

return net
