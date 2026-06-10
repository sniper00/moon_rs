---@meta
-- IDE annotation file only. Do not require this file at runtime.

--- WebSocket connection userdata.
---@class ws_connection

--- Native WebSocket module (`require("ws.core")`).
---@class ws.core
local ws = {}

--- Connect to a WebSocket server asynchronously.
---@param opts table @ `{ url, connect_timeout?, max_message_size?, max_frame_size? }`
---@return integer session
function ws.connect(opts) end

--- Listen for WebSocket connections. Returns a listener fd.
---@param addr string
---@param opts? table @ `{ max_message_size?, max_frame_size?, max_connections?, origins? }` (origins = string[] Origin allow-list for CSWSH protection)
---@return integer fd
---@return false? err
---@return string? errmsg
function ws.listen(addr, opts) end

--- Accept the next WebSocket connection on a listener fd.
---@param fd integer
---@return integer session
---@return false? err
---@return string? errmsg
function ws.accept(fd) end

--- Look up a WebSocket connection by fd.
---@param id integer
---@return ws_connection? conn
function ws.find_connection(id) end

--- Read the next WebSocket frame asynchronously.
---@param conn ws_connection
---@return integer session
---@return false? err
---@return string? errmsg
function ws_connection:read() end

--- Write a WebSocket frame.
---@param conn ws_connection
---@param data string|buffer_ptr
---@param kind? string @ `"t"` text (default), `"b"` binary, `"p"` ping, `"o"` pong, `"c"` close
---@return boolean|table
function ws_connection:write(data, kind) end

--- Close the WebSocket connection.
---@param conn ws_connection
---@return boolean|table
function ws_connection:close() end

return ws
