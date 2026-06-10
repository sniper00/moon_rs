---@meta
-- IDE annotation file only. Do not require this file at runtime.
--- Legacy filename for `moon.core` IDE stubs. See also `moon/core.lua`.

--- lightuserdata, Rust `Buffer*`
---@class buffer_ptr

--- Arc<Buffer> userdata (`shared_buffer` metatable)
---@class buffer_arc_ptr

--- lightuserdata, Rust `Message*`
---@class message_ptr

--- lightuserdata, Rust `char*` + length pair
---@class cstring_ptr

--- Native runtime module (`require("moon.core")`).
---@class moon.core
---@field public id integer @ Current service actor id
---@field public name string @ Current service name
local core = {}

--- Monotonic clock (same as `os.clock()`), in seconds.
---@return number
function core.clock() end

--- Convert a C string pointer + length to a Lua string.
---@param sz cstring_ptr
---@param len integer
---@return string
function core.tostring(sz, len) end

--- Set or get an environment variable.
---@param key string
---@param value? string
---@return string
function core.env(key, value) end

--- Print a log message. First argument is log level string: `DBUG`, `INFO`, `WARN`, `EROR`.
---@param loglv string
---@vararg any
function core.log(loglv, ...) end

--- Get or set the global log level.
---@param lv? string @ `DBUG`, `INFO`, `WARN`, `EROR`
---@return integer
function core.loglevel(lv) end

--- Create a new Lua service (actor).
---@param opts table @ `{ name?, source, unique?, memlimit? }`
---@param params string @ Bootstrap params (PATH env is prepended)
---@return integer session @ Session for the create response
function core.new_service(opts, params) end

--- Look up a unique service id by name. Returns `0` if not found.
---@param name string
---@return integer
function core.query(name) end

--- Kill a service by actor id.
---@param addr integer
---@return boolean success
---@return string? err
function core.kill(addr) end

--- Send a message to another service.
---@param ptype integer @ Message type (`PTYPE_*`, must be > 0)
---@param to integer @ Receiver actor id
---@param data buffer_ptr|string @ Message body
---@param session? integer @ Defaults to `next_session()`
---@param from? integer @ Defaults to current service id
---@return integer session
---@return integer to
function core.send(ptype, to, data, session, from) end

--- Register the message dispatch callback for this service.
---@param fn fun(msg: message_ptr, ptype: integer)
function core.callback(fn) end

--- Schedule a timer. `interval <= 0` fires immediately.
---@param interval integer @ Milliseconds; `<= 0` for immediate
---@return integer timer_id
function core.timeout(interval) end

--- Allocate the next session id for outbound requests.
---@return integer
function core.next_session() end

--- Shut down the server. Non-negative exit code waits for all services to quit.
---@param exitcode integer
function core.exit(exitcode) end

--- Server timestamp in milliseconds.
---@return integer
function core.now() end

--- Decode fields from a runtime message.
---
--- Pattern characters:
--- - `'S'` sender id
--- - `'R'` receiver id
--- - `'E'` session id
--- - `'Z'` message bytes as string
--- - `'N'` message size
--- - `'B'` message buffer lightuserdata
--- - `'C'` buffer data pointer + size
---@param msg message_ptr
---@param pattern string
---@return ...
---@nodiscard
function core.decode(msg, pattern) end

--- Decode a runtime message into Lua values (central Rust decoder registry).
---@param msg message_ptr
---@return ...
function core.decode_message(msg) end

return core
