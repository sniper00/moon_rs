---@meta
-- IDE annotation file only. Do not require this file at runtime.

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

--- Set or get an environment variable. With `value`, stores it and returns nothing;
--- otherwise returns the stored value, or `nil` when the key is unset.
---@param key string
---@param value? string
---@return string?
function core.env(key, value) end

--- Print a log message.
---@param loglv integer @ Log level (`LOG_DEBUG`/`LOG_INFO`/`LOG_WARN`/`LOG_ERROR`)
---@param stack_level integer @ `lua_getstack` level used to attach the source location
---@vararg any @ Values to log; each is serialized and joined with spaces
function core.log(loglv, stack_level, ...) end

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
--- An integer argument is returned unchanged (treated as an already-resolved id).
---@param name string|integer
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
---@param data? buffer_ptr|string @ Message body
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

--- Server UTC timestamp. Returns `timestamp_millis / unit`, so `now(1)` is
--- milliseconds and `now(1000)` is seconds.
---@param unit? integer @ Divisor applied to the millisecond timestamp; defaults to `1` (clamped to `>= 1`)
---@return integer
function core.now(unit) end

--- Query runtime statistics.
---
--- Called with no argument, returns a JSON string snapshot keyed by the same
--- names below. Called with a `key`, returns the matching scalar counter:
--- - `"service.count"` live actor count
--- - `"service.registered"` routing entries (includes pseudo-actors)
--- - `"service.unique"` unique/named services
--- - `"service.created"` total actors ever created
--- - `"log.error_count"` total error-level logs
--- - `"log.queue"` log lines enqueued but not yet flushed by the logger thread
--- - `"timer.count"` scheduled-but-unfired timers
--- - `"env.count"` runtime env vars
--- - `"time.offset"` simulated-clock offset (ms)
--- - `"time.now"` server timestamp (ms)
--- - `"uptime"` process uptime (s)
--- - `"memory.total"` total Lua memory across all actors (bytes)
--- - `"message.total"` total messages dispatched across all actors
--- - `"cpu.total_ms"` total dispatch time across all actors (ms)
---
--- The JSON snapshot additionally contains a `services` array with one entry per
--- actor: `{ id, name, memory, messages, cpu_ms }` (per-actor stats tracked on
--- each actor's watchdog).
---@param key? string @ Counter name; omit to get the full JSON snapshot
---@return string|integer @ JSON string when `key` is omitted; an integer (`0` for unknown keys) otherwise
function core.server_stats(key) end

--- Decode fields from a runtime message.
---
--- Pattern characters (one return value each, except `'C'` which returns two):
--- - `'T'` message type (`PTYPE_*`)
--- - `'S'` sender id
--- - `'R'` receiver id
--- - `'E'` session id
--- - `'Z'` message bytes as string (`nil` if no buffer)
--- - `'N'` message size (`0` if no buffer)
--- - `'B'` message buffer lightuserdata (`nil` if no buffer)
--- - `'C'` buffer data pointer + size (null pointer + `0` if no buffer)
--- - `'L'` transfers buffer ownership to Lua as a lightuserdata; the receiver
---   must release it (`buffer.drop` / `buffer.into_arc_buffer`) or forward it
---   via `moon.send` (`nil` if no buffer)
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
