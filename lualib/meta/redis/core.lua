---@meta
-- IDE annotation file only. Do not require this file at runtime.

--- Per-connection pool statistics.
---@class pool_stats
---@field pending integer @ Requests dispatched but not yet replied to (current backpressure).
---@field total integer @ Cumulative requests ever dispatched (lifetime).
---@field peak integer @ High-water mark of simultaneous pending requests.
---@field workers integer @ Number of worker tasks backing the connection.

--- Redis connection pool userdata.
---@class redis_pool
local redis_pool = {}

--- Redis pub/sub watch session userdata.
---@class redis_watch
local redis_watch = {}

--- Native Redis driver (`require("redis.core")`).
---@class redis.core
local redis = {}

--- Connect and register a named connection pool asynchronously.
--- All settings are carried in a single connection URL of the form
--- `redis://username:password@host:port/db?param=value&...`, e.g.
--- `"redis://:123456@127.0.0.1:6379/0?name=main&pool_size=2"`.
--- Pool query params: `name` (default "default"), `connect_timeout` (ms),
--- `pool_size`/`max_connections`, `read_timeout` (ms), `queue_capacity`.
---@param url string @ connection URL with `?param=value` pool settings
---@return integer session
function redis.connect(url) end

--- Look up a registered pool by name.
---@param name string
---@return redis_pool? pool
function redis.find_connection(name) end

--- Open a dedicated pub/sub connection asynchronously.
--- Accepts the same `redis://...` URL as `connect`.
---@param url string @ connection URL, e.g. `"redis://127.0.0.1:6379/0"`
---@return integer session
function redis.watch(url) end

--- Statistics per named pool.
---@return table<string, pool_stats>
function redis.stats() end

--- Execute a Redis command asynchronously.
---@param pool redis_pool
---@param cmd string
---@vararg string|number
---@return integer session
function redis_pool:command(cmd, ...) end

--- Execute a Redis pipeline asynchronously.
---@param pool redis_pool
---@vararg table @ Each element is `{ cmd, ...args }`
---@return integer session
function redis_pool:pipeline(...) end

--- Fire-and-forget command (no response).
---@param pool redis_pool
---@param cmd string
---@vararg string|number
---@return boolean
function redis_pool:exec_command(cmd, ...) end

--- Fire-and-forget pipeline.
---@param pool redis_pool
---@vararg table
---@return boolean
function redis_pool:exec_pipeline(...) end

--- Pending request counts per pool worker.
---@param pool redis_pool
---@return integer[]
function redis_pool:len() end

--- Close and unregister the pool.
---@param pool redis_pool
function redis_pool:close() end

--- Subscribe to channels.
---@param watch redis_watch
---@vararg string channel
---@return boolean|table @ `true` or `{ code, message }`
function redis_watch:subscribe(...) end

--- Pattern-subscribe.
---@param watch redis_watch
---@vararg string pattern
---@return boolean|table
function redis_watch:psubscribe(...) end

--- Unsubscribe from channels.
---@param watch redis_watch
---@vararg string channel
---@return boolean|table
function redis_watch:unsubscribe(...) end

--- Pattern-unsubscribe.
---@param watch redis_watch
---@vararg string pattern
---@return boolean|table
function redis_watch:punsubscribe(...) end

--- Wait for the next pub/sub message asynchronously.
---@param watch redis_watch
---@return integer session|table @ Session id or error table
function redis_watch:message() end

--- Close the watch connection.
---@param watch redis_watch
---@return boolean
function redis_watch:close() end

return redis
