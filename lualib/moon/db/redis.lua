-- Redis driver backed by the native `redis.core` Rust extension.
--
-- Connection pooling and reconnect are handled natively by the Rust pool.
-- This module is a thin, session-based async wrapper around `redis.core`,
-- mirroring `moon.db.pg`.

local moon = require("moon")
local c = require("redis.core")

moon.register_protocol {
    name = "redis",
    PTYPE = moon.PTYPE_REDIS,
    pack = function(...) return ... end,
}

---@class redis_result
---@field public code? string @ error kind: "SOCKET" | "REDIS"
---@field public message? string @ error message

---@class redis
---@field obj redis_pool
local M = {}
M.__index = M

local string_upper = string.upper

---Look up an already-connected pool by name and wrap it.
---@param name string
---@return redis
function M.find_connection(name)
    local o = {
        obj = assert(c.find_connection(name), "redis connection not found: " .. tostring(name))
    }
    return setmetatable(o, M)
end

---Connect and register a named pool.
---
---All parameters live in a single connection URL of the form
---`redis://username:password@host:port/db?param=value&...`.
---The `/db` segment is optional and defaults to `0`.
---The pool settings are supplied as `?param=value` query parameters:
---  * `name` — pool name for `find_connection` (default "default")
---  * `connect_timeout` — connect timeout in ms (default 5000)
---  * `pool_size`/`max_connections` — pool size (default 1)
---  * `read_timeout` — read timeout in ms (default 10000)
---  * `queue_capacity` — per-worker request queue capacity (default 1024)
---
---e.g. `"redis://:123456@127.0.0.1:6379/0?name=main&pool_size=2"`
---@async
---@param url string connection URL (`redis://user:pass@host:port/db?param=value`)
---@return redis|nil @ connection object, or nil on error
---@return string|nil @ error message on failure
function M.connect(url)
    local res = moon.wait(c.connect(url))
    if res.code then
        return nil, res.message
    end
    return M.find_connection(res.name)
end

---Pending request count per pool worker (async queue lengths).
---@return integer[]
function M:len()
    return self.obj:len()
end

---Statistics (pending/total/peak/workers) per named pool.
---@nodiscard
---@return table<string, pool_stats>
function M.stats()
    return c.stats()
end

function M:close()
    self.obj:close()
end
M.disconnect = M.close

-- Dynamic command dispatch via __index metamethod.
-- M:get("key") → obj:command("GET", "key") → wait
-- M:set("key", "val") → obj:command("SET", "key", "val") → wait
-- M:hgetall("key") → obj:command("HGETALL", "key") → wait
local command_cache = {}

setmetatable(M, {
    __index = function(_, cmd)
        local f = command_cache[cmd]
        if f then return f end
        local CMD = string_upper(cmd)
        f = function(self, ...)
            local res = self.obj:command(CMD, ...)
            if type(res) == "table" then
                return res
            end
            ---@diagnostic disable-next-line: await-in-sync
            return moon.wait(res)
        end
        command_cache[cmd] = f
        return f
    end
})

---Execute a single Redis command (fire-and-forget, no response).
---@param cmd string @ e.g. "SET"
---@vararg any
function M:execute(cmd, ...)
    local res = self.obj:exec_command(string_upper(cmd), ...)
    if type(res) == "table" and res.code then
        moon.error(table.tostring(res))
    end
end

---Execute pipelined commands (async, await all responses).
---@async
---@param ops table @ `{ {"SET", "k", "v"}, {"GET", "k"}, ... }`
---@return table @ array of results
function M:pipeline(ops)
    local res = self.obj:pipeline(ops)
    if type(res) == "table" then
        return res
    end
    return moon.wait(res)
end

---Execute pipelined commands (fire-and-forget).
---@param ops table @ `{ {"SET", "k", "v"}, {"GET", "k"}, ... }`
function M:execute_pipeline(ops)
    local res = self.obj:exec_pipeline(ops)
    if type(res) == "table" and res.code then
        moon.error(table.tostring(res))
    end
end

---------------------------------------------------------------------------
-- Pub/Sub watch (native redis.core)
---------------------------------------------------------------------------

---@class redis_watcher
---@field obj redis_watch
local watch_meta = {}
watch_meta.__index = watch_meta

---Subscribe to channels.
---@vararg string
---@return boolean|table
function watch_meta:subscribe(...)
    return self.obj:subscribe(...)
end

---Pattern-subscribe.
---@vararg string
---@return boolean|table
function watch_meta:psubscribe(...)
    return self.obj:psubscribe(...)
end

---Unsubscribe from channels.
---@vararg string
---@return boolean|table
function watch_meta:unsubscribe(...)
    return self.obj:unsubscribe(...)
end

---Pattern-unsubscribe.
---@vararg string
---@return boolean|table
function watch_meta:punsubscribe(...)
    return self.obj:punsubscribe(...)
end

---Close the watch connection.
---@return boolean
function watch_meta:disconnect()
    return self.obj:close()
end

---Wait for the next pub/sub message.
---@async
---@return string|nil message
---@return string|nil channel
---@return string|nil pattern
function watch_meta:message()
    local session = self.obj:message()
    if type(session) == "table" and session.code then
        return nil
    end
    local res = moon.wait(session)
    if type(res) == "table" and res.code then
        return nil
    end
    local ttype = res[1]
    if ttype == "message" then
        return res[3], res[2]
    elseif ttype == "pmessage" then
        return res[4], res[3], res[2]
    end
end

---Create a dedicated pub/sub connection (Rust `redis.core.watch`).
---Accepts the same `redis://...` URL as `connect` (pool-only params such as
---`name`/`pool_size` are ignored).
---@async
---@param url string @ connection URL, e.g. `"redis://127.0.0.1:6379/0"`
---@return redis_watcher|nil watcher
---@return string|nil err
function M.watch(url)
    local session = c.watch(url)
    local res = moon.wait(session)
    if type(res) == "table" and res.code then
        return nil, res.message
    end
    return setmetatable({ obj = res }, watch_meta)
end

return M
