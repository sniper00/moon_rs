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
---@field public obj userdata
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
---@async
---@param opts table @ `{ host = "127.0.0.1", port = 6379, auth = "password", db = 0 }`
---@param name? string @ Pool name for lookup (default "default")
---@param timeout? integer @ Connect timeout in milliseconds (default 5000)
---@param pool_size? integer @ Pool size (default 1)
---@param queue_capacity? integer @ Per-worker request queue capacity (default 1024)
---@return redis|nil @ connection object, or nil on error
---@return string|nil @ error message on failure
function M.connect(opts, name, timeout, pool_size, queue_capacity)
    ---@diagnostic disable-next-line: redundant-parameter
    local res = moon.wait(c.connect(opts, name, timeout, pool_size, queue_capacity))
    if res.code then
        return nil, res.message
    end
    return M.find_connection(name or "default")
end

---Pending request count per pool worker (async queue lengths).
---@return integer[]
function M:len()
    return self.obj:len()
end

---Total pending requests across all named pools.
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

local watch_meta = {}
watch_meta.__index = watch_meta

function watch_meta:subscribe(...)
    return self.obj:subscribe(...)
end

function watch_meta:psubscribe(...)
    return self.obj:psubscribe(...)
end

function watch_meta:unsubscribe(...)
    return self.obj:unsubscribe(...)
end

function watch_meta:punsubscribe(...)
    return self.obj:punsubscribe(...)
end

function watch_meta:disconnect()
    return self.obj:close()
end

---Wait for the next pub/sub message.
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
---@param db_conf table @ `{ host, port?, auth?, db?, timeout? }`
---@return table|nil watcher
---@return string|nil err
function M.watch(db_conf)
    local session = c.watch(db_conf)
    local res = moon.wait(session)
    if type(res) == "table" and res.code then
        return nil, res.message
    end
    return setmetatable({ obj = res }, watch_meta)
end

return M
