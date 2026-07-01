local moon = require("moon")
local redis = require("moon.db.redis")

---@class RedisStream
---@field private url string
---@field private pool_name string
---@field private db table?
local RedisStream = {}

local nil_ok_commands = {
    XREADGROUP = true,
    XREAD = true,
    XPENDING = true,
}

local function is_socket_error(res)
    return type(res) == "table" and res.code == "SOCKET"
end

---@param url string @ base connection URL, e.g. `"redis://127.0.0.1:6379/0"`
---@param pool_name? string
---@return RedisStream
function RedisStream.new(url, pool_name)
    return setmetatable({
        url = url,
        pool_name = pool_name or "redis_stream",
        db = nil,
    }, { __index = RedisStream })
end

---@param cmd string
---@return any
function RedisStream:exec(cmd, ...)
    local failed_times = 0
    local err, res

    while true do
        if self.db then
            local method = self.db[string.lower(cmd)]
            res, err = method(self.db, ...)
            if is_socket_error(res) then
                self.db:close()
                self.db = nil
                failed_times = failed_times + 1
            elseif not res and not nil_ok_commands[cmd] then
                if err then
                    error(string.format("Redis command '%s' failed: %s", cmd, err))
                else
                    error(string.format("Redis command '%s' returned nil", cmd))
                end
            else
                return res
            end
        else
            if failed_times > 3 then
                error(string.format(
                    "Failed to connect to Redis after %d attempts: %s",
                    failed_times, err or "unknown error"))
            end
            self.db, err = redis.connect(string.format("%s?name=%s&connect_timeout=5000&pool_size=1",
                self.url, self.pool_name))
            if not self.db then
                failed_times = failed_times + 1
                moon.sleep(1000)
            end
        end
    end
end

function RedisStream:xgroup_create(stream_key, group_name, start_id)
    start_id = start_id or "0"
    return self:exec("XGROUP", "CREATE", stream_key, group_name, start_id, "MKSTREAM")
end

function RedisStream:xgroup_destroy(stream_key, group_name)
    return self:exec("XGROUP", "DESTROY", stream_key, group_name)
end

function RedisStream:xadd(stream_key, ...)
    return self:exec("XADD", stream_key, "*", ...)
end

function RedisStream:xreadgroup(stream_key, group_name, consumer_name, count, block)
    count = count or 100
    if block then
        return self:exec("XREADGROUP", "GROUP", group_name, consumer_name,
            "COUNT", tostring(count), "BLOCK", tostring(block),
            "STREAMS", stream_key, ">")
    end
    return self:exec("XREADGROUP", "GROUP", group_name, consumer_name,
        "COUNT", tostring(count), "STREAMS", stream_key, ">")
end

function RedisStream:xack(stream_key, group_name, ...)
    return self:exec("XACK", stream_key, group_name, ...)
end

function RedisStream:xdel(stream_key, ...)
    return self:exec("XDEL", stream_key, ...)
end

function RedisStream:xtrim(stream_key, maxlen, approximate)
    if approximate == nil then
        approximate = true
    end
    if approximate then
        return self:exec("XTRIM", stream_key, "MAXLEN", "~", tostring(maxlen))
    end
    return self:exec("XTRIM", stream_key, "MAXLEN", tostring(maxlen))
end

function RedisStream:xlen(stream_key)
    return self:exec("XLEN", stream_key)
end

function RedisStream:xpending(stream_key, group_name)
    return self:exec("XPENDING", stream_key, group_name)
end

function RedisStream:disconnect()
    if self.db then
        self.db:close()
        self.db = nil
    end
end

return RedisStream
