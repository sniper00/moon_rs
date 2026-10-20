local moon = require "moon"
local c = require "mongodb.core"
local cursor_iterator = require "moon.db.cursor"

moon.register_protocol {
    name = "mongodb",
    PTYPE = moon.PTYPE_MONGODB,
    pack = function(...) return ... end,
}

---@async
local function operators(self, ...)
    local res = self.obj:operators(self.op_name, self.db_name, self.col_name, ...)
    if type(res) == "table" then
        return res
    end
    return moon.wait(res)
end

---@class Collection
---@field find_one fun(query:table, opts?:table):table
---@field find fun(query:table, opts?:table):table
---@field insert_one fun(doc:table):table
---@field insert_many fun(docs:table):table
---@field update_one fun(filter:table, update?:table):table
---@field update_many fun(filter:table, update?:table):table
---@field delete_one fun(filter:table):table
---@field delete_many fun(filter:table):table
---@field count fun(filter:table):table

---@class MongoDB
---@field obj mongodb_connection
local M = {}

---@async
---@nodiscard
---@param database_url string Database url e. "mongodb://127.0.0.1:27017"
---@param name string Connection name for find by other services
---@param queue_capacity? integer Request queue capacity. Default 1024
---@return MongoDB
function M.connect(database_url, name, queue_capacity)
    ---@diagnostic disable-next-line: redundant-parameter
    local res = moon.wait(c.connect(database_url, name, queue_capacity))
    if res.kind then
        error(string.format("connect database failed: %s", res.message))
    end
    return M.find_connection(name)
end

---@nodiscard
---@param name string Connection name
---@return MongoDB
function M.find_connection(name)
    local o = {
        obj = assert(c.find_connection(name), "connection not found")
    }
    return setmetatable(o, { __index = M })
end

---Statistics (pending/total/peak/workers) per named connection.
---@nodiscard
---@return table<string, pool_stats>
function M.stats()
    return c.stats()
end

function M:close()
    self.obj:close()
end

---@async
---@nodiscard
---@param db_name string
---@param col_name string
---@return Collection
function M:collection(db_name, col_name)
    return setmetatable({
        db_name = db_name,
        col_name = col_name,
        obj = self.obj
    }, {
        __index = function(t, op_name)
            t.op_name = op_name
            return t
        end,
        __call = function(_t, ...)
            ---@diagnostic disable-next-line: await-in-sync
            return operators(...)
        end
    })
end

-- Streaming cursor iteration that auto-releases the server-side cursor via the
-- generic-for to-be-closed mechanism.
--
---@async
---@param db_name string
---@param col_name string
---@param query table Filter document
---@param opts? table FindOptions
---@param batch_size? integer Documents per batch (default 100)
---@return fun():table|nil Iterator function returning one doc at a time
---@return nil
---@return nil
---@return table to-be-closed stream state
function M:find_stream(db_name, col_name, query, opts, batch_size)
    local res = self.obj:operators("find_stream", db_name, col_name, query, opts, batch_size)
    if type(res) == "table" then
        ---@diagnostic disable-next-line: return-type-mismatch, missing-return-value
        return nil, res.message
    end
    return cursor_iterator(res)
end

return M
