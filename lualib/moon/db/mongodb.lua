local moon = require "moon"
local c = require "mongodb.core"

moon.register_protocol {
    name = "mongodb",
    PTYPE = moon.PTYPE_MONGODB,
    pack = function(...) return ... end,
    unpack = function(val)
        return c.decode(val)
    end
}

local function operators(self, ...)
    local res = self.obj:operators(moon.next_session(), self.op_name, self.db_name, self.col_name, ...)
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
local M = {}

---@async
---@nodiscard
---@param database_url string Database url e. "mongodb://127.0.0.1:27017"
---@param name string Connection name for find by other services
---@return MongoDB
function M.connect(database_url, name)
    local res = moon.wait(c.connect(database_url, name))
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

function M.stats()
    return c.stats()
end

function M:close()
    self.obj:close()
end

---@async
---@nodiscard
---@param sql string
---@vararg any
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
        __call = function(t, ...)
            return operators(...)
        end
    })
end

return M
