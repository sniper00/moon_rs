local moon = require "moon"
local c = require "sqlx.core"
local json = require("json")
moon.register_protocol {
    name = "sqlx",
    PTYPE = moon.PTYPE_SQLX,
    pack = function(...) return ... end,
    unpack = function(val)
        return c.decode(val)
    end
}

---@class SqlX
local M = {}

---@nodiscard
---@param database_url string Database url e.g. "postgres://postgres:123456@localhost/postgres"
---@param name string Connection name for find by other services
---@param timeout? integer Connect timeout in milliseconds. Default 5000ms
---@param max_connections? integer Maximum number of connections in pool. Default 5
---@return SqlX
function M.connect(database_url, name, timeout, max_connections)
    local res = moon.wait(c.connect(database_url, name, timeout, max_connections))
    if res.kind then
        error(string.format("connect database failed: %s", res.message))
    end
    local o = {
        obj = c.find_connection(name)
    }
    return setmetatable(o, { __index = M })
end

---@nodiscard
---@param name string Connection name
function M.find_connection(name)
    return c.find_connection(name)
end

function M.stats()
    return c.stats()
end

function M:close()
    self.obj:close()
end

---@param sql string
---@vararg any
function M:execute(sql, ...)
    local res = self.obj:query(0, sql, ...)
    if type(res) == "table" then
        moon.error(print_r(res, true))
    end
end

---@async
---@nodiscard
---@param sql string
---@vararg any
---@return table
function M:query(sql, ...)
    local session = self.obj:query(moon.next_session(), sql, ...)
    if type(session) == "table" then
        return session
    end
    return moon.wait(session)
end

---@async
---@nodiscard
---@param querys table
---@return table
function M:transaction(querys)
    local trans = c.make_transaction()
    for _, v in ipairs(querys) do
        trans:push(table.unpack(v))
    end
    local session = self.obj:transaction(moon.next_session(), trans)
    if type(session) == "table" then
        return session
    end
    return moon.wait(session)
end

---@param querys table
function M:execute_transaction(querys)
    local trans = c.make_transaction()
    for _, v in ipairs(querys) do
        trans:push(table.unpack(v))
    end
    local res = self.obj:transaction(0, trans)
    if type(res) == "table" then
        moon.error(print_r(res, true))
    end
end

--- Wrap a value as an explicit JSON query parameter. Use this instead of
--- passing raw JSON strings, which avoids fragile content-based type guessing.
---
--- ```lua
--- db:query("INSERT INTO t (data) VALUES ($1)", sqlx.json({key = "value"}))
--- db:query("SELECT * FROM t WHERE data @> $1", sqlx.json('{"key":"value"}'))
--- ```
---@param value string|table JSON string or Lua table to encode as JSON
---@return userdata json_param Opaque JSON parameter for use in query/execute
function M.json(value)

    if type(value) == "table" then
        return c.json_param(json.encode(value))
    elseif type(value) == "string" then
        return c.json_param(value)
    else
        error("sqlx.json: expected string or table, got " .. type(value))
    end
end

return M
