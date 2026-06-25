local moon = require "moon"
local c = require "sqlx.core"
local json = require("json")
moon.register_protocol {
    name = "sqlx",
    PTYPE = moon.PTYPE_SQLX,
    pack = function(...) return ... end,
}

---@class SqlX
---@field obj sqlx_connection
local M = {}

---@nodiscard
---@async
---@param database_url string Database url e.g. "postgres://postgres:123456@localhost/postgres"
---@param name string Connection name for find by other services
---@param timeout? integer Connect timeout in milliseconds. Default 5000ms
---@param max_connections? integer Maximum number of connections in pool. Default 5
---@param queue_capacity? integer Request queue capacity. Default 1024
---@return SqlX
function M.connect(database_url, name, timeout, max_connections, queue_capacity)
    ---@diagnostic disable-next-line: redundant-parameter
    local res = moon.wait(c.connect(database_url, name, timeout, max_connections, queue_capacity))
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
---@return sqlx_connection?
function M.find_connection(name)
    return c.find_connection(name)
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

---Fire-and-forget query.
---
---**Trust requirement:** `sql` is passed to the driver verbatim — only the
---varargs are bound as parameters. Keep `sql` a trusted, statically-known
---statement; never concatenate untrusted input into it. Use `$1`/`?`
---placeholders + varargs for all values.
---@param sql string trusted, statically-known SQL
---@vararg any bound query parameters
function M:execute(sql, ...)
    local res = self.obj:exec_query(sql, ...)
    if type(res) == "table" then
        moon.error(print_r(res, true))
    end
end

---**Trust requirement:** `sql` is passed to the driver verbatim — only the
---varargs are bound as parameters. Keep `sql` a trusted, statically-known
---statement; never concatenate untrusted input into it. Use `$1`/`?`
---placeholders + varargs for all values.
---
---**Result cap:** this materializes the whole result set in memory and fails
---if it exceeds 100,000 rows. Use `query_stream` for larger result sets.
---@async
---@nodiscard
---@param sql string trusted, statically-known SQL
---@vararg any bound query parameters
---@return table
function M:query(sql, ...)
    local session = self.obj:query(sql, ...)
    if type(session) == "table" then
        return session
    end
    return moon.wait(session)
end

---@async
---@nodiscard
---@param queries table @ `{ {sql, p1, ...}, {sql, p1, ...}, ... }`
---@return table
function M:transaction(queries)
    local trans = c.make_transaction()
    for _, v in ipairs(queries) do
        trans:push(table.unpack(v))
    end
    local session = self.obj:transaction(trans)
    if type(session) == "table" then
        return session
    end
    return moon.wait(session)
end

---@param queries table @ `{ {sql, p1, ...}, {sql, p1, ...}, ... }`
function M:execute_transaction(queries)
    local trans = c.make_transaction()
    for _, v in ipairs(queries) do
        trans:push(table.unpack(v))
    end
    local res = self.obj:exec_transaction(trans)
    if type(res) == "table" then
        moon.error(print_r(res, true))
    end
end

---@async
---@nodiscard
---@param sql string SQL query
---@param batch_size? integer Rows per batch (default 100)
---@vararg any Query parameters
---@return fun():table|nil Iterator function returning one row at a time
---@return nil
---@return nil
---@return table to-be-closed stream state
function M:query_stream(sql, batch_size, ...)
    local res = self.obj:query_stream(batch_size, sql, ...)
    if type(res) == "table" then
        ---@diagnostic disable-next-line: return-type-mismatch, missing-return-value
        return nil, res.message or res.kind
    end
    local current_session = res
    local buffer
    local idx = 0
    local done = false
    local pending_cursor

    local stream_state = setmetatable({}, {
        __close = function()
            if pending_cursor then
                pending_cursor:close()
                pending_cursor = nil
            end
        end,
    })

    ---@async
    local function iter()
        while true do
            if buffer and idx < #buffer then
                idx = idx + 1
                return buffer[idx]
            end
            if done then
                return nil
            end
            if pending_cursor then
                current_session = pending_cursor:next()
                pending_cursor = nil
            end
            local rows, cursor_handle = moon.wait(current_session)
            if not rows or #rows == 0 then
                done = true
                return nil
            end
            buffer = rows
            idx = 0
            if cursor_handle then
                pending_cursor = cursor_handle
            else
                done = true
            end
        end
    end

    return iter, nil, nil, stream_state
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
