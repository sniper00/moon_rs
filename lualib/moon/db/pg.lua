-- PostgreSQL driver backed by the native `pg.core` Rust extension.
--
-- The previous pure-Lua wire-protocol implementation (over `moon.socket`) has
-- been moved into Rust (`crates/moon-modules/src/lua_pg.rs`) for tighter
-- protocol control and fewer data copies. This module is now a thin,
-- session-based async wrapper around `pg.core`, mirroring `moon.db.sqlx`.
--
-- Connection pooling and reconnect are handled natively by the pool, so the
-- old `service/sqldriver.lua` is no longer needed for PostgreSQL.

local moon = require("moon")
local c = require("pg.core")

moon.register_protocol {
    name = "pg",
    PTYPE = moon.PTYPE_PG,
    pack = function(...) return ... end,
    unpack = function(val)
        return c.decode(val)
    end
}

---@class pg_result
---@field public code? string @ error kind: "SOCKET" | "CONFIG" | "ENCODE" | db SQLSTATE
---@field public message? string @ error message
---@field public data? table @ rows / aggregated results
---@field public num_queries? integer
---@field public notifications? table

---@class pg
local M = {}
M.__index = M

---Look up an already-connected pool by name and wrap it.
---@param name string
---@return pg
function M.find_connection(name)
    local o = {
        obj = assert(c.find_connection(name), "pg connection not found: " .. tostring(name))
    }
    return setmetatable(o, M)
end

---Connect and register a named pool.
---@async
---@param database_url string e.g. "postgres://postgres:123456@127.0.0.1:5432/postgres"
---@param name string Connection name for lookup by other services
---@param timeout? integer Connect timeout in milliseconds (default 5000)
---@param max_connections? integer Pool size (default 5)
---@param read_timeout? integer Read timeout in milliseconds (default max(timeout, 30000))
---@return pg|pg_result @ connection object, or an error table (check `.code`)
function M.connect(database_url, name, timeout, max_connections, read_timeout)
    local res = moon.wait(c.connect(database_url, name, timeout, max_connections, read_timeout))
    if res.code then
        return res
    end
    return M.find_connection(name)
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

---Simple query protocol; `sql` may contain multiple `;`-separated statements.
---@async
---@nodiscard
---@param sql string
---@return pg_result
function M:query(sql)
    local res = self.obj:query(sql)
    if type(res) == "table" then
        return res
    end
    return moon.wait(res)
end

---Fire-and-forget simple query (no response awaited).
---@param sql string
function M:execute(sql)
    local res = self.obj:exec_query(sql)
    if type(res) == "table" then
        moon.error(table.tostring(res))
    end
end

---Extended query protocol with bound parameters ($1, $2, ...).
---@async
---@nodiscard
---@param sql string
---@vararg any
---@return pg_result
function M:query_params(sql, ...)
    local res = self.obj:query_params(sql, ...)
    if type(res) == "table" then
        return res
    end
    return moon.wait(res)
end

---Fire-and-forget extended query.
---@param sql string
---@vararg any
function M:execute_params(sql, ...)
    local res = self.obj:exec_query_params(sql, ...)
    if type(res) == "table" then
        moon.error(table.tostring(res))
    end
end

---Pipeline multiple parameterized statements inside an implicit
---`BEGIN`/`COMMIT` transaction.
---@async
---@nodiscard
---@param queries table @ `{ {sql, p1, ...}, {sql, p1, ...}, ... }`
---@return pg_result
function M:pipe(queries)
    local res = self.obj:pipe(queries)
    if type(res) == "table" then
        return res
    end
    return moon.wait(res)
end

---Fire-and-forget pipeline.
---@param queries table
function M:execute_pipe(queries)
    local res = self.obj:exec_pipe(queries)
    if type(res) == "table" then
        moon.error(table.tostring(res))
    end
end

---Bulk INSERT/UPSERT. Rows are packed into one multi-row `VALUES` statement
---(one Parse, one Bind, one Execute, one plan), auto-chunked under the 65535
---parameter limit and wrapped in a transaction when more than one chunk.
---
---Note: a single multi-row UPSERT cannot touch the same conflict key twice —
---de-duplicate keys (keep the latest) before calling.
---@async
---@nodiscard
---@param table_name string e.g. "userdata"
---@param columns string[] column names, e.g. { "uid", "key", "value" }
---@param rows table @ array of value arrays (one value per column)
---@param conflict? string e.g. "ON CONFLICT (uid,key) DO UPDATE SET value = EXCLUDED.value"
---@return pg_result
function M:insert_many(table_name, columns, rows, conflict)
    local res = self.obj:insert_many(table_name, columns, rows, conflict)
    if type(res) == "table" then
        return res
    end
    return moon.wait(res)
end

---Fire-and-forget bulk INSERT/UPSERT.
function M:execute_insert_many(table_name, columns, rows, conflict)
    local res = self.obj:exec_insert_many(table_name, columns, rows, conflict)
    if type(res) == "table" then
        moon.error(table.tostring(res))
    end
end

---Bulk UPDATE via `UPDATE ... FROM (VALUES ...)`. Each row is
---`{ key, set1, set2, ... }` (key value first, then one value per
---`set_columns` entry). Auto-chunked and transaction-wrapped like insert_many.
---
---`key_type` (e.g. "bigint") casts the join key so the index on `key_column`
---stays usable; omit it and the key is compared as text (works for any type,
---but no index).
---@async
---@nodiscard
---@param table_name string
---@param key_column string the column matched in the WHERE clause
---@param set_columns string[] columns to assign, e.g. { "value", "name" }
---@param rows table @ array of `{ key, set1, set2, ... }`
---@param key_type? string e.g. "bigint" — cast the join key to keep its index usable
---@return pg_result
function M:update_many(table_name, key_column, set_columns, rows, key_type)
    local res = self.obj:update_many(table_name, key_column, set_columns, rows, key_type)
    if type(res) == "table" then
        return res
    end
    return moon.wait(res)
end

---Fire-and-forget bulk UPDATE.
function M:execute_update_many(table_name, key_column, set_columns, rows, key_type)
    local res = self.obj:exec_update_many(table_name, key_column, set_columns, rows, key_type)
    if type(res) == "table" then
        moon.error(table.tostring(res))
    end
end

return M
