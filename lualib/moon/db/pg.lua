-- PostgreSQL driver backed by the native `pg.core` Rust extension.
--
-- The previous pure-Lua wire-protocol implementation (over `moon.socket`) has
-- been moved into Rust (`crates/moon-runtime/src/modules/lua_pg.rs`) for tighter
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
}

---@class pg_result
---@field public code? string @ error kind: "SOCKET" | "CONFIG" | "ENCODE" | db SQLSTATE
---@field public message? string @ error message
---@field public data? table @ rows / aggregated results
---@field public num_queries? integer
---@field public notifications? table

---Structured, injection-safe `ON CONFLICT` specification for `insert_many`.
---All identifiers are quoted by the native layer. Provide at most one of
---`columns`/`constraint`; provide `update` for `DO UPDATE SET`, omit for
---`DO NOTHING`.
---@class pg_conflict
---@field public columns? string[] @ conflict-target columns, e.g. { "uid", "key" }
---@field public constraint? string @ conflict-target constraint name (alternative to columns)
---@field public update? string[] @ columns to set from EXCLUDED; omit/empty => DO NOTHING

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
---@param read_timeout? integer Read timeout in milliseconds (default 10000)
---@param queue_capacity? integer Per-worker request queue capacity (default 1024)
---@return pg|pg_result @ connection object, or an error table (check `.code`)
function M.connect(database_url, name, timeout, max_connections, read_timeout, queue_capacity)
    ---@diagnostic disable-next-line: redundant-parameter
    local res = moon.wait(c.connect(database_url, name, timeout, max_connections, read_timeout, queue_capacity))
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
---
---**Trust requirement:** `sql` is sent on the wire verbatim with no parameter
---binding (the simple-query protocol has no placeholders), so it is fully
---caller-controlled SQL. Never build it from untrusted input — use `query_params`
---(or `insert_many`/`update_many`) with bound `$1, $2, ...` parameters for
---anything that includes user data.
---
---**Result cap:** the reply is buffered in memory and the call fails if it
---returns more than 100,000 rows. Paginate large reads with `LIMIT`/`OFFSET`.
---@async
---@nodiscard
---@param sql string trusted, statically-known SQL (no parameter binding)
---@return pg_result
function M:query(sql)
    local res = self.obj:query(sql)
    if type(res) == "table" then
        return res
    end
    return moon.wait(res)
end

---Fire-and-forget simple query (no response awaited).
---
---**Trust requirement:** like `query`, `sql` is sent verbatim with no binding —
---never build it from untrusted input; use `execute_params` for user data.
---@param sql string trusted, statically-known SQL (no parameter binding)
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
---
---**Conflict handling:** row *values* are always bound as parameters and safe
---for user data. For the `ON CONFLICT` clause prefer the **table form**, which
---is built from quoted identifiers and is safe even with untrusted input:
---  `{ columns = {"uid","key"}, update = {"value"} }`
---  → `ON CONFLICT ("uid","key") DO UPDATE SET "value"=EXCLUDED."value"`
---  `{ constraint = "pk" }` → `ON CONFLICT ON CONSTRAINT "pk" DO NOTHING`
---A **string** `conflict` is still accepted but is appended verbatim as trusted,
---caller-controlled SQL (only a defense-in-depth `ON CONFLICT` prefix / no
---`;`,`--`,`/*` check); never build the string form from untrusted input.
---@async
---@nodiscard
---@param table_name string e.g. "userdata"
---@param columns string[] column names, e.g. { "uid", "key", "value" }
---@param rows table @ array of value arrays (one value per column)
---@param conflict? pg_conflict|string table form (safe) or trusted "ON CONFLICT ..." string
---@return pg_result
function M:insert_many(table_name, columns, rows, conflict)
    local res = self.obj:insert_many(table_name, columns, rows, conflict)
    if type(res) == "table" then
        return res
    end
    return moon.wait(res)
end

---Fire-and-forget bulk INSERT/UPSERT. Same `conflict` rules as `insert_many`
---(prefer the safe table form; the string form is trusted, verbatim SQL).
---@param conflict? pg_conflict|string table form (safe) or trusted "ON CONFLICT ..." string
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
