---@meta
-- IDE annotation file only. Do not require this file at runtime.

--- PostgreSQL connection pool userdata.
---@class pg_pool

--- Native PostgreSQL driver (`require("pg.core")`).
---@class pg.core
local pg = {}

--- Connect and register a named pool asynchronously.
---@param database_url string @ PostgreSQL connection URL
---@param name string @ Pool name
---@param timeout? integer @ Connect timeout in ms (default 5000)
---@param max_connections? integer @ Pool size (default 5)
---@param read_timeout? integer @ Query read timeout in ms (default 10000)
---@param queue_capacity? integer @ Per-worker request queue capacity (default 1024)
---@return integer session
function pg.connect(database_url, name, timeout, max_connections, read_timeout, queue_capacity) end

--- Look up a registered pool by name.
---@param name string
---@return pg_pool? pool
function pg.find_connection(name) end

--- Pending request counts per named pool.
---@return table<string, integer>
function pg.stats() end

--- Execute a SQL query asynchronously.
---@param pool pg_pool
---@param sql string
---@return integer session
function pg_pool:query(sql) end

--- Execute a parameterized query asynchronously.
---@param pool pg_pool
---@param sql string
---@vararg any @ Query parameters
---@return integer session
function pg_pool:query_params(sql, ...) end

--- Execute a pipeline of queries asynchronously.
---@param pool pg_pool
---@vararg string sql
---@return integer session
function pg_pool:pipe(...) end

--- Bulk insert asynchronously.
---@param pool pg_pool
---@param table_name string
---@param columns string[]
---@param rows any[][]
---@return integer session
function pg_pool:insert_many(table_name, columns, rows) end

--- Bulk update asynchronously.
---@param pool pg_pool
---@param table_name string
---@param set_columns string[]
---@param where_column string
---@param rows any[][]
---@return integer session
function pg_pool:update_many(table_name, set_columns, where_column, rows) end

--- Fire-and-forget query.
---@param pool pg_pool
---@param sql string
---@return boolean
function pg_pool:exec_query(sql) end

--- Fire-and-forget parameterized query.
---@param pool pg_pool
---@param sql string
---@vararg any
---@return boolean
function pg_pool:exec_query_params(sql, ...) end

--- Fire-and-forget pipeline.
---@param pool pg_pool
---@vararg string
---@return boolean
function pg_pool:exec_pipe(...) end

--- Fire-and-forget bulk insert.
---@param pool pg_pool
---@param table_name string
---@param columns string[]
---@param rows any[][]
---@return boolean
function pg_pool:exec_insert_many(table_name, columns, rows) end

--- Fire-and-forget bulk update.
---@param pool pg_pool
---@param table_name string
---@param set_columns string[]
---@param where_column string
---@param rows any[][]
---@return boolean
function pg_pool:exec_update_many(table_name, set_columns, where_column, rows) end

--- Pending request counts per pool worker.
---@param pool pg_pool
---@return integer[]
function pg_pool:len() end

--- Close and unregister the pool.
---@param pool pg_pool
function pg_pool:close() end

return pg
