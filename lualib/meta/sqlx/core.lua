---@meta
-- IDE annotation file only. Do not require this file at runtime.

--- SQLx database connection userdata.
---@class sqlx_connection
local sqlx_connection = {}

--- SQLx streaming cursor handle.
---@class sqlx_cursor
local sqlx_cursor = {}

--- SQLx transaction query builder userdata.
---@class sqlx_transaction
local sqlx_transaction = {}

--- SQLx JSON query parameter userdata.
---@class sqlx_json_param

--- Native SQLx driver (`require("sqlx.core")`).
---@class sqlx.core
local sqlx = {}

--- Connect and register a named pool asynchronously.
---@param database_url string
---@param name string
---@param connect_timeout? integer @ ms (default 5000)
---@param max_connections? integer @ default 5
---@param queue_capacity? integer @ request queue capacity (default 1024)
---@return integer session
function sqlx.connect(database_url, name, connect_timeout, max_connections, queue_capacity) end

--- Look up a registered connection by name.
---@param name string
---@return sqlx_connection? conn
function sqlx.find_connection(name) end

--- Statistics per named connection.
---@return table<string, pool_stats>
function sqlx.stats() end

--- Create a transaction query builder.
---@return sqlx_transaction
function sqlx.make_transaction() end

--- Wrap a JSON string as a typed query parameter.
---@param json string
---@return sqlx_json_param
function sqlx.json_param(json) end

--- Execute a query asynchronously.
---@param conn sqlx_connection
---@param sql string
---@vararg any @ Query parameters
---@return integer session
function sqlx_connection:query(sql, ...) end

--- Fire-and-forget query.
---@param conn sqlx_connection
---@param sql string
---@vararg any
---@return boolean|table
function sqlx_connection:exec_query(sql, ...) end

--- Start a streaming query asynchronously.
---@param conn sqlx_connection
---@param batch_size? integer @ Rows per batch (default 100)
---@param sql string
---@vararg any @ Query parameters
---@return integer session
function sqlx_connection:query_stream(batch_size, sql, ...) end

--- Execute a transaction asynchronously.
---@param conn sqlx_connection
---@param tx sqlx_transaction
---@return integer session
function sqlx_connection:transaction(tx) end

--- Fire-and-forget transaction.
---@param conn sqlx_connection
---@param tx sqlx_transaction
---@return boolean|table
function sqlx_connection:exec_transaction(tx) end

--- Close the connection.
---@param conn sqlx_connection
---@return boolean|table
function sqlx_connection:close() end

--- Fetch the next batch from a stream cursor asynchronously.
---@param cursor sqlx_cursor
---@return integer session
---@return false? err
---@return string? errmsg
function sqlx_cursor:next() end

--- Close a stream cursor.
---@param cursor sqlx_cursor
function sqlx_cursor:close() end

--- Append a query to a transaction builder.
---@param tx sqlx_transaction
---@param sql string
---@vararg any
function sqlx_transaction:push(sql, ...) end

return sqlx
