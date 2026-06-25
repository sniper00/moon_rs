---@meta
-- IDE annotation file only. Do not require this file at runtime.

--- MongoDB connection userdata.
---@class mongodb_connection
local mongodb_connection = {}

--- MongoDB cursor handle.
---@class mongodb_cursor
local mongodb_cursor = {}

--- Native MongoDB driver (`require("mongodb.core")`).
---@class mongodb.core
local mongodb = {}

--- Connect and register a named connection asynchronously.
---@param database_url string
---@param name string
---@param queue_capacity? integer @ request queue capacity (default 1024)
---@return integer session
function mongodb.connect(database_url, name, queue_capacity) end

--- Look up a registered connection by name.
---@param name string
---@return mongodb_connection? conn
function mongodb.find_connection(name) end

--- Convert a Lua table to a BSON-compatible Lua value (ObjectId, Date, etc.).
---@param t table
---@return any
function mongodb.tt(t) end

--- Statistics per named connection.
---@return table<string, pool_stats>
function mongodb.stats() end

--- Dispatch a MongoDB operation asynchronously.
---@param conn mongodb_connection
---@param op string @ Operation name (e.g. `"find"`, `"insertOne"`)
---@param db string
---@param collection string
---@vararg any @ Operation-specific arguments
---@return integer session|table @ Session or inline error `{ kind, message }`
function mongodb_connection:operators(op, db, collection, ...) end

--- Close the connection.
---@param conn mongodb_connection
function mongodb_connection:close() end

--- Fetch the next cursor batch asynchronously.
---@param cursor mongodb_cursor
---@return integer session
---@return false? err
---@return string? errmsg
function mongodb_cursor:next() end

--- Close a cursor.
---@param cursor mongodb_cursor
function mongodb_cursor:close() end

return mongodb
