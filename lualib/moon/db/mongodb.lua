local moon = require "moon"
local c = require "mongodb.core"

moon.register_protocol {
    name = "mongodb",
    PTYPE = moon.PTYPE_MONGODB,
    pack = function(...) return ... end,
}

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

-- Streaming cursor iteration that auto-releases the server-side cursor via the
-- generic-for to-be-closed mechanism.
--
-- Per the Lua 5.4/5.5 Reference Manual (this project uses lua55):
--   §3.3.5 Generic for: evaluating `explist` produces four values -- the iterator
--           function, the state, the initial control variable, and a closing
--           value (the 4th value). The closing value behaves like a to-be-closed
--           variable that releases resources when the loop ends; otherwise it
--           does not interfere with the loop.
--   §3.3.8 To-be-closed: the value is closed whenever the variable goes out of
--           scope -- normal termination, exiting via break/goto/return, or
--           exiting by an error. Closing means calling its `__close` metamethod
--           as `__close(value, err)`, where err is the error object that caused
--           the exit (nil if none). The closing value must have a `__close`
--           metamethod or be a false value, otherwise the loop errors at start.
--
-- Hence this function returns the `__close`-equipped `stream_state` as the 4th
-- value, so that `for doc in db:find_stream(...) do ... end` always closes the
-- cursor whether the loop finishes normally, breaks early, or errors out.
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
        return nil, res.message
    end
    local current_session = res
    local buffer
    local idx = 0
    local done = false
    local pending_cursor

    -- To-be-closed sentinel: `__close` is invoked automatically by the VM when the
    -- for loop ends (normal/break/error). It shares the `pending_cursor` upvalue
    -- with iter, so it sees the latest cursor handle and closes it. Setting nil
    -- keeps it idempotent and avoids a double close.
    local stream_state = setmetatable({}, {
        __close = function()
            if pending_cursor then
                pending_cursor:close()
                pending_cursor = nil
            end
        end,
    })

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
            local docs, cursor_handle = moon.wait(current_session)
            if not docs or #docs == 0 then
                done = true
                return nil
            end
            buffer = docs
            idx = 0
            if cursor_handle then
                pending_cursor = cursor_handle
            else
                done = true
            end
        end
    end

    -- Four values for the generic-for protocol: iterator, state, control, closing value (§3.3.5).
    return iter, nil, nil, stream_state
end

return M
