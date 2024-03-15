---@meta
error("DO NOT REQUIRE THIS FILE")

--- Represents a `Rust Buffer` object, which is not managed by `Lua GC`. 
--- This is often used for data transmission between Lua and Rust layers.
--- This object can be used as an argument for `moon.raw_send` or `socket.write`, 
--- and it will be automatically released. Otherwise, `buffer.drop` should be used to release it.
--- The object's memory is divided into two parts: head and data. 
--- When writing the data part first and then adding content to the head of the data, 
--- the reserved head part can be used to avoid memory copying.
---@class buffer
local buffer = {}

--- Creates a `Rust Buffer` object that is not managed by `Lua GC`. 
--- This object can be used as an argument for `moon.raw_send` or `socket.write`, 
--- and it will be automatically released. Otherwise, `buffer.drop` should be used to release it.
---@param capacity? integer @ The initial capacity of the Buffer, default value is `240`.
---@param headreserved? integer @ The reserved space at the head of the Buffer, default value is `16`.
---@return buffer_ptr
function buffer.new(capacity, headreserved) end

--- Releases a `Rust Buffer` object.
---@param buf buffer_ptr
function buffer.drop(buf) end

--- Clears the data in the buffer.
--- @param buf buffer_ptr
function buffer.clear(buf) end

--- buffer.unpack(buf, pos, count) returns a portion of the buffer data. 
--- The optional parameter `pos` (default is 0) marks where to start reading from the buffer, 
--- and `count` indicates how much data to read.
---
--- buffer.unpack(buf, fmt, pos) unpacks the buffer data according to the `fmt` format. 
--- The optional parameter `pos` (default is 0) marks where to start reading from the buffer.
---
--- @param buf buffer_ptr
--- @param fmt? string @ like string.unpack but only supports '>', '<', 'h', 'H', 'i', 'I'
--- @param pos? integer @ start position
--- @param count? integer @ number of elements to read
--- @return string | any
--- @overload fun(buf:buffer_ptr, pos:integer, count?:integer)
function buffer.unpack(buf, fmt, pos, count) end

--- Read n bytes from buffer
---@param buf buffer_ptr
---@param n integer
---@return string
function buffer.read(buf, n) end

--- Write string to buffer's head part
---@param buf buffer_ptr
---@param ... string
---@return boolean
function buffer.write_front(buf, ...) end

--- Writes data into the buffer. The parameters can be any Lua type that can be converted to a string, 
--- such as string, number, boolean.
---@param buf buffer_ptr
---@param ... any
function buffer.write(buf, ...) end

--- Moves the read position of the buffer from the current position. 
--- The `pos` can be a positive or negative number.
---@param buf buffer_ptr
---@param pos integer
---@return boolean
function buffer.seek(buf, pos) end

--- Moves the write position of the buffer forward.
---@param buf buffer_ptr
---@param n integer
---@return boolean
function buffer.commit(buf, n) end

--- Ensures that the buffer can accommodate `n` characters, reallocating character array objects as necessary.
---@param buf buffer_ptr
---@param n integer
---@return lightuserdata void*
function buffer.prepare(buf, n) end

--- Converts the parameters to a string and saves it in the buffer, 
--- then returns a lightuserdata. This is often used for data transmission between Lua and Rust layers, 
--- to avoid creating Lua GC objects.
---@return buffer_ptr
function buffer.concat(...) end

--- Converts the parameters to a string and saves it in the buffer.
---@return string
function buffer.concat_string(...) end

---@param buf buffer_ptr
function buffer.size(buf) end

return buffer