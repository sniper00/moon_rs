---@meta
-- IDE annotation file only. Do not require this file at runtime.

--- lightuserdata, Rust `Buffer*`
---@class buffer_ptr

--- Arc<Buffer> userdata (`shared_buffer` metatable)
---@class buffer_arc_ptr

--- Represents a Rust `Buffer` object, not managed by Lua GC.
--- Often used for data transmission between Lua and Rust layers.
--- When passed to `moon.raw_send` or `socket.write`, it is auto-released;
--- otherwise call `buffer.drop` to release it.
---@class buffer
local buffer = {}

--- Create a new buffer.
---@param capacity? integer @ Initial capacity (default 128)
---@param headreserved? integer @ Reserved head space
---@return buffer_ptr
function buffer.new(capacity, headreserved) end

--- Release a buffer pointer.
---@param buf buffer_ptr
function buffer.drop(buf) end

--- Clear buffer contents.
---@param buf buffer_ptr
function buffer.clear(buf) end

--- Convert a buffer pointer to an Arc-backed shared buffer userdata.
---@param buf buffer_ptr
---@return buffer_arc_ptr
function buffer.into_arc_buffer(buf) end

--- Unpack buffer data.
---
--- `buffer.unpack(buf, pos, count)` returns raw bytes.
--- `buffer.unpack(buf, fmt, pos)` unpacks using a binary format (`>`, `<`, `h`, `H`, `i`, `I`).
---@param buf buffer_ptr
---@param fmt? string
---@param pos? integer @ Start position (default 0)
---@param count? integer
---@return string|any
---@overload fun(buf: buffer_ptr, pos: integer, count?: integer): string
function buffer.unpack(buf, fmt, pos, count) end

--- Read `n` bytes from the buffer.
---@param buf buffer_ptr
---@param n integer
---@return string
function buffer.read(buf, n) end

--- Write strings to the head (front) of the buffer.
---@param buf buffer_ptr
---@vararg string
---@return boolean
function buffer.write_front(buf, ...) end

--- Write data into the buffer tail.
---@param buf buffer_ptr
---@vararg any @ string, number, or boolean
function buffer.write(buf, ...) end

--- Move the read cursor relative to current position.
---@param buf buffer_ptr
---@param pos integer
---@return boolean
function buffer.seek(buf, pos) end

--- Advance the write cursor by `n` bytes after a `prepare` call.
---@param buf buffer_ptr
---@param n integer
---@return boolean
function buffer.commit(buf, n) end

--- Ensure at least `n` writable bytes; returns a writable pointer.
---@param buf buffer_ptr
---@param n integer
---@return lightuserdata void*
function buffer.prepare(buf, n) end

--- Concatenate arguments into a new buffer.
---@vararg any
---@return buffer_ptr
function buffer.concat(...) end

--- Concatenate arguments into a Lua string.
---@vararg any
---@return string
function buffer.concat_string(...) end

--- Current buffer data size in bytes.
---@param buf buffer_ptr
---@return integer
function buffer.size(buf) end

return buffer
