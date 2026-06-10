---@meta
-- IDE annotation file only. Do not require this file at runtime.

--- Binary serialization module (`require("seri")`).
---@class seri
local seri = {}

--- Pack Lua values into a binary buffer (lightuserdata + length).
---@vararg any
---@return lightuserdata buf
---@return integer len
function seri.pack(...) end

--- Pack Lua values into a Lua string.
---@vararg any
---@return string
function seri.packstring(...) end

--- Unpack all values from a buffer.
---@param buf lightuserdata|string
---@return ...
function seri.unpack(buf) end

--- Unpack one value from a buffer without consuming it.
---@param buf lightuserdata|string
---@param seek? boolean @ If true, advance the read position
---@return any value
---@return lightuserdata buf @ Same buffer pointer
---@return integer len @ Remaining length
function seri.unpack_one(buf, seek) end

return seri
