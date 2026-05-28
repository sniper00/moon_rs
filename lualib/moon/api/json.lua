---@meta
---@class json
local json = {}

--- A sentinel value representing JSON `null`.
---@type lightuserdata
json.null = nil

--- Decode a JSON string (or file) into a Lua value.
---
--- If `str` starts with `@`, the remainder is treated as a **file path** and
--- the JSON content is read from that file. For example:
--- ```lua
--- local cfg = json.decode("@config.json")
--- ```
---
--- **Security note**: The `@path` feature performs no path sanitization.
--- It can read any file the process has access to. Only use with trusted
--- Lua scripts; do not pass untrusted user input as the argument.
---
---@param str string JSON string, or `@<filepath>` to read from file
---@return any value Decoded Lua value (table, string, number, boolean, or `json.null`)
---@return string? err Error message if decoding failed (first return is `nil`)
function json.decode(str) end

--- Encode a Lua value into a JSON string.
---@param value any Lua value to encode (table, string, number, boolean, or `json.null`)
---@param pretty? boolean If true, output indented/pretty-printed JSON
---@return string json Encoded JSON string
function json.encode(value, pretty) end

--- Encode one or more Lua values as JSON fragments and concatenate them
--- into a single `buffer_ptr`. Useful for building bulk payloads without
--- intermediate string allocations.
---@param ... any Lua values to encode and concatenate
---@return buffer_ptr buf Buffer containing concatenated JSON
function json.concat(...) end

--- Encode values as Redis RESP (REdis Serialization Protocol) bulk strings
--- and concatenate them into a `buffer_ptr`.
---@param ... any Values to encode as RESP
---@return buffer_ptr buf Buffer containing RESP-encoded data
function json.concat_resp(...) end

--- Get or set a JSON codec option. Returns the previous value.
---
--- Available options:
--- - `"encode_empty_as_array"` (default `true`): encode empty tables as `[]` instead of `{}`
--- - `"enable_number_key"` (default `true`): allow numeric table keys (encoded as string keys)
--- - `"enable_sparse_array"` (default `false`): encode sparse Lua arrays as JSON arrays with `null` holes
---
---@param key string Option name
---@param value boolean New value
---@return boolean previous Previous value of the option
function json.options(key, value) end

--- Create or mark a table as a JSON object. The table will always encode
--- as `{}` even when empty, bypassing the `encode_empty_as_array` option.
---
--- ```lua
--- local t = json.object()       -- new empty object
--- local t = json.object(16)     -- pre-allocate 16 hash slots
--- local t = json.object({a=1})  -- mark existing table as object
--- ```
---@param t? table|integer Existing table to mark, or initial hash capacity
---@return table object Table with JSON object metatable
function json.object(t) end

--- Create or mark a table as a JSON array. The table will always encode
--- as `[]` even when empty.
---
--- ```lua
--- local t = json.array()        -- new empty array
--- local t = json.array(16)      -- pre-allocate 16 array slots
--- local t = json.array({1,2,3}) -- mark existing table as array
--- ```
---@param t? table|integer Existing table to mark, or initial array capacity
---@return table array Table with JSON array metatable
function json.array(t) end

return json
