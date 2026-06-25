---@meta
-- IDE annotation file only. Do not require this file at runtime.

--- Runtime schema validator for Lua tables (`require("schema")`).
---
--- A protobuf-like schema is loaded once (typically at startup) and then shared
--- by every actor in the process. `validate` checks that a table matches a named
--- proto and raises a Lua error (catchable with `pcall`) describing the first
--- mismatch, including a `trace:` path to the offending value.
---
--- Definition format (a table of proto name -> proto definition):
---
--- ```lua
--- {
---     -- a wrapper proto: the whole validated table is treated as the value of
---     -- its single `data` field (here, a sequence of int64).
---     array_int64 = {
---         wrapper = true,
---         data = { container = "array", value_type = "int64" },
---     },
---     ItemData = {
---         id    = { value_type = "int32" },
---         count = { value_type = "int64" },
---     },
---     UserData = {
---         uid   = { value_type = "int64" },
---         name  = { value_type = "string" },
---         level = { value_type = "int32" },
---         -- map<int32, ItemData>
---         itemlist = { container = "object", key_type = "int32", value_type = "ItemData" },
---     },
--- }
--- ```
---
--- Field options:
--- - `value_type` (required): a primitive or the name of another proto.
---   Primitives: `int32 uint32 int64 uint64 sint32 sint64 fixed32 fixed64`
---   `sfixed32 sfixed64 float double bool string bytes`. Integer types are
---   range/sign checked.
--- - `container` (optional): `"array"` (a Lua sequence of `value_type`) or
---   `"object"` (a map of `key_type` -> `value_type`). Omit for a single value.
--- - `key_type` (object only): the primitive type of the map keys.
--- - `wrapper` (optional, proto-level boolean): see `array_int64` above.
---
--- An object table whose first key is the integer `1` must carry an `__object`
--- metafield to disambiguate it from an array.
---@class schema
local schema = {}

--- Load (or reload) the schema definitions. Resolves all type references and
--- raises a Lua error if a definition is malformed or references an unknown type.
--- Safe to call again to hot-reload; in-flight validations keep the old schema.
---@param define table @ Map of proto name to proto definition (see module docs)
function schema.load(define) end

--- Validate `data` against the named proto. Returns nothing on success; raises a
--- Lua error (use `pcall`) on the first mismatch, with a message of the form
--- `"<type> expected, got <type>, value '<v>'. trace: <path>"`.
---@param proto string @ Proto name previously registered via `schema.load`
---@param data table @ The table to validate
function schema.validate(proto, data) end

return schema
