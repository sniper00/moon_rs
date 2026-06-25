---@meta
-- IDE annotation file only. Do not require this file at runtime.

--- Protocol Buffers encode/decode (`require("protobuf")`).
---
--- Loads a serialized `FileDescriptorSet` (the binary produced by
--- `protoc --descriptor_set_out=...`) into a process-global descriptor, then
--- encodes/decodes Lua tables against the messages it defines.
---
--- ```lua
--- local protobuf = require("protobuf")
--- local fs = require("fs")
---
--- protobuf.load(fs.read("proto/game.pb"))  -- raw FileDescriptorSet bytes
--- local bytes = protobuf.encode("game.Login", { account = "bob", token = 42 })
--- local msg = protobuf.decode("game.Login", bytes)
--- ```
---@class protobuf
local protobuf = {}

--- Field wire types, matching protobuf `FieldDescriptorProto.Type`.
--- The integer values returned by `protobuf.fields` use this enumeration.
---@enum protobuf.field_type
protobuf.field_type = {
    DOUBLE = 1,
    FLOAT = 2,
    INT64 = 3,
    UINT64 = 4,
    INT32 = 5,
    FIXED64 = 6,
    FIXED32 = 7,
    BOOL = 8,
    STRING = 9,
    GROUP = 10,
    MESSAGE = 11,
    BYTES = 12,
    UINT32 = 13,
    ENUM = 14,
    SFIXED32 = 15,
    SFIXED64 = 16,
    SINT32 = 17,
    SINT64 = 18,
}

--- Load a serialized `FileDescriptorSet` into the process-global descriptor.
---
--- `data` is the raw binary content of a `FileDescriptorSet`, typically generated
--- with `protoc --descriptor_set_out=out.pb --include_imports your.proto` and then
--- read from disk. Loading replaces any previously loaded descriptor.
---
--- Raises a Lua error if the descriptor cannot be parsed.
---@param data string Raw `FileDescriptorSet` bytes
---@return boolean ok Always `true` on success
function protobuf.load(data) end

--- Encode a Lua table into a protobuf wire-format string.
---
--- Requires a descriptor to be loaded first. Raises a Lua error if no descriptor
--- is loaded, the message is unknown, or a field value is invalid.
---@param message string Fully-qualified message name (e.g. `"game.Login"`)
---@param tbl table Lua table whose fields match the message definition
---@return string bytes Encoded protobuf wire-format data
function protobuf.encode(message, tbl) end

--- Decode protobuf wire-format data into a Lua table.
---
--- The payload may be supplied either as a Lua string, or as a raw memory pointer
--- plus length (`lightuserdata, integer`) for zero-copy decoding from a buffer.
--- Requires a descriptor to be loaded first. Raises a Lua error if no descriptor
--- is loaded, the message is unknown, or the data is malformed.
---@param message string Fully-qualified message name (e.g. `"game.Login"`)
---@param data string|lightuserdata Encoded data, or a pointer when used with `len`
---@param len? integer Byte length, required when `data` is a `lightuserdata`
---@return table tbl Decoded Lua table
function protobuf.decode(message, data, len) end

--- List all messages in the loaded descriptor.
---
--- Returns a map from each message's fully-qualified name to its short name.
--- Raises a Lua error if no descriptor is loaded.
---@return table<string, string> messages Map of full name -> short name
---@nodiscard
function protobuf.messages() end

--- List the fields of a message and their wire types.
---
--- Returns a map from field name to its `protobuf.field_type` value. Returns an
--- empty table if the message is not found. Raises a Lua error if no descriptor
--- is loaded.
---@param message string Fully-qualified message name
---@return table<string, protobuf.field_type> fields Map of field name -> type
---@nodiscard
function protobuf.fields(message) end

--- List all enum type names in the loaded descriptor.
---
--- Raises a Lua error if no descriptor is loaded.
---@return string[] enums Array of fully-qualified enum names
---@nodiscard
function protobuf.enums() end

return protobuf
