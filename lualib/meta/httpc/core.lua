---@meta
-- IDE annotation file only. Do not require this file at runtime.

--- HTTP client module (`require("httpc.core")`).
---@class httpc.core
local httpc = {}

--- Issue an async HTTP request. Returns a session; response arrives via `PTYPE_HTTPC`.
---@param opts table @ `{ method?, url, body?, headers?, timeout?, proxy? }`
---@return integer session
function httpc.request(opts) end

--- URL-encode a table of key/value pairs.
---@param params table<string, string>
---@return string
function httpc.form_urlencode(params) end

--- URL-decode a query string into a table.
---@param query string
---@return table<string, string>
function httpc.form_urldecode(query) end

--- Parse a raw HTTP response string.
---@param raw string
---@return table|false @ `{ version, status_code, headers }` or `(false, err)`
function httpc.parse_response(raw) end

--- Parse a raw HTTP request string.
---@param raw string
---@return table|false @ `{ method, path, query_string, headers }` or `(false, err)`
function httpc.parse_request(raw) end

return httpc
