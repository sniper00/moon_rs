---@meta
-- IDE annotation file only. Do not require this file at runtime.

--- One-shot HTTP server response handle (returned with decoded request via `moon.core.decode_message`).
---@class httpd_response_handle

--- HTTP server module (`require("httpd.core")`).
---@class httpd.core
local httpd = {}

--- Start an HTTP listener. Requests are delivered via `PTYPE_HTTPD`.
---@param addr string @ e.g. `"0.0.0.0:8080"`
---@param opts? table @ `{ max_body_size? }`
---@return integer fd
function httpd.listen(addr, opts) end

--- Send an HTTP response (handle is consumed).
---@param handle httpd_response_handle
---@param status? integer @ Default 200
---@param headers? table<string, string>
---@param body? string
---@return boolean
---@return false? err
---@return string? errmsg
function httpd.response(handle, status, headers, body) end

--- Stop an HTTP listener by fd.
---@param fd integer
---@return boolean
function httpd.close(fd) end

return httpd
