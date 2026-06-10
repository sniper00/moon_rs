---@meta
-- IDE annotation file only. Do not require this file at runtime.

---@alias hash_algorithm
---| 'md5'
---| 'sha1'
---| 'sha224'
---| 'sha256'
---| 'sha384'
---| 'sha512'

--- Utility functions (`require("utils")`).
---@class utils
local utils = {}

--- Number of logical CPUs.
---@return integer
function utils.num_cpus() end

--- Compute a hex digest.
---@param alg hash_algorithm
---@param data string
---@return string hex
function utils.hash(alg, data) end

--- Block the current OS thread for `ms` milliseconds.
---@param ms integer
function utils.thread_sleep(ms) end

--- Base64-encode binary data.
---@param data string
---@return string
function utils.base64_encode(data) end

--- Base64-decode a string. On error returns `(err_string)` and throws.
---@param base64str string
---@return string data
function utils.base64_decode(base64str) end

return utils
