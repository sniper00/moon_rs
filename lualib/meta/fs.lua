---@meta
-- IDE annotation file only. Do not require this file at runtime.

--- Native filesystem helpers (`require("fs")`).
---@class fs
local fs = {}

--- Recursively list files under `path`.
---@param path string
---@param max_depth? integer @ `0` = unlimited; `1` = immediate children only
---@param ext? string @ Optional suffix filter (e.g. `".lua"`)
---@return string[] paths
function fs.listdir(path, max_depth, ext) end

--- Create a directory (and parents).
---@param path string
---@return boolean
function fs.mkdir(path) end

--- Check whether a path exists.
---@param path string
---@return boolean
function fs.exists(path) end

--- Check whether a path is a directory.
---@param path string
---@return boolean
function fs.isdir(path) end

--- Split a path into parent, filename, and extension.
---@param path string
---@return string? parent
---@return string? filename
---@return string? ext
function fs.split(path) end

--- Get file extension (with dot).
---@param path string
---@return string? ext
function fs.ext(path) end

--- Get file stem (filename without extension).
---@param path string
---@return string? stem
function fs.stem(path) end

--- Join path segments and lexically clean the result (like Go's
--- `filepath.Join`): `.` is dropped and `..` is resolved against earlier
--- segments, so the output never contains redundant traversal components.
---@vararg string
---@return string
function fs.join(...) end

--- Current working directory.
---@return string? pwd
function fs.pwd() end

--- Canonical absolute path.
---@param path string
---@return string? abspath
function fs.abspath(path) end

--- Remove a file or directory tree.
---@param path string
---@return integer @ `1` on success
function fs.remove(path) end

return fs
