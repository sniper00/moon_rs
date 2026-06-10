---@meta
-- IDE annotation file only. Do not require this file at runtime.

--- Excel/CSV reader (`require("excel")`, feature-gated).
---@class excel
local excel = {}

--- Read a `.csv` or `.xlsx` file.
---
--- Returns an array of sheets; each sheet is `{ sheet_name, data }` where
--- `data` is an array of row arrays.
---@param path string
---@param max_row? integer @ Maximum rows per sheet (default unlimited)
---@return table[] sheets
---@return false? err
---@return string? errmsg
function excel.read(path, max_row) end

return excel
