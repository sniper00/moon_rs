---@meta
-- IDE annotation file only. Do not require this file at runtime.

--- Random helpers (`require("random")`).
---@class random
local random = {}

--- Generate a random integer in the inclusive range `[min, max]`.
---@param min integer
---@param max integer
---@return integer
function random.rand_range(min, max) end

--- Generate `count` unique random integers in the inclusive range `[min, max]`.
---@param min integer
---@param max integer
---@param count integer
---@return integer[]
function random.rand_range_some(min, max, count) end

--- Generate a random float in the half-open range `[min, max)`.
---@param min number
---@param max number
---@return number
function random.randf_range(min, max) end

--- Return true with probability `percent`.
---@param percent number
---@return boolean
function random.randf_percent(percent) end

--- Randomly select one value based on corresponding weights.
---@param values integer[]
---@param weights integer[]
---@return integer?
function random.rand_weight(values, weights) end

--- Randomly select `count` values based on weights without repetition.
---@param values integer[]
---@param weights integer[]
---@param count integer
---@return integer[]?
function random.rand_weight_some(values, weights, count) end

return random
