---@meta
-- IDE annotation file only. Do not require this file at runtime.

--- High-performance ordered set with ranking (`require("zset")`).
---
--- Skiplist + dictionary sorted set for leaderboards, time-series and priority
--- queues. Elements are ordered by score, then timestamp, then key.
---
--- Performance:
--- - O(log N): update, erase, rank
--- - O(1): score lookup, existence check, size
--- - O(log N + M): range queries for M elements
---
--- Ordering:
--- - Primary: score (descending by default, ascending when `reverse` is true)
--- - Secondary: timestamp (always ascending)
--- - Tertiary: key (always ascending, for determinism)
---@class zset
local zset = {}

--- Create a new ordered set.
---@param maxcount integer @ Maximum number of elements (excess lowest-ranked elements are evicted)
---@param reverse? boolean @ Sort order: false=descending (default), true=ascending
---@return zset
---@nodiscard
function zset.new(maxcount, reverse) end

--- Insert or update an element (O(log N)).
---
--- Adds a new element or repositions an existing one. When at capacity, the
--- lowest-ranked element is evicted (or the update is rejected if it would rank
--- below the current worst element). Elements with `key == 0` are ignored.
---@param key integer @ Unique element identifier
---@param score integer @ Sorting score
---@param timestamp integer @ Secondary sort key
function zset:update(key, score, timestamp) end

--- Get an element's 1-based rank (O(log N)). Returns nil if not found.
---@param key integer
---@return integer|nil
---@nodiscard
function zset:rank(key) end

--- Get an element's score (O(1)). Returns 0 if not found.
---@param key integer
---@return integer
---@nodiscard
function zset:score(key) end

--- Check whether an element exists (O(1)).
---@param key integer
---@return boolean
---@nodiscard
function zset:has(key) end

--- Get the current number of elements (O(1)).
---@return integer
---@nodiscard
function zset:size() end

--- Remove all elements.
function zset:clear() end

--- Remove a specific element (O(log N)). Returns the number removed (0 or 1).
---@param key integer
---@return integer
function zset:erase(key) end

--- Get a range of keys by rank (O(log N + M)).
---
--- 1-based inclusive indices; negative indices count from the end. Out-of-range
--- indices are clamped. Returns nil when the range is empty.
---@param start integer @ Starting rank (1-based; negative counts from end)
---@param stop integer @ Ending rank (1-based, inclusive; negative counts from end)
---@param reverse? boolean @ Iterate the range in reverse order (default false)
---@return integer[]|nil
---@nodiscard
function zset:range(start, stop, reverse) end

--- Get the key at a 1-based rank (O(log N)). Returns nil if out of bounds.
---@param rank integer @ 1-based rank (1 = best)
---@return integer|nil
---@nodiscard
function zset:key_by_rank(rank) end

return zset
