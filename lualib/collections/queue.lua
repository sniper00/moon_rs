--- A FIFO queue backed by a plain table.
---
--- Elements are stored at integer keys between a head index `h` and a tail index
--- `t`; pushing appends at `t+1`, popping removes from `h`. Indices grow
--- monotonically and are only reset to `{ h = 1, t = 0 }` when the queue becomes
--- empty, so memory for popped slots is reclaimed (set to `nil`) as you go.
---
--- All operations are O(1). The queue is not thread-safe and stores values by
--- reference; `nil` should not be pushed (it is indistinguishable from an empty
--- slot and breaks `size`).
---
--- ```lua
--- local queue = require("collections.queue")
--- local q = queue.new()
--- queue.push(q, "a")
--- queue.push(q, "b")
--- print(queue.front(q)) --> "a"
--- print(queue.pop(q))   --> "a"
--- print(queue.size(q))  --> 1
--- ```
---@class queue
---@field h integer @ Head index (index of the front element).
---@field t integer @ Tail index (index of the last element); empty when `t < h`.
local queue = {}

--- Create a new empty queue.
---@return queue
---@nodiscard
function queue.new()
    return { h = 1, t = 0 }
end

--- Append a value to the back of the queue (O(1)).
---@param q queue
---@param v any @ Value to enqueue; do not push `nil`.
function queue.push(q, v)
    local t = q.t + 1
	q.t = t
	q[t] = v
end

--- Return the value at the front without removing it (O(1)).
--- Returns `nil` when the queue is empty (and resets its indices).
---@param q queue
---@return any|nil @ The front value, or `nil` if empty.
---@nodiscard
function queue.front(q)
    if q.h > q.t then
        -- queue is empty
        q.h = 1
        q.t = 0
        return
    end
    local h = q.h
    return q[h]
end

--- Remove and return the value at the front of the queue (O(1)).
--- Returns `nil` when the queue is empty (and resets its indices).
---@param q queue
---@return any|nil @ The dequeued value, or `nil` if empty.
function queue.pop(q)
    if q.h > q.t then
        -- queue is empty
        q.h = 1
        q.t = 0
        return
    end
    -- pop queue
    local h = q.h
    local v = q[h]
    q[h] = nil
    q.h = h + 1
    return v
end

--- Return the number of elements currently in the queue (O(1)).
---@param q queue
---@return integer
---@nodiscard
function queue.size(q)
    return q.t - q.h + 1
end

return queue
