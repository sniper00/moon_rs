local moon = require("moon")
local list = require("collections.queue")

local coroutine = coroutine

---@class mutex_scope
--- A to-be-closed handle returned when a coroutine lock is acquired.
--- Bind it with `local guard <close> = lock()`; releasing happens automatically
--- when `guard` leaves scope, which decrements the hold count and—when it reaches
--- zero—wakes the next queued coroutine.

--- A function returned by `mutex()`. Acquire/query the lock:
--- calling `lock()` acquires it (blocks the coroutine until free) and returns a
--- `mutex_scope` to `<close>`; calling `lock(true)` returns the current hold
--- count (integer) without acquiring. Reentrant: the holding coroutine may
--- acquire again without blocking (each acquire must be matched by a scope close).
---@alias mutex_acquire fun(refcount?: boolean): mutex_scope|integer

--- Creates an actor-local coroutine lock (mutual exclusion across coroutines of
--- the same actor). Only one coroutine holds the lock at a time; others are
--- queued and yielded until it is released.
---
--- ```lua
--- local mutex = require("moon.mutex")
--- local lock = mutex()
--- moon.async(function()
---     local guard <close> = lock()  -- acquire; auto-release at end of scope
---     -- critical section: runs without interleaving other holders
--- end)
--- ```
---@return mutex_acquire @ The acquire function for this lock.
local function mutex()
	---@type thread? @ The coroutine currently holding the lock (nil when free).
	local current_thread
	---@type integer @ Reentrant hold count of `current_thread`.
	local ref = 0
	-- FIFO of coroutines waiting to acquire.
	local thread_queue = list.new()

	---@type mutex_scope
	local scope = setmetatable({}, { __close = function()
		ref = ref - 1
		if ref == 0 then
			current_thread = list.pop(thread_queue)
			if current_thread then
				moon.wakeup(current_thread)
			end
		end
	end})

	---@async
	---@nodiscard
	---@param refcount? boolean @ When truthy, return the current hold count instead of acquiring.
	---@return mutex_scope|integer @ A `<close>` scope when acquiring, or the hold count when `refcount` is truthy.
	return function(refcount)
		if refcount then
			return ref
		end
		local thread = coroutine.running()
		if current_thread and current_thread ~= thread then
			list.push(thread_queue, thread)
			coroutine.yield()
			assert(ref == 0)	-- current_thread == thread
		end
		current_thread = thread
		ref = ref + 1
		return scope
	end
end

return mutex
