-- Verify the to-be-closed (closing value) feature of the generic for loop.
--
-- Per the Lua 5.4/5.5 Reference Manual (this project uses lua55):
--   §3.3.5 Generic for: evaluating `explist` produces four values -- the iterator
--           function, the state, the initial control variable, and a 4th closing
--           value that behaves like a to-be-closed variable.
--   §3.3.8 To-be-closed: its `__close` metamethod is called when the variable
--           goes out of scope -- normal termination, exiting via
--           break/goto/return, or exiting by an error; called as
--           `__close(value, err)` (err is nil when there was no error); the
--           closing value must have a `__close` metamethod or be a false value.
--
-- The three cases below cover the three exit triggers listed in §3.3.8.
local moon = require "moon"

local log = {}

local function make_closeable(name)
    return setmetatable({}, {
        -- §3.3.8: closing calls `__close` with the signature (value, err). Args ignored here.
        __close = function()
            print("__close called for " .. name)
            log[#log + 1] = name .. " closed"
        end,
    })
end

local function make_iter(n, err_at)
    local i = 0
    local state = make_closeable("iter_" .. n .. (err_at and "_err" or ""))
    local function next_fn()
        i = i + 1
        if err_at and i == err_at then
            error("deliberate error at " .. i)
        end
        if i > n then return nil end
        return i
    end
    -- §3.3.5: four return values = iterator, state, control, closing value (=state).
    return next_fn, nil, nil, state
end

moon.async(function()
    -- §3.3.8 trigger 1: normal block termination (loop runs to completion)
    print("===== 1. 正常结束 =====")
    log = {}
    for v in make_iter(3) do
        log[#log + 1] = "got " .. v
    end
    assert(log[#log] == "iter_3 closed", "FAIL: expected close after normal end, got: " .. tostring(log[#log]))
    print("  PASS: __close called after normal loop end")
    print("  log: " .. table.concat(log, ", "))

    -- §3.3.8 trigger 2: exiting its block by break/goto/return
    print("\n===== 2. break 退出 =====")
    log = {}
    for v in make_iter(100) do
        log[#log + 1] = "got " .. v
        print("got " .. v)
        if v == 3 then break end
    end
    assert(log[#log] == "iter_100 closed", "FAIL: expected close after break, got: " .. tostring(log[#log]))
    print("  PASS: __close called after break")
    print("  log: " .. table.concat(log, ", "))

    -- §3.3.8 trigger 3: exiting by an error (err is passed as __close's 2nd argument)
    print("\n===== 3. error 退出 =====")
    log = {}
    local ok, err = pcall(function()
        for v in make_iter(10, 4) do
            log[#log + 1] = "got " .. v
        end
    end)
    assert(not ok, "FAIL: expected error")
    assert(log[#log] == "iter_10_err closed", "FAIL: expected close after error, got: " .. tostring(log[#log]))
    print("  PASS: __close called after error")
    print("  log: " .. table.concat(log, ", "))
    print("  error: " .. err)

    print("\n===== ALL PASSED =====")
    moon.exit(0)
end)
