-- Test: watchdog should interrupt an infinite loop inside a coroutine.
-- The switchL mechanism ensures the hook is installed on the correct
-- coroutine state even when execution switches between coroutines.

local moon = require("moon")

print("Starting coroutine infinite loop test...")
print("This should be interrupted by the watchdog after ~10 seconds.")

moon.dispatch("lua", function(msg, ...)
    print("Received message, creating coroutine with infinite loop...")

    local co = coroutine.create(function()
        local i = 0
        while true do
            i = i + 1
        end
    end)

    -- This resume should be interrupted by switchL + signal_hook
    local ok, err = coroutine.resume(co)
    print("coroutine.resume returned:", ok, err)
    if not ok and err:find("interrupted") then
        print("SUCCESS: coroutine was interrupted by watchdog!")
    else
        print("FAILURE: coroutine was not properly interrupted")
    end
end)

moon.send("lua", moon.id, "trigger")
