-- Test: watchdog should interrupt an infinite loop after timeout.
-- The monitor detects the stuck actor (>10s) and sets the trap flag,
-- which causes signal_hook to fire and raise a Lua error.

local moon = require("moon")

print("Starting infinite loop test...")
print("This should be interrupted by the watchdog after ~10 seconds.")

-- Register a message handler that enters an infinite loop
moon.dispatch("lua", function(msg, ...)
    print("Received message, entering infinite loop...")
    local i = 0
    while true do
        i = i + 1
    end
end)

-- Send a message to self to trigger the infinite loop
moon.send("lua", moon.id, "trigger")

moon.system("timeout_kill", function(sender, ...)
    moon.error("test error", ...)
end)

print("Message sent, waiting for watchdog interrupt...")
