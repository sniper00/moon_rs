local moon = require("moon")
local utils = require("utils")

local arg = ...

if arg and arg.runner then
    moon.async(function (...)
        moon.sleep(0)
        local i = 0
        while i<5 do
            utils.thread_sleep(20000)
            moon.sleep(0)
            i=i+1
        end
    end)
else

    moon.async(function()
        local receiver = moon.new_service({
            name="test",
            source = "test_endless_loop.lua",
            runner = true
        })
    end)

    moon.system("slow_message", function (who, what)
        print(who..":",  what)
    end)
end