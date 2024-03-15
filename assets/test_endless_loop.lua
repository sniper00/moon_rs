local moon = require("moon")

local arg = ...

if arg and arg.runner then
    moon.async(function (...)
        moon.sleep(0)
        local i = 0
        while i<5 do
            moon.thread_sleep(6000)
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

    moon.system("endless_loop", function (who, what)
        print(who..":",  what)
    end)
end