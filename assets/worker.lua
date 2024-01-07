local moon= require "moon"

local CMD = {}

function CMD.add(a, b)
    return a+b
end

function CMD.sub(a, b)
    assert(false)
end


moon.dispatch("lua", function (sender, session, cmd, ...)
    --moon.print("dispatch", sender, session)
    --print_r(cmd, ...)

    -- print(cmd, ...)

    moon.response("lua", sender, session, CMD[cmd](...))
end)
