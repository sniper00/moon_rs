local moon= require "moon"

local CMD = {}

function CMD.test_send(a, b)
    print("recv", a, b)
end

function CMD.add(a, b)
    return a+b
end

function CMD.sub(a, b)
    assert(false)
end

function CMD.call_then_quit(a, b)
    moon.quit()
    moon.sleep(10000000)
end



moon.dispatch("lua", function (sender, session, cmd, ...)
    --moon.print("dispatch", sender, session)
    --print_r(cmd, ...)

    -- print(cmd, ...)

    moon.response("lua", sender, session, CMD[cmd](...))
end)
