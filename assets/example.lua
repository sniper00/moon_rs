local moon = require "moon"

local conf = ...

if conf.worker then
    local CMD = {}

    function CMD.test_send(a, b)
        print("recv", a, b)
    end

    function CMD.add(a, b)
        return a + b
    end

    function CMD.sub(a, b)
        assert(false)
    end

    function CMD.call_then_quit(a, b)
        moon.quit()
        moon.sleep(10000000)
    end

    moon.dispatch("lua", function(sender, session, cmd, ...)
        --moon.print("dispatch", sender, session)
        --print_r(cmd, ...)

        -- print(cmd, ...)

        moon.response("lua", sender, session, CMD[cmd](...))
    end)

    return
end

moon.dispatch("lua", function(sender, session, arg)
    assert(arg.a == 1 and arg.b == 2)
    print_r(arg)
end)

moon.async(function()
    print(moon.hash("md5", "12"))

    print("before sleep")
    moon.sleep(1000)
    print("end sleep")

    -- send to self
    moon.send("lua", moon.id, { a = 1, b = 2 })

    local workerid = moon.new_service({
        name = "example",
        source = "example.lua",
        worker = true,
    })

    -- send to other actor
    moon.send("lua", workerid, "test_send", 1, 2)

    local bt = moon.clock()
    for i = 1, 100000 do
        moon.call("lua", workerid, "add", 1, 2)
    end

    print("10w times call cost", moon.clock() - bt)

    --- test error
    local ok, err = moon.call("lua", workerid, "sub", 1, 2)
    assert(not ok)

    --- test service quit
    print(moon.call("lua", workerid, "call_then_quit"))

    --- test not exist
    print(moon.call("lua", workerid, "helloworld"))

    moon.exit(0)
end)

moon.shutdown(function()
    print("server shutdown")
    moon.async(function(...)
        moon.quit()
    end)
end)
