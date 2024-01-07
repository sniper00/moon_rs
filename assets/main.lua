local moon= require "moon"
local httpc = require "moon.http.client"

moon.dispatch("lua", function(sender, session, msg)
    --moon.print("dispatch", sender,session)
    print_r(msg)
end)


moon.async(function()
    print("before sleep")
    moon.sleep(1000)
    print("end sleep")
    print(moon.send("lua", moon.id, {a=1,b=2}))

    local workerid = moon.new_service({
        name = "worker",
        source = "worker.lua",
    })

    print(workerid)

    local bt = moon.clock()
    for i=1,100000 do
        moon.call("lua", workerid, "add", 1, 2)
    end

    print("cost", moon.clock()-bt)

    print(moon.call("lua", workerid, "sub", 1, 2))

    moon.kill(workerid)

    print(moon.call("lua", workerid, "helloworld"))

    moon.quit()

    -- print_r(httpc.get("https://192.168.1.111"))

    -- local form = { username = "wang", passwd = "456", age = 110 }
    -- print_r(httpc.post_form("http://127.0.0.1:9991/login",form))

end)

moon.shutdown(function()
    print("server shutdown")

    moon.async(function (...)
        -- for i=1,20 do
        --     moon.sleep(100)
        --     print("server shutdown", i)
        -- end

        moon.quit()
    end)
end)
