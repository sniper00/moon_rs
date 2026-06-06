local moon = require("moon")

local conf = ...

if conf and conf.slave then
    if conf.auto_quit then
        print("auto quit, bye bye")
        -- 使服务退出
        moon.timeout(0, function()
            moon.quit()
        end)
    end
else
    moon.async(function()
        while true do
            moon.new_service( {
                name = "create_service",
                source = "benchmark_create_service.lua",
                message = "Hello create_service",
                slave = true,
                auto_quit = true
            })
        end
    end)
end



