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
            local id = moon.new_service( {
                name = "",
                source = "benchmark_create_service.lua",
                message = "Hello create_service",
                slave = true,
                auto_quit = true
            })
            assert(id and id ~= 0, "failed to create benchmark service")
        end
    end)
end


