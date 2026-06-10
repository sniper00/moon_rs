local moon = require("moon")
local fs = require("fs")

local conf = ...

local name = "sharedata"

if conf and conf.agent then
    local data
    local command = {}

    command.LOAD = function()
        local sharetable = require("moon.sharetable")
        local res = sharetable.queryall()
        for k, v in pairs(res) do
            print("queryall result:", k, v)
        end
        data = res[name]
        print("LOAD data:")
        print_r(data)
        assert(data, "queryall should return sharedata")
        assert(data.a == 1)
        assert(data.b == "hello")
        assert(#data.c == 5)
        return true
    end

    command.UPDATE = function()
        local sharetable = require("moon.sharetable")
        data = sharetable.query(name .. ".lua")
        print("UPDATE data:")
        print_r(data)
        assert(data, "query should return updated sharedata")
        assert(data.a == 2)
        assert(data.b == "world")
        assert(data.c[1] == 7)
        return true
    end

    moon.dispatch('lua', function(sender, session, cmd, ...)
        local f = command[cmd]
        if f then
            moon.response("lua", sender, session, f(...))
        else
            moon.error(moon.name, "recv unknown cmd " .. cmd)
        end
    end)
else
    local content_old = [[
        local M = {
            a = 1,
            b = "hello",
            c = {
                1,2,3,4,5
            }
        }
        return M
    ]]

    local content_new = [[
        local M = {
            a = 2,
            b = "world",
            c = {
                7,8,9,10,11
            }
        }
        return M
    ]]

    moon.async(function()
        fs.mkdir("./table")
        io.writefile("./table/" .. name .. ".lua", content_old)

        moon.new_service({
            unique = true,
            name = "sharetable",
            source = "../../lualib/moon/sharetable.lua",
            dir = "./table"
        })

        local agent = moon.new_service({
            name = "agent",
            source = "test_sharetable.lua",
            agent = true
        })

        assert(agent > 0, "Failed to create agent service")

        local ok = moon.call("lua", agent, "LOAD")
        assert(ok, "LOAD failed")
        print("LOAD test passed")

        io.writefile("./table/" .. name .. ".lua", content_new)
        local sharetable = require("moon.sharetable")
        local load_ok = sharetable.loadfile(name .. ".lua")
        print("loadfile result:", load_ok)

        ok = moon.call("lua", agent, "UPDATE")
        assert(ok, "UPDATE failed")
        print("UPDATE test passed")

        moon.kill(agent)
        moon.sleep(100)
        moon.kill(moon.query("sharetable"))
        moon.sleep(100)

        fs.remove("./table/" .. name .. ".lua")
        fs.remove("./table")

        print("All sharetable tests passed!")
        moon.exit(0)
    end)
end
