local moon = require "moon"
local conf = ...

if conf.worker then
    local empty_sends = 0
    moon.dispatch("lua", function(sender, session, ...)
        local count = select('#', ...)
        local command = ...
        if session == 0 then
            assert(count == 0)
            empty_sends = empty_sends + 1
        elseif command == "stop" then
            moon.response("lua", sender, session, true)
            moon.quit()
        elseif command == "empty_sends" then
            moon.response("lua", sender, session, empty_sends)
        elseif count == 0 then
            moon.response("lua", sender, session)
        else
            moon.response("lua", sender, session, count)
        end
    end)
    return
end

moon.timeout(10000, function()
    moon.error("service regression timed out")
    moon.exit(1)
end)

moon.async(function()
    local ok, err = xpcall(function()
        local source = "test_service.lua"
        for index, flags in ipairs({{false, false}, {false, true}, {true, false}, {true, true}}) do
            local name = "service_regression_" .. index
            local first = moon.new_service({name = name, unique = flags[1], source = source, worker = true})
            assert(first and first ~= 0)
            local duplicate = moon.new_service({name = name, unique = flags[2], source = source, worker = true})
            assert(not duplicate or duplicate == 0, "duplicate name was accepted")
            assert(moon.query(name) == (flags[1] and first or 0), "failed creation changed registry")

            assert(select('#', moon.call("lua", first)) == 0, "empty call/response added values")
            assert(moon.call("lua", first, nil) == 1, "explicit nil must remain one value")
            moon.send("lua", first)
            assert(moon.call("lua", first, "empty_sends") == 1)
            assert(moon.call("lua", first, "stop"))
            moon.sleep(20)

            local replacement = moon.new_service({name = name, unique = flags[2], source = source, worker = true})
            assert(replacement and replacement ~= 0, "exit did not release name")
            assert(moon.call("lua", replacement, "stop"))
        end
    end, debug.traceback)
    if not ok then
        moon.error(err)
    else
        print("service regression passed: name ownership and empty messages")
    end
    moon.exit(ok and 0 or 1)
end)
