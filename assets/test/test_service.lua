local moon = require "moon"
local conf = ...

if conf.exit_target then
    moon.dispatch("lua", function()
        moon.send("lua", conf.main, "accepted")
        moon.sleep(60000) -- Deliberately leave the accepted call unanswered.
    end)
    return
elseif conf.exit_watcher then
    moon.dispatch("lua", function()
        for _ = 1, 2 do
            moon.async(function()
                local result, err = moon.call("lua", conf.target, "hold")
                assert(result == false and type(err) == "string")
                moon.send("lua", conf.main, "released", conf.unique)
            end)
        end
    end)
    return
end

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

        local accepted, ordinary_released, unique_released = 0, 0, 0
        moon.dispatch("lua", function(_, _, command, unique)
            if command == "accepted" then
                accepted = accepted + 1
            elseif unique then
                unique_released = unique_released + 1
            else
                ordinary_released = ordinary_released + 1
            end
        end)
        local target = moon.new_service({source = source, exit_target = true, main = moon.id})
        assert(target and target ~= 0)
        local watchers = {}
        for _, unique in ipairs({false, true}) do
            local watcher = moon.new_service({
                name = "exit_watcher_" .. tostring(unique), source = source, unique = unique,
                exit_watcher = true, main = moon.id, target = target,
            })
            assert(watcher and watcher ~= 0)
            watchers[#watchers + 1] = watcher
            moon.send("lua", watcher, "start")
        end
        while accepted < 4 do moon.sleep(1) end
        moon.kill(target)
        while unique_released < 2 do moon.sleep(1) end
        assert(ordinary_released == 0 and unique_released == 2)
        for _, watcher in ipairs(watchers) do moon.kill(watcher) end
    end, debug.traceback)
    if not ok then
        moon.error(err)
    else
        print("service regression passed: name ownership, empty messages and unique service-exit waiters")
    end
    moon.exit(ok and 0 or 1)
end)
