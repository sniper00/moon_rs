-- Both main Lua-state and nested-coroutine loops must be interrupted (~30s).
local moon = require "moon"
moon.loglevel("INFO")
local conf = ...
moon.shutdown(function() moon.quit() end)
if conf and conf.mode then
    moon.dispatch("lua",function()
        local function endless() local i=0; while true do i=i+1 end end
        local ok, err
        if conf.mode == "coroutine" then
            ok, err = coroutine.resume(coroutine.create(endless))
        else
            ok, err = pcall(endless)
        end
        moon.send("lua",conf.parent,conf.mode,not ok and tostring(err):find("interrupted") ~= nil)
        moon.quit()
    end)
    return
end
moon.timeout(45000,function() moon.error("watchdog test timed out");moon.exit(1) end)
local count=0
moon.dispatch("lua",function(_,_,mode,passed)
    assert(passed,"watchdog did not interrupt "..mode)
    count=count+1
    if count==2 then print("WATCHDOG PASSED");moon.exit(0) end
end)
moon.async(function()
    for _,mode in ipairs({"main","coroutine"}) do
        local id=moon.new_service({source="test_watchdog.lua",mode=mode,parent=moon.id})
        assert(id and id>0)
        moon.send("lua",id,"start")
    end
end)
