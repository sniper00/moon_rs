local moon = require("moon")
local corunning = coroutine.running
local costatus = coroutine.status

local traceback = debug.traceback

local ipairs = ipairs

local xpcall = xpcall

local _M = {}

function _M.wait_all(fnlist)
    local n = #fnlist
    local res = {}
    if n == 0 then
        return res
    end
    local co = corunning()
    moon.timeout(0, function()
        for i,fn in ipairs(fnlist) do
            moon.async(function ()
                local one = {xpcall(fn, traceback)}
                if one[1] then
                    table.remove(one, 1)
                end
                res[i] = one
                n=n-1
                if n==0 then
                    if costatus(co) == "suspended" then
                        moon.wakeup(co)
                    end
                end
            end)
        end
    end)
    moon.wait()
    return res
end

return _M