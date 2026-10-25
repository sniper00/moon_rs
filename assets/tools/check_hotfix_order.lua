-- Run: moon_rs assets/tools/check_hotfix_order.lua old.lua new.lua
-- Exit 0: checked; 1: mismatch/syntax error; 2: manual review needed.
local moon = require "moon"
local check = require("hotfix").check_order
local args = moon.args()
local ok, code = pcall(function()
    assert(args[1] and args[2], "usage: check_hotfix_order.lua old.lua new.lua")
    local function read(path)
        local file = assert(io.open(path, "rb"))
        local source = file:read("a"); file:close()
        return source, assert(load(source, "@" .. path, "t"))
    end
    local oldsource, oldloader = read(args[1])
    local newsource, newloader = read(args[2])
    if oldsource == newsource then print("hotfix order: unchanged"); return 0 end
    local valid, errors, uncertain = check(oldloader, newloader)
    for _, message in ipairs(errors) do print("hotfix order error:", message) end
    for _, message in ipairs(uncertain) do print("hotfix order review:", message) end
    if valid then print("hotfix order: passed"); return 0 end
    return #errors > 0 and 1 or 2
end)
if not ok then print(code); code = 1 end
moon.exit(code)
