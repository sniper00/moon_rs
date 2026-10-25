-- Run: moon_rs assets/benchmark/benchmark_hotfix.lua [FUNCTIONS=400] [ROUNDS=12]
-- Values measure the current Lua state's heap after two full collections, not RSS.
local moon = require "moon"
local hotfix = require "hotfix"
local core = require "hotfix.core"
local args = moon.args()
local count, rounds = tonumber(args[1]) or 400, tonumber(args[2]) or 12
assert(count >= 1 and count % 1 == 0 and rounds >= 1 and rounds % 1 == 0)
collectgarbage("restart") -- service initialization initially pauses automatic GC

local function source(step)
    local lines = { "local n=0;local M={}" }
    for i = 1, count do
        lines[#lines + 1] = ("function M.f%d()n=n+%d;return n end"):format(i, step)
    end
    lines[#lines + 1] = "return M"
    return table.concat(lines, "\n")
end

local function memory()
    collectgarbage("collect")
    collectgarbage("collect")
    return collectgarbage("count")
end

local before = memory()
local loader = assert(load(source(1), "@hotfix_memory.lua"))
local module = loader()
local compiled = memory()
hotfix.register("hotfix_memory", loader, module)
local registered = memory()
loader = nil

local nextloader
hotfix.addsearcher(function(name) if name == "hotfix_memory" then return nextloader end end)
local cached = {}
for step = 1, rounds do
    cached[step] = core.resolve(module.f1)
    nextloader = assert(load(source(step + 1), "@hotfix_memory.lua"))
    assert(hotfix.update("hotfix_memory"))
end
local retained = memory()
cached = nil
local released = memory()

print(("hotfix memory: functions=%d rounds=%d"):format(count, rounds))
print(("compiled module: %.2f KiB"):format(compiled - before))
print(("registration overhead: %.2f KiB"):format(registered - compiled))
print(("total with cached old functions: %.2f KiB"):format(retained - before))
print(("total after releasing old functions: %.2f KiB"):format(released - before))
moon.exit(0)
