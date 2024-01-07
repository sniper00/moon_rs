local moon= require "moon"
local json = require "json"

local str = io.readfile("twitter.json")

local tt = json.decode_v2(str)

local bt = moon.clock()
for i=1,100 do
    json.decode_v2(str)
end

print("json.decode", moon.clock() - bt)

-- local bt = moon.clock()
-- for i=1,100 do
--     json.encode(tt)
-- end

-- print("json.encode", moon.clock() - bt)