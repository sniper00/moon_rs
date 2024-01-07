local moon = require "moon"
local seri = require "seri"

local str = seri.packstring(1,2,3, "hello", {a=1,b=2})

print(seri.unpack(str))

local p = seri.pack(1,2,3, "hello", {a=1,b=2})
print(seri.unpack_one(p, true))
print(seri.unpack_one(p, true))
print(seri.unpack_one(p, true))
print(seri.unpack_one(p, true))
print(seri.unpack_one(p, true))
print(seri.unpack_one(p, true))

