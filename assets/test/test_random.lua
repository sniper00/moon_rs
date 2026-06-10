local moon = require "moon"
local random = require "random"

local value = random.rand_range(1, 3)
assert(value >= 1 and value <= 3)

local some = random.rand_range_some(1, 5, 3)
assert(#some == 3)
local seen = {}
for _, v in ipairs(some) do
    assert(v >= 1 and v <= 5)
    assert(not seen[v])
    seen[v] = true
end

local f = random.randf_range(0.0, 1.0)
assert(f >= 0.0 and f < 1.0)
assert(random.randf_percent(1.0) == true)
assert(random.randf_percent(0.0) == false)

local weighted = random.rand_weight({ 10, 20 }, { 0, 1 })
assert(weighted == 20)

local weighted_some = random.rand_weight_some({ 10, 20, 30 }, { 0, 1, 1 }, 2)
assert(#weighted_some == 2)

print("test_random passed")
moon.exit(0)
