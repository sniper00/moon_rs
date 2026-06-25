local moon = require "moon"
local schema = require "schema"

-- Schema mirrors moon's example_schema.lua, but uses the explicit `wrapper`
-- flag (instead of an `array_`/`map_` name prefix) for the wrapper proto.
local define = {
    array_int64 = {
        wrapper = true,
        data = { container = "array", value_type = "int64" },
    },
    Reward = {
        type           = { value_type = "int32" },
        rewardtimes    = { value_type = "int32" },
        buyrewardtimes = { value_type = "int32" },
    },
    ItemData = {
        id     = { value_type = "int32" },
        count  = { value_type = "int64" },
        reward = { value_type = "Reward" },
    },
    UserData = {
        uid   = { value_type = "int64" },
        name  = { value_type = "string" },
        level = { value_type = "int32" },
        -- map<int32, ItemData>
        itemlist = { container = "object", key_type = "int32", value_type = "ItemData" },
        -- map<int32, array_int64>
        taskrewardgetlist = { container = "object", key_type = "int32", value_type = "array_int64" },
    },
}

schema.load(define)

local function ok(...)
    assert(pcall(schema.validate, ...), "expected validation to pass")
end

local function bad(...)
    assert(not pcall(schema.validate, ...), "expected validation to fail")
end

do
    -- valid: a full, correctly-typed UserData
    -- Note: object keys use a value other than integer 1 to avoid the
    -- array-vs-object ambiguity rule (see the __object test below).
    ok("UserData", {
        uid = 1001,
        name = "alice",
        level = 30,
        itemlist = {
            [100] = { id = 1, count = 99, reward = { type = 1, rewardtimes = 2, buyrewardtimes = 3 } },
        },
        taskrewardgetlist = {
            [5] = { 1, 2, 3 },
        },
    })

    -- partial tables are valid (only present fields are checked)
    ok("UserData", { uid = 1 })
    ok("UserData", {})
end

do
    -- primitive mismatches
    bad("UserData", { name = 123 })   -- string expected, got number
    bad("UserData", { uid = "x" })    -- int64 expected, got string
    bad("UserData", { level = 1.234 }) -- int32 rejects a float
end

do
    -- integer range / sign checks (improvement over the C++ original)
    bad("UserData", { level = 2147483648 })  -- > i32::MAX
    bad("UserData", { level = -2147483649 }) -- < i32::MIN
    -- int64 (uid) accepts large magnitudes and negatives
    ok("UserData", { uid = -9007199254740991 })
end

do
    -- undefined field
    bad("UserData", { nope = 1 })
end

do
    -- object key type + nested message checks
    bad("UserData", { itemlist = { a = { id = 1, count = 1 } } }) -- $key int32 expected, got string
    bad("UserData", { itemlist = { [100] = 123 } })               -- ItemData table expected, got number
    bad("UserData", { itemlist = { [100] = { unknown = 1 } } })   -- undefined field ItemData.unknown
    bad("UserData", {
        itemlist = { [100] = { reward = { type = 1, buyrewardtimes = false, rewardtimes = 100 } } },
    }) -- Reward.buyrewardtimes int32 expected, got boolean
end

do
    -- wrapper proto reached through an object value: array of int64
    ok("UserData", { taskrewardgetlist = { [5] = { 1, 2, 3 } } })
    bad("UserData", { taskrewardgetlist = { [5] = { 1, 2, false } } }) -- bool in int64 array
end

do
    -- validating the wrapper proto directly
    ok("array_int64", { 1, 2, 3 })
    ok("array_int64", {})
    bad("array_int64", { 1, 2, "x" })
end

do
    -- object with integer key 1 requires the __object metafield
    local obj = { [1] = { id = 1, count = 1, reward = { type = 1, rewardtimes = 1, buyrewardtimes = 1 } } }
    bad("UserData", { itemlist = obj })
    setmetatable(obj, { __object = true })
    ok("UserData", { itemlist = obj })
end

do
    -- using an undefined proto errors
    bad("DoesNotExist", {})
end

print("test_schema passed")
moon.exit(0)
