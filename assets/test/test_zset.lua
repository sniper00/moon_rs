local moon = require "moon"
local random = require "random"
local zset = require "zset"

do
    -- ordering + key_by_rank against a reference sort
    local max_count = 5000
    local item = {}
    for i = 1, max_count do
        item[i] = { i, random.rand_range(10000000, 2000000000) }
    end

    local sorted = {}
    for i = 1, max_count do
        sorted[i] = { item[i][1], item[i][2] }
    end
    table.sort(sorted, function(a, b)
        if a[2] == b[2] then
            return a[1] < b[1]
        end
        return a[2] > b[2]
    end)

    local rank = zset.new(max_count)
    for i = 1, max_count do
        rank:update(item[i][1], item[i][2], 1)
    end
    assert(rank:size() == max_count)

    for i = 1, max_count do
        assert(rank:key_by_rank(i) == sorted[i][1],
            string.format("rank %d: got %s want %d", i, tostring(rank:key_by_rank(i)), sorted[i][1]))
    end
end

do
    -- max count eviction
    local rank = zset.new(2)
    rank:update(1, 100, 1)
    rank:update(2, 200, 1)
    rank:update(3, 300, 1)
    assert(rank:size() == 2)
    assert(rank:rank(3) == 1)
    assert(rank:rank(2) == 2)
    assert(rank:rank(1) == nil)

    assert(rank:score(1) == 0)
    assert(rank:score(2) == 200)
    assert(rank:score(3) == 300)

    rank:erase(3)
    rank:update(1, 100, 1)
    assert(rank:size() == 2)
    assert(rank:rank(3) == nil)
    assert(rank:rank(2) == 1)
    assert(rank:rank(1) == 2)
end

do
    -- reverse ordering
    local rank = zset.new(3, true)
    rank:update(1, 100, 1)
    rank:update(2, 200, 1)
    rank:update(3, 300, 1)
    rank:update(4, 400, 1)
    assert(rank:size() == 3)
    assert(rank:rank(1) == 1)
    assert(rank:rank(2) == 2)
    assert(rank:rank(3) == 3)
    assert(rank:rank(4) == nil)
    assert(rank:score(1) == 100)
end

do
    -- range queries
    local rank = zset.new(4)
    rank:update(11, 100, 1)
    rank:update(21, 200, 1)
    rank:update(31, 300, 1)
    rank:update(41, 400, 1)

    local t = rank:range(1, 2)
    assert(t[1] == 41 and t[2] == 31 and #t == 2)

    t = rank:range(1, 2, true)
    assert(t[1] == 11 and t[2] == 21 and #t == 2)

    t = rank:range(1, 100)
    assert(t[1] == 41 and t[2] == 31 and t[3] == 21 and t[4] == 11 and #t == 4)

    -- Negative indices follow the original C++ semantics (arg-1 then +llen):
    -- range(-2, -1) maps to 0-based [1, 2] => keys 31, 21.
    t = rank:range(-2, -1)
    assert(t[1] == 31 and t[2] == 21 and #t == 2)
end

do
    -- key_by_rank after erase
    local rank = zset.new(4)
    rank:update(1, 100, 1)
    rank:update(2, 200, 1)
    rank:update(3, 300, 1)
    rank:update(4, 400, 1)

    assert(rank:key_by_rank(1) == 4)
    assert(rank:key_by_rank(2) == 3)
    assert(rank:key_by_rank(3) == 2)
    assert(rank:key_by_rank(4) == 1)

    rank:erase(2)
    assert(rank:key_by_rank(1) == 4)
    assert(rank:key_by_rank(2) == 3)
    assert(rank:key_by_rank(3) == 1)
end

do
    -- has / clear
    local rank = zset.new(10)
    rank:update(1, 100, 1)
    assert(rank:has(1))
    assert(not rank:has(2))
    rank:clear()
    assert(rank:size() == 0)
    assert(not rank:has(1))
end

print("test_zset passed")
moon.exit(0)
