---
--- benchmark_zset.lua — Throughput/latency benchmark for the native `zset`.
---
--- Builds a leaderboard of N elements and benchmarks every operation
--- (update/insert, rank, score, has, key_by_rank, reposition, range, erase,
--- reinsert), reporting ops/sec and average per-op latency.
---
--- Run:  moon_rs assets/benchmark/benchmark_zset.lua [N] [OPS]
---       moon_rs assets/benchmark/benchmark_zset.lua 1000000 200000
---

local moon   = require("moon")
local random = require("random")
local zset   = require("zset")

moon.loglevel("INFO")

---------------------------------------------------------------------------
-- Configuration
---------------------------------------------------------------------------

-- moon_rs moon.args() returns { arg1, arg2, ... } (no script path at [1]).
local args     = moon.args()
local N        = tonumber(args[1]) or 1000000 -- elements in the leaderboard
local OPS      = tonumber(args[2]) or 200000  -- iterations per timed op
local RANGE_OPS = math.max(1, math.floor(OPS / 20)) -- range returns 100 keys/call
local RANGE_LEN = 100

local SCORE_LO = 10000000
local SCORE_HI = 2000000000

---------------------------------------------------------------------------
-- Helpers
---------------------------------------------------------------------------

local clock = moon.clock

local rows = {}

-- Time `count` invocations of `fn(i)` and record throughput + avg latency.
local function bench(label, count, fn)
    local bt = clock()
    fn(count)
    local elapsed = clock() - bt
    rows[#rows + 1] = {
        label   = label,
        count   = count,
        ms      = elapsed * 1000,
        ops     = count / elapsed,
        avg_us  = (elapsed / count) * 1e6,
    }
    moon.info(string.format("  [%-16s] %10d ops in %8.1f ms  (%10.0f ops/s, %7.3f us/op)",
        label, count, elapsed * 1000, count / elapsed, (elapsed / count) * 1e6))
end

local function print_results()
    print("\n================================================================")
    print(string.format("  zset benchmark  (N=%d, OPS=%d)", N, OPS))
    print("================================================================")
    local hdr = string.format("%-18s %12s %12s %14s %12s", "op", "count", "total ms", "ops/sec", "avg us")
    print(hdr)
    print(string.rep("-", #hdr))
    for _, r in ipairs(rows) do
        print(string.format("%-18s %12d %12.1f %14.0f %12.3f",
            r.label, r.count, r.ms, r.ops, r.avg_us))
    end
    print(string.rep("-", #hdr))
end

---------------------------------------------------------------------------
-- Precompute inputs (kept out of timed loops to isolate zset cost)
---------------------------------------------------------------------------

moon.info(string.format("preparing inputs: N=%d, OPS=%d ...", N, OPS))

local scores = {}
for i = 1, N do
    scores[i] = random.rand_range(SCORE_LO, SCORE_HI)
end

-- Random existing keys / ranks / new scores for the timed loops.
local rkeys = {}     -- random keys in [1, N]
local rranks = {}    -- random ranks in [1, N]
local uscores = {}   -- new random scores for repositioning
for j = 1, OPS do
    rkeys[j]   = random.rand_range(1, N)
    rranks[j]  = random.rand_range(1, N)
    uscores[j] = random.rand_range(SCORE_LO, SCORE_HI)
end

---------------------------------------------------------------------------
-- Benchmark
---------------------------------------------------------------------------

local z = zset.new(N)

-- 1) build: insert N brand-new elements.
bench("build(insert)", N, function()
    for i = 1, N do
        z:update(i, scores[i], 1)
    end
end)
assert(z:size() == N, "expected full leaderboard")

-- 2) rank lookups (O(log N)).
local sink = 0
bench("rank", OPS, function(count)
    for j = 1, count do
        sink = sink + (z:rank(rkeys[j]) or 0)
    end
end)

-- 3) score lookups (O(1)).
bench("score", OPS, function(count)
    for j = 1, count do
        sink = sink + z:score(rkeys[j])
    end
end)

-- 4) existence checks (O(1)).
bench("has", OPS, function(count)
    for j = 1, count do
        if z:has(rkeys[j]) then sink = sink + 1 end
    end
end)

-- 5) key_by_rank (O(log N)).
bench("key_by_rank", OPS, function(count)
    for j = 1, count do
        sink = sink + (z:key_by_rank(rranks[j]) or 0)
    end
end)

-- 6) reposition existing elements with a new score (O(log N), remove+insert).
bench("update(reposition)", OPS, function(count)
    for j = 1, count do
        z:update(rkeys[j], uscores[j], 1)
    end
end)
assert(z:size() == N, "size must be stable after repositioning")

-- 7) range query of the top RANGE_LEN keys (O(log N + M)).
bench("range(top100)", RANGE_OPS, function(count)
    for _ = 1, count do
        local t = z:range(1, RANGE_LEN)
        sink = sink + #t
    end
end)

-- 8) erase a distinct batch (O(log N)).
local ERASE_N = math.min(OPS, N)
bench("erase", ERASE_N, function(count)
    for i = 1, count do
        z:erase(i)
    end
end)
assert(z:size() == N - ERASE_N, "size must drop by erased count")

-- 9) reinsert the erased batch into the now-populated set (O(log N)).
bench("reinsert", ERASE_N, function(count)
    for i = 1, count do
        z:update(i, scores[i], 1)
    end
end)
assert(z:size() == N)

print_results()
-- Keep `sink` observable so the loops can't be optimized away conceptually.
moon.info(string.format("(checksum=%d)", sink))

moon.exit(0)
