---
--- benchmark_namesearch.lua — Player-name search performance on PostgreSQL.
---
--- Generates 1,000,000 player rows and benchmarks the three search modes of
--- `moon.namesearch` (prefix / substring / similarity), reporting latency
--- percentiles and throughput, plus an EXPLAIN of each query to confirm the
--- index is used.
---
--- Run:  moon_rs assets/benchmark/benchmark_namesearch.lua [TOTAL]
---       moon_rs assets/benchmark/benchmark_namesearch.lua 1000000
---
--- Requires a reachable PostgreSQL and the pg_trgm extension (for substring /
--- similarity modes). Adjust PG_URL below for your environment.
---

local moon       = require("moon")
local namesearch = require("moon.namesearch")
local pg         = require("moon.db.pg")

moon.loglevel("INFO")

---------------------------------------------------------------------------
-- Configuration
---------------------------------------------------------------------------

local PG_URL   = "postgres://postgres:123456@127.0.0.1:5432/postgres"
local TABLE    = "players_bench"
-- moon.args() returns { script_path, arg1, ... }; arg1 (index 2) overrides TOTAL.
local TOTAL    = tonumber(moon.args()[2]) or 200000 -- rows to generate
local GEN_BATCH = 20000   -- rows per bulk insert (2 cols => 40000 bound params)
local QUERIES   = 2000    -- timed queries per search mode
local LIMIT     = 20      -- rows returned per search

---------------------------------------------------------------------------
-- Name generation vocabulary (latin words + Chinese given names)
---------------------------------------------------------------------------

---@type string[]
local LATIN = {
    "dragon", "shadow", "knight", "phoenix", "raven", "frost", "storm", "blaze",
    "venom", "titan", "rogue", "wizard", "hunter", "ranger", "ghost", "viper",
    "falcon", "wolf", "tiger", "lion", "eagle", "cobra", "panther", "demon",
    "angel", "saint", "reaper", "slayer", "guard", "mage", "ninja", "samurai",
    "warrior", "archer", "paladin", "druid", "monk", "berserk", "nomad", "pilot",
}
---@type string[]
local CHINESE_SURNAME = { "李", "王", "张", "刘", "陈", "杨", "赵", "黄", "周", "吴", "徐", "孙", "马", "朱", "胡" }
---@type string[]
local CHINESE_GIVEN   = { "伟", "芳", "娜", "敏", "静", "磊", "强", "军", "洋", "勇", "艳", "杰", "涛", "明", "霞", "平", "刚", "健" }

local rand = math.random

local function gen_name()
    if rand() < 0.4 then
        local n = CHINESE_SURNAME[rand(#CHINESE_SURNAME)]
        n = n .. CHINESE_GIVEN[rand(#CHINESE_GIVEN)]
        if rand() < 0.5 then n = n .. CHINESE_GIVEN[rand(#CHINESE_GIVEN)] end
        return n
    else
        return LATIN[rand(#LATIN)] .. tostring(rand(0, 99999))
    end
end

-- Build query terms that are guaranteed to match a portion of the data.
-- Broad prefix: a whole token (matches many rows; stresses ORDER BY sort).
local function prefix_term()
    if rand() < 0.4 then
        return CHINESE_SURNAME[rand(#CHINESE_SURNAME)]
    end
    return LATIN[rand(#LATIN)]
end

-- Selective prefix: token + first digits (few matches; typical autocomplete).
local function prefix_sel_term()
    return LATIN[rand(#LATIN)] .. tostring(rand(0, 9)) .. tostring(rand(0, 9))
end

local function substring_term()
    local w = LATIN[rand(#LATIN)]
    local len = #w
    if len <= 4 then return w end
    local s = rand(1, len - 3)
    return w:sub(s, s + 2) -- 3-char latin slice (safe byte boundary)
end

local function similar_term()
    -- introduce a small typo into a latin word for fuzzy matching
    local w = LATIN[rand(#LATIN)]
    if #w < 4 then return w end
    local pos = rand(2, #w - 1)
    local repl = string.char(rand(97, 122))
    return w:sub(1, pos - 1) .. repl .. w:sub(pos + 1)
end

---------------------------------------------------------------------------
-- Latency statistics
---------------------------------------------------------------------------

local function stats(samples)
    table.sort(samples)
    local n = #samples
    local sum = 0
    for _, v in ipairs(samples) do sum = sum + v end
    local function pct(p)
        local idx = math.max(1, math.ceil(p / 100 * n))
        return samples[idx]
    end
    return {
        n   = n,
        avg = sum / n,
        p50 = pct(50),
        p95 = pct(95),
        p99 = pct(99),
        max = samples[n],
    }
end

local rows = {} -- result table for final print

local function run_mode(label, ns, term_fn, search_fn)
    local lat = {}
    local total_hits = 0
    -- warm-up
    for _ = 1, 50 do search_fn(ns, term_fn(), LIMIT) end

    local bt = moon.clock()
    for _ = 1, QUERIES do
        local term = term_fn()
        local t0 = moon.clock()
        local data = search_fn(ns, term, LIMIT)
        lat[#lat + 1] = (moon.clock() - t0) * 1000 -- ms
        total_hits = total_hits + #data
    end
    local elapsed = moon.clock() - bt

    local s = stats(lat)
    rows[#rows + 1] = {
        label = label,
        qps   = QUERIES / elapsed,
        avg   = s.avg,
        p50   = s.p50,
        p95   = s.p95,
        p99   = s.p99,
        max   = s.max,
        hits  = total_hits / QUERIES,
    }
    moon.info(string.format("  [%s] done: %.0f qps, avg %.3f ms", label, QUERIES / elapsed, s.avg))
end

local function print_results()
    print("\n================================================================")
    print(string.format("  Player-name search benchmark  (rows=%d, queries/mode=%d, LIMIT=%d)",
        TOTAL, QUERIES, LIMIT))
    print("================================================================")
    local hdr = string.format("%-12s %9s %9s %9s %9s %9s %9s %8s",
        "mode", "qps", "avg ms", "p50 ms", "p95 ms", "p99 ms", "max ms", "avg hits")
    print(hdr)
    print(string.rep("-", #hdr))
    for _, r in ipairs(rows) do
        print(string.format("%-12s %9.0f %9.3f %9.3f %9.3f %9.3f %9.3f %8.1f",
            r.label, r.qps, r.avg, r.p50, r.p95, r.p99, r.max, r.hits))
    end
    print(string.rep("-", #hdr))
end

---------------------------------------------------------------------------
-- Data generation
---------------------------------------------------------------------------

local function ensure_data(db, ns)
    local cnt = db:query(string.format("SELECT count(*) AS c FROM %s", TABLE))
    local have = cnt.data and tonumber(cnt.data[1].c) or 0
    if have >= TOTAL then
        print(string.format("table already has %d rows (>= %d), skipping generation", have, TOTAL))
        return
    end

    print(string.format("generating %d rows (current %d)...", TOTAL, have))
    db:query(string.format("TRUNCATE %s", TABLE))

    local bt = moon.clock()
    local batch = {}
    local inserted = 0
    for i = 1, TOTAL do
        batch[#batch + 1] = { i, gen_name() }
        if #batch >= GEN_BATCH then
            local res = ns.db:insert_many(TABLE, { "uid", "name" }, batch)
            assert(not res.code, res.code and (res.code .. " " .. tostring(res.message)))
            inserted = inserted + #batch
            batch = {}
            if inserted % (GEN_BATCH * 10) == 0 then
                moon.info(string.format("  inserted %d / %d", inserted, TOTAL))
            end
        end
    end
    if #batch > 0 then
        local res = ns.db:insert_many(TABLE, { "uid", "name" }, batch)
        assert(not res.code, res.code and (res.code .. " " .. tostring(res.message)))
        inserted = inserted + #batch
    end
    moon.info(string.format("  inserted %d / %d", inserted, TOTAL))
    print(string.format("generation took %.1fs (%.0f rows/s)",
        moon.clock() - bt, TOTAL / (moon.clock() - bt)))

    print("running ANALYZE...")
    db:query(string.format("ANALYZE %s", TABLE))
end

---------------------------------------------------------------------------
-- EXPLAIN — confirm index usage
---------------------------------------------------------------------------

local function explain(db, title, sql, ...)
    print("\n[EXPLAIN] " .. title)
    local res = db:query_params("EXPLAIN " .. sql, ...)
    if res.code then
        print("  (explain failed: " .. tostring(res.message) .. ")")
        return
    end
    for _, row in ipairs(res.data) do
        -- EXPLAIN returns a single column whose name is "QUERY PLAN"
        for _, v in pairs(row) do print("  " .. tostring(v)) end
    end
end

---------------------------------------------------------------------------
-- Main
---------------------------------------------------------------------------

moon.async(function()
    local db = pg.connect(PG_URL, "namesearch_bench", 5000, 1)
    if type(db) == "table" and db.code then
        moon.error("pg connect failed: " .. tostring(db.message))
        moon.exit(-1)
        return
    end

    ---@cast db pg
    local ns = namesearch.new(db, TABLE)
    local trgm = ns:init_schema()
    if not trgm then
        print("WARNING: pg_trgm not available — substring/similar modes will be skipped")
    end

    ensure_data(db, ns)

    -- Show the chosen plans on representative terms.
    explain(db, "prefix (LIKE 'dragon%')",
        string.format("SELECT uid,name FROM %s WHERE name LIKE $1 ESCAPE '\\' ORDER BY name LIMIT %d", TABLE, LIMIT),
        "dragon%")
    if trgm then
        explain(db, "substring (ILIKE '%rag%')",
            string.format("SELECT uid,name FROM %s WHERE name ILIKE $1 ESCAPE '\\' LIMIT %d", TABLE, LIMIT),
            "%rag%")
        explain(db, "similar (name % 'dragoon')",
            string.format("SELECT uid,name,similarity(name,$1) AS sim FROM %s WHERE name %% $1 ORDER BY sim DESC LIMIT %d", TABLE, LIMIT),
            "dragoon")
    end

    moon.info("running benchmarks...")
    run_mode("prefix(broad)", ns, prefix_term, ns.search_prefix)
    run_mode("prefix(sel)", ns, prefix_sel_term, ns.search_prefix)
    if trgm then
        run_mode("substring", ns, substring_term, ns.search_substring)
        run_mode("similar", ns, similar_term, ns.search_similar)
    end

    -- Demonstrate the frequent add/remove path latency too.
    do
        local lat = {}
        local base = TOTAL + 1
        local bt = moon.clock()
        for i = 1, QUERIES do
            local t0 = moon.clock()
            ns:add(base + i, gen_name())
            lat[#lat + 1] = (moon.clock() - t0) * 1000
        end
        local elapsed = moon.clock() - bt
        local s = stats(lat)
        rows[#rows + 1] = { label = "add(upsert)", qps = QUERIES / elapsed,
            avg = s.avg, p50 = s.p50, p95 = s.p95, p99 = s.p99, max = s.max, hits = 0 }
        for i = 1, QUERIES do ns:remove(base + i) end
    end

    print_results()

    db:query(string.format("DROP TABLE IF EXISTS %s", TABLE))
    db:close()
    moon.exit(0)
end)

moon.shutdown(function()
    moon.quit()
end)
