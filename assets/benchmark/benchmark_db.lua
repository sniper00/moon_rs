---
--- benchmark_db.lua — Database performance comparison benchmark.
---
--- Compares: pg (native), sqlx-postgresql, sqlx-mysql, mongodb, redis
--- Common operations show relative percentage (pg = 100%).
--- Driver-specific operations show raw numbers only.
---
--- Run:  moon_rs assets/benchmark/benchmark_db.lua
---

local moon = require("moon")
local json = require("json")

moon.loglevel("INFO")

---------------------------------------------------------------------------
-- Configuration — adjust connection strings for your environment
---------------------------------------------------------------------------

local PG_URL       = "postgres://postgres:123456@127.0.0.1:5432/postgres"
local SQLX_PG_URL  = "postgres://postgres:123456@127.0.0.1:5432/postgres"
local SQLX_MY_URL  = "mysql://root:root@127.0.0.1:3306/mysql?ssl-mode=DISABLED"
local MONGO_URL    = "mongodb://root:root@127.0.0.1:27017/?serverSelectionTimeoutMS=2000"
local REDIS_HOST   = "127.0.0.1"
local REDIS_PORT   = 6379

local N_SINGLE     = 1000   -- single read/write iterations
local N_BATCH      = 5000   -- batch read/write row count

---------------------------------------------------------------------------
-- Helpers
---------------------------------------------------------------------------

local drivers_ok = {}     -- { name = true/false }
local results    = {}     -- { [test_name] = { {driver, ops, elapsed}, ... } }

local function record(test_name, driver, ops, elapsed)
    if not results[test_name] then results[test_name] = {} end
    table.insert(results[test_name], { driver = driver, ops = ops, elapsed = elapsed })
end

local function print_final_tables()
    local common_order = {
        { key = "single_write", label = "Single Write" },
        { key = "single_read",  label = "Single Read" },
        { key = "batch_write",  label = "Batch Write" },
        { key = "batch_read",   label = "Batch Read" },
    }
    local specific_order = {
        { key = "pg_pipe",                label = "pg pipe" },
        { key = "pg_update_many",         label = "pg update_many" },
        { key = "pg_multi_stmt",          label = "pg multi_stmt" },
        { key = "pg_await",              label = "pg await" },
        { key = "pg_fire_forget",        label = "pg fire&forget" },
        { key = "sqlx_pg_transaction",   label = "sqlx-pg txn" },
        { key = "sqlx_mysql_transaction", label = "sqlx-my txn" },
        { key = "mongo_insert_many",     label = "mongo ins_many" },
        { key = "redis_pipeline",        label = "redis pipeline" },
    }
    local W = { 14, 16, 6, 10, 10, 8, 5 }
    local H = { "Test", "Driver", "Ops", "ms", "ops/s", "us/op", "%" }
    local function mksep(n)
        local p = {}
        for i = 1, n do p[i] = string.rep("-", W[i] + 2) end
        return "+" .. table.concat(p, "+") .. "+"
    end
    local function mkhdr(n)
        local p = {}
        for i = 1, n do
            local a = i <= 2 and "-" or ""
            p[i] = string.format(" %" .. a .. W[i] .. "s ", H[i])
        end
        return "|" .. table.concat(p, "|") .. "|"
    end
    local function mkrow(n, vals)
        local p = {}
        for i = 1, n do
            local a = i <= 2 and "-" or ""
            p[i] = string.format(" %" .. a .. W[i] .. "s ", tostring(vals[i] or ""))
        end
        return "|" .. table.concat(p, "|") .. "|"
    end
    local function data(n, label, drv, ops, elapsed, pct)
        local ops_s = string.format("%.0f", ops / elapsed)
        local us = string.format("%.1f", elapsed / ops * 1e6)
        local ms = string.format("%.2f", elapsed * 1000)
        if n == 7 then
            return mkrow(7, { label, drv, ops, ms, ops_s, us,
                pct and string.format("%.0f", pct) or "-" })
        end
        return mkrow(6, { label, drv, ops, ms, ops_s, us })
    end

    print("================================================================")
    print("  Database Performance Comparison Benchmark")
    print(string.format("  N_single=%d  N_batch=%d  (pg = 100%% baseline)",
        N_SINGLE, N_BATCH))
    print("================================================================\n")

    local sc = mksep(7)
    print("Common Operations (pg = 100%)")
    print(sc)
    print(mkhdr(7))
    print(sc)
    local first = true
    for _, t in ipairs(common_order) do
        local entries = results[t.key]
        if entries and #entries > 0 then
            if not first then print(sc) end
            first = false
            local base
            for _, e in ipairs(entries) do
                if e.driver == "pg" then base = e.elapsed; break end
            end
            for ei, e in ipairs(entries) do
                local lbl = ei == 1 and t.label or ""
                local pct = base and (base / e.elapsed * 100) or nil
                print(data(7, lbl, e.driver, e.ops, e.elapsed, pct))
            end
        end
    end
    print(sc)

    local ss = mksep(6)
    print("\nDriver-Specific Operations")
    print(ss)
    print(mkhdr(6))
    print(ss)
    for _, t in ipairs(specific_order) do
        local entries = results[t.key]
        if entries and #entries > 0 then
            for _, e in ipairs(entries) do
                print(data(6, t.label, e.driver, e.ops, e.elapsed))
            end
        end
    end
    print(ss)
end

---------------------------------------------------------------------------
-- Driver loaders — each returns { ok, obj, setup(), teardown() }
---------------------------------------------------------------------------

local pg_db, sqlxpg_db, sqlxmy_db, mongo_coll, redis_db

local function load_pg()
    local ok, pg = pcall(require, "moon.db.pg")
    if not ok then return false end
    local _seq = 0
    _seq = _seq + 1
    local db = pg.connect(PG_URL .. "?name=bench_pg_" .. _seq .. "&connect_timeout=5000&max_connections=1")
    if type(db) == "table" and db.code then
        print("  [pg] connect failed: " .. tostring(db.message))
        return false
    end
    db:query("DROP TABLE IF EXISTS bench_kv")
    db:query([[CREATE TABLE bench_kv (
        id bigint PRIMARY KEY, name text NOT NULL, info text)]])
    pg_db = db
    drivers_ok.pg = true
    return true
end

local function load_sqlx_pg()
    local ok, sqlx = pcall(require, "moon.db.sqlx")
    if not ok then return false end
    local ok2, db = pcall(sqlx.connect, SQLX_PG_URL, "bench_sqlxpg", 5000, 1)
    if not ok2 or (type(db) == "table" and db.kind) then
        print("  [sqlx-pg] connect failed: " .. tostring(db and db.message or db))
        return false
    end
    db:query("DROP TABLE IF EXISTS bench_sqlx_kv")
    db:query([[CREATE TABLE bench_sqlx_kv (
        id bigint PRIMARY KEY, name text NOT NULL, info text)]])
    sqlxpg_db = db
    drivers_ok.sqlx_pg = true
    return true
end

local function load_sqlx_mysql()
    local ok, sqlx = pcall(require, "moon.db.sqlx")
    if not ok then return false end
    local ok2, db = pcall(sqlx.connect, SQLX_MY_URL, "bench_sqlxmy", 5000, 1)
    if not ok2 or (type(db) == "table" and db.kind) then
        print("  [sqlx-mysql] connect failed: " .. tostring(db and db.message or db))
        return false
    end
    db:query("DROP TABLE IF EXISTS bench_kv_my")
    db:query([[CREATE TABLE bench_kv_my (
        id BIGINT PRIMARY KEY, name VARCHAR(255) NOT NULL, info TEXT)]])
    sqlxmy_db = db
    drivers_ok.sqlx_mysql = true
    return true
end

local function load_mongodb()
    local ok, mongodb = pcall(require, "moon.db.mongodb")
    if not ok then return false end
    local ok2, db = pcall(mongodb.connect, MONGO_URL, "bench_mongo")
    if not ok2 or (type(db) == "table" and db.kind) then
        print("  [mongodb] connect failed: " .. tostring(db and db.message or db))
        return false
    end
    local coll = db:collection("bench_db", "bench_kv")
    coll:delete_many({})
    mongo_coll = coll
    drivers_ok.mongodb = true
    return true
end

local function load_redis()
    local ok, redis = pcall(require, "moon.db.redis")
    if not ok then return false end
    local db, err = redis.connect(string.format("redis://%s:%d/0?connect_timeout=2000", REDIS_HOST, REDIS_PORT))
    if not db then
        print("  [redis] connect failed: " .. tostring(err))
        return false
    end
    db:flushdb()
    redis_db = db
    drivers_ok.redis = true
    return true
end

---------------------------------------------------------------------------
-- 1. Single Write — upsert one record × N
---------------------------------------------------------------------------

local function bench_single_write(N)
    local test = "single_write"
    local info = json.encode({ seq = 0, ts = os.date() })

    if drivers_ok.pg then
        local sql = "INSERT INTO bench_kv(id,name,info) VALUES($1,$2,$3) ON CONFLICT(id) DO UPDATE SET name=EXCLUDED.name,info=EXCLUDED.info"
        local bt = moon.clock()
        for i = 1, N do
            pg_db:query_params(sql, i, "pg_" .. i, info)
        end
        record(test, "pg", N, moon.clock() - bt)
    end

    if drivers_ok.sqlx_pg then
        local sql = "INSERT INTO bench_sqlx_kv(id,name,info) VALUES($1,$2,$3) ON CONFLICT(id) DO UPDATE SET name=EXCLUDED.name,info=EXCLUDED.info"
        local bt = moon.clock()
        for i = 1, N do
            sqlxpg_db:query(sql, i, "sp_" .. i, info)
        end
        record(test, "sqlx-pg", N, moon.clock() - bt)
    end

    if drivers_ok.sqlx_mysql then
        local sql = "INSERT INTO bench_kv_my(id,name,info) VALUES(?,?,?) ON DUPLICATE KEY UPDATE name=VALUES(name),info=VALUES(info)"
        local bt = moon.clock()
        for i = 1, N do
            sqlxmy_db:query(sql, i, "sm_" .. i, info)
        end
        record(test, "sqlx-mysql", N, moon.clock() - bt)
    end

    if drivers_ok.mongodb then
        local bt = moon.clock()
        for i = 1, N do
            mongo_coll:update_one(
                { _id = i },
                { ["$set"] = { name = "mg_" .. i, info = info } },
                { upsert = true })
        end
        record(test, "mongodb", N, moon.clock() - bt)
    end

    if drivers_ok.redis then
        local bt = moon.clock()
        for i = 1, N do
            redis_db:set("bench:" .. i, "rd_" .. i .. "|" .. info)
        end
        record(test, "redis", N, moon.clock() - bt)
    end
end

---------------------------------------------------------------------------
-- 2. Single Read — read one record × N
---------------------------------------------------------------------------

local function bench_single_read(N)
    local test = "single_read"

    if drivers_ok.pg then
        local bt = moon.clock()
        for i = 1, N do
            pg_db:query_params("SELECT id,name,info FROM bench_kv WHERE id=$1", (i % N) + 1)
        end
        record(test, "pg", N, moon.clock() - bt)
    end

    if drivers_ok.sqlx_pg then
        local bt = moon.clock()
        for i = 1, N do
            sqlxpg_db:query("SELECT id,name,info FROM bench_sqlx_kv WHERE id=$1", (i % N) + 1)
        end
        record(test, "sqlx-pg", N, moon.clock() - bt)
    end

    if drivers_ok.sqlx_mysql then
        local bt = moon.clock()
        for i = 1, N do
            sqlxmy_db:query("SELECT id,name,info FROM bench_kv_my WHERE id=?", (i % N) + 1)
        end
        record(test, "sqlx-mysql", N, moon.clock() - bt)
    end

    if drivers_ok.mongodb then
        local bt = moon.clock()
        for i = 1, N do
            mongo_coll:find_one({ _id = (i % N) + 1 })
        end
        record(test, "mongodb", N, moon.clock() - bt)
    end

    if drivers_ok.redis then
        local bt = moon.clock()
        for i = 1, N do
            redis_db:get("bench:" .. ((i % N) + 1))
        end
        record(test, "redis", N, moon.clock() - bt)
    end
end

---------------------------------------------------------------------------
-- 3. Batch Write — write N records in best available bulk method
---------------------------------------------------------------------------

local function bench_batch_write(N)
    local test = "batch_write"
    local info = json.encode({ seq = 0, ts = os.date() })

    if drivers_ok.pg then
        pg_db:query("DELETE FROM bench_kv")
        local rows = {}
        for i = 1, N do rows[i] = { i, "pg_" .. i, info } end
        local bt = moon.clock()
        pg_db:insert_many("bench_kv", { "id", "name", "info" }, rows,
            "ON CONFLICT(id) DO UPDATE SET name=EXCLUDED.name,info=EXCLUDED.info")
        record(test, "pg", N, moon.clock() - bt)
    end

    if drivers_ok.sqlx_pg then
        sqlxpg_db:query("DELETE FROM bench_sqlx_kv")
        local sql = "INSERT INTO bench_sqlx_kv(id,name,info) VALUES($1,$2,$3) ON CONFLICT(id) DO UPDATE SET name=EXCLUDED.name,info=EXCLUDED.info"
        local trans = {}
        for i = 1, N do trans[i] = { sql, i, "sp_" .. i, info } end
        local bt = moon.clock()
        sqlxpg_db:transaction(trans)
        record(test, "sqlx-pg", N, moon.clock() - bt)
    end

    if drivers_ok.sqlx_mysql then
        sqlxmy_db:query("DELETE FROM bench_kv_my")
        local sql = "INSERT INTO bench_kv_my(id,name,info) VALUES(?,?,?) ON DUPLICATE KEY UPDATE name=VALUES(name),info=VALUES(info)"
        local trans = {}
        for i = 1, N do trans[i] = { sql, i, "sm_" .. i, info } end
        local bt = moon.clock()
        sqlxmy_db:transaction(trans)
        record(test, "sqlx-mysql", N, moon.clock() - bt)
    end

    if drivers_ok.mongodb then
        mongo_coll:delete_many({})
        local docs = {}
        for i = 1, N do docs[i] = { _id = i, name = "mg_" .. i, info = info } end
        local bt = moon.clock()
        mongo_coll:insert_many(docs)
        record(test, "mongodb", N, moon.clock() - bt)
    end

    if drivers_ok.redis then
        redis_db:flushdb()
        local cmds = {}
        for i = 1, N do
            cmds[i] = { "SET", "bench:" .. i, "rd_" .. i .. "|" .. info }
        end
        local bt = moon.clock()
        redis_db:pipeline(cmds)
        record(test, "redis", N, moon.clock() - bt)
    end
end

---------------------------------------------------------------------------
-- 4. Batch Read — read N records in best available bulk method
---------------------------------------------------------------------------

local function bench_batch_read(N)
    local test = "batch_read"

    if drivers_ok.pg then
        local bt = moon.clock()
        pg_db:query(string.format("SELECT id,name,info FROM bench_kv LIMIT %d", N))
        record(test, "pg", N, moon.clock() - bt)
    end

    if drivers_ok.sqlx_pg then
        local bt = moon.clock()
        sqlxpg_db:query(string.format("SELECT id,name,info FROM bench_sqlx_kv LIMIT %d", N))
        record(test, "sqlx-pg", N, moon.clock() - bt)
    end

    if drivers_ok.sqlx_mysql then
        local bt = moon.clock()
        sqlxmy_db:query(string.format("SELECT id,name,info FROM bench_kv_my LIMIT %d", N))
        record(test, "sqlx-mysql", N, moon.clock() - bt)
    end

    if drivers_ok.mongodb then
        local bt = moon.clock()
        mongo_coll:find({}, { limit = N })
        record(test, "mongodb", N, moon.clock() - bt)
    end

    if drivers_ok.redis then
        local cmds = {}
        for i = 1, N do cmds[i] = { "GET", "bench:" .. i } end
        local bt = moon.clock()
        redis_db:pipeline(cmds)
        record(test, "redis", N, moon.clock() - bt)
    end
end

---------------------------------------------------------------------------
-- 5. pg-only: pipe, insert_many, update_many, multi-stmt, fire-and-forget
---------------------------------------------------------------------------

local function bench_pg_specific(N)
    if not drivers_ok.pg then return end
    local info = json.encode({ seq = 0, ts = os.date() })

    -- pipe
    pg_db:query("DELETE FROM bench_kv")
    local sql = "INSERT INTO bench_kv(id,name,info) VALUES($1,$2,$3) ON CONFLICT(id) DO UPDATE SET name=EXCLUDED.name,info=EXCLUDED.info"
    local queries = {}
    for i = 1, N do queries[i] = { sql, i, "pipe_" .. i, info } end
    local bt = moon.clock()
    pg_db:pipe(queries)
    record("pg_pipe", "pg", N, moon.clock() - bt)

    -- update_many
    local rows = {}
    for i = 1, N do rows[i] = { i, "upd_" .. i } end
    bt = moon.clock()
    pg_db:update_many("bench_kv", "id", { "name" }, rows, "bigint")
    record("pg_update_many", "pg", N, moon.clock() - bt)

    -- multi-stmt simple query
    pg_db:query("DELETE FROM bench_kv")
    local parts = {}
    for i = 1, N do
        parts[i] = string.format(
            "INSERT INTO bench_kv(id,name,info) VALUES(%d,'ms_%d','%s') ON CONFLICT(id) DO UPDATE SET name=EXCLUDED.name",
            i, i, info)
    end
    bt = moon.clock()
    pg_db:query(table.concat(parts, ";"))
    record("pg_multi_stmt", "pg", N, moon.clock() - bt)

    -- fire-and-forget vs await — use a larger pool to avoid channel overflow
    local pg = require("moon.db.pg")
    local ff_db = pg.connect(PG_URL .. "?name=bench_pg_ff&connect_timeout=5000&max_connections=5")
    if type(ff_db) ~= "table" or not ff_db.code then
        ff_db:query("DROP TABLE IF EXISTS bench_kv_ff")
        ff_db:query("CREATE TABLE bench_kv_ff (id bigint PRIMARY KEY, name text, info text)")
        local N_FF = 1000
        local sql_ff = "INSERT INTO bench_kv_ff(id,name,info) VALUES($1,$2,$3) ON CONFLICT(id) DO UPDATE SET name=EXCLUDED.name,info=EXCLUDED.info"

        bt = moon.clock()
        for i = 1, N_FF do
            ff_db:query_params(sql_ff, i, "ff_" .. i, info)
        end
        local t_await = moon.clock() - bt
        record("pg_await", "pg (await)", N_FF, t_await)

        bt = moon.clock()
        for i = 1, N_FF do
            ff_db:execute_params(sql_ff, i, "ff_" .. i, info)
        end
        local t_fire = moon.clock() - bt
        ff_db:query("SELECT 1")
        record("pg_fire_forget", "pg (fire&forget)", N_FF, t_fire)

        ff_db:query("DROP TABLE IF EXISTS bench_kv_ff")
        ff_db:close()
    end
end

---------------------------------------------------------------------------
-- 6. sqlx-only: transaction
---------------------------------------------------------------------------

local function bench_sqlx_specific(N)
    local info = json.encode({ seq = 0, ts = os.date() })

    if drivers_ok.sqlx_pg then
        sqlxpg_db:query("DELETE FROM bench_sqlx_kv")
        local sql = "INSERT INTO bench_sqlx_kv(id,name,info) VALUES($1,$2,$3) ON CONFLICT(id) DO UPDATE SET name=EXCLUDED.name,info=EXCLUDED.info"
        local trans = {}
        for i = 1, N do trans[i] = { sql, i, "tx_" .. i, info } end
        local bt = moon.clock()
        sqlxpg_db:transaction(trans)
        record("sqlx_pg_transaction", "sqlx-pg", N, moon.clock() - bt)
    end

    if drivers_ok.sqlx_mysql then
        sqlxmy_db:query("DELETE FROM bench_kv_my")
        local sql = "INSERT INTO bench_kv_my(id,name,info) VALUES(?,?,?) ON DUPLICATE KEY UPDATE name=VALUES(name),info=VALUES(info)"
        local trans = {}
        for i = 1, N do trans[i] = { sql, i, "tx_" .. i, info } end
        local bt = moon.clock()
        sqlxmy_db:transaction(trans)
        record("sqlx_mysql_transaction", "sqlx-mysql", N, moon.clock() - bt)
    end
end

---------------------------------------------------------------------------
-- 7. mongodb-only: insert_many (native)
---------------------------------------------------------------------------

local function bench_mongo_specific(N)
    if not drivers_ok.mongodb then return end
    local info = json.encode({ seq = 0, ts = os.date() })

    mongo_coll:delete_many({})
    local docs = {}
    for i = 1, N do docs[i] = { name = "mi_" .. i, info = info } end
    local bt = moon.clock()
    mongo_coll:insert_many(docs)
    record("mongo_insert_many", "mongodb", N, moon.clock() - bt)
end

---------------------------------------------------------------------------
-- 8. redis-only: pipeline
---------------------------------------------------------------------------

local function bench_redis_specific(N)
    if not drivers_ok.redis then return end

    redis_db:flushdb()
    local cmds = {}
    for i = 1, N do cmds[i] = { "SET", "rp:" .. i, "val_" .. i } end
    local bt = moon.clock()
    redis_db:pipeline(cmds)
    record("redis_pipeline", "redis", N, moon.clock() - bt)
end

---------------------------------------------------------------------------
-- Cleanup
---------------------------------------------------------------------------

local function cleanup()
    if pg_db then
        pcall(function() pg_db:query("DROP TABLE IF EXISTS bench_kv") end)
        pg_db:close()
    end
    if sqlxpg_db then
        pcall(function() sqlxpg_db:query("DROP TABLE IF EXISTS bench_sqlx_kv") end)
        sqlxpg_db:close()
    end
    if sqlxmy_db then
        pcall(function() sqlxmy_db:query("DROP TABLE IF EXISTS bench_kv_my") end)
        sqlxmy_db:close()
    end
    if mongo_coll then
        pcall(function() mongo_coll:delete_many({}) end)
    end
    if redis_db then
        pcall(function() redis_db:flushdb() end)
        redis_db:disconnect()
    end
end

---------------------------------------------------------------------------
-- Main
---------------------------------------------------------------------------

moon.async(function()
    print("Connecting drivers...")
    load_pg()
    load_sqlx_pg()
    load_sqlx_mysql()
    load_mongodb()
    load_redis()

    local active = {}
    for k, v in pairs(drivers_ok) do if v then active[#active + 1] = k end end
    table.sort(active)
    print("  Active: " .. table.concat(active, ", "))

    io.write("\nRunning benchmarks")
    bench_single_write(N_SINGLE);  io.write("."); io.flush()
    bench_single_read(N_SINGLE);   io.write("."); io.flush()
    bench_batch_write(N_BATCH);    io.write("."); io.flush()
    bench_batch_read(N_BATCH);     io.write("."); io.flush()
    bench_pg_specific(N_BATCH);    io.write("."); io.flush()
    bench_sqlx_specific(N_BATCH);  io.write("."); io.flush()
    bench_mongo_specific(N_BATCH); io.write("."); io.flush()
    bench_redis_specific(N_BATCH); io.write("."); io.flush()
    print(" done\n")

    print_final_tables()

    cleanup()
    moon.exit(0)
end)

moon.shutdown(function()
    moon.quit()
end)
