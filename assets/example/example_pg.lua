---
--- example_pg.lua — Self-contained showcase of the native PostgreSQL driver.
---
--- Run:  moon_rs assets/example/example_pg.lua
---
--- Covers every public API: connect, find_connection, query, query_params,
--- pipe, insert_many, update_many, execute / execute_params / execute_pipe /
--- execute_insert_many / execute_update_many, stats, len, close.
---

local moon = require("moon")
local json = require("json")
local pg   = require("moon.db.pg")

local DB_URL = "postgres://postgres:123456@127.0.0.1:5432/postgres"

---------------------------------------------------------------------------
-- Helpers
---------------------------------------------------------------------------

local _seq = 0
local function connect(pool_size)
    _seq = _seq + 1
    return pg.connect(string.format("%s?name=example_%d&connect_timeout=5000&max_connections=%d",
        DB_URL, _seq, pool_size or 1))
end

local function assert_ok(res, msg)
    assert(not res.code, string.format("%s: [%s] %s", msg or "pg error", res.code, res.message))
    return res
end

local function section(title)
    print(string.format("\n========== %s ==========", title))
end

---------------------------------------------------------------------------
-- 1. Connect & find_connection
---------------------------------------------------------------------------
local function example_connect()
    section("connect & find_connection")

    local db = connect()
    assert(db.query, "expected pg object")
    print("connected via pg.connect")

    local db2 = pg.find_connection("example_1")
    assert(db2.query, "expected pg object from find_connection")
    print("found via pg.find_connection")

    db:close()
    print("closed")
end

---------------------------------------------------------------------------
-- 2. Simple query — single and multi-statement
---------------------------------------------------------------------------
local function example_query(db)
    section("query (simple protocol)")

    assert_ok(db:query("DROP TABLE IF EXISTS demo"))
    assert_ok(db:query([[
        CREATE TABLE demo (
            id   bigint PRIMARY KEY,
            name text NOT NULL,
            info jsonb
        )
    ]]))
    print("table created")

    -- Multi-statement: semicolons separate independent queries in one call.
    assert_ok(db:query([[
        INSERT INTO demo VALUES (1, 'alice', '{"role":"admin"}');
        INSERT INTO demo VALUES (2, 'bob',   '{"role":"user"}');
        INSERT INTO demo VALUES (3, 'carol', '{"role":"user"}')
    ]]))
    print("3 rows inserted via multi-statement query")

    local res = assert_ok(db:query("SELECT id, name, info FROM demo ORDER BY id"))
    print(string.format("SELECT returned %d rows:", #res.data))
    for _, row in ipairs(res.data) do
        print(string.format("  id=%s name=%s info=%s", row.id, row.name, row.info))
    end
end

---------------------------------------------------------------------------
-- 3. Parameterized query (extended protocol)
---------------------------------------------------------------------------
local function example_query_params(db)
    section("query_params (extended protocol)")

    local res = assert_ok(db:query_params(
        "SELECT id, name FROM demo WHERE name = $1", "bob"))
    print(string.format("query_params returned %d row(s)", #res.data))
    print_r(res.data)

    -- JSONB parameter: Lua table auto-encoded to JSON.
    assert_ok(db:query_params(
        "UPDATE demo SET info = $1 WHERE id = $2",
        { role = "superadmin", level = 99 }, 1))
    local chk = assert_ok(db:query("SELECT info FROM demo WHERE id = 1"))
    print("updated info:", chk.data[1].info)
end

---------------------------------------------------------------------------
-- 4. Pipeline (transaction of parameterized statements)
---------------------------------------------------------------------------
local function example_pipe(db)
    section("pipe (implicit transaction)")

    local sql = "INSERT INTO demo(id, name, info) VALUES($1, $2, $3) " ..
                "ON CONFLICT (id) DO UPDATE SET name = EXCLUDED.name"
    local res = assert_ok(db:pipe({
        { sql, 10, "pipe_a", { seq = 10 } },
        { sql, 11, "pipe_b", { seq = 11 } },
        { sql, 12, "pipe_c", { seq = 12 } },
    }))
    print(string.format("pipe executed %d statements", res.num_queries))

    local cnt = assert_ok(db:query("SELECT count(*) AS c FROM demo"))
    print("total rows:", cnt.data[1].c)
end

---------------------------------------------------------------------------
-- 5. insert_many (bulk INSERT / UPSERT)
---------------------------------------------------------------------------
local function example_insert_many(db)
    section("insert_many (bulk INSERT)")

    -- Plain INSERT
    local rows = {}
    for i = 100, 109 do
        rows[#rows + 1] = { i, "im_" .. i, { idx = i } }
    end
    assert_ok(db:insert_many("demo", { "id", "name", "info" }, rows))
    print("inserted 10 rows")

    -- UPSERT with ON CONFLICT
    assert_ok(db:insert_many("demo", { "id", "name", "info" }, {
        { 100, "upserted", { v = 1 } },
        { 101, "upserted", { v = 2 } },
    }, "ON CONFLICT (id) DO UPDATE SET name = EXCLUDED.name, info = EXCLUDED.info"))
    local chk = assert_ok(db:query_params("SELECT name FROM demo WHERE id = $1", 100))
    assert(chk.data[1].name == "upserted", "upsert verification failed")
    print("upsert verified: name =", chk.data[1].name)
end

---------------------------------------------------------------------------
-- 6. update_many (bulk UPDATE)
---------------------------------------------------------------------------
local function example_update_many(db)
    section("update_many (bulk UPDATE)")

    assert_ok(db:update_many("demo", "id", { "name" }, {
        { 100, "renamed_100" },
        { 101, "renamed_101" },
    }, "bigint"))
    local chk = assert_ok(db:query_params("SELECT name FROM demo WHERE id = $1", 100))
    assert(chk.data[1].name == "renamed_100", "update_many verification failed")
    print("update_many verified: name =", chk.data[1].name)
end

---------------------------------------------------------------------------
-- 7. Fire-and-forget (execute_*)
---------------------------------------------------------------------------
local function example_fire_and_forget(db)
    section("fire-and-forget (execute_*)")

    -- execute — no response awaited
    db:execute("INSERT INTO demo(id, name, info) VALUES(200, 'ff', '{}') ON CONFLICT DO NOTHING")
    print("execute: sent (fire-and-forget)")

    -- execute_params
    db:execute_params(
        "INSERT INTO demo(id, name, info) VALUES($1, $2, $3) ON CONFLICT DO NOTHING",
        201, "ff_params", { x = 1 })
    print("execute_params: sent")

    -- execute_pipe
    local sql = "INSERT INTO demo(id, name, info) VALUES($1, $2, $3) ON CONFLICT DO NOTHING"
    db:execute_pipe({
        { sql, 202, "ff_pipe_a", {} },
        { sql, 203, "ff_pipe_b", {} },
    })
    print("execute_pipe: sent")

    -- execute_insert_many
    db:execute_insert_many("demo", { "id", "name", "info" }, {
        { 204, "ff_im", {} },
        { 205, "ff_im", {} },
    }, "ON CONFLICT DO NOTHING")
    print("execute_insert_many: sent")

    -- execute_update_many
    db:execute_update_many("demo", "id", { "name" }, {
        { 200, "ff_updated" },
    }, "bigint")
    print("execute_update_many: sent")

    -- Drain: a synchronous query ensures all fire-and-forget requests complete.
    db:query("SELECT 1")
    print("all fire-and-forget requests drained")
end

---------------------------------------------------------------------------
-- 8. Stats & queue length
---------------------------------------------------------------------------
local function example_stats(db)
    section("stats & len")

    local lengths = db:len()
    print("pool queue lengths:", table.concat(lengths, ", "))

    local stats = pg.stats()
    print("global stats:")
    print_r(stats)
end

---------------------------------------------------------------------------
-- 9. JSONB deep operations
---------------------------------------------------------------------------
local function example_jsonb(db)
    section("JSONB operations")

    assert_ok(db:query("DROP TABLE IF EXISTS jdemo"))
    assert_ok(db:query("CREATE TABLE jdemo (id int PRIMARY KEY, data jsonb)"))

    local doc = {
        user = "alice",
        scores = { 90, 85, 92 },
        meta = { created = "2024-01-01", tags = { "vip", "active" } },
    }
    assert_ok(db:query_params("INSERT INTO jdemo(id, data) VALUES($1, $2)", 1, doc))
    print("inserted JSONB document")

    -- jsonb_set: update a nested field
    assert_ok(db:query("UPDATE jdemo SET data = jsonb_set(data, '{meta,tags}', '[\"vip\",\"premium\"]') WHERE id = 1"))
    local res = assert_ok(db:query("SELECT data->'meta'->>'tags' AS tags FROM jdemo WHERE id = 1"))
    print("updated tags:", res.data[1].tags)

    -- jsonb_path_query: SQL/JSON path
    res = assert_ok(db:query("SELECT jsonb_path_query(data, '$.scores[*] ? (@ > 88)') AS high FROM jdemo WHERE id = 1"))
    print("scores > 88:")
    for _, row in ipairs(res.data) do
        print("  ", row.high)
    end

    assert_ok(db:query("DROP TABLE jdemo"))
end

---------------------------------------------------------------------------
-- 10. Pool concurrency (multiple coroutines)
---------------------------------------------------------------------------
local function example_concurrency()
    section("pool concurrency")

    local db = connect(3)
    if db.code then moon.error(print_r(db, true)); return end

    assert_ok(db:query("DROP TABLE IF EXISTS co_demo"))
    assert_ok(db:query("CREATE TABLE co_demo (id bigint PRIMARY KEY, val text)"))

    local N_CO, OPS = 5, 20
    local done = 0
    local sql = "INSERT INTO co_demo(id, val) VALUES($1, $2) ON CONFLICT (id) DO UPDATE SET val = EXCLUDED.val"

    for c = 1, N_CO do
        moon.async(function()
            local base = c * 10000
            for i = 1, OPS do
                assert_ok(db:query_params(sql, base + i, "co" .. c), "concurrency")
            end
            done = done + OPS
        end)
    end

    while done < N_CO * OPS do
        moon.sleep(1)
    end

    local cnt = assert_ok(db:query("SELECT count(*) AS c FROM co_demo"))
    print(string.format("%d coroutines x %d ops = %d rows, actual count = %s",
        N_CO, OPS, N_CO * OPS, cnt.data[1].c))

    assert_ok(db:query("DROP TABLE co_demo"))
    db:close()
end

---------------------------------------------------------------------------
-- Main
---------------------------------------------------------------------------
moon.async(function()
    local db = connect()
    if db.code then
        moon.error("connect failed: " .. db.message)
        moon.exit(-1)
        return
    end

    example_connect()

    example_query(db)
    example_query_params(db)
    example_pipe(db)
    example_insert_many(db)
    example_update_many(db)
    example_fire_and_forget(db)
    example_stats(db)
    example_jsonb(db)

    db:query("DROP TABLE IF EXISTS demo")
    db:close()

    example_concurrency()

    section("ALL EXAMPLES PASSED")
    moon.exit(-1)
end)

moon.shutdown(function()
    moon.quit()
end)
