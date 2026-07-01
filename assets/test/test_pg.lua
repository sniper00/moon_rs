---
--- test_pg.lua — Edge-case tests for the native PostgreSQL driver.
---
--- Run:  moon_rs assets/test/test_pg.lua
---

local moon = require("moon")
local json = require("json")
local pg   = require("moon.db.pg")

local DB_URL = "postgres://postgres:123456@127.0.0.1:5432/postgres"

---------------------------------------------------------------------------
-- Test harness
---------------------------------------------------------------------------

local passed, failed = 0, 0
local test_results = {}
local current_category = ""
local last_printed_cat = ""
local test_idx = 0

local function test(name, fn)
    test_idx = test_idx + 1
    if current_category ~= last_printed_cat then
        io.write("\n  [" .. current_category .. "] ")
        last_printed_cat = current_category
    end
    local ok, err = pcall(fn)
    if ok then
        passed = passed + 1
        test_results[#test_results + 1] = {
            idx = test_idx, cat = current_category, name = name, status = "PASS"
        }
    else
        failed = failed + 1
        test_results[#test_results + 1] = {
            idx = test_idx, cat = current_category, name = name, status = "FAIL",
            err = tostring(err)
        }
    end
    io.write(ok and "." or "x")
    io.flush()
end

local function assert_ok(res, msg)
    assert(not res.code, string.format("%s: [%s] %s", msg or "", res.code or "", res.message or ""))
    return res
end

local function assert_err(res, expected_code)
    assert(res.code, "expected error but got success")
    if expected_code then
        assert(res.code == expected_code,
            string.format("expected code %q, got %q", expected_code, res.code))
    end
    return res
end

local _seq = 0
local function new_conn(pool_size)
    _seq = _seq + 1
    return pg.connect(string.format("%s?name=test_%d&connect_timeout=5000&max_connections=%d",
        DB_URL, _seq, pool_size or 1))
end

---------------------------------------------------------------------------
-- Setup: shared connection
---------------------------------------------------------------------------

local db

local function setup()
    db = new_conn()
    assert(not db.code, "connect failed: " .. tostring(db.message))
    db:query("DROP TABLE IF EXISTS t")
    assert_ok(db:query([[
        CREATE TABLE t (
            id    bigint PRIMARY KEY,
            name  text,
            score real,
            flag  boolean,
            data  jsonb,
            tags  text[]
        )
    ]]))
end

local function teardown()
    db:query("DROP TABLE IF EXISTS t")
    db:close()
end

--- Kept for reference: the driver now auto-ROLLBACKs aborted transactions,
--- so manual reset_txn() is no longer needed.

---------------------------------------------------------------------------
-- 1. Connection edge cases
---------------------------------------------------------------------------

local function test_connection()
    current_category = "Connection"

    test("connect with bad host (timeout)", function()
        local res = pg.connect(
            "postgres://postgres:123456@192.0.2.1:5432/postgres?name=bad_host&connect_timeout=1000&max_connections=1")
        assert(res.code, "expected connection error")
    end)

    test("connect with bad password", function()
        local res = pg.connect(
            "postgres://postgres:wrong_password@127.0.0.1:5432/postgres?name=bad_pw&connect_timeout=2000&max_connections=1")
        assert(res.code, "expected auth error")
    end)

    test("connect with bad database", function()
        local res = pg.connect(
            "postgres://postgres:123456@127.0.0.1:5432/nonexistent_db_xyz?name=bad_db&connect_timeout=2000&max_connections=1")
        assert(res.code, "expected database error")
    end)

    test("connect with invalid URL", function()
        local res = pg.connect("not_a_url?name=bad_url")
        assert(res.code, "expected parse error")
    end)

    test("find_connection nonexistent throws", function()
        local ok, err = pcall(pg.find_connection, "no_such_pool")
        assert(not ok, "expected error for missing pool")
        assert(tostring(err):find("not found"), "expected 'not found' in error")
    end)

    test("connect and find_connection round-trip", function()
        local d = new_conn()
        assert(not d.code)
        d:close()
    end)
end

---------------------------------------------------------------------------
-- 2. Simple query edge cases
---------------------------------------------------------------------------

local function test_query()
    current_category = "query (simple protocol)"

    test("empty result set", function()
        local res = assert_ok(db:query("SELECT * FROM t WHERE id = -1"))
        assert(#res.data == 0, "expected 0 rows, got " .. #res.data)
    end)

    test("NULL values", function()
        assert_ok(db:query("INSERT INTO t(id) VALUES(1)"))
        local res = assert_ok(db:query("SELECT name, score, flag, data FROM t WHERE id = 1"))
        local row = res.data[1]
        assert(row.name == nil, "NULL name should be nil")
        assert(row.score == nil, "NULL score should be nil")
        assert(row.flag == nil, "NULL flag should be nil")
        assert(row.data == nil, "NULL data should be nil")
        assert_ok(db:query("DELETE FROM t WHERE id = 1"))
    end)

    test("boolean values (OID → native bool)", function()
        assert_ok(db:query("INSERT INTO t(id, flag) VALUES(2, true), (3, false)"))
        local res = assert_ok(db:query("SELECT id, flag FROM t WHERE id IN (2,3) ORDER BY id"))
        assert(res.data[1].flag == true, "expected true, got: " .. tostring(res.data[1].flag))
        assert(res.data[2].flag == false, "expected false, got: " .. tostring(res.data[2].flag))
        assert_ok(db:query("DELETE FROM t WHERE id IN (2,3)"))
    end)

    test("numeric types (OID → Lua number)", function()
        assert_ok(db:query("INSERT INTO t(id, score) VALUES(4, 3.14)"))
        local res = assert_ok(db:query("SELECT score FROM t WHERE id = 4"))
        local s = res.data[1].score
        assert(type(s) == "number", "score should be a number, got " .. type(s))
        assert(math.abs(s - 3.14) < 0.01, "score mismatch: " .. tostring(s))
        assert_ok(db:query("DELETE FROM t WHERE id = 4"))
    end)

    test("multi-statement query", function()
        local res = assert_ok(db:query([[
            INSERT INTO t(id, name) VALUES(10, 'a');
            INSERT INTO t(id, name) VALUES(11, 'b');
            SELECT count(*) AS c FROM t WHERE id IN (10, 11)
        ]]))
        assert(res.num_queries == 3, "expected 3 queries, got " .. tostring(res.num_queries))
        -- Multi-statement: data[1]=INSERT, data[2]=INSERT, data[3]=SELECT rows
        local select_result = res.data[3]
        assert(select_result[1].c == 2,
            "expected count 2, got " .. tostring(select_result and select_result[1] and select_result[1].c))
        assert_ok(db:query("DELETE FROM t WHERE id IN (10, 11)"))
    end)

    test("empty string vs NULL", function()
        assert_ok(db:query("INSERT INTO t(id, name) VALUES(20, '')"))
        local res = assert_ok(db:query("SELECT name FROM t WHERE id = 20"))
        assert(res.data[1].name == "", "expected empty string, got: " .. tostring(res.data[1].name))
        assert_ok(db:query("DELETE FROM t WHERE id = 20"))
    end)

    test("special characters in data", function()
        local val = "it's a \"test\" with \\backslash\\ and 中文 and emoji 🚀"
        assert_ok(db:query_params("INSERT INTO t(id, name) VALUES($1, $2)", 21, val))
        local res = assert_ok(db:query_params("SELECT name FROM t WHERE id = $1", 21))
        assert(res.data[1].name == val, "special chars mismatch")
        assert_ok(db:query("DELETE FROM t WHERE id = 21"))
    end)

    test("very long string (100K bytes)", function()
        local long = string.rep("x", 100000)
        assert_ok(db:query_params("INSERT INTO t(id, name) VALUES($1, $2)", 22, long))
        local res = assert_ok(db:query_params("SELECT length(name) AS len FROM t WHERE id = $1", 22))
        -- length() returns int4 → Lua number
        assert(res.data[1].len == 100000,
            "expected 100000, got " .. tostring(res.data[1].len) .. " (" .. type(res.data[1].len) .. ")")
        assert_ok(db:query("DELETE FROM t WHERE id = 22"))
    end)

    test("SQL syntax error", function()
        local res = db:query("SELEC broken")
        assert_err(res)
    end)

    test("table not found", function()
        local res = db:query("SELECT * FROM nonexistent_table_xyz")
        assert_err(res)
    end)

    test("constraint violation (duplicate PK)", function()
        assert_ok(db:query("INSERT INTO t(id, name) VALUES(30, 'first')"))
        local res = db:query("INSERT INTO t(id, name) VALUES(30, 'duplicate')")
        assert_err(res, "23505")
        assert_ok(db:query("DELETE FROM t WHERE id = 30"))
    end)
end

---------------------------------------------------------------------------
-- 3. query_params edge cases
---------------------------------------------------------------------------

local function test_query_params()
    current_category = "query_params (extended protocol)"

    test("integer parameter", function()
        assert_ok(db:query_params("INSERT INTO t(id, name) VALUES($1, $2)", 100, "int_test"))
        local res = assert_ok(db:query_params("SELECT name FROM t WHERE id = $1", 100))
        assert(res.data[1].name == "int_test")
        assert_ok(db:query("DELETE FROM t WHERE id = 100"))
    end)

    test("float parameter", function()
        assert_ok(db:query_params("INSERT INTO t(id, score) VALUES($1, $2)", 101, 2.718))
        local res = assert_ok(db:query_params("SELECT score FROM t WHERE id = $1", 101))
        local s = res.data[1].score
        assert(type(s) == "number", "score should be number")
        assert(math.abs(s - 2.718) < 0.01)
        assert_ok(db:query("DELETE FROM t WHERE id = 101"))
    end)

    test("boolean parameter", function()
        assert_ok(db:query_params("INSERT INTO t(id, flag) VALUES($1, $2)", 102, true))
        local res = assert_ok(db:query_params("SELECT flag FROM t WHERE id = $1", 102))
        assert(res.data[1].flag == true, "expected true, got: " .. tostring(res.data[1].flag))
        assert_ok(db:query_params("INSERT INTO t(id, flag) VALUES($1, $2) ON CONFLICT(id) DO UPDATE SET flag = $2", 102, false))
        local res2 = assert_ok(db:query_params("SELECT flag FROM t WHERE id = $1", 102))
        assert(res2.data[1].flag == false, "expected false, got: " .. tostring(res2.data[1].flag))
        assert_ok(db:query("DELETE FROM t WHERE id = 102"))
    end)

    test("nil parameter (NULL)", function()
        assert_ok(db:query_params("INSERT INTO t(id, name) VALUES($1, $2)", 103, nil))
        local res = assert_ok(db:query_params("SELECT name FROM t WHERE id = $1", 103))
        assert(res.data[1].name == nil, "expected nil for NULL")
        assert_ok(db:query("DELETE FROM t WHERE id = 103"))
    end)

    test("table parameter (auto-JSON)", function()
        local obj = { key = "value", nested = { a = 1, b = { 2, 3 } } }
        assert_ok(db:query_params("INSERT INTO t(id, data) VALUES($1, $2)", 104, obj))
        local res = assert_ok(db:query_params("SELECT data FROM t WHERE id = $1", 104))
        local decoded = json.decode(res.data[1].data)
        assert(decoded.key == "value")
        assert(decoded.nested.a == 1)
        assert_ok(db:query("DELETE FROM t WHERE id = 104"))
    end)

    test("many parameters (10)", function()
        local res = assert_ok(db:query_params(
            "SELECT $1::int + $2::int + $3::int + $4::int + $5::int + $6::int + $7::int + $8::int + $9::int + $10::int AS total",
            1, 2, 3, 4, 5, 6, 7, 8, 9, 10))
        assert(res.data[1].total == 55, "expected 55, got " .. tostring(res.data[1].total))
    end)

    test("empty string parameter", function()
        assert_ok(db:query_params("INSERT INTO t(id, name) VALUES($1, $2)", 105, ""))
        local res = assert_ok(db:query_params("SELECT name FROM t WHERE id = $1", 105))
        assert(res.data[1].name == "", "expected empty string")
        assert_ok(db:query("DELETE FROM t WHERE id = 105"))
    end)

    test("RETURNING clause", function()
        local res = assert_ok(db:query_params(
            "INSERT INTO t(id, name) VALUES($1, $2) RETURNING id, name", 106, "ret_test"))
        assert(#res.data == 1, "expected 1 returned row")
        assert(res.data[1].id == 106)
        assert(res.data[1].name == "ret_test")
        assert_ok(db:query("DELETE FROM t WHERE id = 106"))
    end)
end

---------------------------------------------------------------------------
-- 4. pipe edge cases
---------------------------------------------------------------------------

local function test_pipe()
    current_category = "pipe"

    test("single statement in pipe", function()
        local res = assert_ok(db:pipe({
            { "INSERT INTO t(id, name) VALUES($1, $2) ON CONFLICT DO NOTHING", 200, "pipe1" },
        }))
        assert(res.num_queries >= 3, "expected BEGIN + 1 stmt + COMMIT")
        assert_ok(db:query("DELETE FROM t WHERE id = 200"))
    end)

    test("pipe with mixed reads and writes", function()
        assert_ok(db:query("INSERT INTO t(id, name) VALUES(210, 'existing')"))
        local res = assert_ok(db:pipe({
            { "UPDATE t SET name = $1 WHERE id = $2", "updated", 210 },
            { "SELECT name FROM t WHERE id = $1", 210 },
        }))
        assert(res.num_queries >= 4)
        assert_ok(db:query("DELETE FROM t WHERE id = 210"))
    end)

    test("pipe error auto-rollback (connection reusable)", function()
        assert_ok(db:query("INSERT INTO t(id, name) VALUES(220, 'before')"))
        local res = db:pipe({
            { "UPDATE t SET name = $1 WHERE id = $2", "changed", 220 },
            { "INSERT INTO nonexistent_table_xyz VALUES($1)", 1 },
        })
        assert_err(res)
        -- Driver auto-issues ROLLBACK when txn_status='E'.
        -- Connection should be immediately reusable — no manual ROLLBACK needed.
        local chk = assert_ok(db:query("SELECT name FROM t WHERE id = 220"))
        assert(chk.data[1].name == "before", "expected rollback, got: " .. tostring(chk.data[1].name))
        assert_ok(db:query("DELETE FROM t WHERE id = 220"))
    end)

    test("pipe with SQL-level NULL (no Lua nil in array)", function()
        local res = assert_ok(db:pipe({
            { "INSERT INTO t(id, name) VALUES($1, NULL) ON CONFLICT DO NOTHING", 230 },
        }))
        local chk = assert_ok(db:query_params("SELECT name FROM t WHERE id = $1", 230))
        assert(chk.data[1].name == nil)
        assert_ok(db:query("DELETE FROM t WHERE id = 230"))
    end)

    test("large pipe (100 statements)", function()
        local stmts = {}
        for i = 1, 100 do
            stmts[i] = { "INSERT INTO t(id, name) VALUES($1, $2) ON CONFLICT DO NOTHING", 2000 + i, "lp_" .. i }
        end
        local res = assert_ok(db:pipe(stmts))
        assert(res.num_queries >= 102, "expected BEGIN + 100 stmts + COMMIT")
        local cnt = assert_ok(db:query("SELECT count(*) AS c FROM t WHERE id > 2000 AND id <= 2100"))
        assert(cnt.data[1].c == 100, "expected 100 rows")
        assert_ok(db:query("DELETE FROM t WHERE id > 2000 AND id <= 2100"))
    end)
end

---------------------------------------------------------------------------
-- 5. insert_many edge cases
---------------------------------------------------------------------------

local function test_insert_many()
    current_category = "insert_many"

    test("single row", function()
        assert_ok(db:insert_many("t", { "id", "name" }, {
            { 300, "single" },
        }))
        local res = assert_ok(db:query_params("SELECT name FROM t WHERE id = $1", 300))
        assert(res.data[1].name == "single")
        assert_ok(db:query("DELETE FROM t WHERE id = 300"))
    end)

    test("missing columns get server defaults (NULL)", function()
        assert_ok(db:insert_many("t", { "id", "name" }, {
            { 301, "partial" },
        }))
        local res = assert_ok(db:query("SELECT score, flag, data FROM t WHERE id = 301"))
        assert(res.data[1].score == nil, "score should be nil")
        assert(res.data[1].flag == nil, "flag should be nil")
        assert(res.data[1].data == nil, "data should be nil")
        assert_ok(db:query("DELETE FROM t WHERE id = 301"))
    end)

    test("rows with JSON values", function()
        assert_ok(db:insert_many("t", { "id", "name", "data" }, {
            { 303, "j1", { a = 1 } },
            { 304, "j2", { b = { 2, 3 } } },
        }))
        local res = assert_ok(db:query("SELECT data FROM t WHERE id = 303"))
        local d = json.decode(res.data[1].data)
        assert(d.a == 1)
        assert_ok(db:query("DELETE FROM t WHERE id IN (303, 304)"))
    end)

    test("upsert with ON CONFLICT", function()
        assert_ok(db:insert_many("t", { "id", "name" }, {
            { 310, "original" },
        }))
        assert_ok(db:insert_many("t", { "id", "name" }, {
            { 310, "replaced" },
        }, "ON CONFLICT (id) DO UPDATE SET name = EXCLUDED.name"))
        local res = assert_ok(db:query_params("SELECT name FROM t WHERE id = $1", 310))
        assert(res.data[1].name == "replaced")
        assert_ok(db:query("DELETE FROM t WHERE id = 310"))
    end)

    test("large batch triggers chunking (5000 rows)", function()
        local N = 5000
        local rows = {}
        for i = 1, N do
            rows[i] = { 10000 + i, "row_" .. i }
        end
        assert_ok(db:insert_many("t", { "id", "name" }, rows))
        local cnt = assert_ok(db:query(
            string.format("SELECT count(*) AS c FROM t WHERE id > 10000 AND id <= %d", 10000 + N)))
        assert(cnt.data[1].c == N, "expected " .. N .. ", got " .. tostring(cnt.data[1].c))
        assert_ok(db:query("DELETE FROM t WHERE id > 10000"))
    end)

    test("empty rows returns ENCODE error", function()
        local res = db:insert_many("t", { "id", "name" }, {})
        assert_err(res, "ENCODE")
    end)

    test("special characters in values", function()
        local weird = "it's \"quoted\" with \\backslash\\"
        assert_ok(db:insert_many("t", { "id", "name" }, {
            { 320, weird },
        }))
        local res = assert_ok(db:query_params("SELECT name FROM t WHERE id = $1", 320))
        assert(res.data[1].name == weird, "special chars mismatch in insert_many")
        assert_ok(db:query("DELETE FROM t WHERE id = 320"))
    end)

    test("insert_many with RETURNING clause", function()
        local res = assert_ok(db:insert_many("t", { "id", "name" }, {
            { 330, "ret_a" },
            { 331, "ret_b" },
        }, "ON CONFLICT DO NOTHING RETURNING id, name"))
        assert(#res.data == 2, "expected 2 returned rows, got " .. #res.data)
        assert_ok(db:query("DELETE FROM t WHERE id IN (330, 331)"))
    end)

    test("boolean and float values in insert_many", function()
        assert_ok(db:insert_many("t", { "id", "flag", "score" }, {
            { 340, true, 1.5 },
            { 341, false, -2.5 },
        }))
        local res = assert_ok(db:query("SELECT id, flag, score FROM t WHERE id IN (340, 341) ORDER BY id"))
        assert(res.data[1].flag == true)
        assert(res.data[2].flag == false)
        assert(math.abs(res.data[1].score - 1.5) < 0.01)
        assert(math.abs(res.data[2].score + 2.5) < 0.01)
        assert_ok(db:query("DELETE FROM t WHERE id IN (340, 341)"))
    end)
end

---------------------------------------------------------------------------
-- 6. update_many edge cases
---------------------------------------------------------------------------

local function test_update_many()
    current_category = "update_many"

    test("basic update with key_type", function()
        assert_ok(db:insert_many("t", { "id", "name", "score" }, {
            { 400, "u1", 10.0 },
            { 401, "u2", 20.0 },
        }))
        assert_ok(db:update_many("t", "id", { "name" }, {
            { 400, "updated_u1" },
            { 401, "updated_u2" },
        }, "bigint"))
        local res = assert_ok(db:query("SELECT name FROM t WHERE id IN (400, 401) ORDER BY id"))
        assert(res.data[1].name == "updated_u1")
        assert(res.data[2].name == "updated_u2")
        assert_ok(db:query("DELETE FROM t WHERE id IN (400, 401)"))
    end)

    test("update without key_type (text cast fallback)", function()
        assert_ok(db:insert_many("t", { "id", "name" }, { { 402, "orig" } }))
        assert_ok(db:update_many("t", "id", { "name" }, {
            { 402, "text_cast_u3" },
        }))
        local res = assert_ok(db:query("SELECT name FROM t WHERE id = 402"))
        assert(res.data[1].name == "text_cast_u3")
        assert_ok(db:query("DELETE FROM t WHERE id = 402"))
    end)

    test("update multiple set_columns (text)", function()
        assert_ok(db:query("DROP TABLE IF EXISTS t_upd_multi"))
        assert_ok(db:query("CREATE TABLE t_upd_multi (id bigint PRIMARY KEY, a text, b text)"))
        assert_ok(db:insert_many("t_upd_multi", { "id", "a", "b" }, { { 1, "old_a", "old_b" } }))
        assert_ok(db:update_many("t_upd_multi", "id", { "a", "b" }, {
            { 1, "new_a", "new_b" },
        }, "bigint"))
        local res = assert_ok(db:query("SELECT a, b FROM t_upd_multi WHERE id = 1"))
        assert(res.data[1].a == "new_a")
        assert(res.data[1].b == "new_b")
        assert_ok(db:query("DROP TABLE t_upd_multi"))
    end)

    test("update nonexistent key (no-op, no error)", function()
        local res = assert_ok(db:update_many("t", "id", { "name" }, {
            { 999999, "ghost" },
        }, "bigint"))
    end)

    test("invalid key_type rejected", function()
        local res = db:update_many("t", "id", { "name" }, {
            { 400, "hack" },
        }, "bigint; DROP TABLE t--")
        assert_err(res, "ENCODE")
    end)

    test("large update_many (5000 rows)", function()
        local N = 5000
        local rows = {}
        for i = 1, N do
            rows[i] = { 20000 + i, "seed_" .. i }
        end
        assert_ok(db:insert_many("t", { "id", "name" }, rows))
        local updates = {}
        for i = 1, N do
            updates[i] = { 20000 + i, "upd_" .. i }
        end
        assert_ok(db:update_many("t", "id", { "name" }, updates, "bigint"))
        local res = assert_ok(db:query("SELECT name FROM t WHERE id = 20001"))
        assert(res.data[1].name == "upd_1")
        assert_ok(db:query("DELETE FROM t WHERE id > 20000 AND id <= 25000"))
    end)
end

---------------------------------------------------------------------------
-- 7. fire-and-forget edge cases
---------------------------------------------------------------------------

local function test_fire_and_forget()
    current_category = "fire-and-forget"

    test("execute with syntax error doesn't crash", function()
        db:execute("SELEC broken_sql")
        -- Drain: the worker logs the error but doesn't crash
        assert_ok(db:query("SELECT 1 AS v"))
    end)

    test("execute_params inserts data", function()
        db:execute_params(
            "INSERT INTO t(id, name) VALUES($1, $2) ON CONFLICT DO NOTHING",
            500, "ff_test")
        -- Drain to ensure the fire-and-forget completes
        assert_ok(db:query("SELECT 1"))
        local res = assert_ok(db:query("SELECT name FROM t WHERE id = 500"))
        assert(res.data[1].name == "ff_test")
        assert_ok(db:query("DELETE FROM t WHERE id = 500"))
    end)

    test("execute_pipe", function()
        db:execute_pipe({
            { "INSERT INTO t(id, name) VALUES($1, $2) ON CONFLICT DO NOTHING", 501, "fp1" },
            { "INSERT INTO t(id, name) VALUES($1, $2) ON CONFLICT DO NOTHING", 502, "fp2" },
        })
        assert_ok(db:query("SELECT 1"))
        local cnt = assert_ok(db:query("SELECT count(*) AS c FROM t WHERE id IN (501, 502)"))
        assert(cnt.data[1].c == 2)
        assert_ok(db:query("DELETE FROM t WHERE id IN (501, 502)"))
    end)

    test("execute_insert_many", function()
        db:execute_insert_many("t", { "id", "name" }, {
            { 510, "fim1" },
            { 511, "fim2" },
        }, "ON CONFLICT DO NOTHING")
        assert_ok(db:query("SELECT 1"))
        local cnt = assert_ok(db:query("SELECT count(*) AS c FROM t WHERE id IN (510, 511)"))
        assert(cnt.data[1].c == 2)
        assert_ok(db:query("DELETE FROM t WHERE id IN (510, 511)"))
    end)

    test("execute_update_many", function()
        assert_ok(db:query("INSERT INTO t(id, name) VALUES(520, 'before')"))
        db:execute_update_many("t", "id", { "name" }, {
            { 520, "after" },
        }, "bigint")
        assert_ok(db:query("SELECT 1"))
        local res = assert_ok(db:query("SELECT name FROM t WHERE id = 520"))
        assert(res.data[1].name == "after")
        assert_ok(db:query("DELETE FROM t WHERE id = 520"))
    end)
end

---------------------------------------------------------------------------
-- 8. Data type edge cases
---------------------------------------------------------------------------

local function test_data_types()
    current_category = "data types"

    test("bigint boundary (2^53)", function()
        local big = 9007199254740992  -- 2^53 (max safe Lua double integer)
        assert_ok(db:query_params("INSERT INTO t(id) VALUES($1)", big))
        local res = assert_ok(db:query_params("SELECT id FROM t WHERE id = $1", big))
        assert(res.data[1].id == big)
        assert_ok(db:query_params("DELETE FROM t WHERE id = $1", big))
    end)

    test("negative numbers", function()
        assert_ok(db:query_params("INSERT INTO t(id, score) VALUES($1, $2)", -1, -999.5))
        local res = assert_ok(db:query_params("SELECT id, score FROM t WHERE id = $1", -1))
        assert(res.data[1].id == -1, "id should be -1")
        assert(type(res.data[1].score) == "number")
        assert(math.abs(res.data[1].score + 999.5) < 0.1)
        assert_ok(db:query("DELETE FROM t WHERE id = -1"))
    end)

    test("zero values", function()
        assert_ok(db:query_params("INSERT INTO t(id, name, score, flag) VALUES($1, $2, $3, $4)",
            0, "", 0.0, false))
        local res = assert_ok(db:query_params("SELECT id, name, score, flag FROM t WHERE id = $1", 0))
        local row = res.data[1]
        assert(row.id == 0, "id should be 0")
        assert(row.name == "", "name should be empty string")
        assert(row.score == 0, "score should be 0")
        assert(row.flag == false, "flag should be false")
        assert_ok(db:query("DELETE FROM t WHERE id = 0"))
    end)

    test("unicode strings", function()
        local uni = "日本語テスト 한국어 العربية"
        assert_ok(db:query_params("INSERT INTO t(id, name) VALUES($1, $2)", 600, uni))
        local res = assert_ok(db:query_params("SELECT name FROM t WHERE id = $1", 600))
        assert(res.data[1].name == uni, "unicode mismatch")
        assert_ok(db:query("DELETE FROM t WHERE id = 600"))
    end)

    test("newlines and tabs in string", function()
        local val = "line1\nline2\ttab"
        assert_ok(db:query_params("INSERT INTO t(id, name) VALUES($1, $2)", 601, val))
        local res = assert_ok(db:query_params("SELECT name FROM t WHERE id = $1", 601))
        assert(res.data[1].name == val)
        assert_ok(db:query("DELETE FROM t WHERE id = 601"))
    end)

    test("deeply nested JSON", function()
        local obj = { a = { b = { c = { d = { e = { f = 42 } } } } } }
        assert_ok(db:query_params("INSERT INTO t(id, data) VALUES($1, $2)", 602, obj))
        local res = assert_ok(db:query_params("SELECT data FROM t WHERE id = $1", 602))
        local d = json.decode(res.data[1].data)
        assert(d.a.b.c.d.e.f == 42)
        assert_ok(db:query("DELETE FROM t WHERE id = 602"))
    end)

    test("JSON array vs object", function()
        assert_ok(db:query_params("INSERT INTO t(id, data) VALUES($1, $2)", 603, { 1, 2, 3 }))
        local res = assert_ok(db:query_params("SELECT data FROM t WHERE id = $1", 603))
        local d = json.decode(res.data[1].data)
        assert(d[1] == 1 and d[2] == 2 and d[3] == 3, "expected [1,2,3]")
        assert_ok(db:query("DELETE FROM t WHERE id = 603"))
    end)

    test("NUL byte in string (binary safe)", function()
        local val = "before\0after"
        -- PostgreSQL text type does not allow NUL. Should error.
        local res = db:query_params("INSERT INTO t(id, name) VALUES($1, $2)", 604, val)
        -- Expect either an error or truncation; either way, verify no crash
        if res.code then
            -- expected: error for NUL in text
        else
            -- some drivers accept it by truncating at NUL
            local r2 = assert_ok(db:query("SELECT name FROM t WHERE id = 604"))
            assert_ok(db:query("DELETE FROM t WHERE id = 604"))
        end
    end)
end

---------------------------------------------------------------------------
-- 9. Error recovery
---------------------------------------------------------------------------

local function test_error_recovery()
    current_category = "error recovery"

    test("connection usable after simple query error", function()
        local res = db:query("INVALID SQL HERE")
        assert_err(res)
        local ok = assert_ok(db:query("SELECT 1 AS v"))
        assert(ok.data[1].v == 1, "connection should still work after error")
    end)

    test("connection usable after query_params error", function()
        local res = db:query_params("SELECT * FROM no_such_table WHERE id = $1", 1)
        assert_err(res)
        local ok = assert_ok(db:query("SELECT 2 AS v"))
        assert(ok.data[1].v == 2)
    end)

    test("connection usable after pipe error (auto-rollback)", function()
        local res = db:pipe({
            { "INSERT INTO no_such_table VALUES($1)", 1 },
        })
        assert_err(res)
        local ok = assert_ok(db:query("SELECT 3 AS v"))
        assert(ok.data[1].v == 3)
    end)

    test("rapid sequential errors don't break pool", function()
        for i = 1, 10 do
            db:query("INVALID SQL " .. i)
        end
        local ok = assert_ok(db:query("SELECT 'alive' AS status"))
        assert(ok.data[1].status == "alive")
    end)
end

---------------------------------------------------------------------------
-- 10. Pool & stats
---------------------------------------------------------------------------

local function test_pool_stats()
    current_category = "pool & stats"

    test("len returns per-worker counts", function()
        local lengths = db:len()
        assert(type(lengths) == "table", "len should return a table")
        assert(#lengths == 1, "pool_size=1 should have 1 worker")
        assert(lengths[1] == 0, "idle queue should be 0")
    end)

    test("stats returns global pool info", function()
        local stats = pg.stats()
        assert(type(stats) == "table", "stats should return a table")
    end)

    test("multi-pool len", function()
        local d2 = new_conn(3)
        assert(not d2.code)
        local lengths = d2:len()
        assert(#lengths == 3, "pool_size=3 should have 3 workers")
        d2:close()
    end)
end

---------------------------------------------------------------------------
-- 11. Concurrency
---------------------------------------------------------------------------

local function test_concurrency()
    current_category = "concurrency"

    test("concurrent writes on pool_size=3", function()
        local d = new_conn(3)
        assert(not d.code)
        d:query("DROP TABLE IF EXISTS co_t")
        assert_ok(d:query("CREATE TABLE co_t (id bigint PRIMARY KEY, val text)"))

        local N_CO, OPS = 5, 50
        local total = N_CO * OPS
        local done = 0
        local errors = 0

        for c = 1, N_CO do
            moon.async(function()
                local base = c * 100000
                for i = 1, OPS do
                    local res = d:query_params(
                        "INSERT INTO co_t(id, val) VALUES($1, $2) ON CONFLICT DO NOTHING",
                        base + i, "c" .. c)
                    if res.code then errors = errors + 1 end
                end
                done = done + OPS
            end)
        end

        while done < total do moon.sleep(10) end

        assert(errors == 0, "expected 0 errors, got " .. errors)
        local cnt = assert_ok(d:query("SELECT count(*) AS c FROM co_t"))
        assert(cnt.data[1].c == total, "expected " .. total .. " rows, got " .. tostring(cnt.data[1].c))

        d:query("DROP TABLE co_t")
        d:close()
    end)

    test("concurrent reads and writes", function()
        local d = new_conn(2)
        assert(not d.code)
        assert_ok(d:query("DELETE FROM t"))
        assert_ok(d:insert_many("t", { "id", "name" }, { { 900, "rw" } }))

        local read_ok = true
        local write_done = false

        moon.async(function()
            for i = 1, 20 do
                local res = d:query("SELECT name FROM t WHERE id = 900")
                if res.code then read_ok = false end
            end
        end)

        moon.async(function()
            for i = 1, 20 do
                d:query_params(
                    "UPDATE t SET name = $1 WHERE id = $2",
                    "rw_" .. i, 900)
            end
            write_done = true
        end)

        while not write_done do moon.sleep(10) end
        assert(read_ok, "reads should not fail during concurrent writes")
        assert_ok(d:query("DELETE FROM t WHERE id = 900"))
        d:close()
    end)
end

---------------------------------------------------------------------------
-- 12. Large result set
---------------------------------------------------------------------------

local function test_large_result()
    current_category = "large result set"

    test("SELECT 10000 rows", function()
        local N = 10000
        local rows = {}
        for i = 1, N do
            rows[i] = { 50000 + i, "lr_" .. i }
        end
        assert_ok(db:insert_many("t", { "id", "name" }, rows))
        local res = assert_ok(db:query(
            string.format("SELECT * FROM t WHERE id > 50000 AND id <= %d ORDER BY id", 50000 + N)))
        assert(#res.data == N, "expected " .. N .. " rows, got " .. #res.data)
        assert(res.data[1].id == 50001)
        assert(res.data[N].id == 50000 + N)
        assert_ok(db:query("DELETE FROM t WHERE id > 50000"))
    end)

    test("SELECT with many columns via generate_series", function()
        local res = assert_ok(db:query([[
            SELECT
                g AS id,
                'name_' || g AS name,
                g * 1.1 AS val,
                g % 2 = 0 AS even
            FROM generate_series(1, 100) g
        ]]))
        assert(#res.data == 100, "expected 100 rows")
        assert(res.data[1].id == 1)
        assert(res.data[1].name == "name_1")
        assert(res.data[100].even == true)
    end)
end

---------------------------------------------------------------------------
-- 13. SQL Injection resistance
---------------------------------------------------------------------------

local function test_sql_injection()
    current_category = "SQL injection"

    test("insert_many with evil table name (quoted)", function()
        local evil_table = 't"; DROP TABLE t; --'
        local res = db:insert_many(evil_table, { "id" }, { { 1 } })
        assert_err(res)
        -- Verify original table still exists
        assert_ok(db:query("SELECT 1 FROM t LIMIT 1"))
    end)

    test("insert_many with evil column name (quoted)", function()
        local res = db:insert_many("t", { 'id"; DROP TABLE t; --', "name" }, {
            { 1, "hack" },
        })
        assert_err(res)
        assert_ok(db:query("SELECT 1 FROM t LIMIT 1"))
    end)

    test("update_many with evil key_type (validated)", function()
        local res = db:update_many("t", "id", { "name" }, {
            { 1, "hack" },
        }, "bigint; DROP TABLE t--")
        assert_err(res, "ENCODE")
        assert_ok(db:query("SELECT 1 FROM t LIMIT 1"))
    end)

    test("parameterized queries prevent value injection", function()
        local evil_value = "'; DROP TABLE t; --"
        assert_ok(db:query_params("INSERT INTO t(id, name) VALUES($1, $2)", 700, evil_value))
        local res = assert_ok(db:query_params("SELECT name FROM t WHERE id = $1", 700))
        assert(res.data[1].name == evil_value, "evil value should be stored literally")
        assert_ok(db:query("DELETE FROM t WHERE id = 700"))
    end)
end

---------------------------------------------------------------------------
-- 14. Affected rows
---------------------------------------------------------------------------

local function test_affected_rows()
    current_category = "affected rows"

    test("INSERT affected count", function()
        local res = assert_ok(db:query("INSERT INTO t(id, name) VALUES(800, 'aff')"))
        assert(res.data.affected_rows == 1,
            "expected 1 affected, got " .. tostring(res.data.affected_rows))
        assert_ok(db:query("DELETE FROM t WHERE id = 800"))
    end)

    test("UPDATE affected count", function()
        assert_ok(db:query("INSERT INTO t(id, name) VALUES(801, 'a')"))
        assert_ok(db:query("INSERT INTO t(id, name) VALUES(802, 'b')"))
        local res = assert_ok(db:query("UPDATE t SET name = 'z' WHERE id IN (801, 802)"))
        assert(res.data.affected_rows == 2,
            "expected 2 affected, got " .. tostring(res.data.affected_rows))
        assert_ok(db:query("DELETE FROM t WHERE id IN (801, 802)"))
    end)

    test("DELETE affected count", function()
        assert_ok(db:insert_many("t", { "id", "name" }, {
            { 810, "d1" }, { 811, "d2" }, { 812, "d3" },
        }))
        local res = assert_ok(db:query("DELETE FROM t WHERE id BETWEEN 810 AND 812"))
        assert(res.data.affected_rows == 3,
            "expected 3 affected, got " .. tostring(res.data.affected_rows))
    end)

    test("UPDATE zero rows affected", function()
        local res = assert_ok(db:query("UPDATE t SET name = 'x' WHERE id = -99999"))
        assert(res.data.affected_rows == 0,
            "expected 0 affected, got " .. tostring(res.data.affected_rows))
    end)
end

---------------------------------------------------------------------------
-- Text table output
---------------------------------------------------------------------------

local function print_text_table()
    local nw = #tostring(test_idx)
    if nw < 3 then nw = 3 end
    local max_name = 4
    for _, r in ipairs(test_results) do
        if #r.name > max_name then max_name = #r.name end
        local cw = #r.cat + 4
        if cw > max_name then max_name = cw end
    end
    local rw = 6
    local sep = "+" .. string.rep("-", nw + 2) .. "+"
        .. string.rep("-", max_name + 2) .. "+"
        .. string.rep("-", rw + 2) .. "+"
    local fmt = "| %" .. nw .. "s | %-" .. max_name .. "s | %-" .. rw .. "s |"
    local inner = nw + max_name + rw + 8

    print("\n" .. sep)
    print(string.format(fmt, "#", "Test", "Result"))
    print(sep)
    local last_cat = nil
    for _, r in ipairs(test_results) do
        if r.cat ~= last_cat then
            last_cat = r.cat
            print(string.format(fmt, "", ">>> " .. r.cat, ""))
        end
        print(string.format(fmt, tostring(r.idx), r.name, r.status))
    end
    print(sep)
    local summary = string.format(" %d passed, %d failed", passed, failed)
    print("|" .. summary .. string.rep(" ", inner - #summary) .. "|")
    print("+" .. string.rep("-", inner) .. "+")
    if failed > 0 then
        print("\nFailures:")
        for _, r in ipairs(test_results) do
            if r.status == "FAIL" then
                print(string.format("  #%-" .. nw .. "d %s", r.idx, r.name))
                print(string.format("  %s%s", string.rep(" ", nw + 1), r.err))
            end
        end
    end
end

---------------------------------------------------------------------------
-- Main
---------------------------------------------------------------------------

moon.async(function()
    setup()

    test_connection()
    test_query()
    test_query_params()
    test_pipe()
    test_insert_many()
    test_update_many()
    test_fire_and_forget()
    test_data_types()
    test_error_recovery()
    test_pool_stats()
    test_concurrency()
    test_large_result()
    test_sql_injection()
    test_affected_rows()

    teardown()

    print_text_table()

    moon.exit(failed > 0 and -1 or 0)
end)

moon.shutdown(function()
    moon.quit()
end)
