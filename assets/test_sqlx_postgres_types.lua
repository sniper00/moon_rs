local moon = require "moon"
local sqlx = require "moon.db.sqlx"

moon.loglevel("INFO")

local passed = 0
local failed = 0

local function assert_eq(name, actual, expected)
    if actual == expected then
        passed = passed + 1
    else
        failed = failed + 1
        print(string.format("  FAIL [%s]: expected=%s(%s), actual=%s(%s)",
            name, tostring(expected), type(expected), tostring(actual), type(actual)))
    end
end

local function assert_near(name, actual, expected, epsilon)
    epsilon = epsilon or 1e-6
    if type(actual) == "number" and type(expected) == "number" and math.abs(actual - expected) < epsilon then
        passed = passed + 1
    else
        failed = failed + 1
        print(string.format("  FAIL [%s]: expected≈%s, actual=%s", name, tostring(expected), tostring(actual)))
    end
end

local function assert_nil(name, actual)
    if actual == nil then
        passed = passed + 1
    else
        failed = failed + 1
        print(string.format("  FAIL [%s]: expected nil, actual=%s(%s)", name, tostring(actual), type(actual)))
    end
end

local function assert_not_nil(name, actual)
    if actual ~= nil then
        passed = passed + 1
    else
        failed = failed + 1
        print(string.format("  FAIL [%s]: expected non-nil, got nil", name))
    end
end

local function assert_true(name, actual)
    assert_eq(name, actual, true)
end

local function assert_match(name, actual, pattern)
    if type(actual) == "string" and actual:match(pattern) then
        passed = passed + 1
    else
        failed = failed + 1
        print(string.format("  FAIL [%s]: '%s' does not match pattern '%s'", name, tostring(actual), pattern))
    end
end

moon.async(function()
    -----------------------------------------------------------------
    -- 1. 连接
    -----------------------------------------------------------------
    print("\n===== 1. 连接测试 =====")
    local db = sqlx.connect("postgres://postgres:123456@localhost/postgres", "pg_test")
    assert_not_nil("connect returns object", db)

    -----------------------------------------------------------------
    -- 2. 建表 (覆盖所有支持类型)
    -----------------------------------------------------------------
    print("\n===== 2. 建表 =====")
    db:query("DROP TABLE IF EXISTS sqlx_test;")
    local res = db:query([[
        CREATE TABLE sqlx_test (
            id              SERIAL PRIMARY KEY,
            -- Integer types
            col_int2        INT2,
            col_int4        INT4,
            col_int8        INT8,
            col_smallint    SMALLINT,
            col_integer     INTEGER,
            col_bigint      BIGINT,
            -- Float types
            col_float4      FLOAT4,
            col_float8      FLOAT8,
            col_real        REAL,
            col_double      DOUBLE PRECISION,
            -- Bool
            col_bool        BOOL,
            col_boolean     BOOLEAN,
            -- Text types
            col_char        CHAR(10),
            col_varchar     VARCHAR(255),
            col_text        TEXT,
            col_bpchar      BPCHAR(10),
            col_name        NAME,
            -- Binary
            col_bytea       BYTEA,
            -- Date / Time
            col_date        DATE,
            col_time        TIME,
            col_time6       TIME(6),
            col_timestamp   TIMESTAMP,
            col_timestamptz TIMESTAMPTZ,
            -- JSON
            col_json        JSON,
            col_jsonb       JSONB,
            -- UUID
            col_uuid        UUID
        );
    ]])
    assert_nil("create table ok", res and res.kind)

    -----------------------------------------------------------------
    -- 3. 整数边界值
    -----------------------------------------------------------------
    print("\n===== 3. 整数边界值 =====")
    db:query([[INSERT INTO sqlx_test (col_int2,col_int4,col_int8,col_smallint,col_integer,col_bigint)
               VALUES (32767, 2147483647, 9223372036854775807, -32768, -2147483648, -9223372036854775808)]])
    db:query([[INSERT INTO sqlx_test (col_int2,col_int4,col_int8) VALUES (0, 0, 0)]])

    res = db:query("SELECT col_int2,col_int4,col_int8,col_smallint,col_integer,col_bigint FROM sqlx_test ORDER BY id;")
    local r1 = res[1]
    assert_eq("int2 max",      r1.col_int2,     32767)
    assert_eq("int4 max",      r1.col_int4,     2147483647)
    assert_eq("int8 max",      r1.col_int8,     9223372036854775807)
    assert_eq("smallint min",  r1.col_smallint, -32768)
    assert_eq("integer min",   r1.col_integer,  -2147483648)
    assert_eq("bigint min",    r1.col_bigint,   -9223372036854775808)

    local r2 = res[2]
    assert_eq("int2 zero", r2.col_int2, 0)
    assert_eq("int4 zero", r2.col_int4, 0)
    assert_eq("int8 zero", r2.col_int8, 0)

    -----------------------------------------------------------------
    -- 4. 浮点精度 & 边界
    -----------------------------------------------------------------
    print("\n===== 4. 浮点类型 =====")
    db:query("DELETE FROM sqlx_test;")
    db:query([[INSERT INTO sqlx_test (col_float4, col_float8, col_real, col_double)
               VALUES (3.14, 2.718281828459045, 1.414, 3.141592653589793)]])
    db:query([[INSERT INTO sqlx_test (col_float4, col_float8) VALUES (0.0, 0.0)]])
    db:query([[INSERT INTO sqlx_test (col_float4, col_float8) VALUES (-1.5, -1.5)]])

    res = db:query("SELECT col_float4,col_float8,col_real,col_double FROM sqlx_test ORDER BY id;")
    assert_near("float4 3.14",  res[1].col_float4, 3.14, 0.001)
    assert_near("float8 e",     res[1].col_float8, 2.718281828459045, 1e-12)
    assert_near("real 1.414",   res[1].col_real,    1.414, 0.001)
    assert_near("double pi",    res[1].col_double,  3.141592653589793, 1e-15)
    assert_eq("float4 zero",    res[2].col_float4,  0.0)
    assert_eq("float8 zero",    res[2].col_float8,  0.0)
    assert_near("float4 neg",   res[3].col_float4, -1.5, 0.001)
    assert_near("float8 neg",   res[3].col_float8, -1.5, 1e-12)

    -----------------------------------------------------------------
    -- 5. 布尔
    -----------------------------------------------------------------
    print("\n===== 5. 布尔类型 =====")
    db:query("DELETE FROM sqlx_test;")
    db:query("INSERT INTO sqlx_test (col_bool, col_boolean) VALUES (TRUE, FALSE)")
    db:query("INSERT INTO sqlx_test (col_bool, col_boolean) VALUES (FALSE, TRUE)")

    res = db:query("SELECT col_bool,col_boolean FROM sqlx_test ORDER BY id;")
    assert_eq("bool true",    res[1].col_bool,    true)
    assert_eq("boolean false",res[1].col_boolean, false)
    assert_eq("bool false",   res[2].col_bool,    false)
    assert_eq("boolean true", res[2].col_boolean, true)

    -----------------------------------------------------------------
    -- 6. 字符串类型
    -----------------------------------------------------------------
    print("\n===== 6. 字符串类型 =====")
    db:query("DELETE FROM sqlx_test;")
    db:query([[INSERT INTO sqlx_test (col_char,col_varchar,col_text,col_bpchar,col_name)
               VALUES ('hello','varchar string','text content','bpchar','pg_name')]])
    db:query([[INSERT INTO sqlx_test (col_varchar,col_text) VALUES ('','')]])
    db:query([[INSERT INTO sqlx_test (col_varchar) VALUES ('中文UTF8测试🎉')]])

    res = db:query("SELECT col_char,col_varchar,col_text,col_bpchar,col_name FROM sqlx_test ORDER BY id;")
    assert_eq("char padded",  res[1].col_char,    "hello     ")
    assert_eq("varchar",      res[1].col_varchar, "varchar string")
    assert_eq("text",         res[1].col_text,    "text content")
    assert_eq("bpchar padded",res[1].col_bpchar,  "bpchar    ")
    assert_eq("name",         res[1].col_name,    "pg_name")
    assert_eq("varchar empty",res[2].col_varchar, "")
    assert_eq("text empty",   res[2].col_text,    "")
    assert_eq("utf8 emoji",   res[3].col_varchar, "中文UTF8测试🎉")

    -----------------------------------------------------------------
    -- 7. 日期 / 时间
    -----------------------------------------------------------------
    print("\n===== 7. 日期/时间类型 =====")
    db:query("DELETE FROM sqlx_test;")
    db:query([[INSERT INTO sqlx_test (col_date,col_time,col_time6,col_timestamp,col_timestamptz)
               VALUES ('2025-01-01','00:00:00','00:00:00.000000','2025-01-01 00:00:00','2025-01-01 00:00:00+00')]])
    db:query([[INSERT INTO sqlx_test (col_date,col_time,col_time6,col_timestamp,col_timestamptz)
               VALUES ('2025-12-31','23:59:59','23:59:59.999999','2025-12-31 23:59:59.999','2025-12-31 23:59:59.999+08')]])
    db:query([[INSERT INTO sqlx_test (col_date,col_time,col_time6,col_timestamp,col_timestamptz)
               VALUES ('1970-01-01','12:00:00','12:00:00.500000','1970-01-01 00:00:00','1970-01-01 00:00:00+00')]])

    res = db:query("SELECT col_date,col_time,col_time6,col_timestamp,col_timestamptz FROM sqlx_test ORDER BY id;")
    -- Row 1: epoch start of 2025
    assert_eq("date 2025-01-01",    res[1].col_date,        "2025-01-01")
    assert_eq("time midnight",      res[1].col_time,        "00:00:00")
    assert_eq("time6 midnight",     res[1].col_time6,       "00:00:00")
    assert_eq("timestamp epoch",    res[1].col_timestamp,   "2025-01-01 00:00:00")
    assert_eq("timestamptz utc",    res[1].col_timestamptz, "2025-01-01 00:00:00 UTC")
    -- Row 2: end of 2025
    assert_eq("date 2025-12-31",    res[2].col_date,        "2025-12-31")
    assert_eq("time 23:59:59",      res[2].col_time,        "23:59:59")
    assert_eq("time6 precision",    res[2].col_time6,       "23:59:59.999999")
    assert_match("timestamp eoy",   res[2].col_timestamp,   "2025%-12%-31 23:59:59")
    assert_match("timestamptz eoy", res[2].col_timestamptz, "2025%-12%-31 15:59:59")
    -- Row 3: Unix epoch
    assert_eq("date epoch",         res[3].col_date,        "1970-01-01")
    assert_eq("time noon",          res[3].col_time,        "12:00:00")
    assert_eq("time6 half sec",     res[3].col_time6,       "12:00:00.500")
    assert_eq("timestamp unix",     res[3].col_timestamp,   "1970-01-01 00:00:00")
    assert_eq("timestamptz unix",   res[3].col_timestamptz, "1970-01-01 00:00:00 UTC")

    -----------------------------------------------------------------
    -- 8. UUID
    -----------------------------------------------------------------
    print("\n===== 8. UUID =====")
    db:query("DELETE FROM sqlx_test;")
    db:query("INSERT INTO sqlx_test (col_uuid) VALUES ('a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11')")
    db:query("INSERT INTO sqlx_test (col_uuid) VALUES ('00000000-0000-0000-0000-000000000000')")

    res = db:query("SELECT col_uuid FROM sqlx_test ORDER BY id;")
    assert_eq("uuid normal",  res[1].col_uuid, "a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11")
    assert_eq("uuid nil",     res[2].col_uuid, "00000000-0000-0000-0000-000000000000")

    -----------------------------------------------------------------
    -- 9. JSON / JSONB
    -----------------------------------------------------------------
    print("\n===== 9. JSON/JSONB =====")
    db:query("DELETE FROM sqlx_test;")
    db:query([[INSERT INTO sqlx_test (col_json, col_jsonb) VALUES
               ('{"name":"test","value":123}', '{"active":true,"count":456}')]])
    db:query([[INSERT INTO sqlx_test (col_json, col_jsonb) VALUES ('[]', '{}')]])
    db:query([[INSERT INTO sqlx_test (col_json, col_jsonb) VALUES ('"string"', '42')]])

    res = db:query("SELECT col_json,col_jsonb FROM sqlx_test ORDER BY id;")
    assert_match("json object",  res[1].col_json,  '"name"')
    assert_match("jsonb object", res[1].col_jsonb, '"active"')
    assert_eq("json array",      res[2].col_json,  "[]")
    assert_eq("jsonb empty obj", res[2].col_jsonb, "{}")
    assert_eq("json string",     res[3].col_json,  '"string"')
    assert_eq("jsonb number",    res[3].col_jsonb, "42")

    -----------------------------------------------------------------
    -- 10. BYTEA (二进制)
    -----------------------------------------------------------------
    print("\n===== 10. BYTEA =====")
    db:query("DELETE FROM sqlx_test;")
    db:query([[INSERT INTO sqlx_test (col_bytea) VALUES (E'\\xDEADBEEF')]])
    db:query([[INSERT INTO sqlx_test (col_bytea) VALUES (E'\\x00')]])
    db:query([[INSERT INTO sqlx_test (col_bytea) VALUES (E'')]])

    res = db:query("SELECT col_bytea FROM sqlx_test ORDER BY id;")
    assert_not_nil("bytea deadbeef", res[1].col_bytea)
    assert_eq("bytea length 4",     #res[1].col_bytea, 4)
    assert_not_nil("bytea 0x00",     res[2].col_bytea)
    assert_eq("bytea empty",        res[3].col_bytea, "")

    -----------------------------------------------------------------
    -- 11. NULL 值处理
    -----------------------------------------------------------------
    print("\n===== 11. NULL 值 =====")
    db:query("DELETE FROM sqlx_test;")
    db:query("INSERT INTO sqlx_test (col_varchar) VALUES ('only_varchar')")

    res = db:query("SELECT col_int2,col_int4,col_float4,col_bool,col_varchar,col_date,col_uuid,col_json FROM sqlx_test;")
    local r = res[1]
    assert_nil("null int2",    r.col_int2)
    assert_nil("null int4",    r.col_int4)
    assert_nil("null float4",  r.col_float4)
    assert_nil("null bool",    r.col_bool)
    assert_eq("non-null varchar", r.col_varchar, "only_varchar")
    assert_nil("null date",    r.col_date)
    assert_nil("null uuid",    r.col_uuid)
    assert_nil("null json",    r.col_json)

    -----------------------------------------------------------------
    -- 12. 参数化查询
    -----------------------------------------------------------------
    print("\n===== 12. 参数化查询 =====")
    db:query("DELETE FROM sqlx_test;")
    res = db:query(
        "INSERT INTO sqlx_test (col_integer,col_varchar,col_bool,col_float8,col_date) VALUES ($1,$2,$3,$4,$5::date)",
        42, "param_test", true, 9.99, "2025-06-15"
    )
    assert_nil("param insert ok", res and res.kind)

    res = db:query("SELECT col_integer,col_varchar,col_bool,col_float8,col_date FROM sqlx_test WHERE col_integer=$1;", 42)
    assert_eq("param int",     res[1].col_integer,  42)
    assert_eq("param varchar", res[1].col_varchar,  "param_test")
    assert_eq("param bool",    res[1].col_bool,     true)
    assert_near("param float", res[1].col_float8,   9.99, 1e-10)
    assert_eq("param date",    res[1].col_date,     "2025-06-15")

    -- JSON table as parameter
    res = db:query(
        "INSERT INTO sqlx_test (col_integer,col_jsonb) VALUES ($1,$2)",
        100, {items = {1,2,3}, nested = {a = "b"}}
    )
    assert_nil("param json insert ok", res and res.kind)

    res = db:query("SELECT col_jsonb FROM sqlx_test WHERE col_integer=$1;", 100)
    assert_match("param jsonb", res[1].col_jsonb, '"items"')

    -----------------------------------------------------------------
    -- 13. 空结果集
    -----------------------------------------------------------------
    print("\n===== 13. 空结果集 =====")
    res = db:query("SELECT * FROM sqlx_test WHERE col_integer = -99999;")
    assert_eq("empty result is table", type(res), "table")
    assert_eq("empty result #", #res, 0)

    -----------------------------------------------------------------
    -- 14. 事务 - 成功
    -----------------------------------------------------------------
    print("\n===== 14. 事务成功 =====")
    db:query("DELETE FROM sqlx_test;")
    local trans = {
        {"INSERT INTO sqlx_test (col_integer, col_varchar) VALUES ($1, $2)", 1, "tx row 1"},
        {"INSERT INTO sqlx_test (col_integer, col_varchar) VALUES ($1, $2)", 2, "tx row 2"},
        {"INSERT INTO sqlx_test (col_integer, col_varchar) VALUES ($1, $2)", 3, "tx row 3"},
    }
    res = db:transaction(trans)
    assert_eq("transaction ok", res.message, "ok")

    res = db:query("SELECT col_integer,col_varchar FROM sqlx_test ORDER BY col_integer;")
    assert_eq("tx row count", #res, 3)
    assert_eq("tx row1 int",  res[1].col_integer, 1)
    assert_eq("tx row1 text", res[1].col_varchar, "tx row 1")
    assert_eq("tx row3 int",  res[3].col_integer, 3)
    assert_eq("tx row3 text", res[3].col_varchar, "tx row 3")

    -----------------------------------------------------------------
    -- 15. 事务 - 回滚 (第二条违反唯一约束)
    -----------------------------------------------------------------
    print("\n===== 15. 事务回滚 =====")
    db:query("DELETE FROM sqlx_test;")
    db:query("CREATE UNIQUE INDEX IF NOT EXISTS idx_test_int ON sqlx_test(col_integer);")
    db:query("INSERT INTO sqlx_test (col_integer, col_varchar) VALUES (1, 'existing')")

    local bad_trans = {
        {"INSERT INTO sqlx_test (col_integer, col_varchar) VALUES ($1, $2)", 99, "should rollback"},
        {"INSERT INTO sqlx_test (col_integer, col_varchar) VALUES ($1, $2)", 1,  "duplicate!"},
    }
    res = db:transaction(bad_trans)
    assert_not_nil("tx rollback has kind", res.kind)

    res = db:query("SELECT count(*) as cnt FROM sqlx_test WHERE col_integer = 99;")
    assert_eq("tx rollback: row 99 absent", res[1].cnt, 0)

    res = db:query("SELECT count(*) as cnt FROM sqlx_test;")
    assert_eq("tx rollback: total unchanged", res[1].cnt, 1)

    db:query("DROP INDEX IF EXISTS idx_test_int;")

    -----------------------------------------------------------------
    -- 16. SQL 错误处理
    -----------------------------------------------------------------
    print("\n===== 16. SQL 错误 =====")
    res = db:query("SELECT * FROM nonexistent_table_xyz;")
    assert_not_nil("error has kind", res.kind)
    assert_match("error message", res.message, "nonexistent_table_xyz")

    res = db:query("INVALID SQL SYNTAX HERE!!!")
    assert_not_nil("syntax error has kind", res.kind)

    -----------------------------------------------------------------
    -- 17. 大量行
    -----------------------------------------------------------------
    print("\n===== 17. 批量数据 =====")
    db:query("DELETE FROM sqlx_test;")
    local batch_trans = {}
    for i = 1, 100 do
        batch_trans[#batch_trans+1] = {
            "INSERT INTO sqlx_test (col_integer, col_varchar) VALUES ($1, $2)",
            i, "batch_" .. i
        }
    end
    res = db:transaction(batch_trans)
    assert_eq("batch tx ok", res.message, "ok")

    res = db:query("SELECT count(*) as cnt FROM sqlx_test;")
    assert_eq("batch count", res[1].cnt, 100)

    res = db:query("SELECT col_integer,col_varchar FROM sqlx_test ORDER BY col_integer LIMIT 3;")
    assert_eq("batch first", res[1].col_integer, 1)
    assert_eq("batch first text", res[1].col_varchar, "batch_1")
    assert_eq("batch third", res[3].col_integer, 3)

    -----------------------------------------------------------------
    -- 18. 统计信息
    -----------------------------------------------------------------
    print("\n===== 18. 统计信息 =====")
    local stats = sqlx.stats()
    assert_not_nil("stats has pg_test", stats.pg_test)

    -----------------------------------------------------------------
    -- 19. 清理
    -----------------------------------------------------------------
    print("\n===== 19. 清理 =====")
    db:query("DROP TABLE IF EXISTS sqlx_test;")
    db:close()

    -----------------------------------------------------------------
    -- 结果汇总
    -----------------------------------------------------------------
    print(string.format("\n========================================"))
    print(string.format("  Total: %d  Passed: %d  Failed: %d", passed + failed, passed, failed))
    print(string.format("========================================"))
    if failed > 0 then
        print("  RESULT: FAILED")
    else
        print("  RESULT: ALL PASSED")
    end

    moon.exit(failed > 0 and 1 or 0)
end)
