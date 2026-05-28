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
    local db = sqlx.connect("mysql://127.0.0.1:3306/mysql?ssl-mode=DISABLED", "mysql_test")
    assert_not_nil("connect returns object", db)

    -----------------------------------------------------------------
    -- 2. 建表
    -----------------------------------------------------------------
    print("\n===== 2. 建表 =====")
    db:query("DROP TABLE IF EXISTS sqlx_test;")
    local res = db:query([[
        CREATE TABLE sqlx_test (
            id              INT AUTO_INCREMENT PRIMARY KEY,
            -- Integer types
            col_tinyint     TINYINT,
            col_smallint    SMALLINT,
            col_mediumint   MEDIUMINT,
            col_int         INT,
            col_bigint      BIGINT,
            -- Float types
            col_float       FLOAT,
            col_double      DOUBLE,
            -- Bool
            col_boolean     BOOLEAN,
            -- Text types
            col_char        CHAR(10),
            col_varchar     VARCHAR(255),
            col_text        TEXT,
            -- Binary
            col_varbinary   VARBINARY(255),
            col_blob        BLOB,
            -- Date / Time
            col_date        DATE,
            col_time        TIME,
            col_datetime    DATETIME(6),
            col_timestamp   TIMESTAMP(6) NULL,
            -- JSON
            col_json        JSON
        );
    ]])
    assert_nil("create table ok", res and res.kind)

    -----------------------------------------------------------------
    -- 3. 整数边界值
    -----------------------------------------------------------------
    print("\n===== 3. 整数边界值 =====")
    db:query([[INSERT INTO sqlx_test (col_tinyint,col_smallint,col_mediumint,col_int,col_bigint)
               VALUES (127, 32767, 8388607, 2147483647, 9223372036854775807)]])
    db:query([[INSERT INTO sqlx_test (col_tinyint,col_smallint,col_mediumint,col_int,col_bigint)
               VALUES (-128, -32768, -8388608, -2147483648, -9223372036854775808)]])
    db:query([[INSERT INTO sqlx_test (col_tinyint,col_smallint,col_int,col_bigint) VALUES (0, 0, 0, 0)]])

    res = db:query("SELECT col_tinyint,col_smallint,col_mediumint,col_int,col_bigint FROM sqlx_test ORDER BY id;")
    local r1 = res[1]
    assert_eq("tinyint max",    r1.col_tinyint,   127)
    assert_eq("smallint max",   r1.col_smallint,  32767)
    assert_eq("mediumint max",  r1.col_mediumint, 8388607)
    assert_eq("int max",        r1.col_int,       2147483647)
    assert_eq("bigint max",     r1.col_bigint,    9223372036854775807)

    local r2 = res[2]
    assert_eq("tinyint min",    r2.col_tinyint,   -128)
    assert_eq("smallint min",   r2.col_smallint,  -32768)
    assert_eq("mediumint min",  r2.col_mediumint, -8388608)
    assert_eq("int min",        r2.col_int,       -2147483648)
    assert_eq("bigint min",     r2.col_bigint,    -9223372036854775808)

    local r3 = res[3]
    assert_eq("tinyint zero",  r3.col_tinyint,  0)
    assert_eq("smallint zero", r3.col_smallint, 0)
    assert_eq("int zero",     r3.col_int,      0)
    assert_eq("bigint zero",  r3.col_bigint,   0)

    -----------------------------------------------------------------
    -- 4. 浮点精度
    -----------------------------------------------------------------
    print("\n===== 4. 浮点类型 =====")
    db:query("DELETE FROM sqlx_test;")
    db:query("INSERT INTO sqlx_test (col_float, col_double) VALUES (3.14, 2.718281828459045)")
    db:query("INSERT INTO sqlx_test (col_float, col_double) VALUES (0.0, 0.0)")
    db:query("INSERT INTO sqlx_test (col_float, col_double) VALUES (-1.5, -99.999)")

    res = db:query("SELECT col_float,col_double FROM sqlx_test ORDER BY id;")
    assert_near("float 3.14",    res[1].col_float,  3.14, 0.001)
    assert_near("double e",      res[1].col_double, 2.718281828459045, 1e-12)
    assert_eq("float zero",      res[2].col_float,  0.0)
    assert_eq("double zero",     res[2].col_double, 0.0)
    assert_near("float neg",     res[3].col_float, -1.5, 0.001)
    assert_near("double neg",    res[3].col_double, -99.999, 1e-10)

    -----------------------------------------------------------------
    -- 5. 布尔 (MySQL BOOLEAN = TINYINT(1))
    -----------------------------------------------------------------
    print("\n===== 5. 布尔类型 =====")
    db:query("DELETE FROM sqlx_test;")
    db:query("INSERT INTO sqlx_test (col_boolean) VALUES (TRUE)")
    db:query("INSERT INTO sqlx_test (col_boolean) VALUES (FALSE)")

    res = db:query("SELECT col_boolean FROM sqlx_test ORDER BY id;")
    assert_eq("boolean true",  res[1].col_boolean, true)
    assert_eq("boolean false", res[2].col_boolean, false)

    -----------------------------------------------------------------
    -- 6. 字符串类型
    -----------------------------------------------------------------
    print("\n===== 6. 字符串类型 =====")
    db:query("DELETE FROM sqlx_test;")
    db:query([[INSERT INTO sqlx_test (col_char,col_varchar,col_text) VALUES ('hello','varchar string','text content')]])
    db:query([[INSERT INTO sqlx_test (col_varchar,col_text) VALUES ('','')]])
    db:query([[INSERT INTO sqlx_test (col_varchar) VALUES ('中文UTF8测试🎉')]])

    res = db:query("SELECT col_char,col_varchar,col_text FROM sqlx_test ORDER BY id;")
    assert_eq("char padded",  res[1].col_char,    "hello")
    assert_eq("varchar",      res[1].col_varchar, "varchar string")
    assert_eq("text",         res[1].col_text,    "text content")
    assert_eq("varchar empty",res[2].col_varchar, "")
    assert_eq("text empty",   res[2].col_text,    "")
    assert_eq("utf8 emoji",   res[3].col_varchar, "中文UTF8测试🎉")

    -----------------------------------------------------------------
    -- 7. 日期 / 时间
    -----------------------------------------------------------------
    print("\n===== 7. 日期/时间类型 =====")
    db:query("DELETE FROM sqlx_test;")
    db:query([[INSERT INTO sqlx_test (col_date,col_time,col_datetime,col_timestamp)
               VALUES ('2025-01-01','00:00:00','2025-01-01 00:00:00','2025-01-01 00:00:00')]])
    db:query([[INSERT INTO sqlx_test (col_date,col_time,col_datetime,col_timestamp)
               VALUES ('2025-12-31','23:59:59','2025-12-31 23:59:59.123456','2025-12-31 23:59:59.999999')]])
    db:query([[INSERT INTO sqlx_test (col_date,col_time,col_datetime)
               VALUES ('1970-01-01','12:30:00','1970-01-01 00:00:00')]])

    res = db:query("SELECT col_date,col_time,col_datetime,col_timestamp FROM sqlx_test ORDER BY id;")
    assert_eq("date 2025-01-01",     res[1].col_date,     "2025-01-01")
    assert_eq("time midnight",       res[1].col_time,     "00:00:00")
    assert_match("datetime epoch25", res[1].col_datetime,  "2025%-01%-01")
    assert_match("timestamp epoch25",res[1].col_timestamp, "2025%-01%-01")
    assert_eq("date 2025-12-31",     res[2].col_date,     "2025-12-31")
    assert_eq("time 23:59:59",       res[2].col_time,     "23:59:59")
    assert_match("datetime eoy",     res[2].col_datetime,  "2025%-12%-31 23:59:59")
    assert_match("timestamp eoy",    res[2].col_timestamp, "2025%-12%-31")
    assert_eq("date epoch",          res[3].col_date,     "1970-01-01")
    assert_eq("time 12:30",          res[3].col_time,     "12:30:00")

    -----------------------------------------------------------------
    -- 8. JSON
    -----------------------------------------------------------------
    print("\n===== 8. JSON =====")
    db:query("DELETE FROM sqlx_test;")
    db:query([[INSERT INTO sqlx_test (col_json) VALUES ('{"name":"test","value":123}')]])
    db:query([[INSERT INTO sqlx_test (col_json) VALUES ('[]')]])
    db:query([[INSERT INTO sqlx_test (col_json) VALUES ('{}')]])
    db:query([[INSERT INTO sqlx_test (col_json) VALUES ('"just a string"')]])
    db:query([[INSERT INTO sqlx_test (col_json) VALUES ('42')]])

    res = db:query("SELECT col_json FROM sqlx_test ORDER BY id;")
    assert_match("json object",  res[1].col_json, '"name"')
    assert_eq("json array",     res[2].col_json, "[]")
    assert_eq("json empty obj", res[3].col_json, "{}")
    assert_eq("json string",    res[4].col_json, '"just a string"')
    assert_eq("json number",    res[5].col_json, "42")

    -----------------------------------------------------------------
    -- 9. 二进制 (VARBINARY / BLOB)
    -----------------------------------------------------------------
    print("\n===== 9. 二进制类型 =====")
    db:query("DELETE FROM sqlx_test;")
    db:query("INSERT INTO sqlx_test (col_varbinary, col_blob) VALUES (UNHEX('DEADBEEF'), UNHEX('0102030405'))")
    db:query("INSERT INTO sqlx_test (col_varbinary, col_blob) VALUES (UNHEX('00'), X'')")

    res = db:query("SELECT col_varbinary,col_blob FROM sqlx_test ORDER BY id;")
    assert_eq("varbinary len",  #res[1].col_varbinary, 4)
    assert_eq("blob len",       #res[1].col_blob, 5)
    assert_eq("varbinary 0x00", #res[2].col_varbinary, 1)
    assert_eq("blob empty",     #res[2].col_blob, 0)

    -----------------------------------------------------------------
    -- 10. NULL 值处理
    -----------------------------------------------------------------
    print("\n===== 10. NULL 值 =====")
    db:query("DELETE FROM sqlx_test;")
    db:query("INSERT INTO sqlx_test (col_varchar) VALUES ('only_varchar')")

    res = db:query("SELECT col_tinyint,col_int,col_float,col_boolean,col_varchar,col_date,col_json FROM sqlx_test;")
    local r = res[1]
    assert_nil("null tinyint",  r.col_tinyint)
    assert_nil("null int",      r.col_int)
    assert_nil("null float",    r.col_float)
    assert_nil("null boolean",  r.col_boolean)
    assert_eq("non-null varchar", r.col_varchar, "only_varchar")
    assert_nil("null date",     r.col_date)
    assert_nil("null json",     r.col_json)

    -----------------------------------------------------------------
    -- 11. 参数化查询
    -----------------------------------------------------------------
    print("\n===== 11. 参数化查询 =====")
    db:query("DELETE FROM sqlx_test;")
    res = db:query(
        "INSERT INTO sqlx_test (col_int,col_varchar,col_boolean,col_double,col_date) VALUES (?,?,?,?,?)",
        42, "param_test", true, 9.99, "2025-06-15"
    )
    assert_nil("param insert ok", res and res.kind)

    res = db:query("SELECT col_int,col_varchar,col_boolean,col_double,col_date FROM sqlx_test WHERE col_int=?;", 42)
    assert_eq("param int",     res[1].col_int,     42)
    assert_eq("param varchar", res[1].col_varchar, "param_test")
    assert_near("param double",res[1].col_double,  9.99, 1e-10)
    assert_eq("param date",    res[1].col_date,    "2025-06-15")

    -- JSON table as parameter
    res = db:query(
        "INSERT INTO sqlx_test (col_int, col_json) VALUES (?, ?)",
        100, {items = {1,2,3}, nested = {a = "b"}}
    )
    assert_nil("param json insert ok", res and res.kind)

    res = db:query("SELECT col_json FROM sqlx_test WHERE col_int=?;", 100)
    assert_match("param json", res[1].col_json, '"items"')

    -----------------------------------------------------------------
    -- 12. 空结果集
    -----------------------------------------------------------------
    print("\n===== 12. 空结果集 =====")
    res = db:query("SELECT * FROM sqlx_test WHERE col_int = -99999;")
    assert_eq("empty result is table", type(res), "table")
    assert_eq("empty result #", #res, 0)

    -----------------------------------------------------------------
    -- 13. 事务 - 成功
    -----------------------------------------------------------------
    print("\n===== 13. 事务成功 =====")
    db:query("DELETE FROM sqlx_test;")
    local trans = {
        {"INSERT INTO sqlx_test (col_int, col_varchar) VALUES (?, ?)", 1, "tx row 1"},
        {"INSERT INTO sqlx_test (col_int, col_varchar) VALUES (?, ?)", 2, "tx row 2"},
        {"INSERT INTO sqlx_test (col_int, col_varchar) VALUES (?, ?)", 3, "tx row 3"},
    }
    res = db:transaction(trans)
    assert_eq("transaction ok", res.message, "ok")

    res = db:query("SELECT col_int,col_varchar FROM sqlx_test ORDER BY col_int;")
    assert_eq("tx row count", #res, 3)
    assert_eq("tx row1 int",  res[1].col_int, 1)
    assert_eq("tx row1 text", res[1].col_varchar, "tx row 1")
    assert_eq("tx row3 int",  res[3].col_int, 3)
    assert_eq("tx row3 text", res[3].col_varchar, "tx row 3")

    -----------------------------------------------------------------
    -- 14. 事务 - 回滚 (唯一约束冲突)
    -----------------------------------------------------------------
    print("\n===== 14. 事务回滚 =====")
    db:query("DELETE FROM sqlx_test;")
    db:query("CREATE UNIQUE INDEX idx_test_int ON sqlx_test(col_int);")
    db:query("INSERT INTO sqlx_test (col_int, col_varchar) VALUES (1, 'existing')")

    local bad_trans = {
        {"INSERT INTO sqlx_test (col_int, col_varchar) VALUES (?, ?)", 99, "should rollback"},
        {"INSERT INTO sqlx_test (col_int, col_varchar) VALUES (?, ?)", 1,  "duplicate!"},
    }
    res = db:transaction(bad_trans)
    assert_not_nil("tx rollback has kind", res.kind)

    res = db:query("SELECT count(*) as cnt FROM sqlx_test WHERE col_int = 99;")
    assert_eq("tx rollback: row 99 absent", res[1].cnt, 0)

    res = db:query("SELECT count(*) as cnt FROM sqlx_test;")
    assert_eq("tx rollback: total unchanged", res[1].cnt, 1)

    db:query("DROP INDEX idx_test_int ON sqlx_test;")

    -----------------------------------------------------------------
    -- 15. SQL 错误处理
    -----------------------------------------------------------------
    print("\n===== 15. SQL 错误 =====")
    res = db:query("SELECT * FROM nonexistent_table_xyz;")
    assert_not_nil("error has kind", res.kind)

    res = db:query("INVALID SQL SYNTAX HERE!!!")
    assert_not_nil("syntax error has kind", res.kind)

    -----------------------------------------------------------------
    -- 16. 批量数据
    -----------------------------------------------------------------
    print("\n===== 16. 批量数据 =====")
    db:query("DELETE FROM sqlx_test;")
    local batch_trans = {}
    for i = 1, 100 do
        batch_trans[#batch_trans+1] = {
            "INSERT INTO sqlx_test (col_int, col_varchar) VALUES (?, ?)",
            i, "batch_" .. i
        }
    end
    res = db:transaction(batch_trans)
    assert_eq("batch tx ok", res.message, "ok")

    res = db:query("SELECT count(*) as cnt FROM sqlx_test;")
    assert_eq("batch count", res[1].cnt, 100)

    res = db:query("SELECT col_int,col_varchar FROM sqlx_test ORDER BY col_int LIMIT 3;")
    assert_eq("batch first",      res[1].col_int, 1)
    assert_eq("batch first text", res[1].col_varchar, "batch_1")
    assert_eq("batch third",      res[3].col_int, 3)

    -----------------------------------------------------------------
    -- 17. 统计信息
    -----------------------------------------------------------------
    print("\n===== 17. 统计信息 =====")
    local stats = sqlx.stats()
    assert_not_nil("stats has mysql_test", stats.mysql_test)

    -----------------------------------------------------------------
    -- 18. 清理
    -----------------------------------------------------------------
    print("\n===== 18. 清理 =====")
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
