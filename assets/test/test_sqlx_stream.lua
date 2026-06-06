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

local function assert_not_nil(name, actual)
    if actual ~= nil then
        passed = passed + 1
    else
        failed = failed + 1
        print(string.format("  FAIL [%s]: expected non-nil, got nil", name))
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

local function assert_true(name, actual)
    assert_eq(name, actual, true)
end

local function section(name)
    print(string.format("\n===== %s =====", name))
end

local DB_URL = "mysql://root:root@127.0.0.1:3306/mysql?ssl-mode=DISABLED"

moon.async(function()
    section("1. 连接")
    local db = sqlx.connect(DB_URL, "stream_test_conn")
    assert_not_nil("connect returns db", db)

    section("2. 建表并插入测试数据")
    db:query("DROP TABLE IF EXISTS sqlx_stream_test")
    db:query([[
        CREATE TABLE sqlx_stream_test (
            id INT AUTO_INCREMENT PRIMARY KEY,
            seq INT NOT NULL,
            name VARCHAR(64) NOT NULL,
            age INT NOT NULL,
            grp VARCHAR(16) NOT NULL
        )
    ]])

    local total_inserted = 250
    for i = 1, total_inserted do
        db:query(
            "INSERT INTO sqlx_stream_test (seq, name, age, grp) VALUES (?, ?, ?, ?)",
            i, "user_" .. i, 20 + (i % 50), "group_" .. (i % 5)
        )
    end

    local count_res = db:query("SELECT COUNT(*) as cnt FROM sqlx_stream_test")
    assert_eq("inserted count", count_res[1].cnt, total_inserted)

    section("3. query_stream 基础测试 - 单循环逐行迭代")
    local total_rows = 0
    for row in db:query_stream("SELECT * FROM sqlx_stream_test ORDER BY seq", 100) do
        total_rows = total_rows + 1
        assert_not_nil("row has name", row.name)
    end
    assert_eq("total rows from stream", total_rows, total_inserted)
    print(string.format("  streamed %d rows one by one", total_rows))

    section("4. query_stream 小批次 (batch_size=50)")
    local total_rows2 = 0
    for _ in db:query_stream("SELECT * FROM sqlx_stream_test", 50) do
        total_rows2 = total_rows2 + 1
    end
    assert_eq("total rows batch_size=50", total_rows2, total_inserted)

    section("5. query_stream 带 WHERE 条件")
    local total_rows3 = 0
    for row in db:query_stream("SELECT * FROM sqlx_stream_test WHERE grp = ?", 10, "group_0") do
        assert_eq("filtered row grp", row.grp, "group_0")
        total_rows3 = total_rows3 + 1
    end
    assert_eq("filtered count (250/5=50)", total_rows3, 50)

    section("6. query_stream 带 ORDER BY + LIMIT")
    local total_rows4 = 0
    local first_seq = nil
    local last_seq = nil
    for row in db:query_stream("SELECT * FROM sqlx_stream_test ORDER BY seq LIMIT 120", 50) do
        total_rows4 = total_rows4 + 1
        if not first_seq then first_seq = row.seq end
        last_seq = row.seq
    end
    assert_eq("limited stream count", total_rows4, 120)
    assert_eq("sorted first seq", first_seq, 1)
    assert_eq("sorted last seq", last_seq, 120)

    section("7. query_stream 提前终止 (break triggers to-be-closed)")
    local count5 = 0
    for row in db:query_stream("SELECT * FROM sqlx_stream_test ORDER BY seq", 30) do
        count5 = count5 + 1
        assert_eq("early stop seq", row.seq, count5)
        if count5 >= 5 then break end
    end
    assert_eq("early stop count", count5, 5)

    section("8. query_stream 空结果集")
    local count6 = 0
    for _ in db:query_stream("SELECT * FROM sqlx_stream_test WHERE name = 'nonexistent'", 100) do
        count6 = count6 + 1
    end
    assert_eq("empty result count", count6, 0)

    section("9. query_stream 默认 batch_size")
    local total_rows7 = 0
    for _ in db:query_stream("SELECT * FROM sqlx_stream_test") do
        total_rows7 = total_rows7 + 1
    end
    assert_eq("default batch total rows", total_rows7, total_inserted)

    section("10. query_stream 数据顺序完整性校验")
    local seq_check = 0
    for row in db:query_stream("SELECT * FROM sqlx_stream_test ORDER BY seq", 80) do
        seq_check = seq_check + 1
        assert_eq("seq order " .. seq_check, row.seq, seq_check)
    end
    assert_eq("integrity total", seq_check, total_inserted)

    section("11. query_stream 带绑定参数的范围查询")
    local range_count = 0
    for row in db:query_stream(
        "SELECT * FROM sqlx_stream_test WHERE age >= ? AND age <= ? ORDER BY seq", 50, 25, 35
    ) do
        range_count = range_count + 1
        assert_true("age in range", row.age >= 25 and row.age <= 35)
    end
    print(string.format("  range query returned %d rows", range_count))

    section("12. 清理")
    db:query("DROP TABLE IF EXISTS sqlx_stream_test")

    print(string.format("\n========================================"))
    print(string.format("  Total: %d, Passed: %d, Failed: %d", passed + failed, passed, failed))
    print(string.format("========================================"))
    if failed > 0 then
        print("  RESULT: FAILED")
    else
        print("  RESULT: ALL PASSED")
    end

    moon.exit(failed > 0 and 1 or 0)
end)
