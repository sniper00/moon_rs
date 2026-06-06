local moon = require "moon"
local mongodb = require "moon.db.mongodb"

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
    print(string.format("===== %s =====", name))
end

local DB_URL = "mongodb://root:root@127.0.0.1:27017"
local DB_NAME = "moon_rs_test"
local COLL_NAME = "test_stream_collection"

moon.async(function()
    section("1. 连接")
    local db = mongodb.connect(DB_URL, "test_stream_conn")
    assert_not_nil("connect returns db", db)

    local coll = db:collection(DB_NAME, COLL_NAME)

    section("2. 清理旧数据")
    coll:delete_many({})

    section("3. 插入测试数据 (250条)")
    local total_inserted = 250
    for i = 1, total_inserted do
        coll:insert_one({
            seq = i,
            name = "user_" .. i,
            age = 20 + (i % 50),
            group = "group_" .. (i % 5),
        })
    end
    local count_res = coll:count({})
    assert_eq("inserted count", count_res.count, total_inserted)

    section("4. find_stream 基础测试 - 单循环逐条迭代")
    local total_docs = 0
    for doc in db:find_stream(DB_NAME, COLL_NAME, {}, nil, 100) do
        total_docs = total_docs + 1
        assert_not_nil("doc has name", doc.name)
    end
    assert_eq("total docs from stream", total_docs, total_inserted)
    print(string.format("  streamed %d docs one by one", total_docs))

    section("5. find_stream 小批次 (batch_size=50)")
    local total_docs2 = 0
    for _ in db:find_stream(DB_NAME, COLL_NAME, {}, nil, 50) do
        total_docs2 = total_docs2 + 1
    end
    assert_eq("total docs batch_size=50", total_docs2, total_inserted)

    section("6. find_stream 带过滤条件")
    local total_docs3 = 0
    for doc in db:find_stream(DB_NAME, COLL_NAME, { group = "group_0" }, nil, 10) do
        assert_eq("filtered doc group", doc.group, "group_0")
        total_docs3 = total_docs3 + 1
    end
    assert_eq("filtered count (250/5=50)", total_docs3, 50)

    section("7. find_stream 带 sort + limit options")
    local total_docs4 = 0
    local first_seq = nil
    local last_seq = nil
    for doc in db:find_stream(DB_NAME, COLL_NAME, {}, { sort = { seq = 1 }, limit = 120 }, 50) do
        total_docs4 = total_docs4 + 1
        if not first_seq then first_seq = doc.seq end
        last_seq = doc.seq
    end
    assert_eq("limited stream count", total_docs4, 120)
    assert_eq("sorted first seq", first_seq, 1)
    assert_eq("sorted last seq", last_seq, 120)

    section("8. find_stream 提前终止 (break triggers to-be-closed)")
    local count5 = 0
    for doc in db:find_stream(DB_NAME, COLL_NAME, {}, { sort = { seq = 1 } }, 30) do
        count5 = count5 + 1
        assert_eq("early stop seq", doc.seq, count5)
        if count5 >= 5 then break end
    end
    assert_eq("early stop count", count5, 5)

    section("9. find_stream 空结果集")
    local count6 = 0
    for _ in db:find_stream(DB_NAME, COLL_NAME, { name = "nonexistent_user" }, nil, 100) do
        count6 = count6 + 1
    end
    assert_eq("empty result count", count6, 0)

    section("10. find_stream 默认 batch_size")
    local total_docs7 = 0
    for _ in db:find_stream(DB_NAME, COLL_NAME, {}) do
        total_docs7 = total_docs7 + 1
    end
    assert_eq("default batch total docs", total_docs7, total_inserted)

    section("11. find_stream 数据顺序完整性校验")
    local seq_check = 0
    for doc in db:find_stream(DB_NAME, COLL_NAME, {}, { sort = { seq = 1 } }, 80) do
        seq_check = seq_check + 1
        assert_eq("seq order " .. seq_check, doc.seq, seq_check)
    end
    assert_eq("integrity total", seq_check, total_inserted)

    section("12. 清理测试数据")
    local final_del = coll:delete_many({})
    assert_not_nil("final cleanup", final_del.deleted_count)
    assert_eq("final cleanup count", final_del.deleted_count, total_inserted)

    print(string.format("========================================"))
    print(string.format("  Total: %d, Passed: %d, Failed: %d", passed + failed, passed, failed))
    print(string.format("========================================"))
    if failed > 0 then
        print("  RESULT: FAILED")
    else
        print("  RESULT: ALL PASSED")
    end

    moon.exit(failed > 0 and 1 or 0)
end)
