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

local function assert_false(name, actual)
    assert_eq(name, actual, false)
end

local function assert_gt(name, actual, threshold)
    if type(actual) == "number" and actual > threshold then
        passed = passed + 1
    else
        failed = failed + 1
        print(string.format("  FAIL [%s]: expected > %s, actual=%s", name, tostring(threshold), tostring(actual)))
    end
end

local function section(name)
    print(string.format("===== %s =====    (%s:%d)", name, "test_mongodb.lua", debug.getinfo(2, "l").currentline))
end

local DB_URL = "mongodb://root:root@127.0.0.1:27017"
local DB_NAME = "moon_rs_test"
local COLL_NAME = "test_collection"

moon.async(function()
    section("1. 连接测试")
    local db = mongodb.connect(DB_URL, "test_conn")
    assert_not_nil("connect returns db", db)

    section("2. find_connection")
    local db2 = mongodb.find_connection("test_conn")
    assert_not_nil("find existing connection", db2)

    local coll = db:collection(DB_NAME, COLL_NAME)
    assert_not_nil("collection proxy created", coll)

    -- Clean up any leftover data from previous runs
    section("3. 清理旧数据 (delete_many)")
    local del_res = coll:delete_many({})
    assert_not_nil("delete_many result", del_res)
    assert_not_nil("delete_many deleted_count", del_res.deleted_count)

    section("4. insert_one")
    local doc = { name = "Alice", age = 30, city = "Beijing" }
    local insert_res = coll:insert_one(doc)
    assert_not_nil("insert_one result", insert_res)
    assert_not_nil("insert_one inserted_id", insert_res.inserted_id)
    print("  inserted_id: " .. tostring(insert_res.inserted_id))

    section("5. insert_many")
    local docs = {
        { name = "Bob",     age = 25, city = "Shanghai" },
        { name = "Charlie", age = 35, city = "Guangzhou" },
        { name = "Diana",   age = 28, city = "Shenzhen" },
        { name = "Eve",     age = 32, city = "Beijing" },
    }
    local insert_many_res = coll:insert_many(docs)
    assert_not_nil("insert_many result", insert_many_res)

    section("6. count")
    local count_res = coll:count({})
    assert_eq("count all docs", count_res.count, 5)

    local count_bj = coll:count({ city = "Beijing" })
    assert_eq("count Beijing docs", count_bj.count, 2)

    section("7. find_one")
    local found = coll:find_one({ name = "Alice" })
    assert_not_nil("find_one result", found)
    assert_eq("find_one name", found.name, "Alice")
    assert_eq("find_one age", found.age, 30)
    assert_eq("find_one city", found.city, "Beijing")

    local not_found = coll:find_one({ name = "Nonexistent" })
    assert_nil("find_one nonexistent", not_found)

    section("8. find (多条查询)")
    local all_docs = coll:find({})
    assert_eq("find all count", #all_docs, 5)

    section("9. find with options (sort + limit)")
    local sorted = coll:find({}, { sort = { age = 1 }, limit = 3 })
    assert_eq("find sorted+limit count", #sorted, 3)
    assert_eq("find sorted first name", sorted[1].name, "Bob")
    assert_eq("find sorted second name", sorted[2].name, "Diana")
    assert_eq("find sorted third name", sorted[3].name, "Alice")

    section("10. find with projection")
    local projected = coll:find({ name = "Alice" }, { projection = { name = 1, age = 1, _id = 0 } })
    assert_eq("projected count", #projected, 1)
    assert_eq("projected name", projected[1].name, "Alice")
    assert_eq("projected age", projected[1].age, 30)
    assert_nil("projected city excluded", projected[1].city)

    section("11. find with skip")
    local skipped = coll:find({}, { sort = { age = 1 }, skip = 2 })
    assert_eq("skip count", #skipped, 3)
    assert_eq("skip first name", skipped[1].name, "Alice")

    section("12. exists")
    local exists_res = coll:exists({ name = "Alice" })
    assert_true("exists Alice", exists_res.exists)

    local not_exists_res = coll:exists({ name = "Nonexistent" })
    assert_false("not exists Nonexistent", not_exists_res.exists)

    section("13. update_one")
    local update_res = coll:update_one(
        { name = "Alice" },
        { ["$set"] = { age = 31, city = "Hangzhou" } }
    )
    assert_eq("update_one matched", update_res.matched_count, 1)
    assert_eq("update_one modified", update_res.modified_count, 1)

    local updated = coll:find_one({ name = "Alice" })
    assert_eq("updated age", updated.age, 31)
    assert_eq("updated city", updated.city, "Hangzhou")

    section("14. update_many")
    local update_many_res = coll:update_many(
        { city = "Beijing" },
        { ["$set"] = { country = "China" } }
    )
    assert_eq("update_many matched", update_many_res.matched_count, 1)
    assert_eq("update_many modified", update_many_res.modified_count, 1)

    section("15. replace_one")
    local replace_res = coll:replace_one(
        { name = "Diana" },
        { name = "Diana", age = 29, city = "Chengdu", replaced = true }
    )
    assert_eq("replace_one matched", replace_res.matched_count, 1)
    assert_eq("replace_one modified", replace_res.modified_count, 1)

    local replaced = coll:find_one({ name = "Diana" })
    assert_eq("replaced age", replaced.age, 29)
    assert_eq("replaced city", replaced.city, "Chengdu")
    assert_true("replaced flag", replaced.replaced)

    section("16. delete_one")
    local del_one_res = coll:delete_one({ name = "Eve" })
    assert_eq("delete_one deleted", del_one_res.deleted_count, 1)

    local after_del = coll:count({})
    assert_eq("count after delete_one", after_del.count, 4)

    section("17. delete_many")
    local del_many_res = coll:delete_many({ name = { ["$in"] = { "Bob", "Charlie" } } })
    assert_eq("delete_many deleted", del_many_res.deleted_count, 2)

    local after_del_many = coll:count({})
    assert_eq("count after delete_many", after_del_many.count, 2)

    section("18. 嵌套文档与数组")
    local nested_doc = {
        name = "Frank",
        age = 40,
        address = {
            street = "123 Main St",
            zip = "100000"
        },
        tags = { "dev", "lua", "rust" },
        scores = { 95, 87, 92 }
    }
    local nested_res = coll:insert_one(nested_doc)
    assert_not_nil("insert nested inserted_id", nested_res.inserted_id)

    local frank = coll:find_one({ name = "Frank" })
    assert_eq("nested address.street", frank.address.street, "123 Main St")
    assert_eq("nested address.zip", frank.address.zip, "100000")
    assert_eq("nested tags count", #frank.tags, 3)
    assert_eq("nested tags[1]", frank.tags[1], "dev")
    assert_eq("nested tags[2]", frank.tags[2], "lua")
    assert_eq("nested tags[3]", frank.tags[3], "rust")
    assert_eq("nested scores[1]", frank.scores[1], 95)
    assert_eq("nested scores[2]", frank.scores[2], 87)
    assert_eq("nested scores[3]", frank.scores[3], 92)

    section("19. create_index")
    local index_res = coll:create_index({ name = 1 }, { unique = true })
    assert_not_nil("create_index name", index_res.name)
    print("  index name: " .. tostring(index_res.name))

    local compound_index_res = coll:create_index({ age = 1, city = -1 })
    assert_not_nil("create compound index", compound_index_res.name)

    section("20. 数值类型")
    coll:delete_many({ name = "NumTest" })
    coll:insert_one({
        name = "NumTest",
        int_val = 42,
        float_val = 3.14,
        negative = -100,
        zero = 0,
        big_int = 2^50,
    })
    local num_doc = coll:find_one({ name = "NumTest" })
    assert_eq("int_val", num_doc.int_val, 42)
    assert_eq("negative", num_doc.negative, -100)
    assert_eq("zero", num_doc.zero, 0)
    assert_eq("big_int", num_doc.big_int, 2^50)

    section("21. 布尔类型")
    coll:delete_many({ name = "BoolTest" })
    coll:insert_one({ name = "BoolTest", active = true, deleted = false })
    local bool_doc = coll:find_one({ name = "BoolTest" })
    assert_true("bool true", bool_doc.active)
    assert_false("bool false", bool_doc.deleted)

    section("22. 空文档查询")
    local empty_query = coll:find({})
    assert_gt("empty query returns docs", #empty_query, 0)

    section("23. $gt / $lt 查询操作符")
    local gt_docs = coll:find({ age = { ["$gt"] = 30 } })
    for _, d in ipairs(gt_docs) do
        assert_true("$gt age > 30: " .. tostring(d.name), d.age > 30)
    end

    section("24. update_one with $inc")
    coll:update_one({ name = "Alice" }, { ["$inc"] = { age = 1 } })
    local inc_doc = coll:find_one({ name = "Alice" })
    assert_eq("$inc age", inc_doc.age, 32)

    section("25. update_one with $unset")
    coll:update_one({ name = "Alice" }, { ["$unset"] = { country = "" } })
    local unset_doc = coll:find_one({ name = "Alice" })
    assert_nil("$unset country", unset_doc.country)

    section("26. 清理测试数据")
    local final_del = coll:delete_many({})
    assert_not_nil("final cleanup deleted_count", final_del.deleted_count)

    local final_count = coll:count({})
    assert_eq("final count is 0", final_count.count, 0)

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
