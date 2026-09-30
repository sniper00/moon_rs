-- Run: moon_rs assets/test/test_request_pool.lua (SQLite only; no external DB).
local moon = require "moon"
local sqlx = require "moon.db.sqlx"
local name = "request_pool_regression"
local unsolicited_reply = false
moon.dispatch("sqlx", function()
    unsolicited_reply = true
end)

moon.timeout(10000, function()
    moon.error("request pool regression timed out")
    moon.exit(1)
end)

local function wait_idle()
    for _ = 1, 100 do
        local stats = sqlx.stats()[name]
        if stats.pending == 0 then
            return stats
        end
        assert(stats.pending > 0, "pending became negative")
        moon.sleep(1)
    end
    error("pending count did not return to zero")
end

moon.async(function()
    local ok, err = xpcall(function()
        local db = sqlx.connect("sqlite://:memory:", name, 3000, 1, 1)
        assert(not db:query("CREATE TABLE values_test (value INTEGER)").kind)
        assert(db:transaction({
            {"INSERT INTO values_test VALUES (?)", 1},
            {"INSERT INTO values_test VALUES (?)", 2},
        }).message == "ok")
        assert(db.obj:exec_query("INSERT INTO values_test VALUES (?)", 3) == true)
        wait_idle()
        assert(#db:query("SELECT value FROM values_test") == 3)
        assert(not unsolicited_reply, "fire-and-forget sent a response")
        assert(db:query("SELECT COUNT(*) AS n FROM values_test")[1].n == 3)
        local literals = db:query("SELECT 42 AS n, 1.5 AS real_value, 'hello' AS text_value, x'0041' AS bytes, NULL AS absent")[1]
        assert(literals.n == 42 and literals.real_value == 1.5 and literals.text_value == "hello")
        assert(literals.bytes == "\0A" and literals.absent == nil)
        local mixed = db:query("SELECT NULL AS value UNION ALL SELECT 7 UNION ALL SELECT 'text' UNION ALL SELECT 2.5")
        assert(mixed[1].value == nil and mixed[2].value == 7 and mixed[3].value == "text" and mixed[4].value == 2.5)
        local stream_values = {}
        for row in db:query_stream("SELECT 42 AS value UNION ALL SELECT 43", 1) do
            stream_values[#stream_values + 1] = row.value
        end
        assert(stream_values[1] == 42 and stream_values[2] == 43)
        local before = wait_idle().total

        local queued
        do
            local iter, _, _, cursor = db:query_stream("SELECT value FROM values_test ORDER BY value", 1)
            local closing <close> = cursor
            assert(iter().value == 1)
            assert(sqlx.stats()[name].pending == 1, "active stream must remain counted")
            queued = db.obj:query("SELECT value FROM values_test ORDER BY value LIMIT 1")
            assert(type(queued) == "number")
            assert(db.obj:query("SELECT 43 AS value").kind == "ERROR", "full queue must reject query")
            assert(db.obj:close().kind == "ERROR", "full queue must report close failure")
            local stats = sqlx.stats()[name]
            assert(stats.pending == 2)
            assert(stats.total == before + 2, "failed sends must not count")
        end
        assert(moon.wait(queued)[1].value == 1)
        assert(wait_idle().total == before + 2)
        assert(db:query("SELECT * FROM missing_table").kind == "DB")
        wait_idle()

        -- A superseded handle must not remove the replacement from the registry.
        local replacement = sqlx.connect("sqlite://:memory:", name, 3000, 1, 1)
        local old_closed = false
        for _ = 1, 100 do
            local res = db.obj:query("SELECT 1")
            if type(res) == "table" then
                assert(res.kind == "ERROR")
                old_closed = true
                break
            end
            moon.wait(res)
            moon.sleep(1)
        end
        assert(old_closed, "replacement did not shut down the old worker")
        assert(not replacement:query("CREATE TABLE replacement_test (value INTEGER)").kind)
        assert(not replacement:query("INSERT INTO replacement_test VALUES (7)").kind)
        assert(replacement:query("SELECT value FROM replacement_test")[1].value == 7)
        wait_idle()
        assert(replacement.obj:close() == true)
        for _ = 1, 100 do
            if not sqlx.stats()[name] then break end
            moon.sleep(1)
        end
        assert(not sqlx.stats()[name], "close did not remove the connection")
    end, debug.traceback)
    if ok then
        print("request pool regression passed")
    else
        moon.error(err)
    end
    moon.exit(ok and 0 or 1)
end)
