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
