local moon = require "moon"
moon.loglevel("INFO")
local pg = require "moon.db.pg"
local namesearch = require "moon.namesearch"
moon.async(function()
    local db
    local ok, err = xpcall(function()
        local url = assert(os.getenv("MOON_PG_TEST_URL"))
        db = pg.connect(url .. "?name=namesearch_test&max_connections=1")
        assert(not db.code, db.message)
        local search = namesearch.new(db, "validation_players")
        assert(search:init_schema(), "pg_trgm unavailable")
        assert(not search:add(1, "Alice").code)
        assert(not search:add(2, "Alicia").code)
        assert(not search:add(3, "A%Literal").code)
        local rows, result = search:search_prefix("Ali")
        assert(not result.code and #rows == 2, result.message)
        rows, result = search:search_prefix("A%")
        assert(not result.code and #rows == 1 and rows[1].uid == 3, result.message)
        rows, result = search:search_substring("LIC")
        assert(not result.code and #rows == 2, result.message)
        rows, result = search:search_similar("Alice")
        assert(not result.code and rows[1].name == "Alice", result.message)
        assert(not search:add(1, "Updated").code)
        assert(not search:remove(2).code)
        assert(not db:query("DROP TABLE validation_players").code)
        print("NAMESEARCH PASSED")
    end, debug.traceback)
    if db then db:close() end
    if not ok then moon.error(err) end
    moon.exit(ok and 0 or 1)
end)
moon.shutdown(function() moon.quit() end)
