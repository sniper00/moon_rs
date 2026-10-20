-- Run with SQLx enabled: cargo run --all-features -- assets/test/test_sqlx_sqlite.lua
local moon = require "moon"
local sqlx = require "moon.db.sqlx"
local json = require "json"

moon.async(function()
    local ok, err = xpcall(function()
        local db = sqlx.connect("sqlite://:memory:", "sqlite_regression", 5000, 1)
        local created = db:query("CREATE TABLE sample (id INTEGER, label TEXT, document JSON, payload BLOB)")
        assert(not created.kind, created.message)
        local payload = string.char(0, 255, 65)
        local document = { nested = { 1, true, "value" }, empty = json.object() }
        for id = 1, 5 do
            local result = db:query("INSERT INTO sample VALUES (?, ?, ?, ?)", id, "row" .. id, document, payload)
            assert(not result.kind, result.message)
        end
        local row = db:query("SELECT * FROM sample WHERE id = ?", 1)[1]
        assert(row.label == "row1" and row.payload == payload)
        assert(json.decode(row.document).nested[3] == "value")
        assert(row.document:find('"empty":{}', 1, true))

        for _, batch_size in ipairs({1, 2, 5, 10}) do
            local count = 0
            for item in db:query_stream("SELECT * FROM sample ORDER BY id", batch_size) do
                count = count + 1
                assert(item.id == count and item.payload == payload)
            end
            assert(count == 5)
        end
        for item in db:query_stream("SELECT * FROM sample", 1) do
            assert(item.id == 1)
            break
        end
        assert(db:query("SELECT COUNT(*) AS n FROM sample")[1].n == 5)
        assert(not pcall(function()
            for _ in db:query_stream("SELECT * FROM sample", 1) do
                error("leave stream")
            end
        end))
        assert(db:query("SELECT COUNT(*) AS n FROM sample")[1].n == 5)
        for _ in db:query_stream("SELECT * FROM sample WHERE id < 0", 2) do
            error("unexpected row")
        end
        local result = db:transaction({
            {"INSERT INTO sample VALUES (?, ?, ?, ?)", 6, "transaction", document, payload},
            {"INSERT INTO sample VALUES (?, ?, ?, ?)", 7, "explicit JSON", sqlx.json(document), payload},
        })
        assert(not result.kind, result.message)
        assert(db:query("SELECT COUNT(*) AS n FROM sample")[1].n == 7)
        db:close()
    end, debug.traceback)
    if not ok then
        moon.error(err)
    else
        print("test_sqlx_sqlite passed")
    end
    moon.exit(ok and 0 or 1)
end)
