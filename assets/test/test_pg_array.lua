-- Run with MOON_PG_TEST_URL set: moon_rs assets/test/test_pg_array.lua
-- Uses one connection and session-local temporary tables only.
local moon = require("moon")
local pg = require("moon.db.pg")
local json = require("json")

local function checked(result)
    assert(not result.code, (result.code or "") .. ": " .. (result.message or ""))
    return result.data
end

moon.async(function()
    local db
    local ok, err = xpcall(function()
        local url = assert(os.getenv("MOON_PG_TEST_URL"), "MOON_PG_TEST_URL is required")
        local separator = url:find("?", 1, true) and "&" or "?"
        db = pg.connect(url .. separator .. "name=array_integration&max_connections=1&connect_timeout=5000")
        assert(pg.NULL == json.null)
        checked(db:query([[
            CREATE TEMP TABLE moon_array_test (
                id bigint PRIMARY KEY,
                ids int4[], tags text[], amounts numeric[], matrix int4[], flags bool[]
            )
        ]]))
        -- Type names must not accept expressions that change SET or WHERE.
        checked(db:query("CREATE TEMP TABLE moon_type_guard(id bigint PRIMARY KEY, flag boolean)"))
        checked(db:query("INSERT INTO moon_type_guard VALUES (1,false),(2,false)"))
        local accepted = pcall(function()
            db:update_many("moon_type_guard", "id", {"flag"}, {{1,false}},
                "bigint", {"boolean OR true"})
        end)
        assert(not accepted, "set_types must reject SQL expressions locally")
        local rejected = db:update_many("moon_type_guard", "id", {"flag"}, {{1,true}},
            "bigint OR true", {"boolean"})
        assert(rejected.code == "ENCODE", "key_type must reject SQL expressions locally")
        local unchanged = checked(db:query("SELECT flag FROM moon_type_guard ORDER BY id"))
        assert(unchanged[1].flag == false and unchanged[2].flag == false)
        checked(db:query("DROP TABLE moon_type_guard"))
        -- A prior session setting must not turn pg.NULL into the string NULL.
        checked(db:query("SET array_nulls=off"))
        local nulls = checked(db:query_params("SELECT $1::text[] AS a",
            pg.array({pg.NULL, "NULL"})))[1].a
        assert(nulls[1] == pg.NULL and nulls[2] == "NULL")
        assert(checked(db:query("SHOW array_nulls"))[1].array_nulls == "on")
        local tags = {"NULL", "", "a,b", 'a"b', "a\\b", "{x}", pg.NULL, "中文"}
        checked(db:query_params([[
            INSERT INTO moon_array_test VALUES ($1, $2::int4[], $3::text[],
                $4::numeric[], $5::int4[], $6::bool[])
        ]], 1, pg.array({1, pg.NULL, -3}), pg.array(tags),
            pg.array({"12345678901234567890.123456789", "-0.001", pg.NULL}),
            pg.array({{1, 2}, {3, 4}}), pg.array({true, false, pg.NULL})))
        local row = checked(db:query("SELECT * FROM moon_array_test WHERE id=1"))[1]
        assert(row.ids[1] == 1 and row.ids[2] == pg.NULL and row.ids[3] == -3)
        for i, value in ipairs(tags) do assert(row.tags[i] == value, "text array mismatch") end
        assert(row.amounts[1] == "12345678901234567890.123456789")
        assert(row.amounts[2] == "-0.001" and row.amounts[3] == pg.NULL)
        assert(row.matrix[1][2] == 2 and row.matrix[2][1] == 3)
        assert(row.flags[1] == true and row.flags[2] == false and row.flags[3] == pg.NULL)
        checked(db:insert_many("moon_array_test", {"id", "ids", "tags"}, {
            {2, pg.array({}), pg.array({})},
            {3, pg.array({7, 8}), pg.array({"bulk"})},
        }))
        checked(db:update_many("moon_array_test", "id", {"ids", "tags"}, {
            {2, pg.array({10, pg.NULL}), pg.array({"updated"})},
            {3, pg.array({20}), pg.array({"also updated"})},
        }, "bigint", {"int4[]", "text[]"}))
        row = checked(db:query("SELECT * FROM moon_array_test WHERE id=2"))[1]
        assert(row.ids[1] == 10 and row.ids[2] == pg.NULL and row.tags[1] == "updated")
        checked(db:pipe({
            {"UPDATE moon_array_test SET ids=$1::int4[] WHERE id=$2", pg.array({30}), 3},
            {"INSERT INTO moon_array_test(id, ids) VALUES ($1,$2::int4[])", 4, pg.array({})},
        }))
        row = checked(db:query("SELECT ids FROM moon_array_test WHERE id=3"))[1]
        assert(row.ids[1] == 30)
        row = checked(db:query("SELECT ids, tags FROM moon_array_test WHERE id=4"))[1]
        assert(#row.ids == 0 and row.tags == nil)
        local matches = checked(db:query_params(
            "SELECT id FROM moon_array_test WHERE id=ANY($1::bigint[]) ORDER BY id", pg.array({1, 3})))
        assert(#matches == 2 and matches[1].id == 1 and matches[2].id == 3)
        row = checked(db:query([[
            SELECT ARRAY[1,2]::int2[] AS shorts, ARRAY[9223372036854775807]::int8[] AS longs,
                ARRAY[1.25,NULL]::float4[] AS floats, ARRAY[2.5]::float8[] AS doubles,
                ARRAY['00000000-0000-0000-0000-000000000001']::uuid[] AS uuids,
                ARRAY['2026-10-17']::date[] AS dates,
                ARRAY['{"a":1}']::jsonb[] AS docs,
                ARRAY[decode('00ff','hex')]::bytea[] AS blobs
        ]]))[1]
        assert(row.shorts[2] == 2 and row.longs[1] == math.maxinteger)
        assert(row.floats[1] == 1.25 and row.floats[2] == pg.NULL and row.doubles[1] == 2.5)
        assert(row.uuids[1] == "00000000-0000-0000-0000-000000000001")
        assert(row.dates[1] == "2026-10-17" and json.decode(row.docs[1]).a == 1)
        assert(row.blobs[1] == "\\x00ff")
        local bounds = db:query("SELECT '[0:1]={1,2}'::int4[] AS ids")
        assert(bounds.code == "PROTOCOL", "non-one bounds must be rejected")
        row = checked(db:query("SELECT ARRAY[1]::int4[] AS ids"))[1]
        assert(row.ids[1] == 1, "connection must remain usable after decode errors")
        checked(db:query("DROP TABLE moon_array_test"))
        print("PG ARRAY INTEGRATION PASSED")
    end, debug.traceback)
    if db then db:close() end
    if not ok then moon.error(err) end
    moon.exit(ok and 0 or -1)
end)

moon.shutdown(function() moon.quit() end)
