local moon = require "moon"

local mongodb = require "moon.db.mongodb"

moon.async(function()
    local db = mongodb.connect("mongodb://127.0.0.1:27017", "gamedb1")
    if db.kind then
        print("connect failed", db.message)
        return
    end

    local coll = db:collection("mydatabase", "mycollection")

    local res = coll:insert_one({
        cc      = 300,
        gpsname = "gps1",
        track   = {
            segments = {
                [1] = {
                    HR        = 73,
                    location  = {
                        [1] = 47.763,
                        [2] = 13.4034,
                    },
                    starttime = "2018-10-14 10:05:14",
                },
                [2] = {
                    HR        = 130,
                    location  = {
                        [1] = 47.706,
                        [2] = 13.2635,
                    },
                    starttime = "2018-10-14 10:39:21",
                },
            },
        },
    })

    print_r(res)

    res = coll:update_one({cc = 300}, {
        ["$set"] = {
            ["track.segments.1.HR"] = 100,
        }
    })

    print_r(res)

    res = coll:find({cc = 300}, {limit = 10})

    print_r(res)

    res = coll:find_one({cc = 300})
    print_r(res)

    print_r(coll:delete_one({cc = 300}))

    print_r(coll:delete_many({cc = 300}))

    res = coll:find_one({cc = 300})
    print_r(res)

    res = coll:insert_one({
        cc      = 300,
        gpsname = "gps1",
        track   = {
            segments = {
                [1] = {
                    HR        = 73,
                    location  = {
                        [1] = 47.763,
                        [2] = 13.4034,
                    },
                    starttime = "2018-10-14 10:05:14",
                },
                [2] = {
                    HR        = 130,
                    location  = {
                        [1] = 47.706,
                        [2] = 13.2635,
                    },
                    starttime = "2018-10-14 10:39:21",
                },
            },
        },
    })

    print_r(res)

    local bt = moon.clock()
    for i=1,10000 do
        coll:insert_one({
            cc      = 300,
            gpsname = "gps1",
            track   = {
                segments = {
                    [1] = {
                        HR        = 73,
                        location  = {
                            [1] = 47.763,
                            [2] = 13.4034,
                        },
                        starttime = "2018-10-14 10:05:14",
                    },
                    [2] = {
                        HR        = 130,
                        location  = {
                            [1] = 47.706,
                            [2] = 13.2635,
                        },
                        starttime = "2018-10-14 10:39:21",
                    },
                },
            },
        })
    end
    print("insert 10000 use time", moon.clock() - bt)

end)