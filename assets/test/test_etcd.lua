local moon = require "moon"
moon.loglevel("INFO")
local protobuf = require "protobuf"
local etcd = require "moon.db.etcd"
assert(protobuf.load(io.readfile("../example/grpc/etcd.pb")))
moon.async(function()
    local ok, err = xpcall(function()
        local client, status = etcd.connect({endpoint=assert(os.getenv("MOON_ETCD_TEST_URL")),name="validation_etcd"})
        assert(client, status and status.message)
        local function checked(result, failure)
            assert(result, failure and failure.message or "missing etcd result")
            return result
        end
        assert(etcd.prefix_end("/test/") == "/test0")
        local key = "/validation/hello"
        checked(client:put(key, "中文"))
        local row = checked(client:range(key))
        assert(row.count == 1 and row.kvs[1].value == "中文")
        local watch <close> = assert(client:watch({key=key}))
        local created = checked(watch:recv())
        assert(created.created)
        checked(client:put(key,"updated"))
        local event = checked(watch:recv())
        assert(event.events[1].kv.value == "updated")
        local lease = checked(client:lease_grant(30))
        assert(lease.ID and lease.TTL > 0)
        checked(client:put("/validation/leased", "temporary", {lease=lease.ID}))
        checked(client:lease_revoke(lease.ID))
        assert(checked(client:range("/validation/leased")).count == 0)
        assert(checked(client:delete(key)) == 1)
        checked(client:status())
        watch:close()
        etcd.close("validation_etcd")
        print("ETCD PASSED")
    end, debug.traceback)
    if not ok then moon.error(err) end
    moon.exit(ok and 0 or 1)
end)
moon.shutdown(function() moon.quit() end)
