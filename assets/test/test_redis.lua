---
--- test_redis.lua — Redis native driver, pub/sub watch, and stream tests.
---
--- Run: moon_rs assets/test/test_redis.lua
---

local moon = require("moon")
local redis = require("moon.db.redis")
local RedisStream = dofile("redis_stream.lua")

local HOST = "127.0.0.1"
local PORT = 6379
local TIMEOUT = 5000

local pass_count = 0
local fail_count = 0
local test_prefix = "test_redis:" .. tostring(math.random(100000))

local function assert_eq(name, expected, actual)
    if expected == actual then
        pass_count = pass_count + 1
    else
        fail_count = fail_count + 1
        print(string.format("FAIL [%s]: expected=%s(%s), actual=%s(%s)",
            name, tostring(expected), type(expected), tostring(actual), type(actual)))
    end
end

local function assert_gte(name, actual, minimum)
    if type(actual) == "number" and actual >= minimum then
        pass_count = pass_count + 1
    else
        fail_count = fail_count + 1
        print(string.format("FAIL [%s]: expected >= %s, got %s",
            name, tostring(minimum), tostring(actual)))
    end
end

local function assert_true(name, value)
    if value then
        pass_count = pass_count + 1
    else
        fail_count = fail_count + 1
        print(string.format("FAIL [%s]: expected true, got %s", name, tostring(value)))
    end
end

local function wait_until(cond_fn, timeout_s)
    local deadline = moon.clock() + timeout_s
    while not cond_fn() and moon.clock() < deadline do
        moon.sleep(50)
    end
    return cond_fn()
end

local function redis_conf()
    return string.format("redis://%s:%d", HOST, PORT)
end

moon.async(function()
    print("=== Redis tests (prefix: " .. test_prefix .. ") ===\n")

    local db, err = redis.connect(string.format("%s?name=test_redis&connect_timeout=%d&pool_size=1",
        redis_conf(), TIMEOUT))
    if not db then
        print("connect failed:", err)
        moon.exit(-1)
        return
    end

    -----------------------------------------------------------------
    -- Basic commands
    -----------------------------------------------------------------
    print("--- Basic commands ---")

    local key = test_prefix .. ":key"
    assert_eq("SET", "OK", db:set(key, "hello"))
    assert_eq("GET", "hello", db:get(key))

    db:set(test_prefix .. ":counter", 0)
    assert_eq("INCR", 1, db:incr(test_prefix .. ":counter"))
    assert_eq("INCRBY", 6, db:incrby(test_prefix .. ":counter", 5))

    local pipe = db:pipeline({
        { "SET", test_prefix .. ":a", "1" },
        { "SET", test_prefix .. ":b", "2" },
        { "GET", test_prefix .. ":a" },
        { "GET", test_prefix .. ":b" },
    })
    assert_eq("pipeline[3]", "1", pipe[3])
    assert_eq("pipeline[4]", "2", pipe[4])

    local no_sub_channel = test_prefix .. ":nobody"
    assert_eq("publish no subscribers", 0, db:publish(no_sub_channel, "ignored"))

    -----------------------------------------------------------------
    -- Pub/Sub watch
    -----------------------------------------------------------------
    print("\n--- Pub/Sub watch ---")

    local channel = test_prefix .. ":pub"
    local watcher, werr = redis.watch(string.format("%s?connect_timeout=%d", redis_conf(), TIMEOUT))
    if not watcher then
        print("watch connect failed:", werr)
        moon.exit(-1)
        return
    end

    local received = false
    moon.async(function()
        local msg, ch = watcher:message()
        assert_eq("pubsub message", "hello-pubsub", msg)
        assert_eq("pubsub channel", channel, ch)
        received = true
    end)

    watcher:subscribe(channel)
    moon.sleep(100)

    local subs = db:publish(channel, "hello-pubsub")
    assert_gte("publish subscriber count", subs, 1)
    assert_true("message received via subscribe", wait_until(function() return received end, 3))

    local pattern = test_prefix .. ":news.*"
    local news_channel = test_prefix .. ":news.sports"
    local preceived = false

    moon.async(function()
        local msg, matched, pat = watcher:message()
        assert_eq("pmessage body", "goal", msg)
        assert_eq("pmessage channel", news_channel, matched)
        assert_eq("pmessage pattern", pattern, pat)
        preceived = true
    end)

    watcher:psubscribe(pattern)
    moon.sleep(100)

    subs = db:publish(news_channel, "goal")
    assert_gte("psubscribe publish count", subs, 1)
    assert_true("message received via psubscribe", wait_until(function() return preceived end, 3))

    local unsub_channel = test_prefix .. ":unsub"
    watcher:subscribe(unsub_channel)
    moon.sleep(100)
    subs = db:publish(unsub_channel, "before")
    assert_gte("publish before unsubscribe", subs, 1)

    watcher:unsubscribe(unsub_channel)
    moon.sleep(100)
    subs = db:publish(unsub_channel, "after")
    assert_eq("publish after unsubscribe", 0, subs)

    local punsub_pattern = test_prefix .. ":alert.*"
    local punsub_channel = test_prefix .. ":alert.fire"
    watcher:psubscribe(punsub_pattern)
    moon.sleep(100)
    subs = db:publish(punsub_channel, "before")
    assert_gte("publish before punsubscribe", subs, 1)

    watcher:punsubscribe(punsub_pattern)
    moon.sleep(100)
    subs = db:publish(punsub_channel, "after")
    assert_eq("publish after punsubscribe", 0, subs)

    watcher:disconnect()

    -----------------------------------------------------------------
    -- Redis Stream (redis_stream.lua helper)
    -----------------------------------------------------------------
    print("\n--- Redis Stream ---")

    local stream = RedisStream.new(redis_conf(), "test_redis_stream")
    local stream_key = test_prefix .. ":stream"
    local group_name = "test_group"
    local consumer_name = "consumer-1"

    stream:xgroup_create(stream_key, group_name, "0")
    assert_eq("initial stream length", 0, stream:xlen(stream_key))

    local msg_id = stream:xadd(stream_key, "event", "login", "user", "alice")
    assert_true("xadd returns id", type(msg_id) == "string" and #msg_id > 0)
    assert_eq("stream length after xadd", 1, stream:xlen(stream_key))

    local read_result = stream:xreadgroup(stream_key, group_name, consumer_name, 10)
    assert_true("xreadgroup returns data", type(read_result) == "table" and #read_result > 0)

    local entries = read_result[1][2]
    assert_true("stream entry exists", type(entries) == "table" and #entries > 0)
    local entry = entries[1]
    assert_eq("stream entry id", msg_id, entry[1])

    local fields = entry[2]
    local field_map = {}
    for i = 1, #fields, 2 do
        field_map[fields[i]] = fields[i + 1]
    end
    assert_eq("stream field event", "login", field_map.event)
    assert_eq("stream field user", "alice", field_map.user)

    local ack_count = stream:xack(stream_key, group_name, msg_id)
    assert_eq("xack count", 1, ack_count)

    local del_count = stream:xdel(stream_key, msg_id)
    assert_eq("xdel count", 1, del_count)
    assert_eq("stream length after xdel", 0, stream:xlen(stream_key))

    stream:xgroup_destroy(stream_key, group_name)
    stream:disconnect()

    -----------------------------------------------------------------
    -- Cleanup
    -----------------------------------------------------------------
    db:del(key, test_prefix .. ":counter", test_prefix .. ":a", test_prefix .. ":b")
    db:del(stream_key)
    db:close()

    print(string.format("\n========================================"))
    print(string.format("  Redis Tests: %d passed, %d failed", pass_count, fail_count))
    print(string.format("========================================"))
    if fail_count > 0 then
        print("SOME TESTS FAILED!")
    else
        print("ALL TESTS PASSED!")
    end

    moon.exit(fail_count)
end)
