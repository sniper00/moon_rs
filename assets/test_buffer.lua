local moon= require("moon")
local buffer = require "buffer"

do
    local p = buffer.concat(string.pack("hI", 11, 12))

    print(buffer.unpack(p, "hI"))
    
    buffer.drop(p)
end

do
    local p = buffer.concat(string.pack(">H", 16), "hello", "world")

    print(buffer.unpack(p, ">HZ"))

    buffer.drop(p)
end

-- Test case for buffer.seek
local function test_buffer_seek()
    local buf = buffer.new(10, 2)
    buffer.write(buf, "test")
    local result = buffer.seek(buf, 2)
    assert(result, "buffer.seek failed")
    result = buffer.seek(buf, -1)
    assert(result, "buffer.seek failed")
    result = buffer.seek(buf, 10)
    assert(not result, "buffer.seek failed")
    buffer.drop(buf)  -- Don't forget to release the buffer
end

-- Test case for buffer.commit
local function test_buffer_commit()
    local buf = buffer.new(10, 2)
    buffer.write(buf, "test")
    local result = buffer.commit(buf, 2)
    assert(result, "buffer.commit failed")
    result = buffer.commit(buf, 10)
    assert(not result, "buffer.commit failed")
    buffer.drop(buf)  -- Don't forget to release the buffer
end

-- Test case for buffer.prepare
local function test_buffer_prepare()
    local buf = buffer.new(10, 2)
    local result = buffer.prepare(buf, 20)
    assert(result, "buffer.prepare failed")
    result = buffer.prepare(buf, 5)
    assert(result, "buffer.prepare failed")
    buffer.drop(buf)  -- Don't forget to release the buffer
end

-- Test case for buffer.size
local function test_buffer_size()
    local buf = buffer.new(10, 2)
    buffer.write(buf, "test")
    local size = buffer.size(buf)
    assert(size == 4, "buffer.size failed")
    buffer.write(buf, "test")
    size = buffer.size(buf)
    assert(size == 8, "buffer.size failed")
    buffer.drop(buf)  -- Don't forget to release the buffer
end

-- Run all test cases
test_buffer_seek()
test_buffer_commit()
test_buffer_prepare()
test_buffer_size()

print("All test cases passed")