local moon = require("moon")
local buffer = require "buffer"

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

local function assert_true(name, actual)
    assert_eq(name, actual, true)
end

local function assert_false(name, actual)
    assert_eq(name, actual, false)
end

local function assert_error(name, fn)
    local ok, _ = pcall(fn)
    if not ok then
        passed = passed + 1
    else
        failed = failed + 1
        print(string.format("  FAIL [%s]: expected error, but succeeded", name))
    end
end

local function section(name)
    print(string.format("===== %s =====", name))
end

-- ========================================
section("1. buffer.new + buffer.size + buffer.drop")
-- ========================================
do
    local buf = buffer.new()
    assert_eq("new default size", buffer.size(buf), 0)
    buffer.drop(buf)
end

do
    local buf = buffer.new(1024)
    assert_eq("new with capacity size", buffer.size(buf), 0)
    buffer.drop(buf)
end

do
    local buf = buffer.new(0)
    assert_eq("new zero capacity size", buffer.size(buf), 0)
    buffer.drop(buf)
end

-- ========================================
section("2. buffer.write + buffer.size")
-- ========================================
do
    local buf = buffer.new()
    buffer.write(buf, "hello")
    assert_eq("write string size", buffer.size(buf), 5)

    buffer.write(buf, " world")
    assert_eq("write append size", buffer.size(buf), 11)

    buffer.drop(buf)
end

do
    local buf = buffer.new()
    buffer.write(buf, "a", "b", "c")
    assert_eq("write multiple args size", buffer.size(buf), 3)
    buffer.drop(buf)
end

do
    local buf = buffer.new()
    buffer.write(buf, 12345)
    assert_eq("write integer size", buffer.size(buf), 5)
    buffer.drop(buf)
end

do
    local buf = buffer.new()
    buffer.write(buf, 3.14)
    assert_eq("write float size > 0", buffer.size(buf) > 0, true)
    buffer.drop(buf)
end

do
    local buf = buffer.new()
    buffer.write(buf, true)
    assert_eq("write true size", buffer.size(buf), 4)
    buffer.write(buf, false)
    assert_eq("write true+false size", buffer.size(buf), 9)
    buffer.drop(buf)
end

do
    local buf = buffer.new()
    buffer.write(buf, "")
    assert_eq("write empty string size", buffer.size(buf), 0)
    buffer.drop(buf)
end

do
    local buf = buffer.new()
    assert_error("write unsupported type (table)", function()
        buffer.write(buf, {})
    end)
    buffer.drop(buf)
end

-- ========================================
section("3. buffer.read")
-- ========================================
do
    local buf = buffer.new()
    buffer.write(buf, "hello world")

    local data = buffer.read(buf, 5)
    assert_eq("read first 5 bytes", data, "hello")
    assert_eq("size after read", buffer.size(buf), 6)

    local rest = buffer.read(buf, 6)
    assert_eq("read remaining", rest, " world")
    assert_eq("size after read all", buffer.size(buf), 0)

    buffer.drop(buf)
end

do
    local buf = buffer.new()
    buffer.write(buf, "abc")
    local data = buffer.read(buf, 0)
    assert_eq("read 0 bytes", data, "")
    assert_eq("size after read 0", buffer.size(buf), 3)
    buffer.drop(buf)
end

do
    local buf = buffer.new()
    buffer.write(buf, "abc")
    assert_error("read beyond size", function()
        buffer.read(buf, 10)
    end)
    buffer.drop(buf)
end

-- ========================================
section("4. buffer.clear")
-- ========================================
do
    local buf = buffer.new()
    buffer.write(buf, "hello world")
    assert_eq("size before clear", buffer.size(buf), 11)
    buffer.clear(buf)
    assert_eq("size after clear", buffer.size(buf), 0)
    buffer.drop(buf)
end

do
    local buf = buffer.new()
    buffer.clear(buf)
    assert_eq("clear empty buffer", buffer.size(buf), 0)
    buffer.drop(buf)
end

-- ========================================
section("5. buffer.seek")
-- ========================================
do
    local buf = buffer.new()
    buffer.write(buf, "abcdef")

    assert_true("seek +2", buffer.seek(buf, 2))
    assert_eq("size after seek +2", buffer.size(buf), 4)

    assert_true("seek +2 again", buffer.seek(buf, 2))
    assert_eq("size after seek +4 total", buffer.size(buf), 2)

    assert_true("seek -1", buffer.seek(buf, -1))
    assert_eq("size after seek -1", buffer.size(buf), 3)

    assert_false("seek beyond end", buffer.seek(buf, 10))

    assert_true("seek 0 noop", buffer.seek(buf, 0))
    assert_eq("size after seek 0", buffer.size(buf), 3)

    buffer.drop(buf)
end

do
    local buf = buffer.new()
    assert_false("seek on empty buffer +1", buffer.seek(buf, 1))
    assert_true("seek on empty buffer 0", buffer.seek(buf, 0))
    buffer.drop(buf)
end

-- ========================================
section("6. buffer.commit")
-- ========================================
do
    local buf = buffer.new()
    buffer.prepare(buf, 10)
    assert_true("commit 5", buffer.commit(buf, 5))
    assert_eq("size after commit", buffer.size(buf), 5)

    assert_false("commit beyond capacity", buffer.commit(buf, 10000))

    buffer.drop(buf)
end

do
    local buf = buffer.new()
    assert_true("commit 0", buffer.commit(buf, 0))
    assert_eq("size after commit 0", buffer.size(buf), 0)
    buffer.drop(buf)
end

-- ========================================
section("7. buffer.prepare")
-- ========================================
do
    local buf = buffer.new(8)
    local ptr = buffer.prepare(buf, 100)
    assert_eq("prepare returns pointer", type(ptr), "userdata")
    buffer.drop(buf)
end

do
    local buf = buffer.new(1024)
    buffer.prepare(buf, 0)
    assert_eq("prepare 0 no crash", buffer.size(buf), 0)
    buffer.drop(buf)
end

-- ========================================
section("8. buffer.write_front")
-- ========================================
do
    -- write_front uses rpos space freed by seek/read
    local buf = buffer.new()
    buffer.write(buf, "xxxxxworld")
    buffer.seek(buf, 5) -- consume 5 bytes, freeing front space
    assert_eq("size after seek", buffer.size(buf), 5)
    buffer.write_front(buf, "hello")
    assert_eq("write_front size", buffer.size(buf), 10)
    local data = buffer.read(buf, 10)
    assert_eq("write_front + read", data, "helloworld")
    buffer.drop(buf)
end

do
    local buf = buffer.new()
    buffer.write(buf, "xxcd")
    buffer.seek(buf, 2)
    buffer.write_front(buf, "ab")
    assert_eq("write_front after seek size", buffer.size(buf), 4)
    local data = buffer.read(buf, 4)
    assert_eq("write_front after seek content", data, "abcd")
    buffer.drop(buf)
end

do
    -- write_front fails when not enough front space
    local buf = buffer.new()
    buffer.write(buf, "data")
    assert_error("write_front no front space", function()
        buffer.write_front(buf, "x")
    end)
    buffer.drop(buf)
end

do
    -- write_front with multiple args (written in reverse order)
    local buf = buffer.new()
    buffer.write(buf, "xxxd")
    buffer.seek(buf, 3) -- free 3 bytes of front space
    buffer.write_front(buf, "a", "b", "c")
    local data = buffer.read(buf, 4)
    assert_eq("write_front multiple args", data, "abcd")
    buffer.drop(buf)
end

do
    -- write_front after read (read also frees front space)
    local buf = buffer.new()
    buffer.write(buf, "xxhello")
    buffer.read(buf, 2) -- consume 2 bytes
    buffer.write_front(buf, "OK")
    local data = buffer.read(buf, 7)
    assert_eq("write_front after read", data, "OKhello")
    buffer.drop(buf)
end

-- ========================================
section("9. buffer.concat + buffer.unpack")
-- ========================================
do
    local p = buffer.concat("hello", " ", "world")
    local str = buffer.unpack(p, "Z")
    assert_eq("concat strings", str, "hello world")
    buffer.drop(p)
end

do
    local p = buffer.concat(string.pack("hI", 11, 12))
    local a, b = buffer.unpack(p, "hI")
    assert_eq("concat pack h", a, 11)
    assert_eq("concat pack I", b, 12)
    buffer.drop(p)
end

do
    local p = buffer.concat(string.pack(">H", 16), "hello")
    local num, str = buffer.unpack(p, ">HZ")
    assert_eq("concat big-endian H", num, 16)
    -- Z returns entire buffer content (not position-aware)
    assert_eq("concat Z returns full buffer", #str, 7)
    buffer.drop(p)
end

do
    local p = buffer.concat(42, " ", true, " ", 3.14)
    local str = buffer.unpack(p, "Z")
    assert_eq("concat mixed types starts with 42", str:sub(1, 2), "42")
    assert_eq("concat mixed contains true", str:find("true") ~= nil, true)
    buffer.drop(p)
end

do
    local p = buffer.concat({ "a", "b", "c" })
    local str = buffer.unpack(p, "Z")
    assert_eq("concat table of strings", str, "abc")
    buffer.drop(p)
end

do
    local p = buffer.concat({ "a", { "b", "c" } })
    local str = buffer.unpack(p, "Z")
    assert_eq("concat nested table", str, "abc")
    buffer.drop(p)
end

do
    assert_error("concat unsupported type", function()
        buffer.concat(function() end)
    end)
end

-- ========================================
section("10. buffer.concat_string")
-- ========================================
do
    local str = buffer.concat_string("hello", " ", "world")
    assert_eq("concat_string result", str, "hello world")
    assert_eq("concat_string type", type(str), "string")
end

do
    local str = buffer.concat_string(1, 2, 3)
    assert_eq("concat_string numbers", str, "123")
end

do
    local str = buffer.concat_string()
    assert_eq("concat_string no args", str, nil)
end

-- ========================================
section("11. buffer.unpack positional read")
-- ========================================
do
    local buf = buffer.new()
    buffer.write(buf, "helloworld")

    local sub = buffer.unpack(buf, 0, 5)
    assert_eq("unpack pos 0 len 5", sub, "hello")

    local sub2 = buffer.unpack(buf, 5, 5)
    assert_eq("unpack pos 5 len 5", sub2, "world")

    local sub3 = buffer.unpack(buf, 0, -1)
    assert_eq("unpack pos 0 len -1 (all)", sub3, "helloworld")

    local sub4 = buffer.unpack(buf, 5, -1)
    assert_eq("unpack pos 5 len -1 (rest)", sub4, "world")

    buffer.drop(buf)
end

do
    local buf = buffer.new()
    buffer.write(buf, "abc")

    local sub = buffer.unpack(buf, 0, 0)
    assert_eq("unpack 0 length", sub, "")

    buffer.drop(buf)
end

-- ========================================
section("12. unpack format: i/I (int32/uint32)")
-- ========================================
do
    local p = buffer.concat(string.pack("<i4", -42))
    local val = buffer.unpack(p, "<i")
    assert_eq("unpack little-endian i32", val, -42)
    buffer.drop(p)
end

do
    local p = buffer.concat(string.pack(">i4", -42))
    local val = buffer.unpack(p, ">i")
    assert_eq("unpack big-endian i32", val, -42)
    buffer.drop(p)
end

do
    local p = buffer.concat(string.pack("<I4", 0xDEADBEEF))
    local val = buffer.unpack(p, "<I")
    assert_eq("unpack little-endian u32", val, 0xDEADBEEF)
    buffer.drop(p)
end

do
    local p = buffer.concat(string.pack(">I4", 0xDEADBEEF))
    local val = buffer.unpack(p, ">I")
    assert_eq("unpack big-endian u32", val, 0xDEADBEEF)
    buffer.drop(p)
end

-- ========================================
section("13. unpack format: C (pointer + length)")
-- ========================================
do
    local buf = buffer.new()
    buffer.write(buf, "test data")
    local ptr, len = buffer.unpack(buf, "C")
    assert_eq("unpack C ptr type", type(ptr), "userdata")
    assert_eq("unpack C len", len, 9)
    buffer.drop(buf)
end

-- ========================================
section("14. unpack format error")
-- ========================================
do
    local p = buffer.concat("ab")
    assert_error("unpack invalid format char", function()
        buffer.unpack(p, "X")
    end)
    buffer.drop(p)
end

do
    local p = buffer.concat("a")
    assert_error("unpack h with insufficient data", function()
        buffer.unpack(p, "h")
    end)
    buffer.drop(p)
end

do
    local p = buffer.concat("ab")
    assert_error("unpack I with insufficient data (2 < 4)", function()
        buffer.unpack(p, "I")
    end)
    buffer.drop(p)
end

-- ========================================
section("15. read + write interleaved")
-- ========================================
do
    local buf = buffer.new()
    buffer.write(buf, "aaa")
    local d1 = buffer.read(buf, 2)
    assert_eq("interleave read1", d1, "aa")

    buffer.write(buf, "bbb")
    assert_eq("interleave size", buffer.size(buf), 4)

    local d2 = buffer.read(buf, 4)
    assert_eq("interleave read2", d2, "abbb")

    buffer.drop(buf)
end

-- ========================================
section("16. seek + read interaction")
-- ========================================
do
    local buf = buffer.new()
    buffer.write(buf, "abcdefgh")
    buffer.seek(buf, 3)
    assert_eq("after seek 3 size", buffer.size(buf), 5)

    local data = buffer.read(buf, 5)
    assert_eq("read after seek", data, "defgh")

    buffer.drop(buf)
end

-- ========================================
section("17. large buffer operations")
-- ========================================
do
    local buf = buffer.new(16)
    local chunk = string.rep("x", 100)
    for _ = 1, 100 do
        buffer.write(buf, chunk)
    end
    assert_eq("large write size", buffer.size(buf), 10000)

    local data = buffer.read(buf, 10000)
    assert_eq("large read length", #data, 10000)
    assert_eq("large read content", data, string.rep("x", 10000))
    assert_eq("size after large read", buffer.size(buf), 0)

    buffer.drop(buf)
end

-- ========================================
section("18. clear + reuse")
-- ========================================
do
    local buf = buffer.new()
    buffer.write(buf, "first")
    buffer.clear(buf)
    buffer.write(buf, "second")
    assert_eq("reuse after clear size", buffer.size(buf), 6)
    local data = buffer.read(buf, 6)
    assert_eq("reuse after clear content", data, "second")
    buffer.drop(buf)
end

-- ========================================
section("19. write nil is ignored")
-- ========================================
do
    local buf = buffer.new()
    buffer.write(buf, nil)
    assert_eq("write nil size", buffer.size(buf), 0)

    buffer.write(buf, "a", nil, "b")
    assert_eq("write with nil in middle size", buffer.size(buf), 2)

    buffer.drop(buf)
end

-- ========================================
section("20. multi-format unpack in one call")
-- ========================================
do
    local packed = string.pack("<h<I", 1234, 5678)
    local p = buffer.concat(packed)
    local a, b = buffer.unpack(p, "<h<I")
    assert_eq("multi-format h", a, 1234)
    assert_eq("multi-format I", b, 5678)
    buffer.drop(p)
end

do
    local packed = string.pack(">H>i4", 9999, -1)
    local p = buffer.concat(packed)
    local a, b = buffer.unpack(p, ">H>i")
    assert_eq("multi-format >H", a, 9999)
    assert_eq("multi-format >i", b, -1)
    buffer.drop(p)
end

-- ========================================
section("21. boundary values for integer packing")
-- ========================================
do
    local p = buffer.concat(string.pack("<h", 32767))
    assert_eq("i16 max", buffer.unpack(p, "h"), 32767)
    buffer.drop(p)
end

do
    local p = buffer.concat(string.pack("<h", -32768))
    assert_eq("i16 min", buffer.unpack(p, "h"), -32768)
    buffer.drop(p)
end

do
    local p = buffer.concat(string.pack("<H", 0))
    assert_eq("u16 zero", buffer.unpack(p, "H"), 0)
    buffer.drop(p)
end

do
    local p = buffer.concat(string.pack("<H", 65535))
    assert_eq("u16 max", buffer.unpack(p, "H"), 65535)
    buffer.drop(p)
end

do
    local p = buffer.concat(string.pack("<i4", 0))
    assert_eq("i32 zero", buffer.unpack(p, "i"), 0)
    buffer.drop(p)
end

do
    local p = buffer.concat(string.pack("<i4", 2147483647))
    assert_eq("i32 max", buffer.unpack(p, "i"), 2147483647)
    buffer.drop(p)
end

do
    local p = buffer.concat(string.pack("<i4", -2147483648))
    assert_eq("i32 min", buffer.unpack(p, "i"), -2147483648)
    buffer.drop(p)
end

-- ========================================
section("22. unpack with offset (3rd arg)")
-- ========================================
do
    local p = buffer.concat(string.pack("<h<h", 100, 200))
    local v1 = buffer.unpack(p, "h", 0)
    assert_eq("unpack offset 0", v1, 100)
    local v2 = buffer.unpack(p, "h", 2)
    assert_eq("unpack offset 2", v2, 200)
    buffer.drop(p)
end

do
    local p = buffer.concat("abc")
    assert_error("unpack offset beyond size", function()
        buffer.unpack(p, "h", 100)
    end)
    buffer.drop(p)
end

-- ========================================
-- Summary
-- ========================================
print(string.format("========================================"))
print(string.format("  Total: %d, Passed: %d, Failed: %d", passed + failed, passed, failed))
print(string.format("========================================"))
if failed > 0 then
    print("  RESULT: FAILED")
else
    print("  RESULT: ALL PASSED")
end

moon.exit(failed > 0 and 1 or 0)
