-- Supplemental tests for native helpers and pure-Lua modules.
local moon = require "moon"
moon.loglevel("INFO")
local fs = require "fs"
local excel = require "excel"
local utils = require "utils"
local queue = require "collections.queue"
local mutex = require "moon.mutex"
local task = require "moon.task"
local fixture = assert(os.getenv("MOON_TEST_FIXTURES"))
moon.timeout(15000, function() moon.error("runtime libraries timed out"); moon.exit(1) end)
moon.async(function()
    local ok, err = xpcall(function()
        assert(_VERSION == "Lua 5.5")
        assert(utils.num_cpus() > 0)
        local bytes = "\0\255中文"
        assert(utils.base64_decode(utils.base64_encode(bytes)) == bytes)
        assert(not pcall(utils.base64_decode, "!invalid!"))
        assert(fs.isdir(fixture) and fs.exists(fixture))
        local path = fs.join(fixture, "nested", "hello.txt")
        assert(fs.mkdir(fs.join(fixture, "nested")))
        assert(io.writefile(path, bytes))
        assert(io.readfile(path) == bytes)
        assert(fs.ext(path) == ".txt" and fs.stem(path) == "hello")
        assert(#fs.listdir(fixture, 0, ".txt") == 1)
        assert(fs.remove(fs.join(fixture, "nested")) == 1)
        assert(not fs.exists(path))
        local csv = fs.join(fixture, "sample.csv")
        assert(io.writefile(csv, "id,name\n1,中文\n2,Bob\n"))
        local sheets = assert(excel.read(csv))
        assert(#sheets == 1 and #sheets[1].data == 3)
        assert(sheets[1].data[2][1] == "1" and sheets[1].data[2][2] == "中文")
        assert(#assert(excel.read(csv, 1))[1].data == 1)
        sheets = assert(excel.read(fs.join(fixture, "sample.xlsx")))
        assert(sheets[1].data[2][1] == "中文")
        assert(sheets[1].data[2][2] == 42 and sheets[1].data[2][3] == 1)
        local bad, message = excel.read(fs.join(fixture, "missing.xlsx"))
        assert(bad == false and type(message) == "string")
        local q = queue.new()
        queue.push(q, "a"); queue.push(q, false)
        assert(queue.size(q) == 2 and queue.front(q) == "a")
        assert(queue.pop(q) == "a" and queue.pop(q) == false and queue.pop(q) == nil)
        assert(string.trim("  hello \t") == "hello" and string.split("a,b", ",")[2] == "b")
        assert(math.clamp(15, 0, 10) == 10 and math.cast_int(1.999) == 1)
        assert(type(os.gettimezone()) == "number")
        assert(os.date("!%Y-%m-%d", os.gettime({2025,1,2,12,0,0})) == "2025-01-02")
        local original = {nested={1,2}}
        local copied = table.deepcopy(original)
        copied.nested[1] = 99
        assert(original.nested[1] == 1 and table.contains({1,2},2))
        local Parent = class("ReadmeParent")
        function Parent:ctor(value) self.value = value end
        local Child = class("ReadmeChild", Parent)
        local child = Child.new(42)
        assert(child.value == 42 and iskindof(child, "ReadmeParent"))
        local lock = mutex()
        local value = 0
        local jobs = {}
        for i=1,5 do
            jobs[i] = function()
                local guard <close> = lock()
                local nested <close> = lock()
                assert(lock(true) == 2)
                local current = value
                moon.sleep(1)
                value = current + 1
                return i
            end
        end
        local results = task.wait_all(jobs)
        assert(value == 5 and lock(true) == 0)
        for i=1,5 do assert(results[i][1] == i) end
        assert(#task.wait_all({}) == 0)
        local failed = task.wait_all({function() error("expected task failure") end})
        assert(failed[1][1] == false and failed[1][2]:find("expected task failure"))
        -- Hot reload preserves old upvalues while updating function behavior.
        local hotfix = require "hotfix"
        local name = "validation_hotfix"
        local function loader(source) return assert(load(source, "@validation_hotfix.lua")) end
        local first = loader("local n=0; return {next=function() n=n+1; return n end}")
        local second = loader("local n=0; return {next=function() n=n+2; return n end}")
        local module = hotfix.register(name, first, first())
        assert(module.next() == 1)
        hotfix.addsearcher(function(key) if key == name then return second end end)
        assert(hotfix.update(name))
        assert(module.next() == 3)
        print("RUNTIME LIBRARIES PASSED")
    end, debug.traceback)
    if not ok then moon.error(err) end
    moon.exit(ok and 0 or 1)
end)
