local moon = require "moon"
local hotfix = require "hotfix"
local core = require "hotfix.core"
local current, serial, passed = {}, 0, 0
hotfix.addsearcher(function(name) return current[name] end)

local function compile(name, source, env)
    return assert(load(source, "@" .. name .. ".lua", "t", env or _ENV))
end

local function module(source, env, ...)
    serial = serial + 1
    local name = "hotfix_regression_" .. serial
    local loader = compile(name, source, env)
    return name, hotfix.register(name, loader, loader(...))
end

local function update(name, source, env)
    current[name] = compile(name, source, env)
    return hotfix.update(name)
end

local function test(name, fn)
    fn()
    passed = passed + 1
    print("hotfix passed:", name)
end

local ok, err = xpcall(function()
    test("state, multiple rounds and cached references", function()
        local src = "local n=0;return {next=function()n=n+1;return n end}"
        local name, m = module(src)
        local cached = m.next
        assert(m.next() == 1)
        assert(update(name, src:gsub("n=n%+1", "n=n+2")))
        assert(m.next() == 3)
        assert(update(name, src:gsub("n=n%+1", "n=n+3")))
        assert(m.next() == 6 and cached() == 9 and m.next() == 12)
    end)
    test("cached old functions do not retain retired loaders", function()
        serial = serial + 1
        local name, m = "hotfix_regression_" .. serial
        local loaders, cached = setmetatable({}, { __mode = "v" }), {}
        local function publish(step)
            local loader = compile(name, ('local M={};function M.f()return %d end;return M'):format(step))
            loaders[step] = loader
            if step == 1 then m = hotfix.register(name, loader, loader())
            else current[name] = loader; assert(hotfix.update(name)) end
        end
        for step = 1, 6 do
            publish(step)
            cached[step] = core.resolve(m.f)
        end
        collectgarbage("collect")
        collectgarbage("collect")
        for step = 1, 5 do
            assert(loaders[step] == nil, "retired loader is still retained")
            assert(cached[step]() == 6)
        end
        assert(loaders[6] and m.f() == 6)
    end)
    test("removed factory instances release captured state", function()
        local src = 'local function make(value)return function()return value end end;return {make=make}'
        local name, m = module(src)
        local weak = setmetatable({}, { __mode = "v" })
        do
            local value = {}
            weak[1] = value
            m.instance = m.make(value)
        end
        assert(update(name, src))
        assert(m.instance() == weak[1])
        m.instance = nil
        collectgarbage("collect")
        collectgarbage("collect")
        assert(weak[1] == nil)
    end)
    test("new declarations append after existing siblings", function()
        local name, m = module('local M={};function M.a()return "a1" end;function M.b()return "b1" end;return M')
        assert(update(name, 'local M={};function M.a()return "a2" end;function M.b()return "b2" end;function M.c()return "c2" end;return M'))
        assert(m.a() == "a2" and m.b() == "b2" and m.c() == "c2")
    end)
    test("new capture and function share existing state", function()
        local name, m = module('local n=5;return {a=function()return 0 end,b=function()return n end}')
        assert(update(name, 'local n=0;return {a=function()return n+1 end,b=function()return n end,peek=function()return n end}'))
        assert(m.a() == 6 and m.b() == 5 and m.peek() == 5)
    end)
    test("restoring a removed capture", function()
        local name, m = module('local n=12;return {get=function()return n end}')
        assert(update(name, 'local n=0;return {get=function()return 100 end}'))
        assert(update(name, 'local n=0;return {get=function()return n+1 end}'))
        assert(m.get() == 13)
    end)
    test("missing state is rejected before replacement", function()
        local name, m = module('return {f=function()return 1 end}')
        local old = m.f
        local success, message = update(name, 'local n=2;return {f=function()return n end}')
        assert(not success and message:find("missing state", 1, true))
        assert(m.f == old and m.f() == 1)
        assert(update(name, 'return {f=function()return 3 end}'))
        assert(m.f() == 3)
    end)
    test("environment introduced by replacement", function()
        local name, m = module('return {f=function()return 1 end}')
        assert(update(name, 'return {f=function()return math.abs(-2) end}'))
        assert(m.f() == 2)
    end)
    test("custom environment", function()
        local env = { VALUE = 17 }
        local name, m = module('return {f=function()return 0 end}', env)
        assert(update(name, 'return {f=function()return VALUE end}', env))
        assert(m.f() == 17)
    end)
    test("factory instances keep separate state", function()
        local src = 'local function make(n)return function()n=n+1;return n end end;return {a=make(0),b=make(10)}'
        local name, m = module(src)
        assert(m.a() == 1 and m.b() == 11)
        assert(update(name, src:gsub('n=n%+1', 'n=n+2')))
        assert(m.a() == 3 and m.b() == 13 and m.a ~= m.b)
    end)
    test("factory instances keep separate environments", function()
        local src = 'local function make(_ENV)return function()return VALUE end end;return {a=make({VALUE="A"}),b=make({VALUE="B"})}'
        local name, m = module(src)
        assert(update(name, src:gsub('return VALUE end', 'return VALUE.."2" end')))
        assert(m.a() == "A2" and m.b() == "B2" and m.a ~= m.b)
    end)
    test("local helpers and source names", function()
        local src = 'local n=0;local function inc()n=n+1;return n end;return {next=function()return inc()end}'
        local name, m = module(src)
        assert(m.next() == 1)
        assert(update(name, src:gsub('n=n%+1', 'n=n+2')))
        assert(m.next() == 3)
    end)
    test("new local helper", function()
        local name, m = module('local n=5;return {get=function()return n end}')
        assert(update(name, 'local n=0;local inc;local M={get=function()return inc()end};inc=function()n=n+2;return n end;return M'))
        assert(m.get() == 7)
    end)
    test("helper method tables and new methods", function()
        local name, m = module('local n=1;local helper={};function helper.step()n=n+1;return n end;return {run=function()return helper.step()end}')
        assert(update(name, 'local n=0;local helper={};function helper.step()n=n+2;return n end;local M={run=function()return helper.step()+helper.extra()end};function helper.extra()n=n+3;return n end;return M'))
        assert(m.run() == 9)
    end)
    test("restored helper uses its latest body", function()
        local src = 'local n=1;local function inc()n=n+1;return n end;return {run=function()return inc()end}'
        local name, m = module(src)
        assert(update(name, src:gsub('return inc%(%)end', 'return 100 end')))
        assert(m.run() == 100)
        assert(update(name, src:gsub('n=n%+1', 'n=n+2')))
        assert(m.run() == 3)
    end)
    test("recursive helpers", function()
        local src = 'local function f(n)if n==0 then return 1 end;return f(n-1)+1 end;return {run=function()return f(3)end}'
        local name, m = module(src)
        assert(m.run() == 4)
        assert(update(name, src:gsub('f%(n%-1%)%+1', 'f(n-1)+2')))
        assert(m.run() == 7)
    end)
    test("nested exports preserve table identity and cycles", function()
        local name, m = module('local n=0;local M={nested={f=function()n=n+1;return n end}};M.self=M;return M')
        local nested = m.nested
        assert(update(name, 'local n=0;local M={nested={f=function()n=n+2;return n end}};M.self=M;return M'))
        assert(m.nested == nested and m.self == m and nested.f() == 2)
    end)
    test("mixed data and external functions", function()
        local name, m = module('local M={print=print,value=17};function M.a()return 1 end;return M')
        assert(update(name, 'local M={print=print,value=99};function M.a()return 2 end;return M'))
        assert(m.print == print and m.value == 17 and m.a() == 2)
    end)
    test("colon methods and underscore names", function()
        local name, m = module('local M={value=7};function M:get_value()return self.value end;return M')
        assert(update(name, 'local M={value=0};function M:get_value()return self.value+1 end;function M:new_value()return self.value+2 end;return M'))
        assert(m:get_value() == 8 and m:new_value() == 9)
    end)
    test("static string assignment", function()
        local name, m = module('local M={};M["f"]=function()return 1 end;return M')
        assert(update(name, 'local M={};M["f"]=function()return 2 end;return M'))
        assert(m.f() == 2)
    end)
    test("static function aliases and additions", function()
        local name, m = module('local n=3;local function f()return n end;return {a=f}')
        assert(update(name, 'local n=0;local function f()return n+1 end;return {a=f,b=f}'))
        assert(m.a() == 4 and m.b() == 4 and m.a == m.b)
    end)
    test("module table aliases", function()
        local name, m = module('local M={};local alias=M;function alias.f()return 1 end;return M')
        assert(update(name, 'local M={};local alias=M;function alias.f()return 2 end;return M'))
        assert(m.f() == 2)
    end)
    test("new nested closure executes normal initialization", function()
        local name, m = module('local M={};function M.f()return 1 end;return M')
        assert(update(name, 'local M={};function M.f()local n=10;local function inner()n=n+1;return n end;return inner()end;return M'))
        assert(m.f() == 11)
    end)
    test("module initialization is not rerun", function()
        local tracker = { count = 0 }
        local env = setmetatable({ tracker = tracker }, { __index = _ENV })
        local src = 'tracker.count=tracker.count+1;return {f=function()return 1 end}'
        local name, m = module(src, env)
        assert(update(name, src:gsub('return 1', 'return 2'), env))
        assert(tracker.count == 1 and m.f() == 2)
    end)
    test("removed function is rejected", function()
        local name, m = module('return {a=function()return 1 end,b=function()return 2 end}')
        local success = update(name, 'return {a=function()return 3 end}')
        assert(not success and m.a() == 1 and m.b() == 2)
    end)
    test("existing closures update independently of initialization bindings", function()
        local name, m = module('local M={};function M.a()return 1 end;return M')
        local cached = m.a
        for _, src in ipairs({
            'local M={};local key="a";M[key]=function()return 2 end;return M',
            'local M={};if true then function M.a()return 2 end end;return M',
            'local M={};local alias=M;alias={};function alias.a()return 2 end;return M',
            'local M={};M.a=(true and function()return 2 end);return M',
        }) do
            assert(update(name, src))
            assert(m.a == cached and cached() == 2)
        end
    end)

    test("commit failure restores values and permits retry", function()
        local name, m = module('return {a=function()return 1 end}')
        current[name] = compile(name, 'return {a=function()return 2 end,b=function()return 3 end,c=function()return 4 end}')
        local original, calls = rawset, 0
        rawset = function(target, key, value)
            if target == m then
                calls = calls + 1
                if calls == 2 then error("injected commit failure") end
            end
            return original(target, key, value)
        end
        local success, message = hotfix.update(name)
        rawset = original
        assert(not success and message:find("injected commit failure", 1, true))
        assert(m.a() == 1 and m.b == nil and m.c == nil)
        assert(hotfix.update(name))
        assert(m.a() == 2 and m.b() == 3 and m.c() == 4)
    end)
    test("rollback restores helper targets without rewriting their upvalue cells", function()
        local src = 'local n=0;local function inc()n=n+1;return n end;return {function()return inc()end,function()return 0 end}'
        local name, m = module(src)
        local cached = m[1]
        local helper = select(2, debug.getupvalue(cached, 1))
        local helper_cell = debug.upvalueid(cached, 1)
        current[name] = compile(name, src:gsub('n=n%+1', 'n=n+2'):gsub('return 0 end}', 'return 10 end,function()return 3 end,function()return 4 end}'))
        local original, calls = rawset, 0
        rawset = function(target, key, value)
            if target == m then
                calls = calls + 1
                if calls == 2 then error("injected rollback failure") end
            end
            return original(target, key, value)
        end
        local success = hotfix.update(name)
        rawset = original
        assert(not success and cached() == 1 and m[2]() == 0)
        assert(hotfix.update(name))
        assert(m[1]() == 3 and m[2]() == 10)
        assert(debug.upvalueid(cached, 1) == helper_cell and select(2, debug.getupvalue(cached, 1)) == helper)
    end)
    test("register an already loaded module", function()
        local name = "hotfix_existing_registration"
        local loader = compile(name, 'return {f=function()return 1 end}')
        local m = loader()
        package.loaded[name] = m
        assert(hotfix.register(name, loader, m) == m)
        assert(update(name, 'return {f=function()return 2 end}'))
        assert(m.f() == 2)
    end)
    test("reentrant updates are rejected without unlocking the outer update", function()
        local name, m = module('return {f=function()return 1 end}')
        hotfix.addsearcher(function(key)
            if key == name then
                local success, message = hotfix.update(name)
                assert(not success and message:find("already updating", 1, true))
                return compile(name, 'return {f=function()return 2 end}')
            end
        end)
        assert(hotfix.update(name) and m.f() == 2)
    end)
    test("prototype inspection and cloning accept checked paths", function()
        local loader = compile("hotfix_paths", 'local function make()local function inner()return 7 end;return inner end;return {make=make}')
        local child = string.pack("<I4", 1)
        local nested = child .. child
        assert(core.proto(loader, "") == core.proto(loader))
        assert(core.proto(loader, child) == core.proto(loader, 1))
        assert(core.proto(loader, "1") == core.proto(loader, 1))
        assert(core.clone(loader, nested)() == 7)
        assert(core.proto(math.abs) == nil and core.proto(loader, -1) == nil)
        for _, path in ipairs({ "x", string.pack("<I4", 0), string.pack("<I4", 0xffffffff), nested .. child }) do
            for _, select in ipairs({ core.proto, core.info, core.clone }) do
                assert(not pcall(select, loader, path))
            end
        end
    end)
    test("prototype information and binding checks survive reader GC", function()
        local loader = compile("hotfix_metadata", 'local n=7;local M={};local alias=M;function alias.f()local function inner()return n end;return inner()end;return M')
        local dumped, offset = string.dump(loader), 1
        local loaded = assert(load(function()
            collectgarbage("collect")
            if offset > #dumped then return nil end
            local chunk = dumped:sub(offset, offset + 16)
            offset = offset + #chunk
            return chunk
        end))
        assert(hotfix.check_order(loader, loaded))
        local function compare(path)
            local a, b = core.info(loader, path), core.info(loaded, path)
            assert(a.children == b.children)
            assert(#a.locals == #b.locals and #a.upvalues == #b.upvalues)
            for i, name in ipairs(a.locals) do
                assert(name == b.locals[i] and a.localscope[i] == b.localscope[i])
            end
            for i, capture in ipairs(a.upvalues) do
                for field, value in pairs(capture) do assert(value == b.upvalues[i][field]) end
            end
            for i = 1, a.children do compare(path .. string.pack("<I4", i)) end
        end
        compare("")
        assert(loaded().f() == 7)
    end)
    test("bindings recovered from official bytecode and GC", function()
        local name = "hotfix_bytecode"
        local src = 'local n=7;local M={};function M.f()return n end;return M'
        local loader = assert(load(string.dump(compile(name, src))))
        assert(string.dump(loader):byte(6) == 0 and string.dump(loader, true):byte(6) == 0)
        local m = hotfix.register(name, loader, loader())
        local replacement = src:gsub('return n end', 'return n+1 end')
            :gsub('return M$', 'function M.extra()return n+10 end;return M')
        current[name] = assert(load(string.dump(compile(name, replacement))))
        collectgarbage("collect")
        assert(hotfix.update(name))
        collectgarbage("collect")
        assert(m.f() == 8 and m.extra() == 17)
        current[name] = assert(load(string.dump(compile(name, src), true)))
        assert(not hotfix.update(name))
        assert(m.f() == 8)
    end)
    test("default searcher bypasses bytecode cache", function()
        local prefix, name = os.tmpname(), "hotfix_disk_fixture"
        os.remove(prefix)
        local path, oldpath = prefix .. "_" .. name .. ".lua", package.path
        package.path = prefix .. "_?.lua;" .. oldpath
        local function save(source)
            local f = assert(io.open(path, "wb")); f:write(source); f:close()
        end
        save('local n=0;return {next=function()n=n+1;return n end}')
        assert(loadfile(path)) -- deliberately populate Moon's old code cache
        local m = hotfix.require(name)
        assert(m.next() == 1)
        save('local n=0;return {next=function()n=n+2;return n end}')
        local success, message = hotfix.update(name)
        package.path = oldpath
        os.remove(path)
        assert(success, message)
        assert(m.next() == 3)
    end)
    test("native redirection shares one target across generations", function()
        local a = assert(load('return function()return 1 end'))()
        local b = assert(load('return function()return 2 end'))()
        local c = assert(load('return function()return 3 end'))()
        assert(core.resolve(a) == a and core.resolve(math.abs) == math.abs)
        core.redirect(a, b)
        core.redirect(b, c)
        assert(a() == 3 and b() == 3 and c() == 3)
        assert(core.resolve(a) == c and core.resolve(b) == c)
        core.redirect(a, a) -- restore the original body without a chain
        assert(a() == 1 and b() == 1 and c() == 1)
        local other = function() return 4 end
        assert(not pcall(core.redirect, other, b))
        assert(other() == 4 and a() == 1)
        assert(not pcall(core.redirect, a, math.abs))
        assert(not pcall(core.redirect, math.abs, a))
        assert(not pcall(core.resolve, {}))
    end)
    test("preserves identity and all cached generations", function()
        local src = 'local n=0;return {next=function()n=n+1;return n end}'
        local name, m = module(src)
        local first = m.next
        local keys = { [first] = true }
        assert(first() == 1)
        assert(update(name, src:gsub('n=n%+1', 'n=n+2')))
        local second = core.resolve(first)
        assert(m.next == first and second ~= first and first() == 3)
        assert(update(name, src:gsub('n=n%+1', 'n=n+3')))
        assert(m.next == first and keys[m.next] and first() == 6 and second() == 9 and m.next() == 12)
        assert(core.resolve(second) == core.resolve(first))
    end)
    test("collects captures from the latest body", function()
        local name, m = module('local n=10;return {run=function()return 0 end,peek=function()return n end}')
        local cached = m.run
        assert(update(name, 'local n=0;return {run=function()n=n+1;return n end,peek=function()return n end}'))
        assert(cached() == 11)
        assert(update(name, 'local n=0;return {run=function()n=n+2;return n end,peek=function()return n end}'))
        assert(cached() == 13 and m.peek() == 13)
    end)
    test("keeps factory instances and external callbacks separate", function()
        local src = 'local function make(n)return function()n=n+1;return n end end;return {a=make(0),b=make(10)}'
        local name, m = module(src)
        local a, b = m.a, m.b
        m.a = nil -- this registered instance is now held only by its caller
        assert(update(name, src:gsub('n=n%+1', 'n=n+2')))
        assert(a() == 2 and b() == 12 and m.b == b)
        a = core.resolve(a) -- only an intermediate generation remains externally held
        collectgarbage("collect")
        assert(update(name, src:gsub('n=n%+1', 'n=n+3')))
        assert(a() == 5 and b() == 15)
    end)
    test("updates helper bodies and preserves aliases", function()
        local src = 'local n=0;local function helper()n=n+1;return n end;local function run()return helper()end;return {a=run}'
        local name, m = module(src)
        local cached = m.a
        assert(cached() == 1)
        assert(update(name, src:gsub('n=n%+1', 'n=n+10'):gsub('{a=run}', '{b=run,a=run}')))
        assert(cached() == 11 and m.a == cached and m.b == cached)
        assert(update(name, src:gsub('n=n%+1', 'n=n+100'):gsub('{a=run}', '{b=run,a=run}')))
        assert(m.b() == 111)
    end)
    test("binds recursive helpers and separate environments", function()
        local src = 'local function f(n)if n==0 then return 1 end;return f(n-1)+1 end;return {run=function()return f(3)end}'
        local name, m = module(src)
        local cached = m.run
        assert(update(name, src:gsub('f%(n%-1%)%+1', 'f(n-1)+2')))
        assert(cached() == 7)
        local envsrc = 'local function make(_ENV)return function()return VALUE end end;return {a=make({VALUE="A"}),b=make({VALUE="B"})}'
        name, m = module(envsrc)
        local a, b = m.a, m.b
        assert(update(name, envsrc:gsub('return VALUE end', 'return VALUE.."2" end')))
        assert(a() == "A2" and b() == "B2")
    end)
    test("distinguishes same-named state in lexical owners", function()
        local src = 'local function make_a()local count=1;return function()return count end end;local function make_b()local count=100;return function()return count end end;return {a=make_a(),b=make_b()}'
        local name, m = module(src)
        local a, b = m.a, m.b
        local revised = src:gsub('return count end end;local function make_b', 'return count+10 end end;local function make_b')
        revised = revised:gsub('return count end end;return', 'return count+20 end end;return')
        assert(update(name, revised))
        assert(a() == 11 and b() == 120)
    end)
    test("leaves suspended frames on their original bodies", function()
        local src = 'local step=0;return {run=function()step=step+1;coroutine.yield("old",step);step=step+1;return "old",step end}'
        local name, m = module(src)
        local cached = m.run
        local co = coroutine.create(cached)
        local success, version, step = coroutine.resume(co)
        assert(success and version == "old" and step == 1)
        assert(update(name, src:gsub('step=step%+1', 'step=step+10'):gsub('"old"', '"new"')))
        success, version, step = coroutine.resume(co)
        assert(success and version == "old" and step == 2 and coroutine.status(co) == "dead")
        co = coroutine.create(cached)
        success, version, step = coroutine.resume(co)
        assert(success and version == "new" and step == 12)
        success, version, step = coroutine.resume(co)
        assert(success and version == "new" and step == 22 and coroutine.status(co) == "dead")
    end)
    test("covers normal tail protected and metamethod calls", function()
        local name, m = module('return {f=function(...)return "old",... end}')
        local cached = m.f
        local function tail(...) return cached(...) end
        local function normal(...) local result = table.pack(cached(...)); return result end
        local callable = setmetatable({}, { __call = cached })
        assert(update(name, 'return {f=function(...)local a,b,c,d,e,f=1,2,3,4,5,6;return "new",... end}'))
        local result = normal(7, nil, 9)
        assert(result.n == 4 and result[1] == "new" and result[2] == 7 and result[3] == nil and result[4] == 9)
        local version, value = tail(11)
        assert(version == "new" and value == 11)
        local success
        success, version, value = pcall(cached, 12)
        assert(success and version == "new" and value == 12)
        local self
        version, self, value = callable(13)
        assert(version == "new" and self == callable and value == 13)
    end)
    test("redirect rollback restores both initial and existing target cells", function()
        local name, m = module('return {a=function()return 1 end}')
        local cached = m.a
        for step = 2, 3 do
            local previous = core.resolve(cached)
            local fields = {}
            for i = 1, step * 2 - 1 do
                fields[#fields + 1] = ('%s=function()return %d end'):format(string.char(96 + i), step)
            end
            current[name] = compile(name, 'return {' .. table.concat(fields, ',') .. '}')
            local original, calls = rawset, 0
            rawset = function(target, key, value)
                if target == m then
                    calls = calls + 1
                    if calls == 2 then error("injected redirect commit failure") end
                end
                return original(target, key, value)
            end
            local success, message = hotfix.update(name)
            rawset = original
            assert(not success and message:find("injected redirect commit failure", 1, true))
            assert(core.resolve(cached) == previous and cached() == step - 1 and m.a == cached)
            if step == 2 then assert(m.b == nil) else assert(m.b() == step - 1) end
            assert(hotfix.update(name))
            assert(cached() == step and m.b() == step)
        end
    end)
    test("redirect preparation failure leaves cached functions unchanged", function()
        local name, m = module('return {f=function()return 1 end}')
        local cached = m.f
        local success, message = update(name, 'local n=10;return {f=function()return n end}')
        assert(not success and message:find("missing state", 1, true))
        assert(core.resolve(cached) == cached and m.f == cached and cached() == 1)
        assert(update(name, 'return {f=function()return 2 end}'))
        assert(cached() == 2)
    end)
    test("redirection is runtime state and is not copied into bytecode or clones", function()
        local old = assert(load('return function()return 1 end'))()
        local new = assert(load('return function()return 2 end'))()
        local original = core.proto(old)
        core.redirect(old, new)
        assert(old() == 2 and core.proto(old) == original)
        local loaded = assert(load(string.dump(old)))
        local cloned = core.clone(old)
        assert(loaded() == 1 and cloned() == 1)
        assert(core.resolve(loaded) == loaded and core.resolve(cloned) == cloned)
    end)
    test("redirection targets survive GC and retired targets are collectible", function()
        for _, mode in ipairs({ "incremental", "generational" }) do
            collectgarbage(mode)
            local weak = setmetatable({}, { __mode = "v" })
            do
                local old = assert(load('return function()return 1 end'))()
                do
                    local target = assert(load('return function()return 2 end'))()
                    weak[1] = target
                    core.redirect(old, target)
                end
                collectgarbage("collect")
                assert(weak[1] and old() == 2)
                local latest = assert(load('return function()return 3 end'))()
                core.redirect(old, latest)
                collectgarbage("collect")
                collectgarbage("collect")
                assert(weak[1] == nil and old() == 3)
                weak[2], weak[3] = old, latest
            end
            collectgarbage("collect")
            collectgarbage("collect")
            assert(weak[2] == nil and weak[3] == nil)
        end
    end)
    test("releases removed factory instances", function()
        local src = 'local function make(value)return function()return value end end;return {make=make}'
        local name, m = module(src)
        local weak = setmetatable({}, { __mode = "v" })
        do
            local value = {}
            weak[1] = value
            m.instance = m.make(value)
        end
        assert(update(name, src))
        assert(m.instance() == weak[1])
        m.instance = nil
        collectgarbage("collect")
        collectgarbage("collect")
        assert(weak[1] == nil)
    end)
    test("require always redirects cached references", function()
        local name = "hotfix_redirect_require"
        current[name] = compile(name, 'return {f=function()return 1 end}')
        local m = hotfix.require(name)
        local cached = m.f
        assert(update(name, 'return {f=function()return 2 end}'))
        assert(cached() == 2 and m.f == cached)
        assert(hotfix.require(name) == m)
    end)
    test("existing function references require no table writes", function()
        local src = 'local n=0;local function helper()n=n+1;return n end;return {a=function()return helper()end}'
        local name, m = module(src)
        local cached = m.a
        current[name] = compile(name, src:gsub('n=n%+1', 'n=n+2'))
        local original = rawset
        rawset = function() error("unexpected table write") end
        local success, message = hotfix.update(name)
        rawset = original
        assert(success, message)
        assert(m.a == cached and cached() == 2)
    end)
    test("ordinary branch assignments are not function aliases", function()
        local src = [[local M={};function M.f(flag)
            local data={safe=1,unsafe=2};local value
            if flag then value=data.safe else value=data.unsafe end
            return value end;return M]]
        local name, m = module(src)
        assert(update(name, src:gsub('return value end', 'return value+10 end')))
        assert(m.f(true) == 11 and m.f(false) == 12)
    end)
    test("top-level data loops do not invalidate module bindings", function()
        local src = 'local values={};for _,kind in ipairs({1,2})do values[kind]=kind end;return {f=function()return values[1]end}'
        local name, m = module(src)
        assert(update(name, src:gsub('return values%[1%]', 'return values[1]+10 ')))
        assert(m.f() == 11)
    end)
    test("reassigned data exports do not become ambiguous function aliases", function()
        local src = 'local a=1;local b=2;local M={};M.value=a;if true then M.value=b end;function M.f()return M.value end;return M'
        local name, m = module(src)
        assert(update(name, src:gsub('b=2', 'b=100'):gsub('return M.value end', 'return M.value+10 end')))
        assert(m.value == 2 and m.f() == 12)
    end)
    test("changed initialization assignments do not replace existing exports", function()
        local src = 'local M={};local function f()return 1 end;M.a=f;return M'
        local name, m = module(src)
        local cached = m.a
        assert(update(name, 'local M={};local function f()return 2 end;if true then M.a=f end;return M'))
        assert(m.a == cached and cached() == 2)
    end)

    test("unused cyclic bindings do not affect prototype matching", function()
        for _, src in ipairs({
            'local A=A.next;function A.f()return 1 end;return A',
            'local A=X[A.Key];function A.f()return 1 end;return A',
        }) do
            local loader = compile('cyclic_alias', src)
            local mapping, errors = hotfix.diff(loader, loader)
            assert(not errors and next(mapping))
        end
    end)

    test("conditional helpers are created normally by updated functions", function()
        local src = 'local M={};function M.f(flag)if flag then local function helper()return 1 end;return helper()end;return 0 end;return M'
        local name, m = module(src)
        local cached = m.f
        assert(update(name, src:gsub('return 1 end', 'return 2 end')))
        assert(cached(true) == 2 and cached(false) == 0)
    end)
    test("do blocks group functions without making their bindings conditional", function()
        local src = 'local M={};do local n=3;function M.f()return n+1 end end;return M'
        local name, m = module(src)
        local cached = m.f
        assert(update(name, src:gsub('n%+1', 'n+2')))
        assert(cached() == 5)
        assert(update(name, 'local M={};if true then do local n=3;function M.f()return n+10 end end end;return M'))
        assert(cached() == 13)
    end)
    test("live conditional closures match their prototype paths", function()
        local src = 'local M={};function M.make()if true then local function helper()return 1 end;return helper end end;return M'
        local name, m = module(src)
        m.saved = m.make()
        assert(update(name, src:gsub('return 1 end', 'return 2 end')))
        assert(m.saved() == 2 and m.make()() == 2)
    end)

    test("enum-key handlers update without retaining their initialization enum", function()
        local src = [[local Kind={Push=1,Subscribe=2};local n=0;local handlers={}
            handlers[Kind.Push]=function()n=n+1;return n end
            handlers[Kind.Subscribe]=function()n=n+10;return n end
            return {run=function(k)return handlers[k]()end}]]
        local name, m = module(src)
        local cached = m.run
        assert(cached(1) == 1)
        assert(update(name, src:gsub('n=n%+1;', 'n=n+2;')))
        assert(cached(1) == 3 and cached(2) == 13)
        assert(update(name, src:gsub('n=n%+1;', 'n=n+3;')))
        assert(cached(1) == 16)
    end)
    test("enum-key constructors preserve nested handler tables", function()
        local src = 'local Kind={Push=1};local handlers={[Kind.Push]={check=function()return 1 end}};return {run=function()return handlers[1].check()end}'
        local name, m = module(src)
        assert(update(name, src:gsub('return 1 end', 'return 2 end')))
        assert(m.run() == 2)
    end)
    test("new enum handlers use captured keys without reinitializing state", function()
        local src = 'local Kind={Push=1,Subscribe=2};local n=3;local handlers={};handlers[Kind.Push]=function()return n end;return {run=function()return handlers[Kind.Push]()end}'
        local name, m = module(src)
        local replacement = src:gsub('local n=3', 'local n=100'):gsub('return {run=function%(%)return handlers%[Kind.Push%]%(%)end}', 'local M={run=function()return handlers[Kind.Push]()end};handlers[Kind.Subscribe]=function()return n+10 end;function M.extra()return handlers[Kind.Subscribe]()end;return M')
        assert(update(name, replacement))
        assert(m.run() == 3 and m.extra() == 13)
    end)
    test("cached enum members have the same binding as their qualified origin", function()
        local src = 'local constant={Kind={Push=1}};local Push=constant.Kind.Push;local handlers={};handlers[Push]=function()return 1 end;return {run=function()return handlers[Push]()end}'
        local name, m = module(src)
        local replacement = src:gsub('handlers%[Push%]=', 'handlers[constant.Kind.Push]='):gsub('return 1 end', 'return 2 end')
        assert(update(name, replacement))
        assert(m.run() == 2)
    end)
    test("cached enum tables resolve new keys through their original dependency", function()
        local src = 'local constant={Kind={Push=1,Subscribe=2}};local Kind=constant.Kind;local handlers={};handlers[Kind.Push]=function()return 1 end;return {run=function()return handlers[Kind.Push]()end}'
        local name, m = module(src)
        local replacement = src:gsub('return {run=function%(%)return handlers%[Kind.Push%]%(%)end}', 'local M={run=function()return handlers[Kind.Push]()end};handlers[Kind.Subscribe]=function()return 2 end;function M.extra()return handlers[Kind.Subscribe]()end;return M')
        assert(update(name, replacement))
        assert(m.run() == 1 and m.extra() == 2)
    end)
    test("unrelated data aliases cannot invalidate enum handler bindings", function()
        local src = 'local context=...;local Kind={Push=1};local key="x";local unused=context[key];local handlers={};handlers[Kind.Push]=function()return 1 end;return {run=function()return handlers[1]()end}'
        local name, m = module(src, nil, {x=7})
        assert(update(name, src:gsub('return 1 end', 'return 2 end')))
        assert(m.run() == 2)
    end)
    test("new enum bindings require an available key and do not guess values", function()
        local src = 'local Kind={Push=1,Subscribe=2};local handlers={};handlers[Kind.Push]=function()return 1 end;return {run=function(k)return handlers[k]()end}'
        local name, m = module(src)
        local replacement = src:gsub('return {run=function%(k%)return handlers%[k%]%(%)end}', 'local M={run=function(k)return handlers[k]()end};handlers[Kind.Subscribe]=function()return 2 end;return M')
        local success, message = update(name, replacement)
        assert(not success and message:find('missing enum key state', 1, true))
        assert(m.run(1) == 1 and not pcall(m.run, 2))
        assert(update(name, src:gsub('handlers%[Kind.Push%]=', 'handlers[Kind.Subscribe]=')))
        assert(m.run(1) == 1 and not pcall(m.run, 2))
    end)
    test("enum key resolution does not invoke metamethods", function()
        local calls = 0
        local kind = setmetatable({ Push = 1 }, { __index = function() calls = calls + 1; return 2 end })
        local env = setmetatable({ Kind = kind }, { __index = _ENV })
        local src = 'local handlers={};handlers[Kind.Push]=function()return 1 end;return {run=function()return handlers[Kind.Push]()end}'
        local name, m = module(src, env)
        local replacement = src:gsub('return {run=function%(%)return handlers%[Kind.Push%]%(%)end}', 'local M={run=function()return handlers[Kind.Push]()end};handlers[Kind.Subscribe]=function()return 2 end;return M')
        assert(not update(name, replacement, env))
        assert(calls == 0 and m.run() == 1)
    end)
    test("existing enum updates do not resolve initialization keys", function()
        local constant = { Kind = { Push = 1 } }
        local env = setmetatable({ CONSTANT = constant }, { __index = _ENV })
        local src = 'local constant=CONSTANT;local Kind=constant.Kind;local handlers={};handlers[Kind.Push]=function()return 1 end;return {run=function()return handlers[Kind.Push]()end,dependency=function()return constant end}'
        local name, m = module(src, env)
        constant.Kind = { Push = 2 }
        assert(update(name, src:gsub('return 1 end', 'return 2 end'), env))
        assert(m.run() == 2)
    end)
    test("enum bindings survive bytecode roundtrips", function()
        local src = 'local Kind={Push=1};local handlers={[Kind.Push]=function()return 1 end};return {run=function()return handlers[1]()end}'
        local name, m = module(src)
        local replacement = compile(name, src:gsub('return 1 end', 'return 2 end'))
        current[name] = assert(load(string.dump(replacement), '@enum_roundtrip', 'b'))
        assert(hotfix.update(name))
        assert(m.run() == 2)
    end)
    test("context loaders and cross-module cached commands preserve state", function()
        local context = { logics = {}, initialized = 0, dirty = 0 }
        local src = 'local context=...;context.initialized=context.initialized+1;return {MarkDirty=function()context.dirty=context.dirty+1;return context.dirty end}'
        local name, user = module(src, nil, context)
        context.logics.User = user
        local _, handler = module('local context=...;local MarkDirty=context.logics.User.MarkDirty;return {save=function()return MarkDirty()end}', nil, context)
        local command = { save = handler.save }
        assert(command.save() == 1)
        assert(update(name, src:gsub('dirty%+1', 'dirty+2')))
        assert(context.initialized == 1 and command.save() == 3 and user.MarkDirty() == 5)
    end)
    test("loop branches retain independent captured state", function()
        local src = 'local H={};for k=1,2 do local n=k*10;if k==1 then H[k]=function()n=n+1;return n end else H[k]=function()n=n+2;return n end end end;return {run=function(k)return H[k]()end}'
        local name, m = module(src)
        local cached = m.run
        assert(cached(1) == 11 and cached(2) == 22)
        assert(update(name, src:gsub('n=n%+1', 'n=n+3'):gsub('n=n%+2', 'n=n+5')))
        assert(cached(1) == 14 and cached(2) == 27)
    end)
    test("discarded loop instances release block-local captured state", function()
        local src = 'local H={};for k=1,2 do local state={n=k};H[k]=function()return state end end;return {get=function(k)return H[k]()end,drop=function(k)H[k]=nil end}'
        local name, m = module(src)
        local weak = setmetatable({ m.get(1) }, { __mode = "v" })
        assert(update(name, src))
        m.drop(1)
        collectgarbage("collect"); collectgarbage("collect")
        assert(weak[1] == nil and m.get(2).n == 2)
    end)
    test("computed handler keys are not reevaluated during body updates", function()
        local calls = 0
        local env = setmetatable({ KEY = function(n) calls = calls + 1; return n * 10 end }, { __index = _ENV })
        local src = 'local H={};H[KEY(1)]=function()return 1 end;return {run=function()return H[10]()end}'
        local name, m = module(src, env)
        assert(update(name, src:gsub('return 1 end', 'return 2 end'), env))
        assert(calls == 1 and m.run() == 2)
    end)
    test("literal string keys and appended exports preserve arbitrary bytes", function()
        local src = 'local M={};M["user.info"]=function()return 1 end;return M'
        local name, m = module(src)
        local lines = { 'local M={};M["user.info"]=function()return 2 end;' }
        local keys = { "new.key", "", "a]b", "a\0b", "你好" }
        for i, key in ipairs(keys) do lines[#lines + 1] = ('M[%q]=function()return %d end;'):format(key, i + 2) end
        lines[#lines + 1] = 'return M'
        assert(update(name, table.concat(lines)))
        assert(m["user.info"]() == 2)
        for i, key in ipairs(keys) do assert(m[key]() == i + 2) end
    end)
    test("array constructors add functions across SETLIST batches", function()
        local name, m = module('return {function()return 1 end}')
        local cached, fields = m[1], {}
        for i = 1, 70 do fields[i] = ('function()return %d end'):format(i + 100) end
        assert(update(name, 'return {' .. table.concat(fields, ',') .. '}'))
        assert(cached() == 101 and m[1] == cached and m[70]() == 170)
    end)
    test("branch-selected export keys are not treated as static bindings", function()
        local reads = 0
        local env = setmetatable({}, { __index = function(_, key)
            reads = reads + 1
            if key == "FLAG" then return true end
            return _ENV[key]
        end })
        local src = 'local Kind={A=1,B=2};local M={};function M.get()return Kind.A end;return M'
        local name, m = module(src, env)
        local replacement = src:gsub('return M$', 'M[FLAG and Kind.A or Kind.B]=function()return 9 end;return M')
        local success, message = update(name, replacement, env)
        assert(not success and message:find('static export binding', 1, true))
        assert(reads == 0 and m[1] == nil and m[2] == nil and m.get() == 1)
    end)
    test("shadowed module names cannot add fields to the wrong table", function()
        local src = 'local M={};function M.get()return 1 end;return M'
        local name, m = module(src)
        local replacement = 'local M={};function M.get()return 2 end;do local M={};function M.extra()return 9 end end;return M'
        local success, message = update(name, replacement)
        assert(not success and message:find('static export binding', 1, true))
        assert(m.extra == nil and m.get() == 1)
    end)
    test("aliases do not follow later table reassignments", function()
        local src = 'local M={};local alias=M;M={};function M.get()return alias end;return M'
        local name, m = module(src)
        local alias = m.get()
        local replacement = src:gsub('return M$', 'function alias.extra()return 9 end;return M')
        local success = update(name, replacement)
        assert(not success and m.extra == nil and alias.extra == nil and m.get() == alias)
    end)
    test("discarded table declarations cannot become new module fields", function()
        local src = 'local M={};function M.unused()return 1 end;M={};return M'
        local name, m = module(src)
        local replacement = 'local M={};function M.unused()return 2 end;function M.extra()return 9 end;M={};return M'
        assert(not update(name, replacement))
        assert(next(m) == nil)
    end)
    test("empty loops cannot hide conditional export bindings", function()
        local src = 'local M={};function M.get()return 1 end;return M'
        local name, m = module(src)
        local replacement = 'local M={};function M.get()return 2 end;while true do end;if FLAG then function M.extra()return 9 end end;return M'
        local success, message = update(name, replacement)
        assert(not success and message:find('static export binding', 1, true))
        assert(m.get() == 1 and m.extra == nil)
    end)
    test("captured metatable callbacks execute their updated bodies", function()
        local src = 'local n=0;local cache=setmetatable({},{__index=function()n=n+1;return n end});return {read=function()return cache.missing end}'
        local name, m = module(src)
        local cached = m.read
        assert(cached() == 1)
        assert(update(name, src:gsub('n=n%+1', 'n=n+10')))
        assert(cached() == 11)
    end)
    test("anonymous callbacks passed to factories remain discoverable", function()
        local src = 'local function make(f)return setmetatable({},{__index=f})end;local cache=make(function()return 1 end);return {read=function()return cache.missing end}'
        local name, m = module(src)
        assert(update(name, src:gsub('return 1 end', 'return 2 end')))
        assert(m.read() == 2)
    end)
    test("finalizers retain identity while their bodies update", function()
        local tracker = { count = 0 }
        local env = setmetatable({ TRACK = tracker }, { __index = _ENV })
        local src = 'local value=setmetatable({},{__gc=function()TRACK.count=TRACK.count+1 end});return {keep=function()return value end}'
        local name, m = module(src, env)
        local value = m.keep()
        local cached = debug.getmetatable(value).__gc
        cached(value)
        assert(update(name, src:gsub('count%+1', 'count+10'), env))
        cached(value)
        assert(tracker.count == 11 and debug.getmetatable(value).__gc == cached)
    end)
    test("appended computed-key exports require explicit binding metadata", function()
        local calls = 0
        local env = setmetatable({ KEY = function(n) calls = calls + 1; return n end }, { __index = _ENV })
        local src = 'local H={};H[KEY(1)]=function()return 1 end;local M={run=function()return H[1]()end};return M'
        local name, m = module(src, env)
        local success, message = update(name, src:gsub('return M$', 'H[KEY(2)]=function()return 2 end;return M'), env)
        assert(not success and message:find('static export binding', 1, true))
        assert(calls == 1 and m.run() == 1)
    end)
    test("offline order checks detect reordering insertion and deletion", function()
        local check = hotfix.check_order
        local old = compile('order', 'local M={};function M.a()return 1 end;function M.b()return 2 end;return M')
        local valid = compile('order', 'local M={};function M.a()return 3 end;function M.b()return 4 end;function M.c()return 5 end;return M')
        assert(check(old, valid))
        for _, src in ipairs({
            'local M={};function M.b()return 3 end;function M.a()return 4 end;return M',
            'local M={};function M.a()return 3 end;function M.c()return 5 end;function M.b()return 4 end;return M',
            'local M={};function M.a()return 3 end;return M',
        }) do
            local ok, errors = check(old, compile('order', src))
            assert(not ok and #errors > 0)
        end
        local nested = 'local M={};function M.make()local function first()end;local function second()end;return first,second end;return M'
        local reordered = nested:gsub('function first%(%)end;local function second%(%)end',
            'function second()end;local function first()end')
        local ok, errors = check(compile('order', nested), compile('order', reordered))
        assert(not ok and #errors > 0)
    end)
    test("offline order checks require review for anonymous callbacks", function()
        local check = hotfix.check_order
        local src = 'local function wrap(f)return f end;return {f=wrap(function()return 1 end)}'
        local ok, errors, review = check(compile('order', src), compile('order', src:gsub('return 1 end', 'return 2 end')))
        assert(not ok and #errors == 0 and #review > 0)
    end)
end, debug.traceback)
if not ok then print(err) end
print("HOTFIX REGRESSIONS:", passed)
moon.exit(ok and 0 or 1)
