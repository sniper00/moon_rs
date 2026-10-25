-- Update Lua functions in declaration order without rerunning initialization.
-- Existing module tables and captured state survive. Cached references execute
-- the latest target on their next call; active frames keep their old bodies.
local core = require "hotfix.core"
local getupvalue, upvalueid = debug.getupvalue, debug.upvalueid
local upvaluejoin, setupvalue = debug.upvaluejoin, debug.setupvalue
local proto, clone = core.proto, core.clone
local resolve, redirect = core.resolve, core.redirect
local hotfix = {}
local modules, searchers = {}, {}

-- Recover static bindings from compiler output, without running module code.
-- Lua-version details are confined to this reader and core.code. Module
-- source, custom load() calls and official bytecode all use the same path.
local op, modes = core.opcodes, core.opmodes

local function atom(value)
    if value.path and not value.path:find("[", 1, true) then
        return "[=" .. value.path .. "]"
    end
    local key = value.literal
    if type(key) == "string" then
        if key:match("^[%a_][%w_]*$") then return key end
        return "[#" .. key:gsub(".", function(c) return ("%02x"):format(c:byte()) end) .. "]"
    elseif type(key) == "number" and math.type(key) == "integer" then
        return "[" .. key .. "]"
    end
end

local function field(base, key)
    local suffix = atom(key)
    if base and suffix then return base == "" and suffix or base .. "." .. suffix end
end

local function analyze(data, children, code, constants, ranges, maxstack)
    local instructions, conditional, boundaries, coverage = {}, {}, {}, {}
    for pc = 0, #code // 4 - 1 do instructions[pc] = string.unpack("=I4", code, pc * 4 + 1) end
    local count = #code // 4
    for pc = 0, count - 1 do
        local word = instructions[pc]
        local opcode, bx = word & 127, word >> 15
        local target
        if opcode == op.JMP then target = pc + 1 + (word >> 7) - 0xffffff
        elseif opcode == op.FORPREP then target = pc + bx + 2
        elseif opcode == op.TFORPREP then target = pc + bx + 1
        elseif opcode == op.FORLOOP or opcode == op.TFORLOOP then target = pc + 1 - bx end
        if target then
            local first, last = math.max(0, math.min(pc, target)), math.min(count, math.max(pc, target))
            if first < last then
                coverage[first], coverage[last] = (coverage[first] or 0) + 1, (coverage[last] or 0) - 1
            end
        end
    end
    local depth = 0
    for pc = 0, count - 1 do depth = depth + (coverage[pc] or 0); conditional[pc] = depth > 0 end
    for _, position in ipairs(ranges) do boundaries[position] = true end
    local registers, active, links, dependents = {}, {}, {}, {}
    local declarations = {}
    for _, name in ipairs(data.locals) do declarations[name] = (declarations[name] or 0) + 1 end
    for name, occurrences in pairs(declarations) do
        if occurrences > 1 then links[name] = "?" end
    end
    local unknown = {}
    local function literal(value) return { literal = value } end
    local function read(register)
        local id = active[register]
        if id then return { path = data.locals[id] } end
        return registers[register] or unknown
    end
    local function link(path, source, uncertain)
        if uncertain then source = "?" end
        if source == path then return end
        if source then
            if links[path] and links[path] ~= source then source = "?" end
            links[path] = source
        elseif links[path] then links[path] = "?" end
    end
    local function depends(binding, path)
        return binding == path or binding:sub(1, #path + 1) == path .. "."
            or binding:find("[=" .. path .. "]", 1, true)
            or binding:find("[=" .. path .. ".", 1, true)
    end
    local function watch(child, name)
        local function prefixes(path)
            while path do
                local entries = dependents[path] or {}
                dependents[path], entries[child] = entries, true
                path = path:match("^(.*)%.[^.]+$")
            end
        end
        prefixes(name)
        for symbol in name:gmatch("%[=([^%]]+)%]") do prefixes(symbol) end
    end
    local function invalidate(path, value)
        if not path or value.path == path then return end
        -- An alias is a snapshot, not a live reference to the source variable
        -- or field. Rebinding that source must not redirect the alias's path.
        for destination, source in pairs(links) do
            if depends(source, path) then links[destination] = "?" end
        end
        for child in pairs(dependents[path] or {}) do
            if child.name and depends(child.name, path) then child.name, child.kind = nil, 5 end
        end
    end
    local bind
    bind = function(value, path, kind, uncertain, seen)
        if not path then return end
        uncertain = uncertain or value.uncertain
        if value.path then link(path, value.path, uncertain); return end
        link(path, nil, uncertain)
        if value.fn then
            local child = children[value.fn]
            child.name, child.kind = path, (uncertain or value.conditional) and 5 or kind
            watch(child, path)
        elseif value.fields then
            seen = seen or {}
            if seen[value] then return end
            seen[value] = true
            for _, entry in ipairs(value.fields) do
                bind(entry.value, field(path, entry.key), kind == 3 and 3 or 2,
                    uncertain or entry.conditional, seen)
            end
        end
    end
    local function write(register, value, pc)
        if conditional[pc] then
            local copy = {}
            for key, item in pairs(value) do copy[key] = item end
            copy.uncertain = true
            value = copy
        end
        registers[register] = value
        if active[register] then
            local path = data.locals[active[register]]
            invalidate(path, value)
            bind(value, path, 1, conditional[pc])
        end
    end
    local function synchronize(pc)
        if not boundaries[pc] then return end
        local nextactive, register = {}, 0
        for i = 1, #data.locals do
            if ranges[i * 2 - 1] <= pc and pc < ranges[i * 2] then
                nextactive[register] = i
                if active[register] ~= i then bind(registers[register] or unknown, data.locals[i], 1, conditional[pc - 1]) end
                register = register + 1
            end
        end
        active = nextactive
    end
    local function upvalue(index)
        local capture = data.upvalues[index + 1]
        return capture and { path = capture.name == "_ENV" and "" or capture.name } or unknown
    end
    local function get(base, key)
        if base.path == "" and declarations[key.literal] then links[key.literal] = "?" end
        return { path = field(base.path, key), uncertain = base.uncertain or key.uncertain }
    end
    local function set(base, key, value, pc)
        local uncertain = conditional[pc] or base.uncertain or key.uncertain
        if base.path then
            local path = field(base.path, key)
            invalidate(path, value)
            bind(value, path, 2, uncertain)
        elseif base.fields then
            base.fields[#base.fields + 1] = { key = key, value = value, conditional = uncertain }
            bind(value, field("$", key), 3, uncertain)
        end
    end
    local returnroot
    local function returned(value)
        local root = not value.uncertain and (value.path or (value.fields and "$")) or false
        if returnroot == nil then returnroot = root
        elseif returnroot ~= root then returnroot = false end
        data.root = returnroot or nil
        if value.fields then bind(value, "$", 2, value.uncertain)
        elseif value.fn then bind(value, "@return", 4) end
    end
    for pc = 0, count - 1 do
        synchronize(pc)
        local word = instructions[pc]
        local opcode, a, b, c = word & 127, (word >> 7) & 255, (word >> 16) & 255, word >> 24
        local bx, k = word >> 15, (word & 0x8000) ~= 0
        local function constant(index) return literal(constants[index + 1]) end
        local function rk(index) return k and constant(index) or read(index) end
        if opcode == op.MOVE then write(a, read(b), pc)
        elseif opcode == op.LOADI or opcode == op.LOADF then write(a, literal(bx - 65535), pc)
        elseif opcode == op.LOADK then write(a, constant(bx), pc)
        elseif opcode == op.LOADKX then write(a, constant(instructions[pc + 1] >> 7), pc)
        elseif opcode == op.LOADFALSE or opcode == op.LFALSESKIP then write(a, literal(false), pc)
        elseif opcode == op.LOADTRUE then write(a, literal(true), pc)
        elseif opcode == op.LOADNIL then for i = a, a + b do write(i, unknown, pc) end
        elseif opcode == op.GETUPVAL then write(a, upvalue(b), pc)
        elseif opcode == op.SETUPVAL then
            local value, path = read(a), upvalue(b).path
            invalidate(path, value); bind(value, path, 1, conditional[pc])
        elseif opcode == op.GETTABUP then write(a, get(upvalue(b), constant(c)), pc)
        elseif opcode == op.GETFIELD then write(a, get(read(b), constant(c)), pc)
        elseif opcode == op.GETI then write(a, get(read(b), literal(c)), pc)
        elseif opcode == op.GETTABLE then write(a, get(read(b), read(c)), pc)
        elseif opcode == op.SETTABUP then set(upvalue(a), constant(b), rk(c), pc)
        elseif opcode == op.SETFIELD then set(read(a), constant(b), rk(c), pc)
        elseif opcode == op.SETI then set(read(a), literal(b), rk(c), pc)
        elseif opcode == op.SETTABLE then set(read(a), read(b), rk(c), pc)
        elseif opcode == op.NEWTABLE then write(a, { fields = {} }, pc)
        elseif opcode == op.SETLIST then
            local n, offset = (word >> 16) & 63, word >> 22
            if k then offset = offset + (instructions[pc + 1] >> 7) * 1024 end
            if n == 0 then n = maxstack - a - 1 end
            for i = 1, n do set(read(a), literal(offset + i), read(a + i), pc) end
        elseif opcode == op.CLOSURE then write(a, { fn = bx + 1, conditional = conditional[pc] }, pc)
        elseif opcode == op.SELF then
            local base = read(b)
            write(a + 1, base, pc); write(a, get(base, constant(c)), pc)
        elseif opcode == op.CALL or opcode == op.VARARG then
            for i = a, maxstack - 1 do write(i, unknown, pc) end
        elseif opcode == op.RETURN1 or (opcode == op.RETURN and b == 2) then returned(read(a))
        elseif opcode == op.TFORCALL then
            for i = a + 4, a + 3 + c do write(i, unknown, pc) end
        elseif opcode == op.FORPREP then
            for i = a, a + 2 do registers[i] = unknown end
        elseif (modes[opcode] & 8) ~= 0 then write(a, unknown, pc) end
    end
    local serialized = {}
    for destination, source in pairs(links) do serialized[#serialized + 1] = destination .. "\t" .. source .. "\n" end
    table.sort(serialized)
    data.links = #serialized > 0 and table.concat(serialized) or nil
end

local function index_bindings(loader, recursive)
    local result = {}
    local function visit(path)
        local data = core.info(loader, path)
        data.kind = 0
        result[path] = data
        local children = {}
        for i = 1, data.children do children[i] = visit(path .. string.pack("<I4", i)) end
        -- Runtime additions need only module-level bindings. The offline
        -- order checker also analyzes nested declarations for diagnostics.
        if recursive ~= false or path == "" then analyze(data, children, core.code(loader, path)) end
        return data
    end
    visit("")
    return result
end

-- Binding metadata is used only to locate newly added fields, never to match
-- existing closures. All reads during key resolution use captured state.

local function parts(path)
    local result, position = {}, 1
    while position <= #path do
        local value, last
        if path:sub(position, position) == "[" then
            last = assert(path:find("]", position, true), "invalid export path")
            local key = path:sub(position + 1, last - 1)
            if key:sub(1, 1) == "=" then value = { symbol = key:sub(2) }
            elseif key:sub(1, 1) == "#" then
                value = key:sub(2):gsub("%x%x", function(byte) return string.char(tonumber(byte, 16)) end)
            else value = assert(tonumber(key), "invalid export key") end
        else
            local first
            first, last = path:find("[^.%[%]]+", position)
            assert(first == position, "invalid export path")
            value = path:sub(first, last)
        end
        result[#result + 1] = value
        position = last + 2
    end
    return result
end

local function index_exports(index)
    local root = index.root
    local links, children, fields, identities, bound = {}, {}, {}, {}, {}
    for dest, source in (root.data.links or ""):gmatch("([^\t\n]+)\t([^\n]+)\n") do
        local old = links[dest]
        links[dest] = source ~= "?" and (old == nil or old == source) and source or false
    end
    local function canonical(path, ancestors)
        if ancestors and ancestors[path] then return end
        local original, seen = path, {}
        while path do
            local first, rest = path:match("^([^.]+)(.*)$")
            local key, alias, suffix = path, links[path], ""
            if alias == nil then key, alias, suffix = first, links[first], rest end
            if alias == false or seen[key] then return end
            if alias == nil then
                if not path:find("[=", 1, true) then return path end
                local nested, valid = {}, true
                for name in pairs(ancestors or {}) do nested[name] = true end
                nested[original] = true
                local result = path:gsub("%[=([^%]]+)%]", function(symbol)
                    local origin = canonical(symbol, nested)
                    if not origin then valid = false; return "" end
                    return "[=" .. origin .. "]"
                end)
                if valid then return result end
                return
            end
            seen[key], path = true, alias .. suffix
        end
    end
    local function add(node, path, owner, alias)
        local field = { node = node, path = parts(path), owner = owner, alias = alias, binding = (owner or "$") .. ":" .. path }
        for _, part in ipairs(field.path) do
            if type(part) == "table" then
                field.dynamic = true
                local origin = parts(part.symbol)
                local first = origin[1]
                part.origins = {}
                if root.locals[first] == 1 then
                    table.remove(origin, 1)
                    part.origins[1] = { capture = "$::" .. first, path = origin }
                elseif not root.locals[first] then part.origins[1] = { capture = "$::_ENV", path = origin } end
                for alias in pairs(links) do
                    if alias ~= first and root.locals[alias] == 1 then
                        local source = canonical(alias)
                        if source == part.symbol or (source and part.symbol:sub(1, #source + 1) == source .. ".") then
                            part.origins[#part.origins + 1] = { capture = "$::" .. alias, path = parts(part.symbol:sub(#source + 2)) }
                        end
                    end
                end
            end
        end
        fields[#fields + 1], bound[node.key] = field, true
        if alias then identities[field.binding] = true end
    end
    local module = root.data.root and canonical(root.data.root)
    for _, node in ipairs(index.top) do
        local name = node.data.name and canonical(node.data.name)
        if name then
            -- Ambiguous aliases cannot be used to add fields. They do not
            -- prevent body updates to closures already present at runtime.
            if children[name] ~= nil then children[name] = false else children[name] = node end
            if node.data.kind == 2 then
                local prefix = module and module .. "."
                if prefix and name:sub(1, #prefix) == prefix then add(node, name:sub(#prefix + 1))
                else
                    local owner, rest = name:match("^([^.]+)%.(.+)$")
                    if owner and root.locals[owner] == 1 then add(node, rest, "$::" .. owner) end
                end
            end
        end
    end
    if module then
        local prefix = module .. "."
        for dest, source in pairs(links) do
            local first, rest = dest:match("^([^.]+)(.*)$")
            local base = canonical(first)
            local destination = base and base .. rest
            local target = source and canonical(source)
            local node = target and children[target]
            if node and destination and destination:sub(1, #prefix) == prefix then
                add(node, destination:sub(#prefix + 1), nil, true)
            end
        end
    end
    return fields, identities, bound
end

-- Check declaration order without executing loaders or changing module state.
function hotfix.check_order(oldloader, newloader)
    local beforebindings, afterbindings = index_bindings(oldloader), index_bindings(newloader)
    local errors, uncertain = {}, {}
    local function visit(path, label)
        local old, new = beforebindings[path], afterbindings[path]
        if new.children < old.children then
            errors[#errors + 1] = label .. ": declarations removed"
        end
        local counts = {}
        for i = 1, old.children do
            local name = beforebindings[path .. string.pack("<I4", i)].name
            if name then counts[name] = (counts[name] or 0) + 1 end
        end
        for i = 1, math.min(old.children, new.children) do
            local child = path .. string.pack("<I4", i)
            local before, after = beforebindings[child], afterbindings[child]
            local location = label .. "/" .. i
            if before.name and after.name and before.name ~= after.name then
                errors[#errors + 1] = location .. ": " .. before.name .. " -> " .. after.name
            elseif not before.name or not after.name or counts[before.name] > 1 then
                uncertain[#uncertain + 1] = location .. ": anonymous or repeated binding requires review"
            end
            visit(child, location)
        end
    end
    visit("", "$")
    return #errors == 0 and #uncertain == 0, errors, uncertain
end

-- State binding and update commit.
local function fail(message)
    error(message, 0)
end

local function cell(f, i)
    local value
    local function hold() return value end
    upvaluejoin(hold, 1, f, i)
    return { func = hold, index = 1, id = upvalueid(hold, 1) }
end

local function read(c)
    local _, value = getupvalue(c.func, c.index)
    return value
end

-- Function order is a source contract: existing children retain their ordinal;
-- newly declared functions are appended in their own lexical parent.
local function index_loader(loader)
    local bindings = index_bindings(loader, false)
    local index = { byproto = {}, bykey = {}, declarations = {}, top = {}, persistent = { ["$::_ENV"] = true } }
    local function visit(path, key, parent)
        local data = bindings[path]
        local node = { prototype_path = path, key = key, data = data,
            modulelevel = parent == index.root, captures = {}, locals = {}, scope = {} }
        if not parent then index.root = node end
        if node.modulelevel then index.top[#index.top + 1] = node end
        for i, name in ipairs(data.locals) do
            node.locals[name] = (node.locals[name] or 0) + 1
            if data.localscope[i] then node.scope[name] = true end
        end
        for i, uv in ipairs(data.upvalues) do
            if not uv.name then fail("hotfix requires unstripped upvalue names") end
            if not parent then
                if uv.name ~= "_ENV" then fail("invalid module loader capture: " .. uv.name) end
                node.captures[i] = "$::_ENV"
            elseif uv.instack then
                -- Shared declarations use a lexical-owner key. Shadowed names
                -- remain private to each closure; joining its actual old cell
                -- preserves sharing without merging independent instances.
                local owner = parent.locals[uv.name] == 1 and parent.key or node.key
                node.captures[i] = owner .. "::" .. uv.name
                if parent == index.root and owner == "$" and parent.scope[uv.name] then
                    index.persistent[node.captures[i]] = true
                end
            else
                node.captures[i] = parent.captures[uv.index]
                if not node.captures[i] then fail("invalid capture origin: " .. uv.name) end
            end
            if uv.name == "_ENV" then node.environment = i end
        end
        if node.modulelevel and data.kind == 1 then
            index.declarations["$::" .. data.name] = node
        end
        node.descriptor = { key = key, captures = node.captures, environment = node.environment, children = data.children }
        index.byproto[data.proto], index.bykey[key] = node, node
        for i = 1, data.children do visit(path .. string.pack("<I4", i), key .. "/" .. i, node) end
        if not parent then node.data = { root = data.root, links = data.links }
        else
            node.data = node.modulelevel and { kind = data.kind, name = data.name } or nil
            node.locals, node.scope = nil, nil
        end
    end
    visit("", "$", nil)
    return index
end

local function compact_index(index)
    for id, node in pairs(index.byproto) do index.byproto[id] = node.descriptor end
    return { byproto = index.byproto, root = index.root.descriptor, persistent = index.persistent }
end

function hotfix.addsearcher(fn)
    assert(type(fn) == "function", "searcher must be a function")
    searchers[#searchers + 1] = fn
end

-- The built-in loader reads fresh source, bypassing Moon's loadfile code cache.
local function findloader(name, environment)
    local messages = {}
    for _, searcher in ipairs(searchers) do
        local f, extra = searcher(name)
        if type(f) == "function" then return f, extra end
        if type(f) == "string" then messages[#messages + 1] = f end
    end
    local path, err = package.searchpath(name, package.path)
    if path then
        local file, openerr = io.open(path, "rb")
        if not file then fail(openerr) end
        local source = file:read("a")
        file:close()
        local f, loaderr = load(source, "@" .. path, "t", environment or _ENV)
        if not f then fail(loaderr) end
        return f, path
    end
    messages[#messages + 1] = err or ""
    fail("module '" .. name .. "' not found:" .. table.concat(messages))
end

local function collect(record)
    local cells, functions = {}, {}
    for key, entries in pairs(record.cells) do
        cells[key] = {}
        for id, c in pairs(entries) do cells[key][id] = c end
    end
    local seen = {}
    local _, environment = next(record.cells["$::_ENV"] or {})
    if environment and type(read(environment)) == "table" then seen[read(environment)] = true end
    local walk
    walk = function(value)
        local kind = type(value)
        if kind == "function" then
            local original = value
            value = resolve(value)
            local node = record.functions[value] or record.index.byproto[proto(value)]
            if not node then return end -- foreign Lua and C functions are untouched
            if functions[value] then return end
            record.functions[value] = node
            local captures = {}
            functions[value] = { node = node, captures = captures, representative = original }
            for i, key in ipairs(node.captures) do
                local id = upvalueid(value, i)
                local entries = cells[key] or {}
                cells[key] = entries
                local c = entries[id] or cell(value, i)
                entries[id] = c
                captures[key] = c
                if node.environment ~= i then
                    walk(read(c))
                end
            end
        elseif kind == "table" and not seen[value] then
            seen[value] = true
            for _, item in next, value do walk(item) end
            walk(debug.getmetatable(value))
        end
    end
    walk(record.root)
    for key, entries in pairs(record.cells) do
        if key ~= "$::_ENV" then
            for _, c in pairs(entries) do walk(read(c)) end
        end
    end
    -- Keep known instances reachable only through external references updatable.
    -- Snapshot before walk() can discover another current target.
    local tracked = {}
    for f in pairs(record.functions) do tracked[#tracked + 1] = f end
    for _, f in ipairs(tracked) do walk(f) end
    return cells, functions
end

function hotfix.register(name, loader, root)
    assert(type(name) == "string", "module name must be a string")
    assert(type(loader) == "function" and proto(loader), "loader must be a Lua function")
    assert(type(root) == "table", "hotfix module must return a table")
    local loaded = debug.getregistry()._LOADED
    if modules[name] then return modules[name].root end
    if loaded[name] ~= nil and loaded[name] ~= root then fail("module already loaded with a different value: " .. name) end
    local index = index_loader(loader)
    local _, fields = index_exports(index)
    local record = { root = root, loader = loader, index = compact_index(index), exports = fields, cells = {},
        functions = setmetatable({}, { __mode = "k" }) }
    -- Anchor the actual loader environment even when exported functions use no globals.
    for i, key in ipairs(record.index.root.captures) do
        local c = cell(loader, i)
        record.cells[key] = { [c.id] = c }
    end
    local cells = collect(record)
    for key, entries in pairs(cells) do
        if record.index.persistent[key] then record.cells[key] = entries end
    end
    modules[name], loaded[name] = record, root
    return root
end

function hotfix.require(name)
    assert(type(name) == "string", "module name must be a string")
    local loaded = debug.getregistry()._LOADED
    if loaded[name] then return loaded[name] end
    local loader, extra = findloader(name)
    local root = loader(name, extra)
    return hotfix.register(name, loader, root)
end

-- Kept for callers that inspect structural compatibility. State validation is
-- performed against actual closure instances by update(), not prototype names.
function hotfix.diff(oldloader, newloader)
    local ok, mapping, errors, added = pcall(function()
        local old, new = index_loader(oldloader), index_loader(newloader)
        local result, missing, additions = {}, {}, {}
        for key, node in pairs(old.bykey) do
            local replacement = new.bykey[key]
            if replacement then result[proto(oldloader, node.prototype_path)] = clone(newloader, replacement.prototype_path)
            else missing[#missing + 1] = "removed function: " .. key end
        end
        for key, node in pairs(new.bykey) do
            if not old.bykey[key] then additions[#additions + 1] = clone(newloader, node.prototype_path) end
        end
        return result, #missing > 0 and missing or nil, additions
    end)
    if not ok then return {}, { tostring(mapping) }, {} end
    return mapping, errors, added
end

local function prepare(record, loader)
    local newindex = index_loader(loader)
    for key in pairs(record.index.persistent) do newindex.persistent[key] = true end
    local oldpaths = {}
    for _, node in pairs(record.index.byproto) do
        oldpaths[node.key] = true
        local replacement = newindex.bykey[node.key]
        if not replacement or replacement.descriptor.children < node.children then fail("removed function: " .. node.key) end
    end
    local fields, identities, bound = index_exports(newindex)
    for binding in pairs(record.exports) do identities[binding] = true end
    for _, node in ipairs(newindex.top) do
        if not oldpaths[node.key] and node.data.kind ~= 1 and not bound[node.key] then
            fail("new function has no static export binding: " .. node.key)
        end
    end
    local cells, functions = collect(record)
    local plan, replacements, fresh, prepared, extra_cells = {}, {}, {}, {}, {}
    local additions = {}
    local function addfield(parent, key, f)
        local keys = additions[parent] or {}
        additions[parent] = keys
        if keys[key] then
            if keys[key] ~= f then fail("conflicting additions to field: " .. tostring(key)) end
            return
        end
        keys[key] = f
        plan[#plan + 1] = { target = parent, key = key, new = f }
    end
    local function unique(key)
        local entries = cells[key]
        local found
        for _, c in pairs(entries or {}) do
            if found then fail("ambiguous state binding: " .. key) end
            found = c
        end
        return found
    end
    local build
    build = function(node, old)
        if old and replacements[old] then return replacements[old] end
        if not old and fresh[node] then return fresh[node] end
        local f = clone(loader, node.prototype_path)
        if old then replacements[old] = f end
        if not old then fresh[node] = f end
        prepared[f] = node.descriptor
        local own = old and functions[old].captures or {}
        for i, key in ipairs(node.captures) do
            local c = own[key] or unique(key) or extra_cells[key]
            if not c then
                local declaration = newindex.declarations[key]
                if declaration and declaration.modulelevel then
                    local value
                    local function hold() return value end
                    c = { func = hold, index = 1, id = upvalueid(hold, 1) }
                    extra_cells[key] = c -- publish before binding recursive helpers
                    setupvalue(hold, 1, build(declaration))
                else
                    fail("missing state binding for " .. node.key .. ": " .. key)
                end
            end
            upvaluejoin(f, i, c.func, c.index)
        end
        return f
    end
    local function replace(old)
        if replacements[old] then return replacements[old] end
        local key = functions[old].node.key
        local node = newindex.bykey[key]
        if not node then fail("removed live function: " .. key) end
        return build(node, old)
    end
    for old in pairs(functions) do
        plan[#plan + 1] = { func = old, old = old, new = replace(old) }
    end
    local function existing_export(node)
        local existing
        for candidate, instance in pairs(functions) do
            if instance.node.key == node.key then
                if existing then fail("ambiguous exported closure: " .. node.key) end
                existing = candidate
            end
        end
        return existing
    end
    local function fieldkey(part)
        if type(part) ~= "table" then return part, true end
        local found, available
        for _, origin in ipairs(part.origins) do
            local c = unique(origin.capture)
            local value = c and read(c)
            for _, key in ipairs(origin.path) do
                if type(value) ~= "table" then value = nil; break end
                value = rawget(value, key)
            end
            local kind = type(value)
            if (kind == "string" or kind == "number" or kind == "boolean") and value == value then
                if available and found ~= value then fail("ambiguous enum key state: " .. part.symbol) end
                found, available = value, true
            end
        end
        return found, available
    end
    local function preparefield(field)
        if record.exports[field.binding] or (not field.alias and oldpaths[field.node.key]) then return end
        local node, path = field.node, field.path
        local keys = path
        if field.dynamic then
            keys = {}
            for i, part in ipairs(path) do
                local key, available = fieldkey(part)
                if not available then
                    fail("missing enum key state: " .. part.symbol)
                end
                keys[i] = key
            end
        end
        local parent = record.root
        if field.owner then
            local owner = unique(field.owner)
            if not owner or type(read(owner)) ~= "table" then fail("missing method table: " .. field.owner) end
            parent = read(owner)
        end
        for i = 1, #keys - 1 do
            parent = rawget(parent, keys[i])
            if type(parent) ~= "table" then fail("missing parent table for " .. node.key) end
        end
        local key = keys[#keys]
        local old = rawget(parent, key)
        if old ~= nil and type(old) ~= "function" then fail("field is not a function: " .. node.key) end
        if old then
            local instance = functions[resolve(old)]
            if not instance or instance.node.key ~= node.key then fail("field binding changed: " .. node.key) end
        else
            local existing = existing_export(node)
            addfield(parent, key, existing and functions[existing].representative or build(node))
        end
    end
    for _, field in ipairs(fields) do preparefield(field) end
    -- Persist only module-level cells; factory instances remain collectible.
    local persistent = {}
    for key, entries in pairs(cells) do
        if newindex.persistent[key] then persistent[key] = entries end
    end
    for key, c in pairs(extra_cells) do persistent[key] = { [c.id] = c } end
    return { writes = plan, index = compact_index(newindex), exports = identities, functions = prepared, cells = persistent, loader = loader }
end

local function commit(record, plan)
    local applied = 0
    -- Create the weak registry before publishing any target or table changes.
    -- Needed closures remain anchored by planned writes and their captures.
    local tracked = setmetatable(plan.functions, { __mode = "k" })
    local function assign(item, value)
        if item.func then redirect(item.func, value)
        else rawset(item.target, item.key, value) end
    end
    local ok, err = pcall(function()
        for i, item in ipairs(plan.writes) do
            assign(item, item.new)
            applied = i
        end
    end)
    if not ok then
        for i = applied, 1, -1 do assign(plan.writes[i], plan.writes[i].old) end
        fail(err)
    end
    -- Every live alias reaches its current target through the shared slot.
    record.functions, record.exports = tracked, plan.exports
    record.loader, record.index, record.cells = plan.loader, plan.index, plan.cells
    return record.root
end

function hotfix.update(name, updatename)
    assert(type(name) == "string", "module name must be a string")
    local acquired
    local ok, result = pcall(function()
        if debug.getregistry()._LOADED[name] == nil then return hotfix.require(name) end
        local record = modules[name]
        if not record then fail("module is not registered for hotfix: " .. name) end
        if record.updating then fail("module is already updating: " .. name) end
        record.updating, acquired = true, record
        if record.root ~= debug.getregistry()._LOADED[name] then fail("loaded module was replaced: " .. name) end
        local _, environment_cell = next(record.cells["$::_ENV"] or {})
        if not environment_cell then fail("module environment is unavailable") end
        local environment = read(environment_cell)
        local loader = findloader(updatename or name, environment)
        return commit(record, prepare(record, loader))
    end)
    if acquired then acquired.updating = nil end
    if not ok then return false, tostring(result) end
    return true, result
end

return hotfix
