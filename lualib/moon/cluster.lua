local moon = require "moon"
local seri = require "seri"
local core = require "cluster.core"

local cluster = {}
local NODE

function cluster.init(node_id, discovery_url)
    NODE = node_id
    return core.init(node_id, discovery_url)
end

function cluster.listen()
    return core.listen()
end

---@param to_node integer
---@param to_sname string
---@param ... any
function cluster.send(to_node, to_sname, ...)
    if to_node == NODE then
        local addr = moon.query(to_sname)
        if addr ~= 0 then
            moon.send("lua", addr, ...)
            return
        end
        error("local service not found: " .. tostring(to_sname))
    end
    core.send(to_node, to_sname, seri.pack(...))
end

---@async
---@param to_node integer
---@param to_sname string
---@param ... any
---@return any ...
function cluster.call(to_node, to_sname, ...)
    if to_node == NODE then
        local addr = moon.query(to_sname)
        if addr ~= 0 then
            return moon.call("lua", addr, ...)
        end
        error("local service not found: " .. tostring(to_sname))
    end
    return moon.wait(core.request(to_node, to_sname, seri.pack(...)))
end

return cluster
