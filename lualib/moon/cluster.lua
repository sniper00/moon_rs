local moon = require "moon"
local seri = require "seri"
---@type cluster.core
local core = require "cluster.core"

local cluster = {}
local NODE
local _on_close_handler

function cluster.init(node_id, discovery_url)
    NODE = node_id
    return core.init(node_id, discovery_url)
end

function cluster.listen()
    return core.listen()
end

function cluster.shutdown()
    NODE = nil
    return core.shutdown()
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

---@param handler fun(node_id: integer, reason: string)
function cluster.on_close(handler)
    _on_close_handler = handler
end

moon.system("_cluster_close", function(_sender, node_id, reason)
    if _on_close_handler then
        _on_close_handler(tonumber(node_id), reason)
    end
end)

return cluster
