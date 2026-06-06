---
--- Cluster discovery HTTP server.
--- Returns node addresses for cluster.init discovery_url.
---
--- The cluster module calls: GET <discovery_url> (with {} replaced by node_id)
--- This server responds with the plain-text address "host:port".
---
--- Usage: moon_rs assets/example/cluster/cluster_etc.lua [node.json path]
---

local moon = require("moon")
local json = require("json")
local httpserver = require("moon.http.server")

local args = moon.args()
local node_file = args[1] or "node.json"

local ETC_HOST = "127.0.0.1"
local ETC_PORT = 9090

local cluster_etc

local function load_cluster_etc()
    cluster_etc = {}
    local content = io.readfile(node_file)
    local res = json.decode(content)
    for _, v in ipairs(res) do
        cluster_etc[v.node] = v
    end
end

load_cluster_etc()

httpserver.on("/reload", function(request, response)
    load_cluster_etc()
    response.status_code = 200
    response:write_header("Content-Type", "text/plain")
    response:write("OK")
end)

httpserver.on("/cluster", function(request, response)
    local query = request:query()
    local node = tonumber(query.node)
    local cfg = cluster_etc[node]
    if not cfg then
        response.status_code = 404
        response:write_header("Content-Type", "text/plain")
        response:write("cluster node not found " .. tostring(query.node))
        return
    end
    response.status_code = 200
    response:write_header("Content-Type", "text/plain")
    response:write(cfg.cluster)
end)

httpserver.on("/conf.node", function(request, response)
    local query = request:query()
    local node = tonumber(query.node)
    local cfg = cluster_etc[node]
    if not cfg then
        response.status_code = 404
        response:write_header("Content-Type", "text/plain")
        response:write("cluster node not found " .. tostring(query.node))
        return
    end
    response.status_code = 200
    response:write_header("Content-Type", "application/json")
    response:write(json.encode(cfg))
end)

httpserver.listen(ETC_HOST .. ":" .. ETC_PORT)
print(string.format("Cluster etc HTTP server started on %s:%d", ETC_HOST, ETC_PORT))
