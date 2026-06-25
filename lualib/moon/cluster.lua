local moon = require "moon"
local seri = require "seri"
---@type cluster.core
local core = require "cluster.core"

--- Cluster RPC layer (`require("moon.cluster")`).
---
--- Provides cross-node message passing on top of `cluster.core`. Each process is
--- a *node* identified by an integer `node_id`; services are addressed by their
--- registered name (see `moon.name`). Calls/sends to the local node are
--- short-circuited to `moon.send`/`moon.call` (resolving the name via
--- `moon.query`), so the same API works whether the target is local or remote.
---
--- Payloads are serialized with `seri.pack` before being sent over the wire and
--- automatically unpacked on the receiving side, so you pass and receive plain
--- Lua values—not buffers.
---
--- Typical bootstrap:
--- ```lua
--- local cluster = require("moon.cluster")
--- cluster.init(1, "http://127.0.0.1:8500/node/{}")  -- {} is replaced with node id
--- cluster.listen()
--- -- ... later, from any service:
--- cluster.send(2, "chat", "hello")
--- local ok, result = cluster.call(2, "db", "get", key)
--- ```
local cluster = {}

--- The id of the local node, set by `cluster.init` and cleared by
--- `cluster.shutdown`. `nil` until initialized.
---@type integer?
local NODE

---@type fun(node_id: integer, reason: string)?
local _on_close_handler

--- Initialize the local node for cluster communication.
---
--- Must be called once before `listen`, `send`, or `call`. `discovery_url` is the
--- address of the discovery/registry service used to resolve a node id to its
--- network address; the literal `{}` in the URL is replaced with the target node
--- id at lookup time. Errors if the cluster is already initialized.
---@param node_id integer @ This process's node id.
---@param discovery_url string @ Discovery service URL; `{}` is substituted with the node id.
---@return boolean ok
function cluster.init(node_id, discovery_url)
    NODE = node_id
    return core.init(node_id, discovery_url)
end

--- Start accepting inbound cluster connections.
---
--- Resolves this node's bind address via the discovery service and begins
--- listening in the background. Requires `cluster.init` to have been called.
---@return boolean ok
function cluster.listen()
    return core.listen()
end

--- Stop cluster background tasks and release any pending outbound calls.
---
--- Outstanding `cluster.call`s are failed with an error. After shutdown the node
--- id is cleared and the cluster must be re-`init`ialized before reuse.
---@return boolean ok
function cluster.shutdown()
    NODE = nil
    return core.shutdown()
end

--- Send a one-way message to a service (no response expected).
---
--- When `to_node` is the local node, the message is delivered directly via
--- `moon.send`, raising an error if the named service is not found locally. For
--- remote nodes the arguments are packed and forwarded; remote delivery failures
--- are logged asynchronously rather than raised here.
---@param to_node integer @ Target node id.
---@param to_sname string @ Target service name on the destination node.
---@param ... any @ Arguments delivered to the target service.
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

--- Send an RPC request and wait for the response (yields the caller coroutine).
---
--- When `to_node` is the local node, this delegates to `moon.call` (erroring if
--- the named service is not found). For remote nodes the request is sent and the
--- coroutine yields until the response arrives; remote calls time out after
--- ~10 seconds, surfacing as an error. Must be called from within a coroutine
--- started by `moon.async`.
---@async
---@param to_node integer @ Target node id.
---@param to_sname string @ Target service name on the destination node.
---@param ... any @ Arguments delivered to the target service.
---@return any ... @ The values returned by the target service.
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

--- Register a callback invoked when a connection to a remote node closes.
---
--- Only one handler is kept; calling this again replaces the previous one. The
--- handler receives the remote node id and a short reason string (e.g. `"EOF"`,
--- `"Shutdown"`, timeout).
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
