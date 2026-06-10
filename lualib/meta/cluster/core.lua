---@meta
-- IDE annotation file only. Do not require this file at runtime.

--- Cluster module (`require("cluster.core")`).
---@class cluster.core
local cluster = {}

--- Initialize this node for cluster communication.
---@param node_id integer
---@param discovery_url string @ URL of the cluster discovery/registry service
---@return boolean success
---@return false? err
---@return string? errmsg
function cluster.init(node_id, discovery_url) end

--- Start listening for inbound cluster connections (uses discovery to resolve bind address).
---@return boolean
function cluster.listen() end

--- Stop cluster background tasks and release pending outbound waits.
---@return boolean
function cluster.shutdown() end

--- Send a one-way cluster message (no response).
---@param to_node integer @ Target node id
---@param to_service string @ Target service name on remote node
---@param body buffer_ptr|string
function cluster.send(to_node, to_service, body) end

--- Send a cluster RPC request asynchronously.
---@param to_node integer
---@param to_service string
---@param body buffer_ptr|string
---@return integer session
function cluster.request(to_node, to_service, body) end

return cluster
