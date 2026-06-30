---@meta
-- IDE annotation file only. Do not require this file at runtime.

--- gRPC connection handle userdata.
---@class grpc_connection
local grpc_connection = {}

--- gRPC stream handle userdata.
---@class grpc_stream
local grpc_stream = {}

--- gRPC server-side stream handle userdata (one inbound RPC).
---@class grpc_server_stream
local grpc_server_stream = {}

--- Native gRPC client module (`require("grpc.core")`).
---
--- Carries raw protobuf bytes over HTTP/2 (tonic). Message encode/decode is the
--- caller's responsibility (use the `protobuf` module); this layer never parses
--- protobuf payloads.
---@class grpc.core
local grpc = {}

--- Connect to a gRPC endpoint asynchronously; on success the channel is
--- registered under `opts.name` (default `"default"`). Returns a session;
--- the reply is delivered via `PTYPE_GRPC` as `true` or `(false, errmsg)`.
---@param opts table @ `{ endpoint, name?, connect_timeout?, tls? }`
---@return integer session
function grpc.connect(opts) end

--- Unregister a named connection (drops its channel).
---@param name string
---@return boolean
function grpc.close(name) end

--- Look up an established connection by name.
---@param name string
---@return grpc_connection? conn
function grpc.find_connection(name) end

--- Look up an open stream by fd.
---@param fd integer
---@return grpc_stream? stream
function grpc.find_stream(fd) end

--- Native counters: `{ connections = integer, streams = integer, servers = integer }`.
---@return table
function grpc.stats() end

--- Start a gRPC (HTTP/2 cleartext) listener. Inbound RPCs are delivered to the
--- calling actor via `PTYPE_GRPC` with `session == 0` as `(path, grpc_server_stream)`.
---@param addr string Listen address, e.g. "0.0.0.0:50051"
---@param opts? table @ `{ max_connections? }`
---@return integer listener_fd
function grpc.listen(addr, opts) end

--- Stop a gRPC listener by fd.
---@param fd integer
---@return boolean success
function grpc.stop(fd) end

--- Unary call. Returns a session; the `PTYPE_GRPC` reply is a table
--- `{ status = integer, message = string, body = string? }` (`body` present
--- only when `status == 0`).
---@param conn grpc_connection
---@param method string Method path, e.g. "/pkg.Service/Method"
---@param request string|buffer_ptr Encoded protobuf request bytes
---@param timeout? integer Per-call timeout in ms (0 = none)
---@param metadata? table<string,string> Request metadata
---@return integer session
function grpc_connection:unary(method, request, timeout, metadata) end

--- Open a server-streaming call (single request). Returns a session; the reply
--- is `{ fd = integer }` on success or `(false, errmsg)`.
---@param conn grpc_connection
---@param method string
---@param request string|buffer_ptr Encoded protobuf request bytes
---@param timeout? integer
---@param metadata? table<string,string>
---@return integer session
function grpc_connection:server_stream(method, request, timeout, metadata) end

--- Open a bidirectional/client-streaming call. Returns a session; the reply is
--- `{ fd = integer }` on success or `(false, errmsg)`.
---@param conn grpc_connection
---@param method string
---@param timeout? integer
---@param metadata? table<string,string>
---@return integer session
function grpc_connection:bidi_stream(method, timeout, metadata) end

--- Request the next message off a response stream. Returns a session; the
--- reply is the message bytes (string), `nil` at clean end-of-stream, or
--- `(false, errmsg)` on stream error.
---@param conn grpc_stream
---@return integer session
function grpc_stream:recv() end

--- Send one encoded request message (client/bidi streaming only).
---@param conn grpc_stream
---@param data string|buffer_ptr Encoded protobuf request bytes
---@return boolean ok
---@return string? err
function grpc_stream:send(data) end

--- Half-close the request stream (no more sends).
---@param conn grpc_stream
---@return boolean ok
function grpc_stream:close_send() end

--- Close the stream and release native resources.
---@param conn grpc_stream
---@return boolean ok
function grpc_stream:close() end

--- (Server) Request the next inbound message. Returns a session; the reply is
--- the request bytes (string), `nil` at clean end-of-request-stream, or
--- `(false, errmsg)` on error.
---@param self grpc_server_stream
---@return integer session
function grpc_server_stream:recv() end

--- (Server) Send one encoded response message (raw protobuf bytes).
---@param self grpc_server_stream
---@param data string|buffer_ptr
---@return boolean ok
---@return string? err
function grpc_server_stream:send(data) end

--- (Server) End the response stream with a gRPC status (default OK). Idempotent.
---@param self grpc_server_stream
---@param code? integer gRPC status code (0 = OK)
---@param message? string
---@return boolean ok
function grpc_server_stream:finish(code, message) end

return grpc
