-- gRPC client backed by the native `grpc.core` Rust extension (tonic/HTTP-2).
--
-- Message (de)serialization is delegated to the `protobuf` module
-- (`lua_protobuf.rs`): you load a `FileDescriptorSet` once with
-- `protobuf.load(...)`, then this wrapper encodes the request table to wire
-- bytes before the call and decodes the reply bytes back into a table.
--
-- The native layer only moves raw protobuf bytes over HTTP/2, so there is a
-- single source of truth for the schema (the loaded descriptor) and no second
-- protobuf implementation.
--
-- Example:
-- ```lua
-- local fs       = require("fs")
-- local protobuf = require("protobuf")
-- local grpc     = require("moon.grpc")
--
-- protobuf.load(fs.read("helloworld.pb")) -- FileDescriptorSet bytes
--
-- local conn, err = grpc.connect({ endpoint = "http://127.0.0.1:50051" })
-- assert(conn, err)
--
-- local reply, rerr = conn:unary(
--     "/helloworld.Greeter/SayHello",  -- method path
--     "helloworld.HelloRequest",       -- request message type
--     { name = "moon" },               -- request table
--     "helloworld.HelloReply"          -- response message type
-- )
-- assert(reply, rerr and rerr.message)
-- print(reply.message)
-- ```

local moon = require("moon")
local protobuf = require("protobuf")
local c = require("grpc.core")

moon.register_protocol {
    name = "grpc",
    PTYPE = moon.PTYPE_GRPC,
    pack = function(...) return ... end,
}

---@class grpc.CallOptions
---@field timeout? integer Per-call timeout in milliseconds (0 = no client-side timeout)
---@field metadata? table<string, string> Request metadata (HTTP/2 headers)

---@class grpc.Status
---@field code integer gRPC status code (0 = OK)
---@field message string Status message

---@class grpc.Stream
---@field obj any            Native stream handle
---@field resp_type string   Response message type name
---@field req_type? string   Request message type name (streaming requests only)
local Stream = {}
Stream.__index = Stream

---@class grpc.Connection
---@field obj any   Native connection handle
local Connection = {}
Connection.__index = Connection

local M = {}

--- Connect to a gRPC endpoint and register the channel under `opts.name`.
---@async
---@param opts table @ `{ endpoint = "http(s)://host:port", name?, connect_timeout?, tls? }`
---  `tls` (optional) `{ domain?, ca?, cert?, key? }` enables/customizes TLS;
---  https endpoints enable TLS automatically with the system/webpki roots.
---@return grpc.Connection? conn
---@return string? err
function M.connect(opts)
    local ok, err = moon.wait(c.connect(opts))
    if not ok then
        return nil, err
    end
    local name = opts.name or "default"
    return M.find_connection(name)
end

--- Wrap an already-connected channel by name (no async).
---@param name string
---@return grpc.Connection? conn
function M.find_connection(name)
    local obj = c.find_connection(name)
    if not obj then
        return nil
    end
    return setmetatable({ obj = obj }, Connection)
end

--- Close (unregister) a named connection.
---@param name string
function M.close(name)
    return c.close(name)
end

--- Native-level stats: `{ connections = n, streams = n }`.
---@return table
function M.stats()
    return c.stats()
end

--- Unary RPC.
---@async
---@param method string         Fully-qualified method path, e.g. "/pkg.Service/Method"
---@param req_type string       Request protobuf message type name
---@param req table             Request table
---@param resp_type string      Response protobuf message type name
---@param opts? grpc.CallOptions
---@return table? reply         Decoded response table, or nil on error
---@return grpc.Status? status  Error status when `reply` is nil
function Connection:unary(method, req_type, req, resp_type, opts)
    opts = opts or {}
    local payload = protobuf.encode(req_type, req)
    local res = moon.wait(self.obj:unary(method, payload, opts.timeout, opts.metadata))
    if res.status ~= 0 then
        return nil, { code = res.status, message = res.message }
    end
    return protobuf.decode(resp_type, res.body)
end

--- Open a server-streaming RPC (one request, a stream of responses).
---@async
---@param method string
---@param req_type string
---@param req table
---@param resp_type string
---@param opts? grpc.CallOptions
---@return grpc.Stream? stream
---@return string? err
function Connection:server_stream(method, req_type, req, resp_type, opts)
    opts = opts or {}
    local payload = protobuf.encode(req_type, req)
    local res, err = moon.wait(self.obj:server_stream(method, payload, opts.timeout, opts.metadata))
    if not res then
        return nil, err
    end
    return M._wrap_stream(res.fd, resp_type, nil)
end

--- Open a bidirectional (or client) streaming RPC.
---@async
---@param method string
---@param req_type string       Request message type for `stream:send`
---@param resp_type string      Response message type for `stream:recv`
---@param opts? grpc.CallOptions
---@return grpc.Stream? stream
---@return string? err
function Connection:bidi_stream(method, req_type, resp_type, opts)
    opts = opts or {}
    local res, err = moon.wait(self.obj:bidi_stream(method, opts.timeout, opts.metadata))
    if not res then
        return nil, err
    end
    return M._wrap_stream(res.fd, resp_type, req_type)
end

function M._wrap_stream(fd, resp_type, req_type)
    local obj = c.find_stream(fd)
    if not obj then
        return nil, "grpc: stream handle not found"
    end
    return setmetatable({
        obj = obj,
        resp_type = resp_type,
        req_type = req_type,
    }, Stream)
end

--- Receive (and decode) the next message from the response stream.
---@async
---@return table? message  Decoded message, or nil at end-of-stream / on error
---@return string? err     Set when the stream errored (not on clean end)
function Stream:recv()
    local data, err = moon.wait(self.obj:recv())
    if data == nil then
        -- nil + no err  => clean end of stream
        -- nil + err     => stream error
        return nil, err
    end
    return protobuf.decode(self.resp_type, data)
end

--- Encode and send one request message (client/bidi streaming only).
---@param req table
---@return boolean ok
---@return string? err
function Stream:send(req)
    local payload = protobuf.encode(self.req_type, req)
    return self.obj:send(payload)
end

--- Half-close the request stream, signalling no more `send`s.
---@return boolean ok
function Stream:close_send()
    return self.obj:close_send()
end

--- Close the stream and release the native resources.
---@return boolean ok
function Stream:close()
    return self.obj:close()
end

--- Lets a stream be used as a to-be-closed variable, auto-releasing native
--- resources on scope exit / break / error (idempotent with `close`):
--- ```lua
--- local stream <close> = conn:server_stream(...)
--- for ... do ... end
--- ```
Stream.__close = function(self) self:close() end

-- ---------------------------------------------------------------------------
-- Server side
-- ---------------------------------------------------------------------------
--
-- A gRPC server is a plain HTTP/2 (h2c) listener. Every inbound RPC — unary,
-- server/client/bidi streaming — is delivered to the listening actor's
-- `grpc.dispatch` handler as a `grpc.ServerStream`. The handle moves *raw
-- protobuf bytes*; you decode requests and encode replies yourself with the
-- `protobuf` module, exactly like the client side.
--
-- Example:
-- ```lua
-- local protobuf = require("protobuf")
-- local grpc     = require("moon.grpc")
--
-- local function readfile(p) local f = assert(io.open(p, "rb")); local d = f:read("a"); f:close(); return d end
-- protobuf.load(readfile("helloworld.pb"))
--
-- grpc.dispatch(function(stream, path)
--     if path == "/helloworld.Greeter/SayHello" then
--         local req = protobuf.decode("helloworld.HelloRequest", stream:recv())
--         stream:send(protobuf.encode("helloworld.HelloReply", { message = "hello " .. req.name }))
--         stream:finish() -- OK (also auto-called if you return without finishing)
--     else
--         stream:finish(12, "method not found") -- 12 = UNIMPLEMENTED
--     end
-- end)
--
-- grpc.listen("0.0.0.0:50051")
-- ```

---@class grpc.ServerStream
---@field obj any       Native server-stream handle
---@field path string   Fully-qualified method path, e.g. "/pkg.Service/Method"
local ServerStream = {}
ServerStream.__index = ServerStream

--- Receive the next inbound request message as raw protobuf bytes.
--- Returns `nil` at the clean end of the request stream, or `(nil, err)` on error.
---@async
---@return string? bytes
---@return string? err
function ServerStream:recv()
    return moon.wait(self.obj:recv())
end

--- Send one response message (raw protobuf bytes).
---@param bytes string
---@return boolean ok
---@return string? err
function ServerStream:send(bytes)
    return self.obj:send(bytes)
end

--- End the response stream with a gRPC status (default OK). Idempotent.
---@param code? integer gRPC status code (0 = OK)
---@param message? string Status message (for non-OK codes)
---@return boolean ok
function ServerStream:finish(code, message)
    return self.obj:finish(code, message)
end

--- Lets a server stream be used as a to-be-closed variable; finishes with OK on
--- scope exit if not already finished (idempotent).
ServerStream.__close = function(self) self:finish(0) end

---@class grpc.ListenOptions
---@field max_connections? integer Max concurrent connections (default 100000)

--- Start a gRPC (HTTP/2 cleartext) server on `addr`. Inbound RPCs are routed to
--- the handler registered with `grpc.dispatch` in this actor.
---@param addr string Listen address, e.g. "0.0.0.0:50051"
---@param opts? grpc.ListenOptions
---@return integer listener_fd
function M.listen(addr, opts)
    return c.listen(addr, opts)
end

--- Stop a gRPC server listener by its fd.
---@param fd integer The listener fd returned by `listen`
---@return boolean success
function M.stop(fd)
    return c.stop(fd)
end

--- Register the inbound RPC handler. `fn(stream, path)` runs in its own
--- coroutine; the response stream is auto-finished with OK if the handler
--- returns without calling `stream:finish`, or INTERNAL (13) if it errors.
---@param fn fun(stream: grpc.ServerStream, path: string)
function M.dispatch(fn)
    moon.dispatch("grpc", function(_sender, _session, path, handle)
        local stream = setmetatable({ obj = handle, path = path }, ServerStream)
        local ok, err = pcall(fn, stream, path)
        if ok then
            stream:finish(0)
        else
            stream:finish(13, tostring(err)) -- 13 = INTERNAL
        end
    end)
end

return M
