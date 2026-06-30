---
--- example_grpc.lua — Native gRPC client (grpc.core) + protobuf encoding.
---
--- Prereqs:
---   1. A running gRPC server. The tonic "helloworld"/"route_guide" examples
---      work out of the box:
---        git clone https://github.com/hyperium/tonic && cd tonic
---        cargo run --release --bin helloworld-server      # :50051
---        cargo run --release --bin routeguide-server      # :10000
---
---   2. A serialized FileDescriptorSet for the proto(s), generated with protoc:
---        protoc --include_imports \
---               --descriptor_set_out=assets/example/grpc/helloworld.pb \
---               -I path/to/protos helloworld.proto route_guide.proto
---
---      (The .proto sources live in the tonic repo under examples/proto/.)
---
--- Run:  moon_rs assets/example/example_grpc.lua
---

local moon     = require("moon")
local protobuf = require("protobuf")
local grpc     = require("moon.grpc")

local DESC_PATH = "assets/example/grpc/helloworld.pb"

local function readfile(path)
    local f = io.open(path, "rb")
    if not f then return nil end
    local data = f:read("*a")
    f:close()
    return data
end

moon.async(function()
    print("=== gRPC Native Client Example ===\n")

    -- 1. Load the protobuf descriptor (shared, process-global).
    local desc = readfile(DESC_PATH)
    if not desc then
        print("missing descriptor: " .. DESC_PATH)
        print("generate it with protoc (see header comment).")
        moon.exit(-1)
        return
    end
    protobuf.load(desc)

    -- 2. Connect (http:// = plaintext h2c; https:// auto-enables TLS).
    local conn, err = grpc.connect({
        endpoint = "http://127.0.0.1:50051",
        name     = "greeter",
    })
    if not conn then
        print("connect failed:", err)
        moon.exit(-1)
        return
    end
    print("connected to greeter")

    -----------------------------------------------------------
    -- 3. Unary call
    -----------------------------------------------------------
    print("\n--- unary: SayHello ---")
    local reply, status = conn:unary(
        "/helloworld.Greeter/SayHello",
        "helloworld.HelloRequest",
        { name = "moon_rs" },
        "helloworld.HelloReply"
    )
    if reply then
        print("reply.message =", reply.message)
    else
        print(string.format("rpc error: code=%d msg=%s", status.code, status.message))
    end

    -----------------------------------------------------------
    -- 4. Server streaming (requires route_guide-server on :10000)
    -----------------------------------------------------------
    -- local rg = grpc.connect({ endpoint = "http://127.0.0.1:10000", name = "rg" })
    -- local stream = rg:server_stream(
    --     "/routeguide.RouteGuide/ListFeatures",
    --     "routeguide.Rectangle",
    --     { lo = { latitude = 400000000, longitude = -750000000 },
    --       hi = { latitude = 420000000, longitude = -730000000 } },
    --     "routeguide.Feature"
    -- )
    -- while true do
    --     local feature, serr = stream:recv()
    --     if not feature then
    --         if serr then print("stream error:", serr) end
    --         break -- nil + no error => end of stream
    --     end
    --     print("feature:", feature.name)
    -- end
    -- stream:close()

    -----------------------------------------------------------
    -- 5. Bidirectional / client streaming
    -----------------------------------------------------------
    -- local chat = rg:bidi_stream(
    --     "/routeguide.RouteGuide/RouteChat",
    --     "routeguide.RouteNote",  -- request type for :send
    --     "routeguide.RouteNote"   -- response type for :recv
    -- )
    -- for i = 1, 3 do
    --     chat:send({ location = { latitude = i, longitude = i }, message = "note-" .. i })
    --     local note = chat:recv()
    --     if note then print("recv:", note.message) end
    -- end
    -- chat:close_send()
    -- chat:close()

    grpc.close("greeter")
    print("\n=== example_grpc done ===")
    moon.exit(0)
end)

moon.shutdown(function()
    moon.quit()
end)
