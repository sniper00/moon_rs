local moon = require "moon"
local json = require "json"
local core = require "httpc.core"

moon.register_protocol {
    name = "http",
    PTYPE = moon.PTYPE_HTTPC,
    pack = function(...) return ... end,
}

---@return table
local function tojson(response)
    if response.status_code ~= 200 then return {} end
    return json.decode(response.body)
end

---@class HttpRequestOptions
---@field headers? table<string,string>
---@field timeout? integer Total request timeout in milliseconds. Default 5000ms for non-streaming requests; 0 disables it for streaming requests
---@field read_timeout? integer Maximum idle time between streaming body chunks in milliseconds. Default 10000ms
---@field proxy? string

local client = {}

---@class HttpStream
---@field next fun(self:HttpStream): string?, string?
---@field close fun(self:HttpStream)
---@field __close fun(self:HttpStream, err?: any)

---@param url string
---@param opts? HttpRequestOptions
---@return table|false response
---@return HttpStream|string? stream_or_error
function client.stream(url, opts)
    opts = opts or {}
    opts.url = url
    local response, handle = moon.wait(core.request_stream(opts))
    if response == false then
        return response, handle
    end

    local stream = {
        _handle = handle,
    }

    function stream:next()
        if not self._handle then
            return nil
        end

        local session, err = self._handle:next()
        if not session then
            self._handle = nil
            return nil, err
        end

        local chunk, next_handle = moon.wait(session)
        if chunk == false then
            self._handle = nil
            return nil, next_handle
        end
        self._handle = next_handle
        return chunk
    end

    function stream:close()
        if self._handle then
            self._handle:close()
            self._handle = nil
        end
    end

    setmetatable(stream, {
        __close = function(self)
            self:close()
        end,
    })

    return response, stream
end

local function parse_sse_frame(frame)
    local data = {}
    for line in frame:gmatch("[^\r\n]+") do
        if line:sub(1, 5) == "data:" then
            local value = line:sub(6)
            if value:sub(1, 1) == " " then
                value = value:sub(2)
            end
            data[#data + 1] = value
        end
    end
    return table.concat(data, "\n")
end

local function next_sse_event(stream)
    local pending = ""
    local done = false

    return function()
        if done then
            return nil
        end

        while true do
            local start, finish = pending:find("\n\n", 1, true)
            if not start then
                start, finish = pending:find("\r\n\r\n", 1, true)
            end

            if start then
                local frame = pending:sub(1, start - 1)
                pending = pending:sub(finish + 1)
                local payload = parse_sse_frame(frame)
                if payload == "" then
                    -- Comment/heartbeat event; keep reading.
                elseif payload == "[DONE]" then
                    done = true
                    stream:close()
                    return nil
                else
                    return payload
                end
            else
                local chunk, err = stream:next()
                if not chunk then
                    done = true
                    if pending ~= "" then
                        local payload = parse_sse_frame(pending)
                        pending = ""
                        if payload ~= "" and payload ~= "[DONE]" then
                            return payload, err
                        end
                    end
                    return nil, err
                end
                pending = pending .. chunk
            end
        end
    end
end

---@param url string
---@param opts? HttpRequestOptions
---@return table|false response
---@return table|string? stream_or_error
function client.stream_sse(url, opts)
    local response, stream = client.stream(url, opts)
    if response == false then
        return response, stream
    end

    local next_event = next_sse_event(stream)
    local events = {}
    function events:next()
        local payload, err = next_event()
        if not payload then
            return nil, err
        end
        local ok, value = pcall(json.decode, payload)
        if not ok then
            return nil, value
        end
        return value
    end
    function events:close()
        stream:close()
    end

    setmetatable(events, {
        __close = function(self)
            self:close()
        end,
    })

    return response, events
end

---@param url string
---@param opts? HttpRequestOptions
---@return HttpResponse
function client.get(url, opts)
    opts = opts or {}
    opts.url = url
    opts.method = "GET"
    return moon.wait(core.request(opts))
end

local json_content_type = { ["Content-Type"] = "application/json" }

---@param url string
---@param data table
---@param opts? HttpRequestOptions
---@return HttpResponse
function client.post_json(url, data, opts)
    opts = opts or {}
    if not opts.headers then
        opts.headers = json_content_type
    else
        if not opts.headers['Content-Type'] then
            opts.headers['Content-Type'] = "application/json"
        end
    end

    opts.url = url
    opts.method = "POST"
    opts.body = json.encode(data)

    local res = moon.wait(core.request(opts))

    if res.status_code == 200 then
        res.body = tojson(res)
    end
    return res
end

---@param url string
---@param data string
---@param opts? HttpRequestOptions
---@return HttpResponse
function client.post(url, data, opts)
    opts = opts or {}
    opts.url = url
    opts.body = data
    opts.method = "POST"
    return moon.wait(core.request(opts))
end

local form_headers = { ["Content-Type"] = "application/x-www-form-urlencoded" }

---@param url string
---@param data table<string,string>
---@param opts? HttpRequestOptions
---@return HttpResponse
function client.post_form(url, data, opts)
    opts = opts or {}
    if not opts.headers then
        opts.headers = form_headers
    else
        if not opts.headers['Content-Type'] then
            opts.headers['Content-Type'] = "application/x-www-form-urlencoded"
        end
    end

    opts.body = {}
    for k, v in pairs(data) do
        opts.body[k] = tostring(v)
    end

    opts.url = url
    opts.method = "POST"
    opts.body = core.form_urlencode(opts.body)

    return moon.wait(core.request(opts))
end

return client
