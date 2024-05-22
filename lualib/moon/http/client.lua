local moon = require "moon"
local json = require "json"
local core = require "http.core"

---@return table
local function tojson(response)
    if response.status_code ~= 200 then return {} end
    return json.decode(response.body)
end

---@return HttpResponse
local function parse_raw_response(raw_response, err)
    if not raw_response then
        return { status_code = -1, content = err }
    end

    local header_len = string.unpack("<I", raw_response)

    local raw_header = string.sub(raw_response, 5, 4 + header_len)
    local response = core.parse_response(raw_header)
    response.body = string.sub(raw_response, 5 + header_len)

    return response
end

---@class HttpRequestOptions
---@field headers? table<string,string>
---@field timeout? integer
---@field proxy? string

local client = {}

---@param url string
---@param opts? HttpRequestOptions
---@return HttpResponse
function client.get(url, opts)
    opts = opts or {}
    opts.url = url
    opts.method = "GET"
    return parse_raw_response(moon.wait(core.request(opts)))
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

    local res = parse_raw_response(moon.wait(core.request(opts)))

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
    return parse_raw_response(moon.wait(core.request(opts)))
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
    opts.body = core.encode_query_string(opts.body)

    return parse_raw_response(moon.wait(core.request(opts)))
end

return client
