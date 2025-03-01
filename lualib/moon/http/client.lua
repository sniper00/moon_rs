local moon = require "moon"
local json = require "json"
local core = require "http.core"

moon.register_protocol {
    name = "http",
    PTYPE = moon.PTYPE_HTTP,
    pack = function(...) return ... end,
    unpack =  function (val)
        return core.decode(val)
    end,
}

---@return table
local function tojson(response)
    if response.status_code ~= 200 then return {} end
    return json.decode(response.body)
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
