local moon = require "moon"
local json = require "json"
local core = require "http.core"

---@class HttpResponse
---@field public version string @ http version
---@field public status_code integer @ Integer Code of responded HTTP Status, e.g. 404 or 200. -1 means socket error and content is error message
---@field public headers table<string,any> @in lower-case key
---@field public body string|table @ raw body string| json table

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

local client = {}

---@param uri string @host:port
---@param headers? table
function client.get(uri, headers)
    return parse_raw_response(moon.wait(core.request({
        uri = uri,
        method = "GET",
        headers = headers
    })))
end

local json_content_type = { ["Content-Type"] = "application/json" }
---@return HttpResponse
function client.post_json(uri, tb)
    local res = parse_raw_response(moon.wait(core.request({
        uri = uri,
        method = "POST",
        headers = json_content_type,
        content = json.encode(tb),
    })))

    if res.status_code == 200 then
        res.body = tojson(res)
    end
    return res
end

local function escape(s)
    return (string.gsub(s, "([^A-Za-z0-9_])", function(c)
        return string.format("%%%02X", string.byte(c))
    end))
end

local form_content_type = { ["Content-Type"] = "application/x-www-form-urlencoded" }
function client.post_form(uri, tb)
    local body = {}
    for k, v in pairs(tb) do
        table.insert(body, string.format("%s=%s", escape(k), escape(v)))
    end

    return parse_raw_response(moon.wait(core.request({
        uri = uri,
        method = "POST",
        content = table.concat(body, "&"),
        headers = form_content_type
    })))
end

return client
