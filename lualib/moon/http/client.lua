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
function client.post_json(uri, tb, headers)
    if not headers then
        headers = json_content_type
    else
        if not headers['Content-Type'] then
            headers['Content-Type'] = "application/json"
        end
    end

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

function client.post(uri, content, headers)
    return parse_raw_response(moon.wait(core.request({
        uri = uri,
        method = "POST",
        content = content,
        headers = headers
    })))
end

local form_headers = { ["Content-Type"] = "application/x-www-form-urlencoded" }

function client.post_form(uri, form, headers)
    if not headers then
        headers = form_headers
    else
        if not headers['Content-Type'] then
            headers['Content-Type'] = "application/x-www-form-urlencoded"
        end
    end
    for k, v in pairs(form) do
        form[k] = tostring(v)
    end

    return parse_raw_response(moon.wait(core.request({
        uri = uri,
        method = "POST",
        content = core.encode_query_string(form),
        headers = headers
    })))
end

return client
