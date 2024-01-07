local moon = require "moon"
local json = require "json"
local core = require "http.core"

local client = {}

function client.get(uri)
    local session = moon.make_session(moon.id)
    core.request({
        id = moon.id,
        session = session,
        uri = uri,
        method = "GET",
    })
    local res, err = moon.wait(session)
    if not res then
        return { status_code = -1, content = err }
    end
    return json.decode(res)
end

local json_content_type = { ["Content-Type"] = "application/json" }
function client.post_json(uri, tb)
    local session = moon.make_session(moon.id)

    core.request({
        id = moon.id,
        session = session,
        uri = uri,
        method = "POST",
        content = json.encode(tb),
        headers = json_content_type
    })

    local res, err = moon.wait(session)
    if not res then
        return { status_code = -1, content = err }
    end
    return json.decode(res)
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

    local session = moon.make_session(moon.id)

    core.request({
        id = moon.id,
        session = session,
        uri = uri,
        method = "POST",
        content = table.concat(body, "&"),
        headers = form_content_type
    })

    local res, err = moon.wait(session)
    if not res then
        return { status_code = -1, content = err }
    end
    return json.decode(res)
end

return client
