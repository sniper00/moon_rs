local moon = require("moon")
local json = require("json")
local httpc = require("moon.http.client")

local http_server = require("moon.http.server")

-- http_server.content_max_len = 8192

http_server.error = function(fd, err)
    print("http server fd", fd, " disconnected:", err)
end

http_server.on("/hello", function(request, response)
    print_r(request:query())
    response:write_header("Content-Type", "text/plain")
    response:write("GET:Hello World")
end)

http_server.on("/chat", function(request, response)
    print(request.body)
    response:write_header("Content-Type", "text/plain")
    response:write("POST:Hello World/home")
end)

http_server.on("/login", function(request, response)
    print_r(request:form())
    response:write_header("Content-Type", "application/json")
    response:write(json.encode({ score = 112, level = 100, item = { id = 1, count = 2 } }))
end)

http_server.listen("127.0.0.1:19991")
print("http_server start", "127.0.0.1", 19991)

moon.async(function()
    local response = httpc.get("https://bing.com")
    assert(response.status_code == 200, "GET request failed")
    
    -- Test case for GET request
    local response = httpc.get("http://127.0.0.1:19991/hello?a=1&b=2")
    assert(response.status_code == 200, "GET request failed")
    assert(response.body == "GET:Hello World", "GET request failed")

    -- Test case for POST request
    local response = httpc.post("http://127.0.0.1:19991/chat", "Hello Post")
    assert(response.status_code == 200, "POST request failed")
    assert(response.body == "POST:Hello World/home", "POST request failed")

    -- Test case for POST form request
    local form = { username = "wang", passwd = "456", age = 110 }
    local response = httpc.post_form("http://127.0.0.1:19991/login", form)
    assert(response.status_code == 200, "POST form request failed")
    local body = json.decode(response.body)
    assert(body.score == 112 and body.level == 100 and body.item.id == 1 and body.item.count == 2, "POST form request failed")

    print("All test cases passed")

    moon.quit()
end)

