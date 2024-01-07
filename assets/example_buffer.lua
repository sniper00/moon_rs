local moon= require("moon")
local buffer = require "buffer"

do
    local p = buffer.concat(string.pack("hI", 11, 12))

    print(buffer.unpack(p, "hI"))
    
    buffer.drop(p)
end

do
    local p = buffer.concat(string.pack(">H", 16), "hello", "world")

    print(buffer.unpack(p, ">HZ"))

    buffer.drop(p)
end
