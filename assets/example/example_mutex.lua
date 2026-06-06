local moon = require("moon")
local mutex = require("moon.mutex")

local lock = mutex()
local tb = {}

for i=1,1000 do
	local co = coroutine.create(function(v)
		print("lock", v)
		local scope_lock<close> = lock()
		if 1==v then
			print(coroutine.yield(v), v)
		end
		print("unlock", v)
	end)
	tb[#tb+1] = co
	coroutine.resume(co,i )
end

print("########")
coroutine.resume( tb[1],"start")

local scope_lock = mutex()

moon.async(function()
	local lock<close> = scope_lock()
	moon.sleep(1000)
	print(1)
end)

moon.async(function()
	local lock<close> = scope_lock()
	moon.sleep(2000)
    print(2)
	assert(false)
end)

moon.async(function()
	local lock<close> = scope_lock()
	moon.sleep(1000)
	print(3)
end)

moon.async(function()
	local lock<close> = scope_lock()
	moon.sleep(500)
	print(4)

    moon.exit(100)
end)
