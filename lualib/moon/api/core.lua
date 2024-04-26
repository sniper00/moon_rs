error("DO NOT REQUIRE THIS FILE")

---@meta

--- lightuserdata, cpp type `buffer*`
---@class buffer_ptr

--- lightuserdata, cpp type `message*`
---@class message_ptr

--- lightuserdata, cpp type `char*`
---@class cstring_ptr

---@class core
---@field public id integer @service's id
---@field public name string @service's name
---@field public timezone integer @server's local timezone
local core = {}

---same as os.clock()
---@return number @in seconds
function core.clock() end

---@alias hash_algorithm
---| 'md5'
---| 'sha1'
---| 'sha256'
---| 'sha512'

---string's md5.
---@param alg hash_algorithm
---@param data string
---@return string
function core.hash(alg, data) end

---convert c-string(char* and size) to lua-string
---@param sz cstring_ptr
---@param len integer
---@return string
function core.tostring(sz, len) end

-- ---@alias server_stats_options
-- ---|>'"service.count"'      # return total services count
-- ---| '"log.error"'         # return log error count

-- ---@param opt? server_stats_options @ nil return all server stats
-- ---@return string @get server stats in json format
-- function core.server_stats(opt) end

---if linked mimalloc, call mimalloc's collect
---@param force boolean @
function core.collect(force) end

--- print console log
function core.log(loglv,...) end

--- set or get log level
---@param lv? string @DEBUG, INFO, WARN, ERROR
---@return integer @ log level
function core.loglevel(lv) end

-- --- get this service's cpu cost time
-- ---@return integer
-- function core.cpu() end

--- remove a service
function core.kill(addr) end

--- Retrieves the ID of a unique service based on its `name`.
--- @param name string @ The name of the service.
--- @return integer @ Returns 0 if the service does not exist.
function core.query(name) end

--- set or get env
---@param key string
---@param value? string
---@return string
function core.env(key, value) end

--- let server exit: exitcode>=0 will wait all services quit.
---@param exitcode integer
function core.exit(exitcode) end

-- --- adjusts server time(millsecond)
-- ---@param milliseconds integer
-- function core.adjtime(milliseconds) end

-- --- set lua callback
-- ---@param fn fun(msg:userdata,ptype:integer)
-- function core.callback(fn)
--     ignore_param(fn)
-- end

--- Get server timestamp in milliseconds
--- @return integer
function core.now() end

--- get message's field
---
--- - 'S' message:sender()
--- - 'R' message:receiver()
--- - 'E' message:sessionid()
--- - 'Z' message:bytes()
--- - 'N' message:size()
--- - 'B' message:buffer()
--- - 'C' message:buffer():data() and message:buffer():size()
---@param msg message_ptr
---@param pattern string
---@return ...
---@nodiscard
function core.decode(msg, pattern) end

---@param data string
---@return string
function core.base64_encode(data) end

---@param base64str string
---@return string
function core.base64_decode(base64str) end


return core
