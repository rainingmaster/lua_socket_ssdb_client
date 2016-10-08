local sub = string.sub
local insert = table.insert
local concat = table.concat
local len = string.len
local pairs = pairs
local unpack = unpack
local setmetatable = setmetatable
local tonumber = tonumber
local error = error
local gmatch = string.gmatch
local remove = table.remove

local host = "10.1.1.43"
local port = "8888"


local _M = {
    _VERSION = '0.02'
}


local mt = { __index = _M }


local commands = {
    "set",                  "get",                 "del",
    "scan",                 "rscan",               "keys",
    "incr",                 "decr",                "exists",
    "multi_set",            "multi_get",           "multi_del",
    "multi_exists",
    "hset",                 "hget",                "hdel",
    "hscan",                "hrscan",              "hkeys",
    "hincr",                "hdecr",               "hexists",
    "hsize",                "hlist",               "hgetall",
    "hclear",
    --[[ "multi_hset", ]]   "multi_hget",          "multi_hdel",
    "multi_hexists",        "multi_hsize",
    "zset",                 "zget",                "zdel",
    "zscan",                "zrscan",              "zkeys",
    "zincr",                "zdecr",               "zexists",
    "zsize",                "zlist",
    --[[ "multi_zset", ]]   "multi_zget",          "multi_zdel",
    "multi_zexists",        "multi_zsize",         "setnx",
    "expire",               "ttl",                 "setx",

}


function _M.connect(host, port)
    local sock   = require "socket"

    local db = sock.connect(host, port)
    if not db then
        return nil, "not initialized"
    end
    return setmetatable({ sock = db, config = config }, mt)
end

local function close(self)
    local sock = self.sock
    if not sock then
        return nil, "not initialized"
    end

    return sock:close()
end
_M.close = close

local function _read_reply(sock)
	local val = {}

	while true do
		-- read block size
		local line, err, partial = sock:receive()
		if not line or len(line)==0 then
			-- packet end
			if err == "timeout" then
                sock:close()
    			return nil, err
            end
            break
		end
		local d_len = tonumber(line)

		-- read block data
		local data, err, partial = sock:receive(d_len)
		
		if not data then
        if err == "timeout" then
            sock:close()
        end
        return nil, err
    end
		
		val[#val + 1] = data;

		-- ignore the trailing lf/crlf after block data
		local line, err, partial = sock:receive()
	end

	local v_num = tonumber(#val)

	if v_num == 1 then
		return val
	else
		remove(val,1)
		return val
	end
end


local function _gen_req(args)
    local req = {}

    for i = 1, #args do
        local arg = args[i]

        if arg then
            req[#req + 1] = len(arg)
            req[#req + 1] = "\n"
            req[#req + 1] = arg
            req[#req + 1] = "\n"
        else
            return nil
        end
    end
    req[#req + 1] = "\n"

    -- it is faster to do string concatenation on the Lua land
    --print("request: ", table.concat(req, ""))

    return concat(req, "")
end


local function _do_cmd(self, ...)
    local args = {...}

    local sock = self.sock
    if not sock then
        return nil, "not initialized"
    end

    local req = _gen_req(args)

    local reqs = self._reqs
    if reqs then
        reqs[#reqs + 1] = req
        return
    end

    local bytes, err = sock:send(req)
    if not bytes then
        return nil, err
    end

    return _read_reply(sock)
end


for i = 1, #commands do
    local cmd = commands[i]

    _M[cmd] =
        function (self, ...)
            return _do_cmd(self, cmd, ...)
        end
end


function _M.multi_hset(self, hashname, ...)
    local args = {...}
    if #args == 1 then
        local t = args[1]
        local array = {}
        for k, v in pairs(t) do
            array[#array + 1] = k
            array[#array + 1] = v
        end
        -- print("key", hashname)
        return _do_cmd(self, "multi_hset", hashname, unpack(array))
    end

    -- backwards compatibility
    return _do_cmd(self, "multi_hset", hashname, ...)
end


function _M.multi_zset(self, keyname, ...)
    local args = {...}
    if #args == 1 then
        local t = args[1]
        local array = {}
        for k, v in pairs(t) do
            array[#array + 1] = k
            array[#array + 1] = v
        end
        -- print("key", keyname)
        return _do_cmd(self, "multi_zset", keyname, unpack(array))
    end

    -- backwards compatibility
    return _do_cmd(self, "multi_zset", keyname, ...)
end


function _M.init_pipeline(self)
    self._reqs = {}
end


function _M.cancel_pipeline(self)
    self._reqs = nil
end


function _M.commit_pipeline(self)
    local reqs = self._reqs
    if not reqs then
        return nil, "no pipeline"
    end

    self._reqs = nil

    local sock = self.sock
    if not sock then
        return nil, "not initialized"
    end

    local bytes, err = sock:send(concat(reqs, ""))
    if not bytes then
        return nil, err
    end

    local vals = {}
    for i = 1, #reqs do
        local res, err = _read_reply(sock)
        if res then
            vals[#vals + 1] = res

        elseif res == nil then
            if err == "timeout" then
                close(self)
            end
            return nil, err

        else
            vals[#vals + 1] = err
        end
    end

    return vals
end


function _M.array_to_hash(self, t)
    local h = {}
    for i = 1, #t, 2 do
        h[t[i]] = t[i + 1]
    end
    return h
end


local class_mt = {
    -- to prevent use of casual module global variables
    __newindex = function (table, key, val)
        error('attempt to write to undeclared variable "' .. key .. '"')
    end
}


function _M.add_commands(...)
    local cmds = {...}
    local newindex = class_mt.__newindex
    class_mt.__newindex = nil
    for i = 1, #cmds do
        local cmd = cmds[i]
        _M[cmd] =
            function (self, ...)
                return _do_cmd(self, cmd, ...)
            end
    end
    class_mt.__newindex = newindex
end


setmetatable(_M, class_mt)


return _M

