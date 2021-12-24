local require = require
local lfs   = require("lfs")
local yaml  = require("tinyyaml")
local cjson = require("cjson.safe")

local tab = require("gw.core.table")
local protocol     = require("gw.plugins.yulv.protocol")

local ngx = ngx
local str_byte  = string.byte

local conf_file = ngx.config.prefix() .. "etc/ss.yaml"
local module = {}
local module_name = "yulv"
local connections = {}
local _M = { version = "0.1"}

_M.name = module_name

function _M.content_phase()
    ngx.log(ngx.ERR, "ip" .. ngx.var.remote_addr)
    ngx.log(ngx.ERR, "port" .. ngx.var.remote_port)
    local key = ngx.var.remote_addr .. ":" ..ngx.var.remote_port
    if connections[key] == nil then
        connections[key] = tab.new(20, 0)
        connections[key].status = protocol.ClientHandShake
    end

    local ctx = connections[key]
    if ctx.status == protocol.ClientHandShake then
        ngx.log(ngx.ERR, "HandleShake")
        ctx.status = protocol.ServerHandShake
        return true, nil
    end

    local reqsock = ngx.req.socket()
    local data, err = reqsock:peek(protocol.Header)
    if not data then
        ngx.log(ngx.ERR, "err:" .. err)
        return nil, "socket get data failed"
    end

    ngx.log(ngx.ERR, "type data:" .. type(data))

    ctx.status = protocol.OK
end

function _M.log_phase()
    local key = ngx.var.remote_addr .. ":" ..ngx.var.remote_port
    connections[key] = nil
end

return _M

