local require = require
local tcp = ngx.socket.tcp

local tab = require("gw.core.table")
local protocol     = require("gw.yulv.protocol")

local ngx = ngx
local str_byte  = string.byte

local conf_file = ngx.config.prefix() .. "etc/ss.yaml"
local module = {}
local module_name = "yulv"
local connections = {}

local ok, new_tab = pcall(require, "table.new")
if not ok then
    new_tab = function (narr, nrec) return {} end
end

local _M = { version = "0.1"}

_M.name = module_name

function _M.stream_init_worker()
    return true, nil
end

function _M.content_phase()
    local key = ngx.var.remote_addr .. ":" ..ngx.var.remote_port
    connections[key] = true

    --[[ sql socket ]]--
    local sqlsock, err = tcp()
    if not sqlsock then
        return nil, err
    end
    local port =  3306
    local host = "192.168.91.1"
    ok, err = sqlsock:connect(host, port)
    if not ok then
        return nil, "socke connect failed:" .. err
    end

    local context = protocol:new({reqsock = ngx.req.socket(), sqlsock = sqlsock})
    ok, err = context:do_handshake()
    if err ~= nil then
        ngx.log(ngx.ERR, err)
        return
    end

    ok, err = context:proxy_sql()
    if err ~= nil then
        ngx.log(ngx.ERR, err)
    end

    return
end

function _M.log_phase()
    local key = ngx.var.remote_addr .. ":" ..ngx.var.remote_port
    connections[key] = nil
end

return _M

