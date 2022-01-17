local require = require
local tcp = ngx.socket.tcp

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
        connections[key].status = protocol.WaitForServerGreeting
        local sock, err = tcp()
        if not sock then
            return nil, err
        end
        connections[key].mysqlsock = sock
        local port =  3306
        local host = "127.0.0.1"
        local ok, err = sock:connect(host, port)
        if not ok then
            return nil, "socke connect failed:" .. err
        end

        local header, data = protocol.get_server_greeting(sock)
        local reqsock = ngx.req.socket()
        local bytes, err = reqsock:send(header)
        if not bytes then
            return nil, "socket send header failed"
        end
        bytes, err = reqsock:send(data)
        if not bytes then
            return nil, "socket send data failed"
        end
        connections[key].status = protocol.WaitForClientHello
    end

    if connections[key].status == protocol.WaitForClientHello then
        local reqsock = ngx.req.socket()
        local header, data = protocol.get_client_hello(reqsock)
        local mysqlsock = connections[key].mysqlsock

        local bytes, err = mysqlsock:send(header)
        if not bytes then
            return nil, "socket send header failed"
        end
        bytes, err = mysqlsock:send(data)
        if not bytes then
            return nil, "socket send data failed"
        end
        connections[key].status = protocol.WaitForServerHello

        header, data = protocol.get_server_hello(mysqlsock)
        if not header then
            return nil, "get server hello data failed"
        end
        bytes, err = reqsock:send(header)
        if not bytes then
            return nil, "socket send header failed"
        end
        bytes, err = reqsock:send(data)
        if not bytes then
            return nil, "socket send data failed"
        end
        connections[key].status = protocol.WaitForAuthSwitchRequest
    end

    if connections[key].status == protocol.WaitForAuthSwitchRequest then
        local reqsock = ngx.req.socket()
        local header, data = protocol.get_client_authrequest_data(reqsock)
        if not header then
            return nil, "get header failed"
        end

        local mysqlsock = connections[key].mysqlsock
        local bytes, err = mysqlsock:send(header)
        if not bytes then
            return nil, "socket send header failed"
        end
        bytes, err = mysqlsock:send(data)
        if not bytes then
            return nil, "socket send data failed"
        end
        connections[key].status = protocol.WaitForAuthSwitchResponse

        header, data = protocol.get_server_authresponse_data(mysqlsock)
        if not header then
            return nil, "get header failed"
        end
        bytes, err = reqsock:send(header)
        if not bytes then
            return nil, "socket send header failed"
        end
        bytes, err = reqsock:send(data)
        if not bytes then
            return nil, "socket send data failed"
        end

        connections[key].status = protocol.OK
    end

    if connections[key].status == protocol.OK then
        local reqsock = ngx.req.socket()
        local header, data = protocol.get_client_data(reqsock)
        if not header then
            return nil, "get header failed"
        end

        local mysqlsock = connections[key].mysqlsock
        local bytes, err = mysqlsock:send(header)
        if not bytes then
            return nil, "socket send header failed"
        end
        bytes, err = mysqlsock:send(data)
        if not bytes then
            return nil, "socket send data failed"
        end

    end
    --[[
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
    --]]
    end

function _M.log_phase()
    local key = ngx.var.remote_addr .. ":" ..ngx.var.remote_port
    connections[key] = nil
end

return _M

