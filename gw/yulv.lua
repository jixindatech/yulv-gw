local require = require
local tcp = ngx.socket.tcp

local tab = require("gw.core.table")
local protocol     = require("gw.yulv.protocol")
local const        = require("gw.yulv.const")

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

    local sql = protocol:new({reqsock = ngx.req.socket(), sqlsock = sqlsock})
    ok, err = sql:do_handshake()
    if err ~= nil then
        ngx.log(ngx.ERR, err)
        return
    end

    while true
    do
        --[[ read from client ]]--
        local resp
        resp, err = sql:get_reqsock_data()
        if err then
            return nil, "get reqsock data failed:" .. err
        end

        --https://dev.mysql.com/doc/internals/en/text-protocol.html
        local reqtyp = sql:parse_req(resp)

        --[[ proxy to sql server ]]--
        local bytes
        bytes, err = sql.write_sqlsock(sql.sqlsock, resp)
        if err then
            return nil, "socket send data failed:" .. err
        end

        --[[ read from sql server ]]--
        local typ
        resp, typ, err = sql:recv_sql_packet()
        if err then
            return nil, "recv sql packet failed:" .. err
        end

        sql._sqldata = resp

        if reqtyp == const.cmd.COM_FIELD_LIST then
            err = sql:cmd_filed_list(typ)
            if err ~= nil then
                return
            end

            bytes, err = sql.write_reqsock(sql.reqsock, sql._sqldata)
            sql._sqldata = nil
        elseif reqtyp == const.cmd.COM_QUERY then
            err = sql:cmd_query()
            bytes, err = sql.write_reqsock(sql.reqsock, sql._sqldata)
            sql._sqldata = nil
        elseif reqtyp == const.cmd.COM_QUIT then
            bytes, err = sql.write_reqsock(sql.reqsock, sql._sqldata)
            sql._sqldata = nil
        elseif reqtyp == const.cmd.COM_PING then
            bytes, err = sql.write_reqsock(sql.reqsock, sql._sqldata)
            sql._sqldata = nil
        elseif reqtyp == const.cmd.COM_INIT_DB then
            bytes, err = sql.write_reqsock(sql.reqsock, sql._sqldata)
            sql._sqldata = nil
        elseif reqtyp == const.cmd.COM_STMT_PREPARE then
            bytes, err = sql.write_reqsock(sql.reqsock, sql._sqldata)
            sql._sqldata = nil
        elseif reqtyp == const.cmd.COM_STMT_SEND_LONG_DATA then
            -- no response
        elseif reqtyp == const.cmd.COM_STMT_EXECUTE then
            bytes, err = sql.write_reqsock(sql.reqsock, sql._sqldata)
            sql._sqldata = nil
        elseif reqtyp == const.cmd.COM_STMT_CLOSE then
            -- no response
        elseif reqtyp == const.cmd.COM_STMT_RESET then
            bytes, err = sql.write_reqsock(sql.reqsock, sql._sqldata)
            sql._sqldata = nil
        elseif reqtyp == const.cmd.COM_SET_OPTION then
            bytes, err = sql.write_reqsock(sql.reqsock, sql._sqldata)
            sql._sqldata = nil
        else
            ngx.log(ngx.ERR, "unsupported command for sql")
        end
    end

    sql.sqlsock:close()
    return
end

function _M.log_phase()
    local key = ngx.var.remote_addr .. ":" ..ngx.var.remote_port
    connections[key] = nil
end

return _M

