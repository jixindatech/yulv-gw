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
        local resp
        resp, err = sql:get_reqsock_data()
        if err then
            return nil, "get reqsock data failed:" .. err
        end

        local reqtyp = sql:parse_req(resp)

        local bytes
        bytes, err = sql.write_sqlsock(sql.sqlsock, resp)
        if err then
            return nil, "socket send data failed:" .. err
        end

        local typ
        resp, typ, err = sql:recv_sql_packet()
        if err then
            return nil, "recv sql packet failed:" .. err
        end

        local packet = resp.data
        if typ == protocol.RESP_ERR then
            bytes, err = sql.write_reqsock(sql.reqsock, resp)
            if err then
                return nil, "write reqsock failed:" .. err
            end

            goto CONTINUE
            --local errno, msg, sqlstate = _parse_err_packet(packet)
            --return nil, msg, errno, sqlstate
        end

        if typ == protocol.RESP_OK then
            bytes, err = sql.write_reqsock(sql.reqsock, resp)
            if err then
                return nil, "write reqsock failed:" .. err
            end

            goto CONTINUE
            --[[
            local res = _parse_ok_packet(packet)
            if res and band(res.server_status, SERVER_MORE_RESULTS_EXISTS) ~= 0 then
                return res, "again"
            end
            self.state = STATE_CONNECTED
            return res
            ]]--
        end

        if typ == protocol.RESP_LOCALINFILE then
            bytes, err = sql.write_reqsock(sql.reqsock, resp)
            if err then
                return nil, err
            end

            goto CONTINUE
            --[[
            self.state = STATE_CONNECTED

            return nil, "packet type " .. typ .. " not supported"
            ]]--
        end

        if reqtyp == const.COM_QUERY then

        end
        -- typ == RESP_DATA or RESP_AUTHMOREDATA(also mean RESP_DATA here)

        --print("read the result set header packet")

        bytes, err = sql.write_reqsock(sql.reqsock, resp)
        if err then
            return nil, err
        end

        local field_count, extra = sql.parse_result_set_header_packet(packet)
        ngx.log(ngx.ERR, 'filed count:'.. field_count)
        --print("field count: ", field_count)

        local cols = new_tab(field_count, 0)
        for i = 1, field_count do
            sql.sqldata = nil
            local col, errno, sqlstate
            col, err, errno, sqlstate = sql:recv_field_packet()
            if sql.sqldata == nil then
                return nil, err
            end

            resp = sql.sqldata
            bytes, err = sql.write_reqsock(sql.reqsock, resp)
            if err then
                return nil, err
            end

            --[[
            if not col then
                return nil, err, errno, sqlstate
            end

            cols[i] = col
            ]]--
        end

        resp, typ, err = sql:recv_sql_packet()
        if err then
            return nil, err
        end

        if typ ~= protocol.RESP_EOF then
            return nil, "unexpected packet type " .. typ .. " while eof packet is "
                    .. "expected"
        end

        bytes, err = sql.write_reqsock(sql.reqsock, resp)
        if err then
            return nil, err
        end

        local rows = new_tab(4, 0)
        local i = 0
        while true do
            --print("reading a row")

            resp, typ, err = sql:recv_sql_packet()
            if not resp then
                return nil, err
            end

            bytes, err = sql.write_reqsock(sql.reqsock, resp)
            if err then
                return nil, err
            end

            if typ == protocol.RESP_EOF then
                goto CONTINUE
                --[[
                local warning_count, status_flags = _parse_eof_packet(packet)

                --print("status flags: ", status_flags)

                if band(status_flags, SERVER_MORE_RESULTS_EXISTS) ~= 0 then
                    return rows, "again"
                end

                break
                ]]--
            end

            --[[
            packet = resp.data
            if typ == RESP_EOF then
                local warning_count, status_flags = _parse_eof_packet(packet)

                --print("status flags: ", status_flags)

                if band(status_flags, SERVER_MORE_RESULTS_EXISTS) ~= 0 then
                    return rows, "again"
                end

                break
            end

            local row = _parse_row_data_packet(packet, cols, nil)
            i = i + 1
            rows[i] = row
            ]]--
        end

        ::CONTINUE::
    end

    return
end

function _M.log_phase()
    local key = ngx.var.remote_addr .. ":" ..ngx.var.remote_port
    connections[key] = nil
end

return _M

