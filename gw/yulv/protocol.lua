local tonumber = tonumber
local setmetatable = setmetatable
local string = string
local sub = string.sub
local strbyte = string.byte
local strchar = string.char
local strfind = string.find
local format = string.format
local strrep = string.rep
local concat = table.concat
local error = error

local bit = bit
local tohex = bit.tohex
local bor = bit.bor
local lshift = bit.lshift
local rshift = bit.rshift
local band = bit.band

local const = require("gw.yulv.const")

-- refer to https://dev.mysql.com/doc/internals/en/capability-flags.html#packet-Protocol::CapabilityFlags
-- CLIENT_LONG_PASSWORD | CLIENT_FOUND_ROWS | CLIENT_LONG_FLAG
-- | CLIENT_CONNECT_WITH_DB | CLIENT_ODBC | CLIENT_LOCAL_FILES
-- | CLIENT_IGNORE_SPACE | CLIENT_PROTOCOL_41 | CLIENT_INTERACTIVE
-- | CLIENT_IGNORE_SIGPIPE | CLIENT_TRANSACTIONS | CLIENT_RESERVED
-- | CLIENT_SECURE_CONNECTION | CLIENT_MULTI_STATEMENTS | CLIENT_MULTI_RESULTS
local DEFAULT_CLIENT_FLAGS = 0x3f7cf
local CLIENT_SSL = 0x00000800
local CLIENT_PLUGIN_AUTH = 0x00080000
local CLIENT_PLUGIN_AUTH_LENENC_CLIENT_DATA = 0x00200000
local DEFAULT_AUTH_PLUGIN = "mysql_native_password"

local SERVER_MORE_RESULTS_EXISTS = 8

local MIN_PROTOCOL_VER = 10

local STATUS_OK = 0

local HEADER_LEN = 4

if (not ngx.config.subsystem
        or ngx.config.subsystem == "http") -- subsystem is http
        and (not ngx.config.ngx_lua_version
        or ngx.config.ngx_lua_version < 9011) -- old version
then
    error("ngx_lua 0.9.11+ required")
end

local ok, new_tab = pcall(require, "table.new")
if not ok then
    new_tab = function (narr, nrec) return {} end
end

local _M = {
}

_M.RESP_OK = "OK"
_M.RESP_AUTHMOREDATA = "AUTHMOREDATA"
_M.RESP_LOCALINFILE = "LOCALINFILE"
_M.RESP_EOF = "EOF"
_M.RESP_ERR = "ERR"
_M.RESP_DATA = "DATA"

local mt = { __index = _M }


local function _dump(data)
    local len = #data
    local bytes = new_tab(len, 0)
    for i = 1, len do
        bytes[i] = format("%x", strbyte(data, i))
    end
    return concat(bytes, " ")
end

local function _dumphex(data)
    local len = #data
    local bytes = new_tab(len, 0)
    for i = 1, len do
        bytes[i] = tohex(strbyte(data, i), 2)
    end
    return concat(bytes, " ")
end


-- mysql field value type converters
local converters = new_tab(0, 9)
for i = 0x01, 0x05 do
    -- tiny, short, long, float, double
    converters[i] = tonumber
end
converters[0x00] = tonumber  -- decimal
-- converters[0x08] = tonumber  -- long long
converters[0x09] = tonumber  -- int24
converters[0x0d] = tonumber  -- year
converters[0xf6] = tonumber  -- newdecimal


local function _get_byte2(data, i)
    local a, b = strbyte(data, i, i + 1)
    return bor(a, lshift(b, 8)), i + 2
end


local function _get_byte3(data, i)
    local a, b, c = strbyte(data, i, i + 2)
    return bor(a, lshift(b, 8), lshift(c, 16)), i + 3
end


local function _get_byte4(data, i)
    local a, b, c, d = strbyte(data, i, i + 3)
    return bor(a, lshift(b, 8), lshift(c, 16), lshift(d, 24)), i + 4
end


local function _get_byte8(data, i)
    local a, b, c, d, e, f, g, h = strbyte(data, i, i + 7)

    -- XXX workaround for the lack of 64-bit support in bitop:
    -- XXX return results in the range of signed 32 bit numbers
    local lo = bor(a, lshift(b, 8), lshift(c, 16))
    local hi = bor(e, lshift(f, 8), lshift(g, 16), lshift(h, 24))
    return lo + 16777216 * d + hi * 4294967296, i + 8

    -- return bor(a, lshift(b, 8), lshift(c, 16), lshift(d, 24), lshift(e, 32),
    -- lshift(f, 40), lshift(g, 48), lshift(h, 56)), i + 8
end


local function _from_cstring(data, i)
    local last = strfind(data, "\0", i, true)
    if not last then
        return nil, nil
    end

    return sub(data, i, last - 1), last + 1
end


function _M.write_reqsock(sock, resp)
    local bytes, err
    bytes, err = sock:send(resp.header)
    if not bytes then
        return nil, "reqsock send header failed"
    end
    bytes, err = sock:send(resp.data)
    if not bytes then
        return nil, "reqsock send data failed"
    end

    return bytes, nil
end


function _M.write_sqlsock(sock, resp)
    local bytes, err
    bytes, err = sock:send(resp.header)
    if not bytes then
        return nil, "sqlsock send header failed:" .. err
    end
    bytes, err = sock:send(resp.data)
    if not bytes then
        return nil, "sqlsock send data failed:" .. err
    end

    return bytes, nil
end


function _M.recv_sql_packet(self)
    local sock = self.sqlsock
    local header, data
    local err

    header, err = sock:receive(HEADER_LEN) -- packet header
    if not header then
        return nil, nil, "failed to receive sql packet header: " .. err
    end

    --print("packet header: ", _dump(data))

    local len, pos = _get_byte3(header, 1)

    --print("packet length: ", len)

    if len == 0 then
        return nil, nil, "empty packet"
    end

    if len > self._max_packet_size then
        return nil, nil, "packet size too big: " .. len
    end

    local num = strbyte(header, pos)

    --print("recv packet: packet no: ", num)

    self.packet_no = num

    data, err = sock:receive(len)

    --print("receive returned")

    if not data then
        return nil, nil, "failed to read packet content: " .. err
    end

    --print("packet content: ", _dump(data))
    --print("packet content (ascii): ", data)
    --ngx.log(ngx.ERR, "packet content: ", _dump(data))

    local field_count = strbyte(data, 1)

    local typ
    if field_count == 0x00 then
        typ = _M.RESP_OK
    elseif field_count == 0x01 then
        typ = _M.RESP_AUTHMOREDATA
    elseif field_count == 0xfb then
        typ = _M.RESP_LOCALINFILE
    elseif field_count == 0xfe then
        typ = _M.RESP_EOF
    elseif field_count == 0xff then
        typ = _M.RESP_ERR
    else
        typ = _M.RESP_DATA
    end

    return { header = header, data = data }, typ
end


local function _parse_err_packet(packet)
    local errno, pos = _get_byte2(packet, 2)
    local marker = sub(packet, pos, pos)
    local sqlstate
    if marker == '#' then
        -- with sqlstate
        pos = pos + 1
        sqlstate = sub(packet, pos, pos + 5 - 1)
        pos = pos + 5
    end

    local message = sub(packet, pos)
    return errno, message, sqlstate
end


local function _read_ok_result(self)
    local resp, typ, err = _M.recv_sql_packet(self)
    if err then
        return "failed to receive the result packet: " .. err
    end

    self.sqldata = resp
    local packet = resp.data
    if typ == _M.RESP_ERR then
        local errno, msg, sqlstate = _parse_err_packet(packet)
        return msg, errno, sqlstate
    end

    if typ ~= _M.RESP_OK then
        return "bad packet type: " .. typ
    end
end


function _M.get_reqsock_data(self)
    local sock = self.reqsock
    local header, err = sock:receive(HEADER_LEN) -- packet header
    if not header then
        return nil, "failed to receive req packet header: " .. err
    end
    local len, pos = _get_byte3(header, 1)
    --print("packet length: ", len)

    if len == 0 then
        return nil, "empty packet"
    end

    local num = strbyte(header, pos)
    --print("recv packet: packet no: ", num)

    local packet_no = num
    self.packet_no = num

    local data
    data, err = sock:receive(len)
    if not data then
        return nil, "failed to read packet content: " .. err
    end

    return { header = header, data = data }, nil
end

-- refer to https://dev.mysql.com/doc/internals/en/connection-phase-packets.html
local function _read_hand_shake_packet(self)
    local resp, typ, err = _M.recv_sql_packet(self)
    if err then
        return nil, nil, err
    end

    local packet = resp.data
    if typ == _M.RESP_ERR then
        local errno, msg, sqlstate = _parse_err_packet(packet)
        return nil, nil, msg, errno, sqlstate
    end

    local protocol_ver = tonumber(strbyte(packet))
    if not protocol_ver then
        return nil, nil,
        "bad handshake initialization packet: bad protocol version"
    end

    if protocol_ver < MIN_PROTOCOL_VER then
        return nil, nil, "unsupported protocol version " .. protocol_ver
                .. ", version " .. MIN_PROTOCOL_VER
                .. " or higher is required"
    end

    self.protocol_ver = protocol_ver

    local server_ver, pos = _from_cstring(packet, 2)
    if not server_ver then
        return nil, nil,
        "bad handshake initialization packet: bad server version"
    end

    self._server_ver = server_ver

    local thread_id
    thread_id, pos = _get_byte4(packet, pos)

    local scramble = sub(packet, pos, pos + 8 - 1)
    if not scramble then
        return nil, nil, "1st part of scramble not found"
    end

    pos = pos + 9 -- skip filler(8 + 1)

    -- two lower bytes
    local capabilities  -- server capabilities
    capabilities, pos = _get_byte2(packet, pos)

    self._server_lang = strbyte(packet, pos)
    pos = pos + 1

    self._server_status, pos = _get_byte2(packet, pos)

    local more_capabilities
    more_capabilities, pos = _get_byte2(packet, pos)

    self.capabilities = bor(capabilities, lshift(more_capabilities, 16))

    pos = pos + 11 -- skip length of auth-plugin-data(1) and reserved(10)

    -- follow official Python library uses the fixed length 12
    -- and the 13th byte is "\0 byte
    local scramble_part2 = sub(packet, pos, pos + 12 - 1)
    if not scramble_part2 then
        return nil, nil, "2nd part of scramble not found"
    end

    pos = pos + 13

    local plugin, _
    if band(self.capabilities, CLIENT_PLUGIN_AUTH) > 0 then
        plugin, _ = _from_cstring(packet, pos)
        if not plugin then
            -- EOF if version (>= 5.5.7 and < 5.5.10) or (>= 5.6.0 and < 5.6.2)
            -- \NUL otherwise
            plugin = sub(packet, pos)
        end

    else
        plugin = DEFAULT_AUTH_PLUGIN
    end

    self.sqldata = resp
    return scramble .. scramble_part2, plugin

end

local function _read_auth_result(self, old_auth_data, plugin)
    local resp, typ, err = _M.recv_sql_packet(self)
    if err then
        return nil, nil, "failed to receive the result packet: " .. err
    end
    self.sqldata = resp

    local packet = resp.data
    if typ == _M.RESP_OK then
        return _M.RESP_OK, ""
    end

    if typ == _M.RESP_AUTHMOREDATA then
        return sub(packet, 2), ""
    end

    if typ == _M.RESP_EOF then
        if #packet == 1 then -- old pre-4.1 authentication protocol
            return nil, "mysql_old_password"
        end

        local pos

        plugin, pos = _from_cstring(packet, 2)
        if not plugin then
            return nil, nil, "malformed packet"
        end

        return sub(packet, pos), plugin
    end

    if typ == _M.RESP_ERR then
        local errno, msg, sqlstate = _parse_err_packet(packet)
        return errno, sqlstate, msg
    end

    return nil, nil, "bad packet type: " .. typ
end

local function _handle_auth_result(self, old_auth_data, plugin)
    local auth_data, new_plugin, err = _read_auth_result(self, old_auth_data, plugin)
    if err ~= nil then
        local errno, sqlstate = auth_data, new_plugin
        return err, errno, sqlstate
    end

    _M.write_reqsock(self.reqsock, self.sqldata)

    if auth_data == _M.RESP_OK then
        return
    end

    if new_plugin ~= "" then
        if not auth_data then
            auth_data = old_auth_data
        else
            old_auth_data = auth_data
        end

        plugin = new_plugin
        --[[
        local auth_resp, err = _auth(self, auth_data, plugin)
        if not auth_resp then
            return err
        end

        local bytes, err = _send_packet(self, auth_resp, #auth_resp)
        if not bytes then
            return "failed to send client authentication packet: " .. err
        end
        ]]--
        local resp
        resp, err = _M.get_reqsock_data(self)
        if err ~= nil then
            return err
        end
        local bytes
        bytes, err = _M.write_sqlsock(self.sqlsock, resp)
        if err ~= nil then
            return err
        end

        auth_data, new_plugin, err = _read_auth_result(self, old_auth_data,
                plugin)
        if err ~= nil then
            local errno, sqlstate = auth_data, new_plugin
            return err, errno, sqlstate
        end

        _M.write_reqsock(self.reqsock, self.sqldata)

        if auth_data == _M.RESP_OK then
            return
        end

        if new_plugin ~= "" then
            return "malformed packet"
        end
    end

    if plugin == "caching_sha2_password" then
        local len = #auth_data
        if len == 0 then
            return
        end

        if len == 1 then
            local status = strbyte(auth_data)
            -- caching_sha2_password fast auth success
            if status == 3 then
                local errno, sqlstate
                err, errno, sqlstate = _read_ok_result(self)

                _M.write_reqsock(self.reqsock, self.resp)
                if err ~= nil  then
                    return err, errno, sqlstate
                end
            end

            -- caching_sha2_password perform full authentication
            if status == 4 then
                if self.is_unix or self.use_ssl then
                    local resp
                    resp, err = _M.get_reqsock_data(self)
                    if err ~= nil then
                        return err
                    end
                    local bytes
                    bytes, err = _M.write_sqlsock(self.sqlsock, resp)
                    if err ~= nil then
                        return err
                    end
                    --[[
                        local bytes, err = _send_packet(self,
                            _to_cstring(self.password),
                            #self.password + 1)

                    if not bytes then
                        return "failed to send cleartext auth packet: "
                                .. err

                    end
                ]]--
                else
                    local resp
                    resp, err = _M.get_reqsock_data(self)
                    if err ~= nil then
                        return err
                    end
                    local bytes
                    bytes, err = _M.write_sqlsock(self.sqlsock, resp)
                    if err ~= nil then
                        return err
                    end

                    local typ
                    resp, typ, err = _M.recv_sql_packet(self)
                    if err then
                        return err
                    end

                    bytes, err = _M.write_reqsock(self.reqsock, resp)
                    if err ~= nil then
                        return err
                    end

                    --[[
                        local public_key = self.public_key
                        if not public_key then
                            -- caching_sha2_password request public_key
                            local bytes, err = _send_packet(self, "\2", 1)
                            if not bytes then
                                return "failed to send password request packet: "
                                        .. err
                            end

                            local packet, _, err = _recv_packet(self)
                            if not packet then
                                return "failed to receive the result packet: "
                                        .. err
                            end

                            public_key = sub(packet, 2)
                        end

                        err = _write_encode_password(self, old_auth_data,
                                public_key)

                        if err then
                            return err
                        end

                        self.public_key = public_key
                        ]]--
                end

                local errno, sqlstate
                err, errno, sqlstate = _read_ok_result(self)
                _M.write_reqsock(self.reqsock, self.resp)
                if err ~= nil  then
                    return err, errno, sqlstate
                end
            end
        end

        return "malformed packet"
    end

    if plugin == "sha256_password" then
        if #auth_data ~= 0 then
            local resp
            resp, err = _M.get_reqsock_data(self)
            if err ~= nil then
                return err
            end
            local bytes
            bytes, err = _M.write_sqlsock(self.sqlsock, resp)
            if err ~= nil then
                return err
            end
            --[[
            local enc, err = _write_encode_password(self, old_auth_data,
                    auth_data)

            if err then
                return err
            end

            return _read_ok_result(self)
            --]]
            local errno, sqlstate
            err, errno, sqlstate = _read_ok_result(self)
            _M.write_reqsock(self.reqsock, self.resp)
            if err ~= nil  then
                return err, errno, sqlstate
            end
        end
    end
end


function _M.do_handshake(self)
    --get greeting data from sql server
    local auth_data, plugin, err, errno, sqlstate = _read_hand_shake_packet(self)
    if err ~= nil then
        return nil, err
    end

    local bytes
    bytes, err = _M.write_reqsock(self.reqsock, self.sqldata)
    if err then
        return nil, err
    end

    local resp
    resp, err = _M.get_reqsock_data(self)
    if err then
        return nil, "socket get req data failed:" .. err
    end

    bytes, err = _M.write_sqlsock(self.sqlsock, resp)
    if err then
        return nil, "socket send data failed:" .. err
    end

    err, errno, sqlstate = _handle_auth_result(self, auth_data, plugin)
    if err then
        return nil, "failed to receive the result packet: " .. err
    end

    self.status = STATUS_OK
end


local function _from_length_coded_bin(data, pos)
    local first = strbyte(data, pos)

    --print("LCB: first: ", first)

    if not first then
        return nil, pos
    end

    if first >= 0 and first <= 250 then
        return first, pos + 1
    end

    if first == 251 then
        return null, pos + 1
    end

    if first == 252 then
        pos = pos + 1
        return _get_byte2(data, pos)
    end

    if first == 253 then
        pos = pos + 1
        return _get_byte3(data, pos)
    end

    if first == 254 then
        pos = pos + 1
        return _get_byte8(data, pos)
    end

    return nil, pos + 1
end


function _M.parse_result_set_header_packet(packet)
    local field_count, pos = _from_length_coded_bin(packet, 1)

    local extra
    extra = _from_length_coded_bin(packet, pos)

    return field_count, extra
end


local function _from_length_coded_str(data, pos)
    local len
    len, pos = _from_length_coded_bin(data, pos)
    if not len or len == null then
        return null, pos
    end

    return sub(data, pos, pos + len - 1), pos + len
end


local function _parse_field_packet(data)
    local col = new_tab(0, 2)
    local catalog, db, table, orig_table, orig_name, charsetnr, length
    local pos
    catalog, pos = _from_length_coded_str(data, 1)

    --print("catalog: ", col.catalog, ", pos:", pos)

    db, pos = _from_length_coded_str(data, pos)
    table, pos = _from_length_coded_str(data, pos)
    orig_table, pos = _from_length_coded_str(data, pos)
    col.name, pos = _from_length_coded_str(data, pos)

    orig_name, pos = _from_length_coded_str(data, pos)

    pos = pos + 1 -- ignore the filler

    charsetnr, pos = _get_byte2(data, pos)

    length, pos = _get_byte4(data, pos)

    col.type = strbyte(data, pos)

    --[[
    pos = pos + 1

    col.flags, pos = _get_byte2(data, pos)

    col.decimals = strbyte(data, pos)
    pos = pos + 1

    local default = sub(data, pos + 2)
    if default and default ~= "" then
        col.default = default
    end
    --]]

    return col
end


function _M.recv_field_packet(self)
    local resp, typ, err = _M.recv_sql_packet(self)
    if not resp then
        return nil, err
    end

    local packet = resp.data
    if typ == _M.RESP_ERR then
        local errno, msg, sqlstate = _parse_err_packet(packet)
        return nil, msg, errno, sqlstate
    end

    if typ ~= _M.RESP_DATA then
        return nil, "bad field packet type: " .. typ
    end

    -- typ == _M.RESP_DATA
    self.sqldata = resp
    return

    --return _parse_field_packet(packet)
end


local function _parse_eof_packet(packet)
    local pos = 2

    local warning_count, pos = _get_byte2(packet, pos)
    local status_flags = _get_byte2(packet, pos)

    return warning_count, status_flags
end


local function _parse_row_data_packet(data, cols, compact)
    local pos = 1
    local ncols = #cols
    local row
    if compact then
        row = new_tab(ncols, 0)
    else
        row = new_tab(0, ncols)
    end
    for i = 1, ncols do
        local value
        value, pos = _from_length_coded_str(data, pos)
        local col = cols[i]
        local typ = col.type
        local name = col.name

        --print("row field value: ", value, ", type: ", typ)

        if value ~= null then
            local conv = converters[typ]
            if conv then
                value = conv(value)
            end
        end

        if compact then
            row[i] = value

        else
            row[name] = value
        end
    end

    return row
end


function _M.filed_list(self)

end


function _M.parse_req(self, resp)
    local data = resp.data
    local cmd = strbyte(data, 1)
    if cmd == const.COM_QUERY then
        ngx.log(ngx.ERR, "query:" .. sub(data, 1, #data))
    elseif cmd == const.COM_FIELD_LIST then
        ngx.log(ngx.ERR, 'filed list')
    end

    return cmd
end


function _M.new(self, opts)
    if opts == nil or opts.reqsock == nil or opts.sqlsock == nil then
        return nil, "invalid options"
    end

    local max_packet_size = opts.max_packet_size
    if not max_packet_size then
        max_packet_size = 1024 * 1024 -- default 1 MB
    end
    self._max_packet_size = max_packet_size

    return setmetatable({ reqsock = opts.reqsock, sqlsock = opts.sqlsock, reqtyp = nil }, mt), nil
end

return _M