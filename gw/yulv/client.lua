local setmetatable = setmetatable
local string = string
local strchar = string.char
local strsub = string.sub
local strrep = string.rep
local strbyte = string.byte
local strlen = string.len
local strfmt = string.format

local bit = bit
local lshift = bit.lshift
local bor = bit.bor
local rshift = bit.rshift
local band = bit.band

local math = math

local tabconcat = table.concat
local tabunpack = table.unpack

local utils = require("gw.utils.util")
local const = require("gw.yulv.const")
local fingerprint = require("gw.yulv.hooks.fingerprint")
local errstate = require("gw.yulv.errstate")
local errmsg = require("gw.yulv.errmsg")
local errno = require("gw.yulv.errno")

local _M = {}
local mt = { __index = _M }

local SERVER_VERSION = 10
local SERVER_VERSION_STR = "5.7.31-log"
local SERVER_STATUS_AUTOCOMMIT = 0x0002
local LEN_NATIVE_SCRAMBLE = 20
local MAX_PAYLOAD_LEN = lshift(1, 24) - 1
local LEN_NATIVE_SCRAMBLE = 20

local DEFAULT_CAPABILITY = bor(
        const.client_capabilities.CLIENT_LONG_PASSWORD,
        const.client_capabilities.CLIENT_LONG_FLAG,
        const.client_capabilities.CLIENT_CONNECT_WITH_DB,
        const.client_capabilities.CLIENT_PROTOCOL_41,
        const.client_capabilities.CLIENT_TRANSACTIONS,
        const.client_capabilities.CLIENT_SECURE_CONNECTION
)

local HEADER_LEN = 4
local HEADER_OK = 0x00

function _M.send_response(self, resp)
    self._packet_no = 0
    return self._sock:send(tabconcat(resp))
end

function _M.get_request(self)
    local sock = self._sock
    local header, err = sock:receive(HEADER_LEN) -- packet header
    if err then
        return nil, err
    end
    local len, pos = utils.get_byte3(header, 1)

    if len == 0 then
    return nil, "empty packet"
    end

    self._packet_no  = strbyte(header, pos)

    local data
    data, err = sock:receive(len)
    if err then
        return nil,  err
    end

    return { header , data  }, nil
end

function _M.send_packet(self, req, size)
    local sock = self._sock
    local i = 1
    local iter = math.modf(size / MAX_PAYLOAD_LEN)
    while iter >= i
    do
        self._packet_no = self._packet_no + 1
        local packet = utils.set_byte3(0xffffff) ..
                strchar(band(self._packet_no, 255)) ..
                strsub(req, (i-1)*MAX_PAYLOAD_LEN + 1,  MAX_PAYLOAD_LEN)
        i = i + 1
        local _, err =sock:send(packet)
        if err ~= nil then
            return err
        end
    end

    local left = math.fmod(size, MAX_PAYLOAD_LEN)
    if left > 0 then
        self._packet_no = self._packet_no + 1
        local packet = utils.set_byte3(left) .. strchar(band(self._packet_no, 255)) .. strsub(req, (i-1)*MAX_PAYLOAD_LEN + 1, left)
        local _, err =sock:send(packet)
        if err ~= nil then
            return err
        end
    end

    return nil
end


local function send_initial_handshake_packet(self)
    local server_ver = strchar(SERVER_VERSION)
    local server_version_str = SERVER_VERSION_STR
    local char_end = strchar(0)
    local connection_id = utils.set_byte4(1234)
    local salt1 = strsub(self._salt, 1, 8)
    local capabilities1 = utils.set_byte2(DEFAULT_CAPABILITY)
    local charset = strchar(33)
    local status = utils.set_byte2(self._status)
    local capabilities2 = utils.set_byte2(rshift(DEFAULT_CAPABILITY, 16))
    local padding1 = strchar(0x15)
    local padding2 = strrep(strchar(0), 10)
    local salt2 = strsub(self._salt, 9)

    local packet = server_ver ..
            server_version_str ..
            char_end ..
            connection_id ..
            salt1 ..
            char_end ..
            capabilities1 ..
            charset ..
            status ..
            capabilities2 ..
            padding1 ..
            padding2 ..
            salt2 ..
            char_end

    local err = _M.send_packet(self, packet, #packet)
    if err ~= nil then
        return err
    end

    return nil
end


local function process_client_handshake(self, find_user)
    local resp, err = _M.get_request(self)
    if err ~= nil then
        return err
    end

    local data = resp[2]

    local pos = 1
    local capabilities, max_packet_size, user

    capabilities, pos = utils.get_byte4(data, pos)
    self._capabilities = capabilities
    max_packet_size, pos = utils.get_byte4(data, pos)
    self._client_charset = strbyte(strsub(data, pos, pos+1))
    pos = pos + 1

    --skip reserved
    pos = pos +23

    user, pos = utils.from_cstring(data, pos)
    if not user then
        return "bad handshake initialization packet: bad user"
    end
    self._user = user

    local authlen = strbyte(data, pos)
    pos = pos + 1
    local auth = strsub(data, pos, authlen+pos)
    local user_conf = find_user(user)
    if user_conf == nil then
        return "invalid user"
    end
    self._proxy_conf = user_conf

    local password = user_conf.password
    local check_auth = utils.compute_token(self._salt, password, LEN_NATIVE_SCRAMBLE)
    if check_auth ~= auth then
        return nil
    end

    pos = pos + authlen

    if bor(capabilities, const.client_capabilities.CLIENT_CONNECT_WITH_DB) > 0 then
        self._db, pos = utils.from_cstring(data, pos)
    end

    return nil
end


function _M.send_error_packet(self, err_str, err)
    local state, msg
    if errmsg[err_str] ~= nil then
        msg = strfmt(errmsg[err_str], tabunpack(err))
    else
        msg = err[1]
    end

    local header = strchar(const.ERR_HEADER)
    local code = utils.set_byte2(errno[err_str])
    if self._capabilities and const.client_capabilities.CLIENT_PROTOCOL_41 > 0 then
        state = errstate[errno]
        if state == nil then
            state = errstate.DEFAULT_MYSQL_STATE
        end
        state = "#" .. state
    end

    local packet
    if state == nil then
        packet = header .. code .. msg
    else
        packet = header .. code .. state .. msg
    end

    return _M.send_packet(self, packet, #packet)
end


function _M.send_ok_packet(self, result)
    local header = strchar(HEADER_OK)
    local affected_rows, insert_id
    if result == nil then
        affected_rows = utils.from_length_coded_int(0)
        insert_id = utils.from_length_coded_int(0)
    else
        affected_rows = utils.from_length_coded_int(self._affected_rows)
        insert_id = utils.from_length_coded_int(self._insert_id)
    end

    local status_msg = nil
    if bor(self._capabilities, const.client_capabilities.CLIENT_PROTOCOL_41) > 0 then
        local status
        if result == nil then
            status = self._status
        else
            status = result.status
        end

        status_msg = strbyte(status) .. strbyte(rshift(status, 8)) .. strchar(0x00)
    end

    local packet
    if status_msg == nil then
        packet = header .. affected_rows .. insert_id
    else
        packet = header .. affected_rows .. insert_id .. status_msg
    end

    return _M.send_packet(self, packet, #packet)
end


function _M.do_handshake(self, find_user)
    local err = send_initial_handshake_packet(self)
    if err ~= nil then
        return err
    end

    err = process_client_handshake(self, find_user)
    if err ~= nil then
        return err
    end

    err = _M.send_ok_packet(self, nil)
    if err ~= nil then
        return err
    end

    self._packet_no = 0

    return nil
end

local function  is_sql_sep(r)
    return r == ' ' or r == ',' or
            r == '\t' or r == '/' or
            r == '\n' or r == '\r'
end

local function get_sql_type(sql)
    local start = -1
    for i=1,strlen(sql) do
        local chr = strsub(sql, i, i)
        if is_sql_sep(chr) then
            if start ~= -1 then
                return strsub(sql, start, i - 1)
            end
        else
            if start == -1 then
                start = i
            end
        end
    end

    return nil
end

function _M.handle_request(self, req, context, proxy)
    local data = req[2]
    local cmd = strbyte(data, 1)
    data = strsub(data, 2)

    context.timestamp = ngx.time()

    self._packet_no = strbyte(req[1], 4, 4)
    context.cmd = cmd
    if cmd == const.cmd.COM_INIT_DB then
        if proxy.database[data] == nil then
            return nil, "ER_DBACCESS_DENIED_ERROR", {self._user, ngx.var.hostname, data}
        end
        proxy.default = data

        self._db = data
        context.db = data
    elseif cmd == const.cmd.COM_PING then
        local err = _M.send_ok_packet(nil)
        if err ~= nil then
            return nil, err
        end
        return true, nil
    elseif cmd == const.cmd.COM_QUERY then
        context.data = data
        context.fingerprint = fingerprint.parse(data)
        context.sqltype = get_sql_type(data)
    elseif cmd == const.cmd.COM_FIELD_LIST then
        return nil, nil
    elseif cmd == const.cmd.COM_STMT_PREPARE then
        return nil, nil
    elseif cmd == const.cmd.COM_STMT_EXECUTE then
        return nil, nil
    elseif cmd == const.cmd.COM_STMT_CLOSE then
        return nil, nil
    elseif cmd == const.cmd.COM_STMT_SEND_LONG_DATA then
        return nil, nil
    elseif cmd == const.cmd.COM_STMT_RESET then
        return nil, nil
    elseif cmd == const.cmd.COM_SET_OPTION then
        return nil, nil
    else
        return nil, "ER_UNKNOWN_ERROR", strfmt("command %d not unsupported ", cmd)
    end

    return nil, nil
end

function _M.new(opts)
    if opts == nil or opts.sock == nil then
        return nil, "invalid options"
    end

    local max_packet_size = opts.max_packet_size
    if not max_packet_size then
        max_packet_size = 1024 * 1024 -- default 1 MB
    end

    local salt = utils.get_random_buf(20)

    return setmetatable({
        _sock = opts.sock,
        _max_packet_size = max_packet_size,
        _salt = salt,
        _packet_no = -1,
        _status = SERVER_STATUS_AUTOCOMMIT,
        _user = nil,
        _proxy_conf = nil,
        _proxy = nil,
        _db = nil,
        _affected_rows = 0,
        _insert_id = 0,
        }, mt), nil
end

return _M