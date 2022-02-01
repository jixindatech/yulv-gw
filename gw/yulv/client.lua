local setmetatable = setmetatable
local string = string
local strchar = string.char
local strsub = string.sub
local strrep = string.rep
local strbyte = string.byte

local bit = bit
local lshift = bit.lshift
local bor = bit.bor
local rshift = bit.rshift
local band = bit.band

local math = math

local tabconcat = table.concat

local utils = require("gw.utils.util")
local const = require("gw.yulv.const")

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

function _M.send_response(self, cmd, resp)
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


local function process_client_handshake(self)
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
    local password = "123456"
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

local function send_ok_packet(self, result)
    local header = strchar(HEADER_OK)
    local affected_rows = utils.from_length_coded_int(self._affected_rows)
    local insert_id = utils.from_length_coded_int(self._insert_id)
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


function _M.do_handshake(self)
    local err = send_initial_handshake_packet(self)
    if err ~= nil then
        return err
    end

    err = process_client_handshake(self)
    if err ~= nil then
        return err
    end

    err = send_ok_packet(self, nil)
    if err ~= nil then
        return err
    end

    self._packet_no = 0

    return nil
end


function _M.get_command_type(self, resp)
    local data = resp[2]
    local cmd = strbyte(data, 1)


    if cmd == const.cmd.COM_FIELD_LIST then

    end
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
        _db = nil,
        _affected_rows = 0,
        _insert_id = 0,
        }, mt), nil
end

return _M