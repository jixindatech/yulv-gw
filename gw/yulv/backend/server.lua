-- Copyright (C) Yichun Zhang (agentzh)
local bit = require "bit"
local resty_sha256 = require "resty.sha256"
local sub = string.sub
local tcp = ngx.socket.tcp
local strbyte = string.byte
local strchar = string.char
local strfmt = string.format
local strrep = string.rep
local strsub = string.sub

local null = ngx.null
local band = bit.band
local bxor = bit.bxor
local bor = bit.bor
local lshift = bit.lshift
local rshift = bit.rshift
local sha1 = ngx.sha1_bin
local tabconcat = table.concat
local setmetatable = setmetatable
local error = error
local tonumber = tonumber
local to_int = math.floor

local has_rsa, resty_rsa = pcall(require, "resty.rsa")
local cjson = require("cjson.safe")
local utils = require("gw.utils.util")
local const = require("gw.yulv.mysql.const")
local io = require("gw.yulv.mysql.io")
local charset = require("gw.yulv.mysql.charset")
local field = require("gw.yulv.mysql.field")

if not ngx.config then
    error("ngx_lua 0.9.11+ or ngx_stream_lua required")
end

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


local _M = { _VERSION = '0.25' }


-- constants

local STATE_CONNECTED = 1
local STATE_COMMAND_SENT = 2

local COM_QUIT = 0x01
local COM_QUERY = 0x03

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

local RESP_OK = "OK"
local RESP_AUTHMOREDATA = "AUTHMOREDATA"
local RESP_LOCALINFILE = "LOCALINFILE"
local RESP_EOF = "EOF"
local RESP_ERR = "ERR"
local RESP_DATA = "DATA"

local MY_RND_MAX_VAL = 0x3FFFFFFF
local MIN_PROTOCOL_VER = 10

local LEN_NATIVE_SCRAMBLE = 20
local LEN_OLD_SCRAMBLE = 8

-- 16MB - 1, the default max allowed packet size used by libmysqlclient
local FULL_PACKET_SIZE = 16777215

local mt = { __index = _M }

local default_capability = const.DEFAULT_CAPABILITY

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

local function _dump(data)
    local len = #data
    local bytes = new_tab(len, 0)
    for i = 1, len do
        bytes[i] = strfmt("%x", strbyte(data, i))
    end
    return tabconcat(bytes, " ")
end

local function _pwd_hash(password)
    local add = 7

    local hash1 = 1345345333
    local hash2 = 0x12345671

    local len = #password
    for i = 1, len do
        -- skip spaces and tabs in password
        local byte = strbyte(password, i)
        if byte ~= 32 and byte ~= 9 then -- not ' ' or '\t'
            hash1 = bxor(hash1, (band(hash1, 63) + add) * byte
                    + lshift(hash1, 8))

            hash2 = bxor(lshift(hash2, 8), hash1) + hash2

            add = add + byte
        end
    end

    -- remove sign bit (1<<31)-1)
    return band(hash1, 0x7FFFFFFF), band(hash2, 0x7FFFFFFF)
end


local function _random_byte(seed1, seed2)
    seed1 = (seed1 * 3 + seed2) % MY_RND_MAX_VAL
    seed2 = (seed1 + seed2 + 33) % MY_RND_MAX_VAL

    return to_int(seed1 * 31 / MY_RND_MAX_VAL), seed1, seed2
end


local function _compute_old_token(password, scramble)
    if password == "" then
        return ""
    end

    scramble = sub(scramble, 1, LEN_OLD_SCRAMBLE)

    local hash_pw1, hash_pw2 = _pwd_hash(password)
    local hash_sc1, hash_sc2 = _pwd_hash(scramble)

    local seed1 = bxor(hash_pw1, hash_sc1) % MY_RND_MAX_VAL
    local seed2 = bxor(hash_pw2, hash_sc2) % MY_RND_MAX_VAL
    local rand_byte

    local bytes = new_tab(LEN_OLD_SCRAMBLE, 0)
    for i = 1, LEN_OLD_SCRAMBLE do
        rand_byte, seed1, seed2 = _random_byte(seed1, seed2)
        bytes[i] = rand_byte + 64
    end

    rand_byte = _random_byte(seed1, seed2)
    for i = 1, LEN_OLD_SCRAMBLE do
        bytes[i] = strchar(bxor(bytes[i], rand_byte))
    end

    return utils.to_cstring(tabconcat(bytes))
end


local function _compute_sha256_token(password, scramble)
    if password == "" then
        return ""
    end

    local sha256 = resty_sha256:new()
    if not sha256 then
        return nil, "failed to create the sha256 object"
    end

    if not sha256:update(password) then
        return nil, "failed to update string to sha256"
    end

    local message1 = sha256:final()

    sha256:reset()

    if not sha256:update(message1) then
        return nil, "failed to update string to sha256"
    end

    local message1_hash = sha256:final()

    sha256:reset()

    if not sha256:update(message1_hash) then
        return nil, "failed to update string to sha256"
    end

    if not sha256:update(scramble) then
        return nil, "failed to update string to sha256"
    end

    local message2 = sha256:final()

    local n = #message2
    local bytes = new_tab(n, 0)
    for i = 1, n do
        bytes[i] = strchar(bxor(strbyte(message1, i), strbyte(message2, i)))
    end

    return tabconcat(bytes)
end


local function _compute_token(password, scramble)
    if password == "" then
        return ""
    end

    scramble = sub(scramble, 1, LEN_NATIVE_SCRAMBLE)

    local stage1 = sha1(password)
    local stage2 = sha1(stage1)
    local stage3 = sha1(scramble .. stage2)
    local n = #stage1
    local bytes = new_tab(n, 0)
    for i = 1, n do
        bytes[i] = strchar(bxor(strbyte(stage3, i), strbyte(stage1, i)))
    end

    return tabconcat(bytes)
end


local function _recv_packet(self)
    local sock = self._sock

    local data, err = sock:receive(4) -- packet header
    if not data then
        return nil, nil, "failed to receive packet header: " .. err
    end

    --print("packet header: ", _dump(data))

    local len, pos = utils.get_byte3(data, 1)

    --print("packet length: ", len)

    if len == 0 then
        return nil, nil, "empty packet"
    end

    if len > self._max_packet_size then
        return nil, nil, "packet size too big: " .. len
    end

    local num = strbyte(data, pos)

    --print("recv packet: packet no: ", num)

    self._packet_no = num

    data, err = sock:receive(len)

    --print("receive returned")

    if not data then
        return nil, nil, "failed to read packet content: " .. err
    end

    --print("packet content: ", _dump(data))
    --print("packet content (ascii): ", data)

    local field_count = strbyte(data, 1)

    local typ
    if field_count == 0x00 then
        typ = RESP_OK
    elseif field_count == 0x01 then
        typ = RESP_AUTHMOREDATA
    elseif field_count == 0xfb then
        typ = RESP_LOCALINFILE
    elseif field_count == 0xfe then
        typ = RESP_EOF
    elseif field_count == 0xff then
        typ = RESP_ERR
    else
        typ = RESP_DATA
    end

    return data, typ
end


local function _recv_response_packet(self)
    local sock = self._sock

    local header, err = sock:receive(4) -- packet header
    if not header then
        return nil, nil, err
    end

    local len, pos = utils.get_byte3(header, 1)

    if len == 0 then
        return nil, nil, "empty packet"
    end

    if len > self._max_packet_size then
        return nil, nil, "packet size too big: " .. len
    end

    local num = strbyte(header, pos)

    self._packet_no = num

    local data
    data, err = sock:receive(len)

    if not data then
        return nil, nil, "failed to read packet content: " .. err
    end

    --print("packet content: ", _dump(data))
    --print("packet content (ascii): ", data)

    local field_count = strbyte(data, 1)

    local typ
    if field_count == 0x00 then
        typ = RESP_OK
    elseif field_count == 0x01 then
        typ = RESP_AUTHMOREDATA
    elseif field_count == 0xfb then
        typ = RESP_LOCALINFILE
    elseif field_count == 0xfe then
        typ = RESP_EOF
    elseif field_count == 0xff then
        typ = RESP_ERR
    else
        typ = RESP_DATA
    end

    return { header, data }, typ
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
        return utils.get_byte2(data, pos)
    end

    if first == 253 then
        pos = pos + 1
        return utils.get_byte3(data, pos)
    end

    if first == 254 then
        pos = pos + 1
        return utils.get_byte8(data, pos)
    end

    return nil, pos + 1
end


local function _from_length_coded_str(data, pos)
    local len
    len, pos = _from_length_coded_bin(data, pos)
    if not len or len == null then
        return null, pos
    end

    return sub(data, pos, pos + len - 1), pos + len
end


local function _parse_ok_packet(packet)
    local res = new_tab(0, 5)
    local pos

    res.affected_rows, pos = _from_length_coded_bin(packet, 2)

    --print("affected rows: ", res.affected_rows, ", pos:", pos)

    res.insert_id, pos = _from_length_coded_bin(packet, pos)

    --print("insert id: ", res.insert_id, ", pos:", pos)

    res.server_status, pos = utils.get_byte2(packet, pos)

    --print("server status: ", res.server_status, ", pos:", pos)

    res.warning_count, pos = utils.get_byte2(packet, pos)

    --print("warning count: ", res.warning_count, ", pos: ", pos)

    local message = _from_length_coded_str(packet, pos)
    if message and message ~= null then
        res.message = message
    end

    --print("message: ", res.message, ", pos:", pos)

    return res
end


local function _parse_eof_packet(packet)
    local pos = 2

    local warning_count, pos = utils.get_byte2(packet, pos)
    local status_flags = utils.get_byte2(packet, pos)

    return warning_count, status_flags
end


local function _parse_err_packet(packet)
    local errno, pos = utils.get_byte2(packet, 2)
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


local function _parse_result_set_header_packet(packet)
    local field_count, pos = _from_length_coded_bin(packet, 1)

    local extra
    extra = _from_length_coded_bin(packet, pos)

    return field_count, extra
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

    charsetnr, pos = utils.get_byte2(data, pos)

    length, pos = utils.get_byte4(data, pos)

    col.type = strbyte(data, pos)

    --[[
    pos = pos + 1

    col.flags, pos = utils.get_byte2(data, pos)

    col.decimals = strbyte(data, pos)
    pos = pos + 1

    local default = sub(data, pos + 2)
    if default and default ~= "" then
        col.default = default
    end
    --]]

    return col
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


local function _recv_field_packet(self)
    local packet, typ, err = _recv_packet(self)
    if not packet then
        return nil, err
    end

    if typ == RESP_ERR then
        local errno, msg, sqlstate = _parse_err_packet(packet)
        return nil, msg, errno, sqlstate
    end

    if typ ~= RESP_DATA then
        return nil, "bad field packet type: " .. typ
    end

    -- typ == RESP_DATA

    return _parse_field_packet(packet)
end


-- refer to https://dev.mysql.com/doc/internals/en/connection-phase-packets.html
local function _read_hand_shake_packet(self)
    local packet, typ, err = _recv_packet(self)
    if not packet then
        return nil, nil, err
    end

    if typ == RESP_ERR then
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

    local server_ver, pos = utils.from_cstring(packet, 2)
    if not server_ver then
        return nil, nil,
        "bad handshake initialization packet: bad server version"
    end

    self._server_ver = server_ver

    local thread_id, pos = utils.get_byte4(packet, pos)

    local scramble = sub(packet, pos, pos + 8 - 1)
    if not scramble then
        return nil, nil, "1st part of scramble not found"
    end

    pos = pos + 9 -- skip filler(8 + 1)

    -- two lower bytes
    local capabilities  -- server capabilities
    capabilities, pos = utils.get_byte2(packet, pos)

    self._server_lang = strbyte(packet, pos)
    pos = pos + 1

    self._server_status, pos = utils.get_byte2(packet, pos)

    local more_capabilities
    more_capabilities, pos = utils.get_byte2(packet, pos)

    self._capabilities = band(self._capabilities, bor(capabilities, lshift(more_capabilities, 16)))

    pos = pos + 11 -- skip length of auth-plugin-data(1) and reserved(10)

    -- follow official Python library uses the fixed length 12
    -- and the 13th byte is "\0 byte
    local scramble_part2 = sub(packet, pos, pos + 12 - 1)
    if not scramble_part2 then
        return nil, nil, "2nd part of scramble not found"
    end

    pos = pos + 13

    local plugin, _
    if band(self._capabilities, CLIENT_PLUGIN_AUTH) > 0 then
        plugin, _ = utils.from_cstring(packet, pos)
        if not plugin then
            -- EOF if version (>= 5.5.7 and < 5.5.10) or (>= 5.6.0 and < 5.6.2)
            -- \NUL otherwise
            plugin = sub(packet, pos)
        end

    else
        plugin = DEFAULT_AUTH_PLUGIN
    end

    return scramble .. scramble_part2, plugin
end


local function _append_auth_length(self, data)
    local n = #data

    if n <= 250 then
        data = strchar(n) .. data
        return data, 1 + n
    end

    self._capabilities = bor(self._capabilities,
            CLIENT_PLUGIN_AUTH_LENENC_CLIENT_DATA)

    if n <= 0xffff then
        data = strchar(0xfc, band(n, 0xff), band(rshift(n, 8), 0xff)) .. data
        return data, 3 + n
    end

    if n <= 0xffffff then
        data = strchar(0xfd,
                band(n, 0xff),
                band(rshift(n, 8), 0xff),
                band(rshift(n, 16), 0xff))
                .. data
        return data, 4 + n
    end

    data = strchar(0xfe,
            band(n, 0xff),
            band(rshift(n, 8), 0xff),
            band(rshift(n, 16), 0xff),
            band(rshift(n, 24), 0xff),
            band(rshift(n, 32), 0xff),
            band(rshift(n, 40), 0xff),
            band(rshift(n, 48), 0xff),
            band(rshift(n, 56), 0xff))
            .. data
    return data, 9 + n
end


local function _write_hand_shake_response(self, auth_resp, plugin)
    local append_auth, len = _append_auth_length(self, auth_resp)

    if self.use_ssl then
        if band(self._capabilities, CLIENT_SSL) == 0 then
            return "ssl disabled on server"
        end

        -- send a SSL Request Packet
        local req = utils.set_byte4(bor(self._capabilities, CLIENT_SSL))
                .. utils.set_byte4(self._max_packet_size)
                .. strchar(self._charset)
                .. strrep("\0", 23)

        local packet_len = 4 + 4 + 1 + 23
        local err = io.send_packet(self, req, packet_len)
        if err then
            return "failed to send client authentication packet: " .. err
        end

        local sock = self._sock
        ok, err = sock:sslhandshake(false, nil, self.ssl_verify)
        if not ok then
            return "failed to do ssl handshake: " .. (err or "")
        end
    end

    local req = utils.set_byte4(self._capabilities)
            .. utils.set_byte4(self._max_packet_size)
            .. strchar(self._charset)
            .. strrep("\0", 23)
            .. utils.to_cstring(self._user)
            .. append_auth
            .. utils.to_cstring(self._db)
            .. utils.to_cstring(plugin)

    local packet_len = 4 + 4 + 1 + 23 + #self._user + 1
            + len + #self._db + 1 + #plugin + 1

    local err = io.send_packet(self, req, packet_len)
    if err ~= nil then
        return "failed to send client authentication packet: " .. err
    end

    return nil
end


local function _read_auth_result(self, old_auth_data, plugin)
    local packet, typ, err = _recv_packet(self)
    if not packet then
        return nil, nil, "failed to receive the result packet: " .. err
    end

    if typ == RESP_OK then
        return RESP_OK, ""
    end

    if typ == RESP_AUTHMOREDATA then
        return sub(packet, 2), ""
    end

    if typ == RESP_EOF then
        if #packet == 1 then -- old pre-4.1 authentication protocol
            return nil, "mysql_old_password"
        end

        local pos

        plugin, pos = utils.from_cstring(packet, 2)
        if not plugin then
            return nil, nil, "malformed packet"
        end

        return sub(packet, pos), plugin
    end

    if typ == RESP_ERR then
        local errno, msg, sqlstate = _parse_err_packet(packet)
        return errno, sqlstate, msg
    end

    return nil, nil, "bad packet type: " .. typ
end


local function _read_ok_result(self)
    local packet, typ, err = _recv_packet(self)
    if not packet then
        return "failed to receive the result packet: " .. err
    end

    if typ == RESP_ERR then
        local errno, msg, sqlstate = _parse_err_packet(packet)
        return msg, errno, sqlstate
    end

    if typ ~= RESP_OK then
        return "bad packet type: " .. typ
    end
end


local function _encrypt_password(self, auth_data, public_key)
    if not has_rsa then
        error("auth plugin caching_sha2_password or sha256_password are not" ..
                " supported because resty.rsa is not installed", 2)
    end

    local password = utils.to_cstring(self.password)
    local n = #password
    local l = #auth_data
    local bytes = new_tab(n, 0)

    for i = 1, n do
        local j = i % l
        bytes[i] = strchar(bxor(strbyte(password, i), strbyte(auth_data, j)))
    end

    local pub, err = resty_rsa:new({
        public_key = public_key,
        key_type = resty_rsa.KEY_TYPE.PKCS8,
        padding = resty_rsa.PADDING.RSA_PKCS1_OAEP_PADDING,
        algorithm = "sha1",
    })
    if not pub then
        return nil, "new rsa err: " .. err
    end

    local enc, err = pub:encrypt(tabconcat(bytes))
    if not enc then
        return nil, "encode password packet: " .. err
    end

    return enc
end


local function _write_encode_password(self, auth_data, public_key)
    local enc, err = _encrypt_password(self, auth_data, public_key)

    err = io.send_packet(self, enc, #enc)
    if err then
        return "failed to send encode password packet: " .. err
    end
end


local function _auth(self, auth_data, plugin)
    local password = self.password

    if plugin == "caching_sha2_password" then
        local auth_resp, err = _compute_sha256_token(password, auth_data)
        if err then
            return nil, "failed to compute sha256 token: " .. err
        end

        return auth_resp
    end

    if plugin == "mysql_old_password" then
        return _compute_old_token(password, auth_data)
    end

    if plugin == "mysql_clear_password" then
        return utils.to_cstring(password)
    end

    if plugin == "mysql_native_password" then
        return _compute_token(password, auth_data)
    end

    if plugin == "sha256_password" then
        if self.is_unix or self.use_ssl or #password == 0 then
            return utils.to_cstring(password)
        end

        local public_key = self.public_key
        if public_key then
            return _encrypt_password(self, auth_data, public_key)
        end

        return "\1" -- request public key from server
    end

    return nil, "unknown plugin: " .. plugin
end


local function _handle_auth_result(self, old_auth_data, plugin)
    local auth_data, new_plugin, err = _read_auth_result(self, old_auth_data,
            plugin)

    if err ~= nil then
        local errno, sqlstate = auth_data, new_plugin
        return err, errno, sqlstate
    end

    if auth_data == RESP_OK then
        return
    end

    if new_plugin ~= "" then
        if not auth_data then
            auth_data = old_auth_data
        else
            old_auth_data = auth_data
        end

        plugin = new_plugin

        local auth_resp, err = _auth(self, auth_data, plugin)
        if not auth_resp then
            return err
        end

        err = io.send_packet(self, auth_resp, #auth_resp)
        if err then
            return "failed to send client authentication packet: " .. err
        end

        auth_data, new_plugin, err = _read_auth_result(self, old_auth_data,
                plugin)

        if err ~= nil then
            local errno, sqlstate = auth_data, new_plugin
            return err, errno, sqlstate
        end

        if auth_data == RESP_OK then
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
                return _read_ok_result(self)
            end

            -- caching_sha2_password perform full authentication
            if status == 4 then
                if self.is_unix or self.use_ssl then
                     err = io.send_packet(self,
                            utils.to_cstring(self.password),
                            #self.password + 1)

                    if err then
                        return "failed to send cleartext auth packet: "
                                .. err
                    end

                else
                    local public_key = self.public_key
                    if not public_key then
                        -- caching_sha2_password request public_key
                        err = io.send_packet(self, "\2", 1)
                        if err then
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
                end

                return _read_ok_result(self)
            end
        end

        return "malformed packet"
    end

    if plugin == "sha256_password" then
        if #auth_data ~= 0 then
            local enc, err = _write_encode_password(self, old_auth_data,
                    auth_data)

            if err then
                return err
            end

            return _read_ok_result(self)
        end
    end
end

function _M.set_timeout(self, timeout)
    local sock = self.sock
    if not sock then
        return nil, "not initialized"
    end

    return sock:settimeout(timeout)
end

--[[
function _M.new(self)
    local sock, err = tcp()
    if not sock then
        return nil, err
    end
    return setmetatable({ sock = sock }, mt)
end
]]--

function _M.new(opts, pool_name)
    local obj = {}

    local sock, err = tcp()
    if not sock then
        return nil, err
    end

    obj._sock = sock

    local max_packet_size = opts.max_packet_size
    if not max_packet_size then
        max_packet_size = 1024 * 1024 -- default 1 MB
    end
    obj._max_packet_size = max_packet_size

    local ok, err

    obj.compact = opts.compact_arrays
    obj._db = opts.database or ""
    obj._user = opts.user or ""

    obj._charset = charset.charset_id[opts.charset or charset.DEFAULT_CHARSET]
    if not obj._charset then
        return nil, "charset '" .. opts.charset .. "' is not supported"
    end

    local pool = opts.pool

    obj.ssl_verify = opts.ssl_verify
    obj.use_ssl = opts.ssl or opts.ssl_verify

    obj.password = opts.password or ""

    local host = opts.host
    if host then
        local port = opts.port or 3306
        if not pool then
            pool = obj._user .. ":" .. obj._db .. ":" .. host .. ":" .. port
        end

        ok, err = sock:connect(host, port, { pool = pool_name,
                                             pool_size = opts.pool_size,
                                             backlog = opts.backlog })
        --[[
        ok, err = sock:connect(host, port)
        ]]--
    else
        local path = opts.path
        if not path then
            return nil, 'neither "host" nor "path" options are specified'
        end

        if not pool then
            pool = obj._user .. ":" .. obj._db .. ":" .. path
        end

        obj.is_unix = true
        ok, err = sock:connect("unix:" .. path, { pool = pool,
                                                  pool_size = opts.pool_size,
                                                  backlog = opts.backlog })
    end

    if not ok then
        return nil, err
    end

    local reused = sock:getreusedtimes()
    if reused and reused > 0 then
        obj._state = STATE_CONNECTED
        return setmetatable(obj, mt)
    end

    obj._capabilities = bor(default_capability, CLIENT_PLUGIN_AUTH)
    obj._packet_no = -1
    local auth_data, plugin, err, errno, sqlstate
    = _read_hand_shake_packet(obj)

    if err ~= nil then
        return nil, err
    end

    local auth_resp, err = _auth(obj, auth_data, plugin)
    if not auth_resp then
        return nil, err
    end

    err = _write_hand_shake_response(obj, auth_resp, plugin)
    if err ~= nil then
        return nil, err
    end

    local err, errno, sqlstate = _handle_auth_result(obj, auth_data, plugin)
    if err ~= nil then
        return nil, err, errno, sqlstate
    end

    obj._state = STATE_CONNECTED
    return setmetatable(obj, mt)
end


function _M.set_keepalive(self, ...)
    local sock = self._sock
    if not sock then
        return nil, "not initialized"
    end

    if self._state ~= STATE_CONNECTED then
        return nil, "cannot be reused in the current connection state: "
                .. (self.state or "nil")
    end

    self._state = nil
    return sock:setkeepalive(...)
end


function _M.get_reused_times(self)
    local sock = self._sock
    if not sock then
        return nil, "not initialized"
    end

    return sock:getreusedtimes()
end


function _M.close(self)
    local sock = self._sock
    if not sock then
        return nil, "not initialized"
    end

    self._state = nil

    local err = io.send_packet(self, strchar(COM_QUIT), 1)
    if err then
        return nil, err
    end

    return sock:close()
end


function _M.server_ver(self)
    return self._server_ver
end


function _M.send_query(self, query)
    local sock = self._sock
    if not sock then
        return "not initialized"
    end

    self._packet_no = -1

    local cmd_packet = strchar(const.cmd.COM_QUERY) .. query
    local packet_len = 1 + #query
    local err = io.send_packet(self, cmd_packet, packet_len)
    if err then
        return  err
    end

    return nil
end

local function read_result(self, est_nrows)
    if self._state ~= STATE_COMMAND_SENT then
        return nil, "cannot read result in the current context: "
                .. (self._state or "nil")
    end

    local sock = self._sock
    if not sock then
        return nil, "not initialized"
    end

    local packet, typ, err = _recv_packet(self)
    if not packet then
        return nil, err
    end

    if typ == RESP_ERR then
        self._state = STATE_CONNECTED

        local errno, msg, sqlstate = _parse_err_packet(packet)
        return nil, msg, errno, sqlstate
    end

    if typ == RESP_OK then
        local res = _parse_ok_packet(packet)
        if res and band(res.server_status, SERVER_MORE_RESULTS_EXISTS) ~= 0 then
            return res, "again"
        end

        self._state = STATE_CONNECTED
        return res
    end

    if typ == RESP_LOCALINFILE then
        self._state = STATE_CONNECTED

        return nil, "packet type " .. typ .. " not supported"
    end

    -- typ == RESP_DATA or RESP_AUTHMOREDATA(also mean RESP_DATA here)

    --print("read the result set header packet")

    local field_count, extra = _parse_result_set_header_packet(packet)

    --print("field count: ", field_count)

    local cols = new_tab(field_count, 0)
    for i = 1, field_count do
        local col, err, errno, sqlstate = _recv_field_packet(self)
        if not col then
            return nil, err, errno, sqlstate
        end

        cols[i] = col
    end

    local packet, typ, err = _recv_packet(self)
    if not packet then
        return nil, err
    end

    if typ ~= RESP_EOF then
        return nil, "unexpected packet type " .. typ .. " while eof packet is "
                .. "expected"
    end

    -- typ == RESP_EOF

    local compact = self.compact

    local rows = new_tab(est_nrows or 4, 0)
    local i = 0
    while true do
        --print("reading a row")

        packet, typ, err = _recv_packet(self)
        if not packet then
            return nil, err
        end

        if typ == RESP_EOF then
            local warning_count, status_flags = _parse_eof_packet(packet)

            --print("status flags: ", status_flags)

            if band(status_flags, SERVER_MORE_RESULTS_EXISTS) ~= 0 then
                return rows, "again"
            end

            break
        end

        local row = _parse_row_data_packet(packet, cols, compact)
        i = i + 1
        rows[i] = row
    end

    self._state = STATE_CONNECTED

    return rows
end
_M.read_result = read_result


function _M.query(self, query, est_nrows)
    local bytes, err = _M.send_query(self, query)
    if not bytes then
        return nil, "failed to send query: " .. err
    end

    return read_result(self, est_nrows)
end


function _M.set_compact_arrays(self, value)
    self.compact = value
end


function _M.send_request(self, sql)
    return self._sock:send(tabconcat(sql))
end


function _M.is_quit_cmd(context)
    return context.cmd == const.cmd.COM_QUIT
end

function _M.get_response(self, context)
    local resp, err
    local typ
    local res = {}
    local cmd = context.cmd

    if cmd == const.cmd.COM_FIELD_LIST then
        while true do
            resp, typ, err = _recv_response_packet(self)
            if err ~= nil then
                return err
            end

            if typ == RESP_ERR then
                return resp
            end

            local index = #res
            res[index+1] = resp[1]
            res[index+2] = resp[2]

            context.filed_count = context.filed_count + 1
            if typ == RESP_EOF then
                break
            end
        end
    elseif cmd == const.cmd.COM_QUERY then
        resp, typ, err = _recv_response_packet(self)
        if typ == RESP_ERR or typ == RESP_OK then
            return resp
        end

        local field_count, extra = _parse_result_set_header_packet(resp[2])
        res = resp
        for i = 1, field_count do
            resp, typ, err = _recv_response_packet(self)
            if not resp then
                return nil, err
            end

            local index = #res
            res[index+1] = resp[1]
            res[index+2] = resp[2]
        end

        context.filed_count = field_count

        resp, typ, err = _recv_response_packet(self)
        if err then
            return nil, err
        end

        local index = #res
        res[index+1] = resp[1]
        res[index+2] = resp[2]

        if typ ~= RESP_EOF then
            return nil, "invalid data"
        end

        while true do
            resp, typ, err = _recv_response_packet(self)
            if not resp then
                return nil, err
            end

            index = #res
            res[index+1] = resp[1]
            res[index+2] = resp[2]

            context.record_count = context.record_count + 1

            if typ == RESP_EOF then
                break
            end
        end
    elseif cmd == const.cmd.COM_PING then
        resp, typ, err = _recv_response_packet(self)
        if typ == RESP_ERR then
            return resp
        end

        res = resp
    elseif cmd == const.cmd.COM_INIT_DB then
        resp, typ, err = _recv_response_packet(self)
        if typ == RESP_ERR then
            return resp
        end

        res = resp
    elseif cmd == const.cmd.COM_STMT_PREPARE then
        resp, typ, err = _recv_response_packet(self)
        if typ == RESP_ERR then
            return resp
        end

        if err ~= nil then
            return nil, err
        end

        local index = #res
        res[index+1] = resp[1]
        res[index+2] = resp[2]

        local pos = 1
        local statement_id, num_columns, num_params
        local header = strsub(resp[2], 2)
        statement_id, pos = utils.get_byte4(header, pos)
        num_columns, pos = utils.get_byte2(header, pos)
        num_params, pos = utils.get_byte2(header, pos)
        if num_params > 0 then
            for i = 1, num_params do
                resp, typ, err = _recv_response_packet(self)
                if not resp then
                    return nil, err
                end

                index = #res
                res[index+1] = resp[1]
                res[index+2] = resp[2]
            end

            resp, typ, err = _recv_response_packet(self)
            if err then
                return nil, err
            end

            index = #res
            res[index+1] = resp[1]
            res[index+2] = resp[2]

            if typ ~= RESP_EOF then
                return nil, "invalid data"
            end
        end

        if num_columns > 0 then
            for i = 1, num_columns do
                resp, typ, err = _recv_response_packet(self)
                if not resp then
                    return nil, err
                end

                index = #res
                res[index+1] = resp[1]
                res[index+2] = resp[2]
            end

            resp, typ, err = _recv_response_packet(self)
            if err then
                return nil, err
            end

            index = #res
            res[index+1] = resp[1]
            res[index+2] = resp[2]

            if typ ~= RESP_EOF then
                return nil, "invalid data"
            end
        end

        return res, nil
    elseif cmd == const.cmd.COM_STMT_SEND_LONG_DATA then
        -- no response
    elseif cmd == const.cmd.COM_STMT_EXECUTE then
        resp, typ, err = _recv_response_packet(self)
        if typ == RESP_ERR or typ == RESP_OK then
            return resp
        end

        local field_count, extra = _parse_result_set_header_packet(resp[2])
        res = resp
        for i = 1, field_count do
            resp, typ, err = _recv_response_packet(self)
            if not resp then
                return nil, err
            end

            local index = #res
            res[index+1] = resp[1]
            res[index+2] = resp[2]
        end

        context.filed_count = field_count

        resp, typ, err = _recv_response_packet(self)
        if err then
            return nil, err
        end

        local index = #res
        res[index+1] = resp[1]
        res[index+2] = resp[2]

        if typ ~= RESP_EOF then
            return nil, "invalid data"
        end

        while true do
            resp, typ, err = _recv_response_packet(self)
            if not resp then
                return nil, err
            end

            index = #res
            res[index+1] = resp[1]
            res[index+2] = resp[2]

            context.record_count = context.record_count + 1

            if typ == RESP_EOF then
                break
            end
        end

        return res, nil
    elseif cmd == const.cmd.COM_STMT_CLOSE then
        -- no response
    elseif cmd == const.cmd.COM_STMT_RESET then
        resp, typ, err = _recv_response_packet(self)
        if typ == RESP_ERR then
            return resp
        end

        res = resp
    elseif cmd == const.cmd.COM_SET_OPTION then
        resp, typ, err = _recv_response_packet(self)
        if typ == RESP_ERR then
            return resp
        end

        res = resp
    else
        ngx.log(ngx.ERR, "unsupported command for sql")
        return nil, "unsupported command for sql"
    end

    return res
end


function _M.cmd_filed_list(self, typ)
    local resp, err
    while true do
        resp, typ, err = _recv_packet(self)
        if err ~= nil then
            return err
        end
        local index = #self._sqldata
        self._sqldata[index+1] = resp[1]
        self._sqldata[index+2] = resp[2]

        if typ == RESP_EOF then
            break
        end
    end
end

function _M.use_db(self, name)
    if self._db == name or #name == 0 then
        return nil
    end

    local err
    err = io.write_command(self, const.cmd.COM_INIT_DB, name)
    if err ~= nil then
        return err
    end

    _, err = io.read_ok(self)
    if err ~= nil then
        return err
    end

    self._db = name

    return nil
end

local function write_command_str_str(self,  cmd, arg1, arg2)
    local data
    if arg2 ~= nil and #arg2 > 0 then
        data = arg1 .. strchar(0) .. strbyte(arg2)
    else
        data = arg1 .. strchar(0)
    end

    return io.write_command(self, cmd, data)
end

function _M.field_list(self, table, wildcard)
    local data, err
    err = write_command_str_str(self, const.cmd.COM_FIELD_LIST, table, wildcard)
    if err ~= nil then
        return nil, err
    end

    --[[
    data, err = io.read_packet(self)
    if err ~= nil then
        return nil, err
    end

    if strbyte(data, 1) == const.ERR_HEADER then
        return nil, io.handle_err_packet(self, strsub(data, 1))
    end
    --]]

    local res = {}
    local res_index = 1
    while true do
        data, err = io.read_packet(self)
        if err ~= nil then
            return nil, err
        end

        if io.is_eof_packet(data) then
            return res, nil
        end

        local item
        item, err = field.parse(data)
        if err ~= nil then
            return nil, err
        end

        res[res_index] = item
        res_index = res_index + 1
    end

    return res, nil
end


function _M.set_charset(self, cset, collation)
    if cset == nil or charset.charset_id[cset] == nil then
        return "invalid charset"
    end

    if collation == nil then
        collation = charset.charsets[cset]
    end

    if collation == nil then
        return "invalid collation"
    end

    local err = io.write_command(self, const.cmd.COM_QUERY, "SET NAMES " .. cset .. " COLLATE " .. collation)
    if err ~= nil then
        return err
    end

    local status
    status, err = io.read_ok(self)
    if err ~= nil then
        return err
    end

    self._charset = cset
    self._collation = collation

    return nil
end

function _M.set_autocommit(self, auto)
    local err
    if auto == nil then
        err = io.exec(self, "set autocommit = 0")
    else
        err = io.exec(self, "set autocommit = 1")
    end

    if err ~= nil then
        self:close()
    end

    return err
end

function _M.ping(self)
    local cmd = strchar(0x0e)
    self._packet_no = -1

    local err = io.send_packet(self, cmd, #cmd)
    if err ~= nil then
        return err
    end

    local ok, err = io.read_ok(self)
    if err ~= nil then
        return err
    end
end

return _M