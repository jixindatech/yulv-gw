local ipairs = ipairs
local string = string
local strchar = string.char
local strsub = string.sub
local strbyte = string.byte
local strfmt = string.format
local strlower = string.lower

local bit = bit
local lshift = bit.lshift
local band = bit.band
local bor = bit.bor
local rshift = bit.rshift

local math = math

local tabconcat = table.concat
local tabunpack = table.unpack

local cjson = require("cjson.safe")
local utils   = require("gw.utils.util")
local const   = require("gw.yulv.mysql.const")
local errstate = require("gw.yulv.mysql.errstate")
local errmsg   = require("gw.yulv.mysql.errmsg")
local errno    = require("gw.yulv.mysql.errno")
local io = require("gw.yulv.mysql.io")
local field = require("gw.yulv.mysql.field")

local HEADER_LEN = 4
local HEADER_OK = 0x00

local _M = {}
local MAX_PAYLOAD_LEN = lshift(1, 24) - 1

local function _from_length_coded_bin(data, pos)
    local first = strbyte(data, pos)
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

function _M.read_packet(obj)
    local sock = obj._sock
    local header, err = sock:receive(HEADER_LEN) -- packet header
    if err then
        return nil, err
    end

    local len, pos = utils.get_byte3(header, 1)

    if len == 0 then
        return nil, "empty packet"
    end

    obj._packet_no  = strbyte(header, pos)

    local data
    data, err = sock:receive(len)
    if err then
        return nil,  err
    else
        if len < const.MAX_PAYLOAD_LEN then
            return data, nil
        end

        local resp
        resp, err = _M.read_packet(obj)
        if err ~= nil then
            return nil, err
        end

        return data .. resp
    end
end

function _M.get_request(obj)
    local sock = obj._sock
    local header, err = sock:receive(HEADER_LEN) -- packet header
    if err then
        return nil, err
    end

    local len, pos = utils.get_byte3(header, 1)

    if len == 0 then
        return nil, "empty packet"
    end

    obj._packet_no  = strbyte(header, pos)

    local data
    data, err = sock:receive(len)
    if err then
        return nil,  err
    end

    return { header , data  }, nil
end

function _M.send_packet(obj, req, size)
    local sock = obj._sock
    local i = 1
    local iter = math.modf(size / MAX_PAYLOAD_LEN)
    while iter >= i
    do
        obj._packet_no = obj._packet_no + 1
        local packet = utils.set_byte3(0xffffff) ..
                strchar(band(obj._packet_no, 255)) ..
                strsub(req, (i-1)*MAX_PAYLOAD_LEN + 1,  MAX_PAYLOAD_LEN)
        i = i + 1
        local _, err = sock:send(packet)
        if err ~= nil then
            return err
        end
    end

    local left = math.fmod(size, MAX_PAYLOAD_LEN)
    if left > 0 then
        obj._packet_no = obj._packet_no + 1
        local packet = utils.set_byte3(left) .. strchar(band(obj._packet_no, 255)) .. strsub(req, (i-1)*MAX_PAYLOAD_LEN + 1, left)
        local _, err =sock:send(packet)
        if err ~= nil then
            return err
        end
    end

    return nil
end

function _M.send_batch_packet(obj, req, total, direct)
    local sock = obj._sock
    local i = 1
    local size = #req
    local iter = math.modf(size / MAX_PAYLOAD_LEN)
    while iter >= i
    do
        obj._packet_no = obj._packet_no + 1
        local packet = utils.set_byte3(0xffffff) ..
                strchar(band(obj._packet_no, 255)) ..
                strsub(req, (i-1)*MAX_PAYLOAD_LEN + 1,  MAX_PAYLOAD_LEN)
        i = i + 1

        local index = #total+1
        total[index] = packet
    end

    local left = math.fmod(size, MAX_PAYLOAD_LEN)
    if left > 0 then
        obj._packet_no = obj._packet_no + 1
        local packet = utils.set_byte3(left) .. strchar(band(obj._packet_no, 255)) .. strsub(req, (i-1)*MAX_PAYLOAD_LEN + 1, left)
        local index = #total+1
        total[index] = packet
    end

    if direct ~= nil then
        local _, err =sock:send(tabconcat(total))
        if err ~= nil then
            return err
        end
    end

    return nil
end

function _M.send_ok_packet(obj, result)
    local header = strchar(HEADER_OK)
    local affected_rows, insert_id
    if result == nil then
        affected_rows = utils.from_length_coded_int(0)
        insert_id = utils.from_length_coded_int(0)
    else
        affected_rows = utils.from_length_coded_int(result.affected_rows)
        insert_id = utils.from_length_coded_int(result.last_insert_id)
    end

    local status_msg
    if bor(obj._capabilities, const.client_capabilities.CLIENT_PROTOCOL_41) > 0 then
        local status
        if result == nil then
            status = obj._status
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

    local bytes, err = _M.send_packet(obj, packet, #packet)
    if err ~= nil then
        return err
    end
end

function _M.send_error_packet(obj, err_str, err)
    local state, msg
    if errmsg[err_str] ~= nil then
        msg = strfmt(errmsg[err_str], tabunpack(err))
    else
        --msg = err[1]
        msg = err_str
    end

    local header = strchar(const.ERR_HEADER)
    local code = utils.set_byte2(errno[err_str])
    if obj._capabilities and const.client_capabilities.CLIENT_PROTOCOL_41 > 0 then
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

    return _M.send_packet(obj, packet, #packet)
end

function _M.send_error(obj, error)
    local data = strchar(const.ERR_HEADER) .. utils.set_byte2(error.code)
    if band(obj._capabilities, const.client_capabilities.CLIENT_PROTOCOL_41) > 0 then
        data = data .. '#' .. error.state
    end
    data = data .. error.message

    local bytes, err = _M.send_packet(obj, data, #data)
    if err ~= nil then
        return err
    end
end

function _M.read_ok(obj)
    local sock = obj._sock

    local header, err = sock:receive(4) -- packet header
    if not header then
        return err
    end

    local len, pos = utils.get_byte3(header, 1)

    if len == 0 then
        return nil, "empty packet"
    end

    if len > obj._max_packet_size then
        return nil, "packet size too big: " .. len
    end

    local num = strbyte(header, pos)

    obj._packet_no = num

    local data
    data, err = sock:receive(len)

    if not data then
        return nil, "failed to read packet content: " .. err
    end

    local field_count = strbyte(data, 1)
    if field_count == const.ERR_HEADER then
        return nil, _M.parse_error_packet(data)
    elseif field_count ~= const.OK_HEADER then
        return nil, "not ok header"
    end

    pos = 4
    local affected_rows, last_insert_id, status, warning
    affected_rows, pos = utils.from_length_coded_bin(data, pos)
    last_insert_id, pos = utils.from_length_coded_bin(data, pos)

    if obj._capabilities ~= nil and band(obj._capabilities, const.client_capabilities.CLIENT_PROTOCOL_41) > 0 then
        status = utils.get_byte2(data, pos)
        warning = utils.get_byte2(data, pos)
    elseif obj._capabilities ~= nil and band(obj._capabilities, const.capabilities.CLIENT_TRANSACTIONS) > 0 then
        status = utils.get_byte2(data, pos)
    end

    return { affected_rows = affected_rows, last_insert_id = last_insert_id, status = status, warning = warning }
end

function _M.exec(obj, cmd, sql, args)
    local cmd_string = strchar(cmd) .. sql
    if args == nil then
        local err = _M.send_packet(obj, cmd_string, #cmd_string)
        if err ~= nil then
            return nil, err
        end
    else

    end

    return _M.read_result(obj, false)
end

function _M.write_command(obj, cmd, sql)
    local sock = obj._sock

    local cmd_packet = strchar(cmd) .. sql
    local packet_len = 1 + #sql

    local packet = utils.set_byte3(packet_len) .. strchar(band(0, 255)) .. cmd_packet

    local bytes, err = sock:send(packet)
    if err ~= nil then
        return err
    end
end

function _M.handle_ok_packet(obj, data)
    local pos = 1
    local affected_rows, last_insert_id
    affected_rows, pos = _from_length_coded_bin(data, pos)
    last_insert_id, pos = _from_length_coded_bin(data, pos)

    local status
    if band(obj._capabilities, const.client_capabilities.CLIENT_PROTOCOL_41) > 0 then
        status, pos = utils.get_byte2(data, pos)
        obj._status = status
        pos = pos + 2
    elseif band(obj._capabilities, const.client_capabilities.CLIENT_TRANSACTIONS) > 0 then
        status, pos = utils.get_byte2(data, pos)
        obj._status = status
        pos = pos + 2
    end

    return  {
        affected_rows = affected_rows,
        last_insert_id = last_insert_id,
        status = status or 0
    }
end

function _M.handle_err_packet(obj, data)
    local code, state, message
    local pos = 2
    code, pos = utils.get_byte2(data, pos)
    if band(obj._capabilities, const.client_capabilities.CLIENT_PROTOCOL_41) > 0 then
        -- skip '#'
        pos = pos + 1
        state = strsub(data, pos, pos + 4)
        pos = pos + 5
    end

    message = strsub(data, pos)

    return {
        code = code,
        state = state,
        state = state,
        message = message
    }
end

function _M.read_result_columns(obj)
    local data, err
    local res = {}
    local index = 1
    while true do
        data, err = _M.read_packet(obj)
        if err ~= nil then
            return nil, err
        end

        if _M.is_eof_packet(data) then
            --[[
            if band(obj._capabilities, const.client_capabilities.CLIENT_PROTOCOL_41) > 0 then
                local status = utils.get_byte2(data, 4, 2)
                obj._status = status
            else
            end
            ]]--
            break
        end

        res[index] = data
        index = index + 1
    end

    return res, nil
end

function _M.read_result_rows(obj)
    local data, err
    local res = {}
    local index = 1
    while true do
        data, err = _M.read_packet(obj)
        if err ~= nil then
            return nil, err
        end

        if _M.is_eof_packet(data) then
            --[[
            if band(obj._capabilities, const.client_capabilities.CLIENT_PROTOCOL_41) > 0 then
                local status = utils.get_byte2(data, 4, 2)
                obj._status = status
            end
            ]]--
            break
        end

        res[index] = data
        index = index + 1
    end

    return res, nil
end


function _M.read_resultset(obj, field_count, binary)
    local status, insert_id, affected_rows
    local colums, rows, err
    colums, err = _M.read_result_columns(obj)
    if err ~= nil then
        return nil, err
    end

    rows, err = _M.read_result_rows(obj)
    if err ~= nil then
        return nil, err
    end

    return {
        status = status or 0,
        insert_id = insert_id or 0,
        affected_rows = affected_rows or 0,
        field_count = field_count,
        colums = colums,
        rows = rows
    }
end

function _M.read_result(obj, binary)
    local data, err = _M.read_packet(obj)
    if err ~= nil then
        return nil, err
    end

    local field_count = strbyte(data, 1)

    if field_count == const.OK_HEADER then
        return _M.handle_ok_packet(obj, data)
    elseif field_count ==const.LOCAL_IN_FILE_HEADER then
        return nil, "ErrMalformPacket"
    elseif field_count == const.ERR_HEADER then
        return nil, _M.handle_err_packet(obj, data)
    end

    return _M.read_resultset(obj, field_count, binary)
end

function _M.is_eof_packet(data)
    return strbyte(data, 1) == const.EOF_HEADER and #data <= 5
end

function _M.get_eof_packet(obj)
    local status = obj._status or 0x0000
    local data = strchar(const.EOF_HEADER)
    if band(obj._capabilities, const.client_capabilities.CLIENT_PROTOCOL_41) > 0 then
        data = data .. utils.set_byte2(0x0000) .. utils.set_byte2(status)
    end

    return data
end

function _M.write_rusult_set(obj, result)
    local colums = result.colums
    local rows = result.rows

    local data = {}
    local index = 1
    local packet
    obj._packet_no = obj._packet_no + 1
    local first = utils.from_length_coded_int(result.field_count)
    data[index] = utils.set_byte3(#first) .. strchar(obj._packet_no) .. strchar(result.field_count)
    index = index + 1

    for _, column in ipairs(colums) do
        obj._packet_no = obj._packet_no + 1
        packet = utils.set_byte3(#column)
            .. strchar(obj._packet_no)
            .. column

        data[index] = packet
        index = index + 1
    end

    local eof = _M.get_eof_packet(obj, 0)

    obj._packet_no = obj._packet_no + 1
    packet = utils.set_byte3(#eof) .. strchar(obj._packet_no) .. eof
    data[index] =  packet
    index = index + 1

    for _, row in ipairs(rows) do
        obj._packet_no = obj._packet_no + 1
        packet = utils.set_byte3(#row)
                .. strchar(obj._packet_no)
                .. row

        data[index] = packet
        index = index + 1
    end

    obj._packet_no = obj._packet_no + 1
    packet = utils.set_byte3(#eof) .. strchar(obj._packet_no) .. eof
    data[index] =  packet
    index = index + 1

    local bytes, err = obj._sock:send(tabconcat(data))
    if err ~= nil then
        return err
    end
end

function _M.parse_error_packet(packet)
   local err_no, pos = utils.get_byte2(packet, 2)
   local marker = strsub(packet, pos, pos)
   local sqlstate
   if marker == '#' then
            -- with sqlstate
        pos = pos + 1
        sqlstate = strsub(packet, pos, pos + 5 - 1)
        pos = pos + 5
   end

   local message = strsub(packet, pos)
   return {
       errno = err_no,
       message = message,
       sqlstate = sqlstate,
   }
end

return _M