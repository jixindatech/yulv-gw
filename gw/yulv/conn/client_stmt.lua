local ipairs = ipairs

local bit = bit
local lshift = bit.lshift
local rshift = bit.rshift
local bor = bit.bor
local band = bit.band
local bnot = bit.bnot

local math = math
local modf = math.modf
local fmod = math.fmod
local floor = math.floor

local string = string
local strbyte = string.byte
local strsub = string.sub
local strrep = string.rep
local strchar = string.char

local table = table
local tabconcat = table.concat

local ok, new_tab = pcall(require, "table.new")
if not ok then
    new_tab = function (narr, nrec) return {} end
end

local pool = require("gw.yulv.backend.pool")
local io = require("gw.yulv.mysql.io")
local const = require("gw.yulv.mysql.const")
local utils = require("gw.utils.util")

local MYSQL_TYPE_DECIMAL   = 0
local MYSQL_TYPE_TINY      = 1
local MYSQL_TYPE_SHORT     = 2
local MYSQL_TYPE_LONG      = 3
local MYSQL_TYPE_FLOAT     = 4
local MYSQL_TYPE_DOUBLE    = 5
local MYSQL_TYPE_NULL      = 6
local MYSQL_TYPE_TIMESTAMP = 7
local MYSQL_TYPE_LONGLONG  = 8
local MYSQL_TYPE_INT24     = 9
local MYSQL_TYPE_DATE      = 10
local MYSQL_TYPE_TIME      = 11
local MYSQL_TYPE_DATETIME  = 12
local MYSQL_TYPE_YEAR      = 13
local MYSQL_TYPE_NEWDATE   = 14
local MYSQL_TYPE_VARCHAR   = 15
local MYSQL_TYPE_BIT       = 16

local MYSQL_TYPE_NEWDECIMAL    =  0xf6
local MYSQL_TYPE_ENUM          =  0xf7
local MYSQL_TYPE_SET           =  0xf8
local MYSQL_TYPE_TINY_BLOB     =  0xf9
local MYSQL_TYPE_MEDIUM_BLOB   =  0xfa
local MYSQL_TYPE_LONG_BLOB     =  0xfb
local MYSQL_TYPE_BLOB          =  0xfc
local MYSQL_TYPE_VAR_STRING    =  0xfd
local MYSQL_TYPE_STRING        =  0xfe
local MYSQL_TYPE_GEOMETRY      =  0xff

local _M = {}

local function write_prepare(obj, tx)
    local total = {}
    local packet = strbyte(const.OK_HEADER)
        .. utils.set_byte4(tx.id)
        .. utils.set_byte2(tx.columns)
        .. utils.set_byte2(tx.params)
        .. utils.set_byte3(0x000000)
    io.send_batch_packet(obj, packet, total, false)

    local eof = io.get_eof_packet(obj)
    if tx.tx_params ~= nil and #tx.tx_params > 0 then
        for _, item in ipairs(tx.params) do
            io.send_batch_packet(obj, item, total, false)
        end
        io.send_batch_packet(obj, eof, total, false)
    end

    if tx.tx_columns ~= nil and #tx.tx_columns > 0 then
        for _, item in ipairs(tx.params) do
            io.send_batch_packet(obj, item, total, false)
        end
    end

    return io.send_batch_packet(obj, eof, total, true)
end

function _M.handle_prepare(obj, query)
    local node, data, err
    node, err = pool.get_db(obj._db, obj._nodes[obj._db])
    if err ~= nil then
        return err
    end
    obj._node = node

    err = node:use_db(obj._db)
    if err ~= nil then
        return err
    end

    err = io.write_command(node, const.cmd.COM_STMT_PREPARE, query)
    if err ~= nil then
        return err
    end

    data, err = io.read_packet(node)
    if err ~= nil then
        return err
    end

    local header = strbyte(data, 1)
    if header == const.ERR_HEADER then
        return io.parse_error_packet(data)
    elseif header ~= const.OK_HEADER then
        return "malformed packet"
    end

    local pos = 2
    local id, columns, params
    local tx_params = {}
    local tx_columns = {}
    id, pos = utils.get_byte4(data, pos)
    columns, pos = utils.get_byte2(data, pos)
    params, pos = utils.get_byte2(data, pos)
    if params > 0 then
        local index = 1
        while true do
            local packet = io.read_packet(node)
            if io.is_eof_packet(packet) then
                break
            end
            tx_params[index] = packet
            index = index + 1
        end
    end

    if columns > 0 then
        local index = 1
        while true do
            local packet = io.read_packet(node)
            if io.is_eof_packet(packet) then
                break
            end
            tx_columns[index] = packet
            index = index + 1
        end
    end

    local tx = {
        id = id,
        params = params,
        columns = columns,
        tx_params = tx_params,
        tx_columns = tx_columns,
        args = { }
    }

    if obj._stmt_id > 0xFFFFFFFE then
        obj._stmt_id = 1
    end

    obj._stmts[obj._stmt_id] = tx

    obj._stmt_id = obj._stmt_id + 1

    err = write_prepare(obj, tx)
    if err ~= nil then
        return err
    end

    data, err = io.read_ok(node)
    if err ~= nil then
        return
    end

    err = io.write_command(node, const.cmd.COM_STMT_CLOSE, utils.get_byte4(tx.id))
    if err ~= nil then
        return err
    end

    pos = 2
    local affected_rows, last_insert_id, status
    affected_rows, pos = utils.from_length_coded_bin(data, pos)
    last_insert_id, pos = utils.from_length_coded_int(data, pos)
    status, pos = utils.get_byte4(data, pos)

    err = io.send_ok_packet(obj, {affected_rows = affected_rows, last_insert_id = last_insert_id, status = status })
    if err ~= nil then
        return err
    end
end

local function bind_stmt_args(stmt, nulls, types, values)
    local pos = 1
    local args = stmt.args
    local value

    for i=1, stmt.params do
        if band(strbyte(nulls, rshift(i, 3)), lshift(1, fmod(i, 8))) > 0 then
            args[i] = nil
            goto CONTINUE
        end

        local tp = strbyte(types, lshift(i, 1))
        local is_unsigned = band(strbyte(types, lshift(i)+1), 0x80) > 0
        if tp == MYSQL_TYPE_NULL then
            args[i] = nil
            goto CONTINUE
        elseif tp == MYSQL_TYPE_TINY then
            if is_unsigned then
                tp = tp .. strchar(0x80)
            end
            args[i] = { type = tp, value = strbyte(values, pos)}
            pos = pos + 1
            goto CONTINUE
        elseif tp == MYSQL_TYPE_SHORT or tp == MYSQL_TYPE_YEAR  then
            if is_unsigned then
                tp = tp .. strchar(0x80)
            end
            value, pos  = utils.get_byte2(values, pos)
            args[i]  = { type = tp, value = value }
            goto CONTINUE
        elseif tp == MYSQL_TYPE_INT24 or tp == MYSQL_TYPE_LONG then
            if is_unsigned then
                tp = tp .. strchar(0x80)
            end
            value, pos = utils.get_byte4(values, pos)
            args[i]  = { type = tp, value = value }
            goto CONTINUE
        elseif tp == MYSQL_TYPE_LONGLONG then
            if is_unsigned then
                tp = tp .. strchar(0x80)
            end
            value, pos = utils.get_byte8(values, pos)
            args[i]  = { type = tp, value = value }
            goto CONTINUE
        elseif tp == MYSQL_TYPE_FLOAT then
            value, pos= utils.get_byte4(values, pos)
            args[i]  = { type = tp, value = value }
            goto CONTINUE
        elseif tp == MYSQL_TYPE_DOUBLE then
            value, pos= utils.get_byte8(values, pos)
            args[i]  = { type = tp, value = value }
            goto CONTINUE
        elseif tp == MYSQL_TYPE_DECIMAL or
                tp == MYSQL_TYPE_NEWDECIMAL or
                tp == MYSQL_TYPE_VARCHAR or
                tp == MYSQL_TYPE_BIT or
                tp == MYSQL_TYPE_ENUM or
                tp == MYSQL_TYPE_SET or
                tp == MYSQL_TYPE_TINY_BLOB or
                tp == MYSQL_TYPE_MEDIUM_BLOB or
                tp == MYSQL_TYPE_LONG_BLOB or
                tp == MYSQL_TYPE_BLOB or
                tp == MYSQL_TYPE_VAR_STRING or
                tp == MYSQL_TYPE_STRING or
                tp == MYSQL_TYPE_GEOMETRY or
                tp == MYSQL_TYPE_DATE or
                tp == MYSQL_TYPE_NEWDATE or
                tp == MYSQL_TYPE_TIMESTAMP or
                tp == MYSQL_TYPE_DATETIME or
                tp == MYSQL_TYPE_TIME then
            if #values > pos then
                local len, null
                len, pos, null = utils.from_length_coded_bin(values, pos)
                if null  then
                    value = nil
                else
                    value = strsub(values, pos, pos + len)
                    pos = pos + len
                end
                args[i]  = { type = tp, value = value }
            end
        else
            return "invalid filed type"
        end

        ::CONTINUE::
    end
end

function _M.write(obj, stmt)
    local nulls, types, values
    local flag = 0

    if stmt.params > 0 then
        types = new_tab(0, lshift(stmt.params, 1))
        values = new_tab(0, stmt.params)
        nulls = new_tab(0, rshift((stmt.params + 7), 3))

        for i, item in ipairs(stmt.args) do
            if item == nil then
                local bits = nulls[modf(i-1, 8) + 1] or 0
                nulls[modf(i-1, 8) + 1] = strbyte(bor(bits, lshift(1, fmod(i - 1, 8))))
                types[i] = strbyte(MYSQL_TYPE_NULL)
            end

            flag = 1
            if item.type  == MYSQL_TYPE_TINY then
                types[i] = MYSQL_TYPE_TINY
                values[i] = item.value
            elseif item.type  == MYSQL_TYPE_SHORT then
                types[i] = MYSQL_TYPE_SHORT
                values[i] = item.value
            elseif item.type  == MYSQL_TYPE_LONG then
                types[i] = MYSQL_TYPE_LONG
                values[i] = item.value
            elseif item.type  == MYSQL_TYPE_LONGLONG then
                types[i] = MYSQL_TYPE_LONGLONG
                values[i] = item.value
            elseif item.type  == MYSQL_TYPE_FLOAT then
                types[i] = MYSQL_TYPE_FLOAT
                values[i] = item.value
            elseif item.type  == MYSQL_TYPE_DOUBLE then
                types[i] = MYSQL_TYPE_DOUBLE
                values[i] = item.value
            elseif item.type == MYSQL_TYPE_DECIMAL or
                    item.type == MYSQL_TYPE_NEWDECIMAL or
                    item.type == MYSQL_TYPE_VARCHAR or
                    item.type == MYSQL_TYPE_BIT or
                    item.type == MYSQL_TYPE_ENUM or
                    item.type == MYSQL_TYPE_SET or
                    item.type == MYSQL_TYPE_TINY_BLOB or
                    item.type == MYSQL_TYPE_MEDIUM_BLOB or
                    item.type == MYSQL_TYPE_LONG_BLOB or
                    item.type == MYSQL_TYPE_BLOB or
                    item.type == MYSQL_TYPE_VAR_STRING or
                    item.type == MYSQL_TYPE_STRING or
                    item.type == MYSQL_TYPE_GEOMETRY or
                    item.type == MYSQL_TYPE_DATE or
                    item.type == MYSQL_TYPE_NEWDATE or
                    item.type == MYSQL_TYPE_TIMESTAMP or
                    item.type == MYSQL_TYPE_DATETIME or
                    item.type == MYSQL_TYPE_TIME then
                types[i] = item.type
                values[i] = utils.from_length_coded_int(#item.value) .. item.value
            else
                return "invalid types"
            end
        end
    end

    local data = utils.get_byte4(stmt.id) .. 0x00 .. utils.set_byte4(1)
    if stmt.params > 0 then
        data = data .. tabconcat(nulls) .. flag
        if flag == 1 then
            data = data .. tabconcat(types) .. tabconcat(values)
        end
    end

    local err = io.write_command(obj, const.cmd.COM_STMT_EXECUTE, data)
    if err ~= nil then
        return er
    end
end

local function parse_prepare(stmt, data)
    local pos = 1
    local id
    id, pos = utils.get_byte4(data, pos)
    if nil == stmt[id] then
        return "invalid stmt id"
    end

    local flag = strbyte(data, pos)
    pos = pos + 1
    if flag ~= 0 then
        return "unsupported flag:" .. flag
    end

    pos = pos + 4
    local nulls, types, values
    if stmt.params > 0 then
        local len = rshift(stmt.params + 7, 3)
        nulls = strsub(data, pos, pos+len)
        pos = pos + len
        if strbyte(data, pos) == 1 then
            pos = pos + 1
            len = lshift(stmt.params, 1)
            types = strsub(data, pos, pos+len)
            pos = pos + len
            values = strsub(data, pos)
        end

        bind_stmt_args(stmt, nulls, types, values)
    end
end

function _M.handle_execute(obj, data)
    local node = obj._node
    if node == nil then
        return "invalid stmt node"
    end

    local err
    err = parse_prepare(obj._stmts, data)
    if err ~= nil then
        return err
    end

    err = io.send_packet(node, data, #data)
    if err ~= nil then
        return err
    end

end

function _M.handle_close(obj, data)
    local node = obj._node
    if node == nil then
        return "invalid stmt node"
    end

    local pos = 1
    local stmtid
    stmtid, pos = utils.get_byte4(data, pos)
    local stmt = obj._stmts[stmtid]
    if stmt == nil then
        return "invalid stmt id"
    end

    obj._stmts[stmtid] = nil

    return nil
end

function _M.handle_long_data(obj, data)
    local node = obj._node
    if node == nil then
        return "invalid stmt node"
    end

    local pos = 1
    local stmtid
    stmtid, pos = utils.get_byte4(data, pos)
    local stmt = obj._stmts[stmtid]
    if stmt == nil then
        return "invalid stmt id"
    end

    local param_id
    param_id, pos = utils.get_byte2(data, pos)
    if stmt.args[param_id] == nil then
        stmt.args[param_id] = strsub(data, pos)
    else
        stmt.args[param_id] = stmt.args[param_id] .. strsub(data, pos)
    end

    return nil
end

function _M.handle_reset(obj, data)
    local node = obj._node
    if node == nil then
        return "invalid stmt node"
    end

    local pos = 1
    local stmtid
    stmtid, pos = utils.get_byte4(data, pos)
    local stmt = obj._stmts[stmtid]
    if stmt == nil then
        return "invalid stmt id"
    end

    stmt.args = {}

    local err = io.send_ok_packet(obj, nil)
    if err ~= nil then
        return err
    end
end

return _M