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

local cjson = require("cjson.safe")
local pool = require("gw.yulv.backend.pool")
local io = require("gw.yulv.mysql.io")
local const = require("gw.yulv.mysql.const")
local utils = require("gw.utils.util")

local _M = {}

local function write_prepare(obj, tx)
    local total = {}
    local packet = strchar(const.OK_HEADER)
        .. utils.set_byte4(tx.id)
        .. utils.set_byte2(tx.columns)
        .. utils.set_byte2(tx.params)
        .. utils.set_byte3(0x000000)
    io.send_batch_packet(obj, packet, total, false)

    local eof = io.get_eof_packet(obj)
    if tx.tx_params ~= nil and #tx.tx_params > 0 then
        for _, item in ipairs(tx.tx_params) do
            io.send_batch_packet(obj, item, total, false)
        end
    end

    if tx.tx_columns ~= nil and #tx.tx_columns > 0 then
        io.send_batch_packet(obj, eof, total, false)
        for _, item in ipairs(tx.tx_columns) do
            io.send_batch_packet(obj, item, total, false)
        end
        return io.send_batch_packet(obj, eof, total, true)
    end

    return io.send_batch_packet(obj, eof, total, true)
end

function _M.handle_prepare(obj, query)
    local node, err
    node, err = pool.get_db(obj._db, obj._nodes[obj._db])
    if err ~= nil then
        return err
    end
    obj._node = node

    err = node:use_db(obj._db)
    if err ~= nil then
        return err
    end

    local tx
    tx, err = io.prepare(node, query)
    if err ~= nil then
        return err
    end

    if obj._stmt_id > 0xFFFFFFFE then
        obj._stmt_id = 1
    end

    obj._stmts[obj._stmt_id] = tx
    tx.id = obj._stmt_id
    obj._stmt_id = obj._stmt_id + 1

    err = io.write_command(node, const.cmd.COM_STMT_CLOSE, utils.set_byte4(tx.id))
    if err ~= nil then
        return err
    end

    err = write_prepare(obj, tx)
    if err ~= nil then
        return err
    end
end

local function bind_stmt_args(stmt, nulls, types, values)
    local pos = 1
    local args = stmt.args
    local value

    for i=1, stmt.params do
        if band(strbyte(nulls, rshift(i-1, 3)+1), lshift(1, fmod(i-1, 8))) > 0 then
            args[i] = nil
            goto CONTINUE
        end

        local tp = strbyte(types, i * 2 - 1)
        local is_unsigned = band(strbyte(types, i * 2), 0x80) > 0

        if tp == const.stmt.MYSQL_TYPE_NULL then
            args[i] = nil
            goto CONTINUE
        elseif tp == const.stmt.MYSQL_TYPE_TINY then
            args[i] = { type = tp, is_unsigned = is_unsigned, value = strchar(values, pos)}
            pos = pos + 1
            goto CONTINUE
        elseif tp == const.stmt.MYSQL_TYPE_SHORT or tp == const.stmt.MYSQL_TYPE_YEAR  then
            --value, pos  = utils.get_byte2(values, pos)
            value = strsub(values, pos, pos + 1)
            pos = pos + 2
            args[i]  = { type = tp, is_unsigned = is_unsigned, value = value }
            goto CONTINUE
        elseif tp == const.stmt.MYSQL_TYPE_INT24 or tp == const.stmt.MYSQL_TYPE_LONG then
            --value, pos = utils.get_byte4(values, pos)
            value = strsub(values, pos, pos + 3)
            pos = pos + 4
            args[i]  = { type = tp, is_unsigned = is_unsigned, value = value }
            goto CONTINUE
        elseif tp == const.stmt.MYSQL_TYPE_LONGLONG then
            --value, pos = utils.get_byte8(values, pos)
            value = strsub(values, pos, pos + 7)
            pos = pos + 8
            args[i]  = { type = tp, is_unsigned = is_unsigned, value = value }
            goto CONTINUE
        elseif tp == const.stmt.MYSQL_TYPE_FLOAT then
            --value, pos= utils.get_byte4(values, pos)
            value = strsub(values, pos, pos + 3)
            pos = pos + 4
            args[i]  = { type = tp, value = value }
            goto CONTINUE
        elseif tp == const.stmt.MYSQL_TYPE_DOUBLE then
            --value, pos= utils.get_byte8(values, pos)
            value = strsub(values, pos, pos + 7)
            pos = pos + 8
            args[i]  = { type = tp, value = value }
            goto CONTINUE
        elseif tp == const.stmt.MYSQL_TYPE_DECIMAL or
                tp == const.stmt.MYSQL_TYPE_NEWDECIMAL or
                tp == const.stmt.MYSQL_TYPE_VARCHAR or
                tp == const.stmt.MYSQL_TYPE_BIT or
                tp == const.stmt.MYSQL_TYPE_ENUM or
                tp == const.stmt.MYSQL_TYPE_SET or
                tp == const.stmt.MYSQL_TYPE_TINY_BLOB or
                tp == const.stmt.MYSQL_TYPE_MEDIUM_BLOB or
                tp == const.stmt.MYSQL_TYPE_LONG_BLOB or
                tp == const.stmt.MYSQL_TYPE_BLOB or
                tp == const.stmt.MYSQL_TYPE_VAR_STRING or
                tp == const.stmt.MYSQL_TYPE_STRING or
                tp == const.stmt.MYSQL_TYPE_GEOMETRY or
                tp == const.stmt.MYSQL_TYPE_DATE or
                tp == const.stmt.MYSQL_TYPE_NEWDATE or
                tp == const.stmt.MYSQL_TYPE_TIMESTAMP or
                tp == const.stmt.MYSQL_TYPE_DATETIME or
                tp == const.stmt.MYSQL_TYPE_TIME then
            if #values > pos then
                local len, null
                len, pos, null = utils.from_length_coded_bin(values, pos)
                if null  then
                    value = nil
                else
                    value = strsub(values, pos, pos + len - 1)
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


local function parse_prepare(stmts, data)
    local pos = 1
    local id
    id, pos = utils.get_byte4(data, pos)
    local stmt =stmts[id]
    if nil == stmt then
        return nil, "invalid stmt id"
    end

    local flag = strbyte(data, pos)
    pos = pos + 1
    if flag ~= 0 then
        return nil, "unsupported flag:" .. flag
    end

    pos = pos + 4
    local nulls, types, values

    if stmt.params > 0 then
        local len = rshift(stmt.params + 7, 3)
        nulls = strsub(data, pos, pos+len-1)
        pos = pos + len
        if strbyte(data, pos) == 1 then
            pos = pos + 1
            len = stmt.params * 2
            types = strsub(data, pos, pos+len-1)
            pos = pos + len
            values = strsub(data, pos)
        end

        bind_stmt_args(stmt, nulls, types, values)
    end

    return id, nil
end

function _M.handle_execute(obj, data)
    local id, err = parse_prepare(obj._stmts, data)
    if err ~= nil then
        return err
    end

    local node
    node, err = pool.get_db(obj._db, obj._nodes[obj._db])
    if err ~= nil then
        return err
    end
    obj._node = node

    local sql = obj._stmts[id].sql
    local result
    result, err = io.exec(node, const.cmd.COM_QUERY, sql, obj._stmts[id].args)
    if err ~= nil then
        return err
    end

    err = io.send_ok_packet(obj, result)
    if err ~= nil then
        return err
    end

end

function _M.handle_close(obj, data)
    local node = obj._node
    if node == nil then
        return "invalid stmt node2"
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
        return "invalid stmt node3"
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
        return "invalid stmt node4"
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