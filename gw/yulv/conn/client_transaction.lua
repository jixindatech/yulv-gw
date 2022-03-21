local bit = bit
local bor = bit.bor
local band = bit.band
local bnot = bit.bnot

local client_const = require("gw.yulv.conn.const")
local pool = require("gw.yulv.backend.pool")
local io = require("gw.yulv.mysql.io")
local const = require("gw.yulv.mysql.const")

local _M = {}

local function is_autoCommit(obj)
    return band(obj._status, client_const.SERVER_STATUS_AUTOCOMMIT) > 0
end

function _M.is_in_transaction(obj)
    return band(obj._status, client_const.SERVER_STATUS_IN_TRANS) > 0 or is_autoCommit(obj) ~= true
end

function _M.handle_begin(obj, data)
    local node, err
    node = obj._node

    local res
    res, err = io.exec(node, const.cmd.COM_QUERY, "begin")
    if err ~= nil then
        return err
    end

    err = io.send_ok_packet(obj, res)
    if err ~= nil then
        return err
    end

    obj._status = bor(obj._status, client_const.SERVER_STATUS_IN_TRANS)
end

function _M.handle_commit(obj, data)
    if _M.is_in_transaction(obj) ~= true or obj._node == nil then
        return "invalid transaction"
    end

    local node = obj._node
    if node == nil then
        return "invalid transaction node"
    end

    local err, res
    res, err = io.exec(node, const.cmd.COM_QUERY, "commit")
    if err ~= nil then
        return err
    end

    obj._status = band(obj._status, bnot(client_const.SERVER_STATUS_IN_TRANS))

    err = io.send_ok_packet(obj, res)
    if err ~= nil then
        return err
    end

    obj._node = nil
end

function _M.handle_rollback(obj, error)
    if _M.is_in_transaction(obj) ~= true or obj._node == nil then
        return "invalid transaction"
    end

    local node = obj._node
    if node == nil then
        return "invalid transaction node"
    end

    local err, res
    res, err = io.exec(node, const.cmd.COM_QUERY, "rollback")
    if err ~= nil then
        return err
    end

    obj._status = band(obj._status, bnot(client_const.SERVER_STATUS_IN_TRANS))
    if error == nil then
        err = io.send_ok_packet(obj, res)
        if err ~= nil then
            return err
        end
    end

    obj._node = nil
end


return _M