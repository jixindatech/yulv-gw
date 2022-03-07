local bit = bit
local lshift = bit.lshift
local bor = bit.bor

local client_const = require("gw.yulv.client.const")

local _M = {}

function _M.is_autoCommit(obj)
    return bor(obj._status, client_const.SERVER_STATUS_AUTOCOMMIT) > 0
end

function _M.is_in_transaction(obj)
    return bor(obj._status, client_const.SERVER_STATUS_IN_TRANS) > 0 or _M.is_autoCommit(obj)
end


return _M