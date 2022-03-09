local ctx = require("gw.yulv.conn.client_transaction")
local backend = require("gw.yulv.backend.server")
local transaction = require("gw.yulv.conn.client_transaction")

local _M = {}

function _M.get_node(obj, opts)
    local node, err
    local db = opts.database
    if ctx.is_in_transaction(obj) ~= true then
        node, err = backend:new(opts)
    else
        node = obj._ctx[db]
        if node == nil then
            node, err = backend:new(opts)
            if ctx.is_autoCommit(obj) == nil then

            else

            end
        end
    end

    return node
end

function _M.handle_query(obj, tokens, data)
    local query_cmd = tokens[1]
    if query_cmd == "begin" then
        return transaction.handle_begin(obj, data)
    elseif query_cmd == "commit" then
        return transaction.handle_commit(obj, data)
    elseif query_cmd == "rollback" then
        return transaction.handle_rollback(obj, data)
    elseif query_cmd == "set" then

    end
end

return _M