local error = error
local tab_remove = table.remove

local const = require("gw.yulv.mysql.const")
local charset = require("gw.yulv.mysql.charset")
local server = require("gw.yulv.backend.server")
--local pool = {}
local _M = {}
local server_context = {}  --server context

function _M.get_db(db, opts)
    local pool_name =  opts.user .. ":" .. opts.database .. ":" .. opts.host .. ":" .. opts.port
    if opts.charset == nil then
        opts.charset = charset.DEFAULT_CHARSET
    end

    local srv, err
    srv, err = server.new(opts, pool_name)
    if err ~= nil then
        return nil, err
    end

    if srv._capabilities == nil then
        local context = server_context[db]
        srv._capabilities = context['capabilitie'] or const.DEFAULT_CAPABILITY
    else
        server_context[db] = {}
        server_context[db]['capabilitie'] = srv._capabilities
    end
    --[[
    err = srv:use_db(db)
    if err ~= nil then
        return nil, err
    end
    ]]--
    local cset = opts.charset or charset.DEFAULT_CHARSET
    local collation = opts.collation or charset.DEFAULT_COLLATION_NAME
    err = srv:set_charset(cset, collation)
    if err ~= nil then
        return nil, err
    end

    return srv, err
end

function _M.close_db(db)
    db:set_keepalive()
end

return _M