local require = require

local strbyte = string.byte
local strsub  = string.sub

local ngx = ngx
local cjson = require("cjson.safe")
local uuid = require("resty.jit-uuid")
local config       = require("gw.yulv.config")
local cli          = require("gw.yulv.conn.client")
local access_hook  = require("gw.yulv.hooks.access")
local req_hook     = require("gw.yulv.hooks.request")
local resp_hook    = require("gw.yulv.hooks.response")
local action_code  = require("gw.yulv.hooks.action")
local io = require("gw.yulv.mysql.io")
local logger       = require("gw.log")

local module_name = "yulv"

local ok, new_tab = pcall(require, "table.new")
if not ok then
    new_tab = function (narr, nrec) return {} end
end

local _M = { version = "0.1"}

_M.name = module_name

function _M.stream_init_worker()
    uuid.seed()

    local err = config.init_worker()
    if err ~= nil then
        return nil, err
    end

    err = access_hook.init_worker()
    if err ~= nil then
        return nil, err
    end

    err = req_hook.init_worker()
    if err ~= nil then
        return nil, err
    end

    err = resp_hook.init_worker()
    if err ~= nil then
        return nil, err
    end

    return true, nil
end

local function new_context(ip)
    local context = new_tab(10, 0)
    context.client = ip
    context.db = nil
    context.cmd = 0
    context.filed_count = 0
    context.record_count = 0
    context.sqltype = nil
    context.fingerprint = nil

    return context
end

function _M.content_phase()
    local pass = false
    local timestamp = ngx.now()
    local transaction = uuid.generate_v4()
    local ip = ngx.var.remote_addr
    --local port = ngx.var.remote_port
    local action = access_hook.access(ip)
    if action == "allow" then
        pass = true
    elseif action == "deny" then
        return "deny"
    end

    local client, err = cli.new({
        sock = ngx.req.socket(),
        users = config.get_users(),
        client = ip,
    })
    if err ~= nil then
        io.send_error_packet(client,"ER_HANDSHAKE_ERROR", {err})
        return
    end

    err = client:do_handshake()
    if err ~= nil then
        io.send_error_packet(client,"ER_HANDSHAKE_ERROR", {err})
        return
    end

    logger.log({
        timestamp = timestamp,
        transaction = transaction,
        ip = ip,
        user = client._user,
        database = client._db or "",
        event = "login",
    }, "access")

    local data
    while true do
        local context = new_context(ip)
        data, err = io.read_packet(client)
        if err ~= nil then
            --"timeout, connection reset by peer, closed"
            break
        end

        timestamp = ngx.now()
        context.timestamp = timestamp
        err = client:dispatch(data, context)
        if err ~= nil then
            if type(err) == "string" then
                ngx.log(ngx.ERR, "yulv error:" .. err)
                err = io.send_error_packet(client, "ER_UNKNOWN_ERROR", {"ER_UNKNOWN_ERROR"})
            else
                err = io.send_error(client, err)
            end

            if err == "timeout" or err == "connection reset by peer" or err == "closed" then
                break
            end
        end

        if context.req_id ~= nil then
            logger.log({
                transaction = transaction,
                timestamp = context.timestamp,
                ip = ip,
                user = client._user,
                database = client._db or "",
                sql = context.sql,
                req_id = context.req_id,
                event = "req_rule"}, "rule")
        end

        timestamp = ngx.now()
        if context.resp_id ~= nil then
            logger.log({
                transaction = transaction,
                timestamp = timestamp,
                ip = ip,
                user = client._user,
                database = client._db or "",
                sql = context.sql,
                resp_id = context.resp_id,
                event = "resp_rule"}, "rule")
        end

        if client:is_closed() then
            break
        end

        ::CONTINUE::
    end

    client:rollback(err)

    timestamp = ngx.now()
    logger.log({
        transaction = transaction,
        timestamp = timestamp,
        ip = ip,
        user = client._user,
        database = client._db or "",
        event = "quit",
    }, "access")

    return err
end


function _M.log_phase()

end

return _M

