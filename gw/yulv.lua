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
        timestamp = ngx.time(),
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

        --[[
        if pass == false then
            action = req_hook.request(context)
            if action ~= nil then
                logger.log({
                    transaction = transaction,
                    id = context.rule_id,
                    timestamp = ngx.time(),
                    cmd = context.cmd,
                    ip = ip,
                    user = client._user,
                    database = proxy.default,
                    event = "rule",
                    sqltype = context.sqltype or "",
                    sql = context.data or "",
                    fingerprint = context.fingerprint or "",
                    rows = context.record_count or 0,
                    action = action_code[action],
                }, "rule")

                if action == "deny" then
                    client:send_error_packet("ER_UNKNOWN_ERROR", {err})
                    goto CONTINUE
                elseif action == "allow" then
                    pass = true
                end
            end
        end
        ]]--

        context. timestamp = ngx.time()
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

        if client:is_closed() then
            break
        end

        --[[
        if srv.is_quit_cmd(context) then
            client:send_ok_packet(nil)
            break
        end

        if proxy.default ~= server_name then
            ngx.log(ngx.ERR, "server_name:" .. server_name)
            --TODO: fix keepalive for connection pool !!!
            -- server:set_keepalive()

            server = srv:new()
            server_name = proxy.default
            ok, err = server:connect(proxy.database[server_name])
            if err ~= nil then
                client:send_error_packet("ER_UNKNOWN_ERROR", {err})
                break
            end
        end

        err = server:send_request(req)
        if err == "timeout" then
            client:send_error_packet("ER_UNKNOWN_ERROR", {err})
            break
        end

        local resp
        resp, err = server:get_response(context)
        if err == "timeout" then
            client:send_error_packet("ER_UNKNOWN_ERROR", {err})
            break
        end

        if err ~= nil then
            ngx.log(ngx.ERR, "err:" .. err)
        end

        if pass == false then
            action = resp_hook.response(context)
            if action ~= nil then
                logger.log({
                    transaction = transaction,
                    cmd = context.cmd,
                    id = context.rule_id,
                    timestamp = ngx.time(),
                    ip = ip,
                    user = client._user,
                    database = proxy.default,
                    event = "rule",
                    sqltype = context.sqltype or "",
                    sql = context.data or "",
                    fingerprint = context.fingerprint or "",
                    rows = context.record_count or 0,
                    action = action_code[action],
                }, "rule")

                if action == "deny" then
                    client:send_error_packet("ER_UNKNOWN_ERROR", {err})
                    goto CONTINUE
                elseif action == "allow" then
                    pass = true
                end
            end
        end

        local bytes
        bytes, err = client:send_response(resp)
        if err == "timeout" then
            client:send_error_packet("ER_UNKNOWN_ERROR", {err})
            break
        end

        logger.log({
            transaction = transaction,
            timestamp = ngx.time(),
            ip = ip,
            user = client._user,
            database = proxy.default,
            cmd = context.cmd,
            event = "sql",
            sqltype = context.sqltype or "",
            sql = context.data or "",
            fingerprint = context.fingerprint or "",
            rows = context.record_count or 0,
        }, "access")
        --]]

        ::CONTINUE::
    end

    local event = "quit"
    logger.log({
        transaction = transaction,
        timestamp = ngx.time(),
        ip = ip,
        user = client._user,
        database = client._db or "",
        event = event,
    }, "access")

    return err
end


function _M.log_phase()

end

return _M

