local require = require

local strbyte = string.byte
local strsub  = string.sub

local ngx = ngx
local cjson = require("cjson.safe")
local uuid = require("resty.jit-uuid")
local config       = require("gw.yulv.config")
local cli          = require("gw.yulv.client")
local srv          = require("gw.yulv.server")
local access_hook  = require("gw.yulv.hooks.access")
local req_hook     = require("gw.yulv.hooks.request")
local resp_hook    = require("gw.yulv.hooks.response")
local errno        = require("gw.yulv.errno")
local action_code  = require("gw.yulv.hooks.action")
local logger       = require("gw.log")

local module_name = "yulv"
local connections = {}

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

local function new_context(ip, db)
    local context = new_tab(10, 0)
    context.client = ip
    context.db = db
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
    local port = ngx.var.remote_port
    local action = access_hook.access(ip)
    if action == "allow" then
        pass = true
    elseif action == "deny" then
        return "deny"
    end

    local client = cli.new({
        sock = ngx.req.socket()
    })
    local key = ip.. ":" .. port
    connections[key] = client

    local err = client:do_handshake(config.get_proxy_config)
    if err ~= nil then
        client:send_error_packet("ER_HANDSHAKE_ERROR", {err})
        return
    end

    local errmsg
    local proxy
    proxy, err = srv.get_proxy(client._proxy_conf, client._db)
    if err ~= nil then
        client:send_error_packet("ER_DBACCESS_DENIED_ERROR", {client._user, ngx.var.hostname, client._db or ""})
        return
    end
    client._proxy = proxy
    logger.log({
        timestamp = ngx.time(),
        transaction = transaction,
        ip = ip,
        user = client._user,
        database = client._db or "",
        event = "access",
    }, "access")

    while true do
        local context = new_context(ip, client._db)

        local req
        req, err = client:get_request()
        if err == "timeout" then
            client:send_error_packet("ER_UNKNOWN_ERROR", {err})
            break
        end

        ok, err, errmsg = client:handle_request(req, context, proxy)
        if err == "timeout" then
            client:send_error_packet("ER_UNKNOWN_ERROR", {err})
            return
        end

        if err ~= nil and errmsg ~= nil then
            client:send_error_packet(err, errmsg)
            goto CONTINUE
        end

        if ok then
            goto CONTINUE
        end

        if pass == false then
            action = req_hook.request(context)
            if action ~= nil then
                logger.log({
                    transaction = transaction,
                    id = context.rule_id,
                    timestamp = ngx.time(),
                    ip = ip,
                    user = client._user,
                    database = client._db or "",
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

        if srv.is_quit_cmd(context) then
            client:send_ok_packet(nil)
            break
        end

        local server = srv:new()
        srv:set_timeout(100000000)
        ok, err = server:connect(proxy.database[proxy.default])
        if err == "timeout" then
            client:send_error_packet("ER_UNKNOWN_ERROR", {err})
            break
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

        server:set_keepalive()

        if pass == false then
            action = resp_hook.response(context)
            if action ~= nil then
                logger.log({
                    transaction = transaction,
                    id = context.rule_id,
                    timestamp = ngx.time(),
                    ip = ip,
                    user = client._user,
                    database = client._db or "",
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
            database = client._db or "",
            event = "sql",
            sqltype = context.type or "",
            sql = context.data or "",
            fingerprint = context.fingerprint or "",
            rows = context.record_count or 0,
        }, "access")

        ::CONTINUE::
    end

    local event = "quit"
    if err == "timeout" then
        event = "timeout"
    end
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
    local key = ngx.var.remote_addr .. ":" ..ngx.var.remote_port
    local client = connections[key]


    connections[key] = nil
end

return _M

