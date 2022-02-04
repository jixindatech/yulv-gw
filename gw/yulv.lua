local require = require

local strbyte = string.byte
local strsub  = string.sub

local ngx = ngx

local config       = require("gw.yulv.config")
local cli          = require("gw.yulv.client")
local srv          = require("gw.yulv.server")
local access_hook  = require("gw.yulv.hooks.access")
local req_hook     = require("gw.yulv.hooks.request")
local resp_hook    = require("gw.yulv.hooks.response")


local module_name = "yulv"
local connections = {}

local ok, new_tab = pcall(require, "table.new")
if not ok then
    new_tab = function (narr, nrec) return {} end
end

local _M = { version = "0.1"}

_M.name = module_name

function _M.stream_init_worker()
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
        ngx.log(ngx.ERR, "handshake error:" .. err)
        return err
    end

    local proxy
    proxy, err = srv.get_proxy(client._proxy_conf)
    if err ~= nil then
        return err
    end
    client._proxy = proxy

    while true do
        local context = new_context(ip, client._db)

        local req
        req, err = client:get_request()
        if err == "timeout" then
            break
        end

        err = client:handle_request(req, context)
        if err ~= nil then
            client:send_error_packet(nil)
            goto CONTINUE
        end

        if pass == false then
            action = req_hook.request(context)
            if action == "deny" then
                return "denied"
            elseif action == "allow" then
                pass = true
            end
        end

        if proxy.is_quit_cmd(context) then
            client:send_ok_packet(nil)
            break
        end

        err = proxy:send_request(req)
        if err == "timeout" then
            break
        end

        local resp
        resp, err = proxy:get_response(context)
        if err == "timeout" then
            break
        end
        if err ~= nil then
            ngx.log(ngx.ERR, "err:" .. err)
        end

        if pass == false then
            err = resp_hook.response(context)
        end

        local bytes
        bytes, err = client:send_response(resp)
        if err == "timeout" then
            break
        end

        ::CONTINUE::
    end

    return err
end


function _M.log_phase()
    local key = ngx.var.remote_addr .. ":" ..ngx.var.remote_port
    local client = connections[key]

    if client.proxy ~= nil then
        client.proxy:set_keepalive()
    end

    connections[key] = nil
end

return _M

