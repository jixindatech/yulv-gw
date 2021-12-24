local require = require
local math    = math
local error   = error
local ngx     = ngx
local tablepool   = require("tablepool")
local balancer = require("ngx.balancer")

local config      = require("gw.core.config")
local yaml_config = require("gw.core.config_yaml")

local seed = ngx.time()

local _M = {version = 0.1}

local config_path =  ngx.config.prefix() .. "etc/config.yaml"

function _M.stream_init()

end

function _M.stream_init_worker()

end

function _M.stream_preread_phase()

end

function _M.stream_balancer_phase()
    local host = "127.0.0.1"
    local port = 3306
    local ok, err = balancer.set_current_peer(host, port)
    if not ok then
        ngx.log(ngx.ERR, "banlancer error:", err)
    end
end

function _M.stream_log_phase()

end

return _M
