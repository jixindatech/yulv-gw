local require = require
local math    = math
local error   = error
local ngx     = ngx
local tablepool   = require("tablepool")
local balancer = require("ngx.balancer")

local config      = require("gw.core.config")
local yaml_config = require("gw.core.config_yaml")
local yulv     = require("gw.yulv")
local seed = ngx.time()

local _M = {version = 0.1}

local config_path =  ngx.config.prefix() .. "etc/config.yaml"

function _M.stream_init()

end

function _M.stream_init_worker()
    local ok, err = yulv.stream_init_worker
    if not ok then
        ngx.log(ngx.ERR, "err init worker:" .. err)
    end
end

function _M.stream_content_phase()
    local ok, err = yulv.content_phase()
    if not ok then
        ngx.log(ngx.ERR, "stream_preread_phase:" .. err)
    end
end

function _M.stream_log_phase()
    ngx.log(ngx.ERR, "stream log phase")
end

return _M
