local require = require

local yulv     = require("gw.yulv")
local config   = require("gw.core.config")
local yaml_config = require("gw.core.config_yaml")

local ngx     = ngx
local seed = ngx.time()

local _M = {version = 0.1}

local config_path =  ngx.config.prefix() .. "etc/config.yaml"

function _M.stream_init()
    require("resty.core")
    math.randomseed(seed)

    local err = config.load_conf(config_path)
    if err ~= nil then
        return err
    end

    return nil
end

function _M.stream_init_worker()
    if config.get_config_type() == "yaml" then
        yaml_config.init_worker()
    end

    local ok, err = yulv.stream_init_worker()
    if not ok then
        ngx.log(ngx.ERR, "err init worker:" .. err)
    end
end

function _M.stream_content_phase()
    local ok, err = yulv.content_phase()
    if err then
        ngx.log(ngx.ERR, "stream_preread_phase:" .. err)
    end
end

function _M.stream_log_phase()
    ngx.log(ngx.ERR, "stream log phase")
end

return _M
