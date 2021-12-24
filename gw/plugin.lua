local require = require
local cjson = require("cjson.safe")
local config = require("gw.core.config")


local _M = {}
local plugins = {  }
local function plugin_init_worker()
    for _, item in ipairs(plugins) do
        if item.init_worker then
            local ok, err = item.init_worker()
            if err ~= nil then
                return nil, err
            end
        end
    end

    return true, nil
end

function _M.init_worker()
    return plugin_init_worker()
end

function _M.run(phase, ctx)
end

return _M
