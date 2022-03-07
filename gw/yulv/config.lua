local ipairs = ipairs

local cjson = require("cjson.safe")
local config = require("gw.core.config")
local schema = require("gw.schema")

local _M = {}

local module_name = "user"
local module
local users = {}

local module_schema = {
    type = "object",
    properties = {
        id = schema.id_schema,
        timestamp = schema.id_schema,
        config = {
            type = "object",
            properties = {
                name = { type = "string"},
                password = { type = "string"},
                database = {
                    type = "array",
                    items = {
                        type = "object",
                        properties = {
                            host = { type = "string"},
                            port = { type = "integer"},
                            name = { type = "string"},
                            password = { type = "string"}
                        },
                        required = {"host", "port", "name", "password"},
                    }
                }
            }
        }
    },
    required = {"id", "timestamp", "config"},
}

local function set_users(values)
    users = {}
    for _, v in ipairs(values) do
        local conf = v.value
        users[conf.user] = conf
    end
end

function _M.init_worker()
    local options = {
        key = module_name,
        schema = module_schema,
        automatic = true,
        interval = 10,
        post_func = set_users,
    }

    local err
    module, err = config.new(module_name, options)
    if err ~= nil then
        return err
    end

    return nil
end

function _M.get_proxy_config(user)
    if module ~= nil and module.values ~= nil and #module.values > 0 then
        for _, v in ipairs(module.values) do
            local conf = v.value
            if conf.user == user then
                return conf
            end
        end
    end

    return nil
end

function _M.get_user(name)
    return users[name]
end

function _M.get_users()
    return users
end

return _M