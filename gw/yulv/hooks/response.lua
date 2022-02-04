
local schema = require("gw.schema")
local config = require("gw.core.config")

local _M = {}
local module_name = "request"
local module

local rule_schema = {
    type = "object",
    properties = {
        id = schema.id_schema,
        timestamp = schema.id_schema,
        config = {
            type = "object",
            properties = {
                action = { type = "integer" },
                decoders = {
                    type = "object",
                    properties = {
                        form = { type = "boolean", default = false },
                        json = { type = "boolean", default = false },
                        multipart = { type = "boolean", default = false }
                    }
                },
                batch = {
                    type = "array",
                    items = schema.id_schema,
                },
                specific = {
                    type = "array",
                    items = schema.id_schema,
                }
            }
        }
    }
}

function _M.init_worker(conf)
    local options = {
        key = module_name,
        schema = rule_schema,
        automatic = true,
        interval = 10,
    }

    local err
    module, err = config.new(module_name, options)
    if err ~= nil then
        return err
    end


end

function _M.response(ip, cmd, resp)

end

function _M.log()

end

return _M