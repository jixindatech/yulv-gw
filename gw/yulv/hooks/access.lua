local cjson = require("cjson.safe")
local iputils = require("resty.iputils")

local schema = require("gw.schema")
local config = require("gw.core.config")

local _M = {}
local module_name = "ip"
local module
local conf_version
local allow_ip
local deny_ip

local remote_addr_def = {
    description = "client IP",
    type = "string",
    anyOf = schema.ip_def,
}

local module_schema = {
    type = "object",
    properties = {
        id = schema.id_schema,
        timestamp = schema.id_schema,
        config = {
            type = "object",
            properties = {
                type = { type = "string" },
                data = {
                    type  = "array",
                    items = remote_addr_def,
                }
            },
            required = {"type", "data"},
        }
    },
    required = {"id", "timestamp", "config"},
}

function _M.init_worker()
    local options = {
        key = module_name,
        schema = module_schema,
        automatic = true,
        interval = 10,
    }

    local err
    module, err = config.new(module_name, options)
    if err ~= nil then
        return err
    end

    return nil
end


function _M.access(ip)
    if conf_version == nil or conf_version ~= module.conf_version then
        for _, item in ipairs(module.values) do
            if item.value.type == "allow" then
                if #item.value.data > 0 then
                    allow_ip = iputils.parse_cidrs(item.value.data)
                else
                    allow_ip = nil
                end
            elseif item.value.type == "deny" then
                if #item.value.data > 0 then
                    deny_ip = iputils.parse_cidrs(item.value.data)
                else
                    deny_ip = nil
                end
            end
        end
    end

    if allow_ip and iputils.ip_in_cidrs(ip, allow_ip) then
        return "allow"
    end

    if deny_ip and iputils.ip_in_cidrs(ip, deny_ip) then
        return "deny"
    end

    return nil
end

return _M