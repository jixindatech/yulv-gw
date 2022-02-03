local strlen = string.len
local strsub = string.sub

local schema = require("gw.schema")
local config = require("gw.core.config")
local const  = require("gw.yulv.const")
local fingerprint = require("gw.yulv.hooks.fingerprint")

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

local tokens = {
    insert      = 1,
    update      = 2,
    delete      = 3,
    replace     = 4,
    set         = 5,
    begin       = 6,
    commit      = 7,
    rollback    = 8,
    admin       = 9,
    select      = 10,
    use         = 11,
    start       = 12,
    transaction = 13,
    show        = 14,
    truncate    = 15,
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

local function  is_sql_sep(r)
    return r == ' ' or r == ',' or
    r == '\t' or r == '/' or
    r == '\n' or r == '\r'
end

local function get_sql_type(sql)
    for i=1,strlen(sql) do
        local char = strsub(sql, i, i)
        if is_sql_sep(char) then
            return strsub(sql, 1, i)
        end
    end

    return nil
end

function _M.request(cmd, data)
    if cmd == const.cmd.COM_QUERY then
        local print = fingerprint.parse(data)
    end
end

function _M.log()

end

return _M