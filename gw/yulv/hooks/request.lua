local ipairs = ipairs
local strlen = string.len
local strsub = string.sub

local cjson = require("cjson.safe")
local schema = require("gw.schema")
local config = require("gw.core.config")
local const  = require("gw.yulv.mysql.const")
local match = require("gw.yulv.hooks.match")

local _M = {}
local module_name = "reqrule"
local module

local module_schema = {
    type = "object",
    properties = {
        id = schema.id_schema,
        timestamp = schema.id_schema,
        config = {
            type = "object",
            properties = {
                ip = { type = schema.remote_addr_def },
                user = { type = "string" },
                database = { type = "string" },
                type = { type = "string" },
                fingerprint = { type = "string" },
                string = {
                    type = "object",
                    properties = {
                        match = { type = "string"},
                        pattern = { type = "string"}
                    }
                },
                rows = { type = "integer"},
                action = { type = "integer", minimum = 1},
            }
        },
        required = {"id", "timestamp", "config"},
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
        schema = module_schema,
        automatic = true,
        interval = 10,
    }

    local err
    module, err = config.new(module_name, options)
    if err ~= nil then
        return err
    end

end

function _M.request(context)
    local ip = context.ip
    local cmd = context.cmd
    local fp = context.fingerprint
    local db = context.db
    local sqltype = context.sqltype
    local data = context.data
    
    if cmd == const.cmd.COM_QUERY or cmd == const.cmd.COM_STMT_PREPARE then
        if module ~= nil and module.values ~= nil and #module.values > 0 then
            for _, item in ipairs(module.values) do
                local rule = item.value
                if rule.matcher.ip ~= nil and rule.matcher.ip ~= ip then
                    goto CONTINUE
                end

                if rule.matcher.database ~= nil and rule.matcher.database ~= db then
                    goto CONTINUE
                end

                if rule.matcher.type ~= nil and rule.matcher.type ~= sqltype then
                    goto CONTINUE
                end

                if rule.matcher.fingerprint ~= nil and rule.matcher.fingerprint ~= fp then
                    goto CONTINUE
                end

                if rule.matcher.string ~= nil then
                    local res, err = match[rule.matcher.string.match](data, rule.matcher.string.pattern)
                    if err ~= nil then
                        ngx.log(ngx.ERR, "match " .. rule.matcher.string.match .. " error:" .. err)
                    end

                    if res ~= nil then
                        goto CONTINUE
                    end
                end

                if true then
                    context.rule_id = rule.id
                    return  rule.matcher.action
                end

                ::CONTINUE::
            end

            return nil
        end
    end
end

function _M.log()

end

return _M