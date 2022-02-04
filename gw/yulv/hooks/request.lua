local ipairs = ipairs
local strlen = string.len
local strsub = string.sub

local cjson = require("cjson.safe")
local schema = require("gw.schema")
local config = require("gw.core.config")
local const  = require("gw.yulv.const")
local fingerprint = require("gw.yulv.hooks.fingerprint")
local match = require("gw.yulv.hooks.match")

local _M = {}
local module_name = "reqrules"
local module

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
                ip = { type = remote_addr_def },
                type = { type = "string" },
                fingerprint = { type = "string" },
                string = {
                    type = "object",
                    properties = {
                        match = { type = "string"},
                        pattern = { type = "string"}
                    }
                }
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

local function  is_sql_sep(r)
    return r == ' ' or r == ',' or
    r == '\t' or r == '/' or
    r == '\n' or r == '\r'
end

local function get_sql_type(sql)
    local start = -1
    for i=1,strlen(sql) do
        local chr = strsub(sql, i, i)
        if is_sql_sep(chr) then
            if start ~= -1 then
                return strsub(sql, start, i - 1)
            end
        else
            if start == -1 then
                start = i
            end
        end
    end

    return nil
end

function _M.request(ip, cmd, data, context)
    if cmd == const.cmd.COM_QUERY then
        local fp = fingerprint.parse(data)
        context.sqltype = get_sql_type(data)

        if module ~= nil and module.values ~= nil and #module.values > 0 then
            for _, item in ipairs(module.values) do
                local rule = item.value
                if rule.matcher.ip ~= nil and rule.matcher.ip ~= ip then
                    goto CONTINUE
                end

                if rule.matcher.type ~= nil and rule.matcher.type ~= context.sqltype then
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