local error = error
local tab_remove = table.remove

--local lrucache = require("resty.lrucache")
local sql_server = require("gw.yulv.backend.server")

local pool = {}
local _M = {}

function _M.get_db(name, opts)
    local res, err
    local data = pool[name]
    if data ~= nil then
        while #data > 0 do
            res = data[1]
            tab_remove(data, 1)
            err = res:ping()
            if err == nil then
                break
            else
                res:close()
            end
        end

        if err == nil and res ~= nil then
            if #data == 0 then
                pool[name] = nil
            else
                pool[name] = data
            end
            return res
        end
    end

    ngx.log(ngx.ERR, "new server:" .. opts.host)
    return  sql_server.new(opts)
end

function _M.close_db(db, name)
    local data = pool[name]
    if data ~= nil then
        local index = #data + 1
        data[index] = db
        pool[name] = data
    else
        data = {}
        data[1] = db
        pool[name] = data
    end
end

return _M