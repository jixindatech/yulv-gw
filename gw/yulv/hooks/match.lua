local ngx = ngx

local _M = {}

local find_flag = "jo"
local re_flag = "ijo"

function _M.str_find(str, pattern)
    return  ngx.re.find(str, pattern, find_flag)
end

function _M.re(str, pattern)
    local res, err =  ngx.re.match(str, pattern, re_flag)
    if err ~= nil then
        return nil, err
    end

    return res, nil
end

return _M