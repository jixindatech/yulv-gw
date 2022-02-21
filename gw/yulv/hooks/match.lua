local ngx = ngx

local _M = {}

function _M.str_find(str, pattern)
    return  ngx.re.find(str, pattern, "jo")
end

function _M.re(str, pattern)
    local res, err =  ngx.re.match(str, pattern, "ijo")
    if err ~= nil then
        return nil, err
    end

    return res, nil
end

return _M