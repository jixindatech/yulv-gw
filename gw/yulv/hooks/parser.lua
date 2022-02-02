local utf8 = require 'lua-utf8'

local _M = {}

function _M.parse(data)
    local sql = utf8.escape(data)

end

return _M