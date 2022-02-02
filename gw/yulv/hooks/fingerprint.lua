local strformat = string.format
local utf8 = require 'lua-utf8'

local _M = {}


local unknown             = 1
local inWord              = 2 -- \S+
local inNumber            = 3   -- [0-9a-fA-Fx.-]
local inSpace             = 4   -- space, tab, \r, \n
local inOp                = 5   -- [=<>!] (usually precedes a number)
local opOrNumber          = 6   -- + in 2 + 2 or +3e-9
local inQuote             = 7   -- '...' or "..."
local subOrOLC            = 8   -- - or start of -- comment
local inDash              = 9   -- -- begins a one-line comment if followed by space
local inOLC               = 10   -- -- comment (at least one space after dash is required)
local divOrMLC            = 11  -- / operator or start of /* comment */
local mlcOrMySQLCode      = 12  -- /* comment */ or /*! MySQL-specific code */
local inMLC               = 13  -- /* comment */
local inValues            = 14  -- VALUES (1), ..., (N)
local moreValuesOrUnknown = 15  -- , (2nd+) or ON DUPLICATE KEY or end of query
local orderBy             = 16  -- ORDER BY
local onDupeKeyUpdate     = 17  -- ON DUPLICATE KEY UPDATE
local inNumberInWord      = 18  -- e.g. db23


local stateName  = {
     "unknown",
     "inWord",
     "inNumber",
     "inSpace",
     "inOp",
     "opOrNumber",
     "inQuote",
     "subOrOLC",
     "inDash",
      "inOLC",
     "divOrMLC",
     "mlcOrMySQLCode",
     "inMLC",
     "inValues",
     "moreValuesOrUnknown",
     "orderBy",
     "onDupeKeyUpdate",
     "inNumberInWord",
}

local Debug = true
local ReplaceNumbersInWords = false

function _M.parse(data)
    local q = " " --// need range to run off end of original query
    local prevWord = ""
    local f = {}
    local fi = 0
    local pr = ""       --// previous rune
    local s = unknown   --// current state
    local sqlState = unknown
    local quoteChar = "" -- rune(0)
    local cpFromOffset = 0
    local cpToOffset = 0
    local addSpace = false
    local escape = false
    local parOpen = 0
    local parOpenTotal = 0
    local valueNo = 0
    local firstPar = 0

    local sql = utf8.escape(data)
    for qi, r in utf8.next, data do
        if Debug then
            ngx.log(ngx.ERR, qi .. ":" .. fi .. " " ..  stateName[s] .. "/" .. stateName[sqlState] .. "[".. cpFromOffset ..":".. cpToOffset .."]" .. strformat("%#x",r) .. " ".. utf8.char(r))
        end
    end
end

return _M