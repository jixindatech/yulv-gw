local _M = {}
local require = require

local bit = bit
local bor = bit.bor
local lshift = bit.lshift
local rshift = bit.rshift
local band = bit.band
local bxor = bit.bxor

local string = string
local strbyte = string.byte
local strchar = string.char
local strfind = string.find
local strsub = string.sub
local strformat = string.format

local tabconcat = table.concat

local sha1 = ngx.sha1_bin

local random = math.random

local ok, new_tab = pcall(require, "table.new")
if not ok then
    new_tab = function (narr, nrec) return {} end
end

-- encode a given string as hex
function _M.hex_encode(str)
    return (str:gsub('.', function (c)
        return strformat('%02x', strbyte(c))
    end))
end

-- decode a given hex string
function _M.hex_decode(str)
    local value

    if (pcall(function()
        value = str:gsub('..', function (cc)
            return strchar(tonumber(cc, 16))
        end)
    end)) then
        return value
    else
        return str
    end
end

function _M.get_random_buf(len)
    local res = {}
    local min, max = 32, 127-1
    for i=1, len do
        res[i] = strchar(random(min, max))
    end

    return tabconcat(res)
end

function _M.get_byte2(data, i)
    local a, b = strbyte(data, i, i + 1)
    return bor(a, lshift(b, 8)), i + 2
end


function _M.get_byte3(data, i)
    local a, b, c = strbyte(data, i, i + 2)
    return bor(a, lshift(b, 8), lshift(c, 16)), i + 3
end


function _M.get_byte4(data, i)
    local a, b, c, d = strbyte(data, i, i + 3)
    return bor(a, lshift(b, 8), lshift(c, 16), lshift(d, 24)), i + 4
end


function _M.get_byte8(data, i)
    local a, b, c, d, e, f, g, h = strbyte(data, i, i + 7)

    -- XXX workaround for the lack of 64-bit support in bitop:
    -- XXX return results in the range of signed 32 bit numbers
    local lo = bor(a, lshift(b, 8), lshift(c, 16))
    local hi = bor(e, lshift(f, 8), lshift(g, 16), lshift(h, 24))
    return lo + 16777216 * d + hi * 4294967296, i + 8

    -- return bor(a, lshift(b, 8), lshift(c, 16), lshift(d, 24), lshift(e, 32),
    -- lshift(f, 40), lshift(g, 48), lshift(h, 56)), i + 8
end


function _M.set_byte2(n)
    return strchar(band(n, 0xff), band(rshift(n, 8), 0xff))
end


function _M.set_byte3(n)
    return strchar(band(n, 0xff),
            band(rshift(n, 8), 0xff),
            band(rshift(n, 16), 0xff))
end


function _M.set_byte4(n)
    return strchar(band(n, 0xff),
            band(rshift(n, 8), 0xff),
            band(rshift(n, 16), 0xff),
            band(rshift(n, 24), 0xff))
end
--[[
TODO: lua number conflicts with rshift when bytes greater than 4
function _M.set_byte8(n)
    return strchar(band(n, 0xff),
            band(rshift(n, 8), 0xff),
            band(rshift(n, 16), 0xff),
            band(rshift(n, 24), 0xff),
            band(rshift(n, 32), 0xff),
            band(rshift(n, 40), 0xff),
            band(rshift(n, 48), 0xff),
            band(rshift(n, 56), 0xff))
end
]]--

function _M.to_cstring(data)
    return data .. "\0"
end

function _M.lenenc_str(data, pos)
    local len, position = _M.from_length_coded_bin(data, pos)
    if len == nil then
        return nil, position
    end

    return strsub(data, pos + len, position + len), position + len
end

function _M.from_cstring(data, i)
    local last = strfind(data, "\0", i, true)
    if not last then
        return nil, nil
    end

    return strsub(data, i, last - 1), last + 1
end

function _M.compute_token(password, scramble, len)
    if password == "" then
        return ""
    end

    scramble = strsub(scramble, 1, len)

    local stage1 = sha1(password)
    local stage2 = sha1(stage1)
    local stage3 = sha1(scramble .. stage2)
    local n = #stage1
    local bytes = new_tab(n, 0)
    for i = 1, n do
        bytes[i] = strchar(bxor(strbyte(stage3, i), strbyte(stage1, i)))
    end

    return tabconcat(bytes)
end


function _M.from_length_coded_int(len)
    if len <= 250 then
        return strchar(len)
    elseif len <= 0xfffff then
        return strchar(0xfc) .. strchar(len) .. strchar(rshift(len, 8))
    elseif len <= 0xffffff then
        return strchar(0xfd) .. strchar(len) .. strchar(rshift(len, 8) .. strchar(rshift(len, 16)))
    elseif len <= 0xffffffffffffffff then
        return strchar(0xfe) ..
                strchar(len) ..
                strchar(rshift(len, 8)) ..
                strchar(rshift(len, 16)) ..
                strchar(rshift(len, 24)) ..
                strchar(rshift(len, 32)) ..
                strchar(rshift(len, 40)) ..
                strchar(rshift(len, 48)) ..
                strchar(rshift(len, 56))
    end
end

function _M.from_length_coded_bin(data, pos)
    local first = strbyte(data, pos)

    if not first then
        return nil, pos
    end

    if first >= 0 and first <= 250 then
        return first, pos + 1
    end

    if first == 251 then
        return nil, pos + 1, true
    end

    if first == 252 then
        pos = pos + 1
        return _M.get_byte2(data, pos)
    end

    if first == 253 then
        pos = pos + 1
        return _M.get_byte3(data, pos)
    end

    if first == 254 then
        pos = pos + 1
        return _M.get_byte8(data, pos)
    end

    return nil, pos + 1
end


return _M
