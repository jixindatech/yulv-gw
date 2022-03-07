local string = string
local strchar = string.char
local strsub = string.sub
local strbyte = string.byte

local cjson = require("cjson.safe")
local utils = require("gw.utils.util")
local _M = {}

local function length_encoded_int(data, pos) --(num uint64, isNull bool, n int) {
    local first = strbyte(data, pos)
    local num
    if first == 0xfb then
        return 1, true, pos + 1
    elseif first == 0xfc then
        pos = pos + 1
        num = utils.get_byte2(data, pos)
        return num, false, pos + 3
    elseif first == 0xfd then
        pos = pos + 1
        num = utils.get_byte3(data, pos)
        return num, false, pos + 4
    elseif first == 0xfe then
        pos = pos + 1
        num = utils.get_byte8(data, pos)
        return num, false, pos + 9
    end

    return first, false, pos + 1
end

function _M.parse(data)
    local pos = 1
    local catalog, schema, table, org_table, name, org_name, typ, flag, decimal
    local charset, column_length, default_value
    catalog, pos = utils.lenenc_str(data, pos)
    schema, pos = utils.lenenc_str(data, pos)
    table, pos = utils.lenenc_str(data, pos)
    org_table, pos = utils.lenenc_str(data, pos)
    name, pos = utils.lenenc_str(data, pos)
    org_name, pos = utils.lenenc_str(data, pos)
    pos = pos + 1

    charset, pos = utils.get_byte2(data, pos)
    column_length, pos = utils.get_byte4(data, pos)
    typ = strbyte(data, pos, pos + 1)
    pos = pos + 1
    flag, pos = utils.get_byte2(data, pos)
    decimal = strbyte(data, pos, pos + 1)
    pos = pos + 1
    --skip 0x00 0x00
    pos = pos + 2

    if #data > pos then
        local len, null
        len, null, pos = length_encoded_int(data, pos)
        if pos > #data then
            return nil, "invalid data length"
        end

        default_value = strsub(data, pos, pos + len)
    end

    return {
        data = data,
        catalog = catalog,
        schema = schema,
        table = table,
        org_table = org_table,
        name = name,
        org_name = org_name,
        typ = typ,
        flag = flag,
        decimal = decimal,
        charset = charset,
        column_length = column_length,
        default_value= default_value
    }
end

function _M.dump(field)
    if field.data ~= nil then
        return field.data
    end

    ngx.log(ngx.ERR, cjson.encode(field))

    local res = utils.from_length_coded_int(#field.catalog) .. field.catalog
    .. utils.from_length_coded_int(#field.schema) .. field.schema
    .. utils.from_length_coded_int(#field.org_table) .. field.org_table
    .. utils.from_length_coded_int(#field.catalog) .. field.catalog
    .. utils.from_length_coded_int(#field.catalog) .. field.catalog
    .. utils.from_length_coded_int(#field.catalog) .. field.catalog
    .. strchar(0x0c)
    .. utils.set_byte2(field.charset)
    .. utils.set_byte4(field.column_length)
    .. strchar(field.typ)
    .. utils.set_byte2(field.flag)
    .. strchar(field.decimal)
    .. strchar(0x00) .. strchar(0x00)

    if field.default_value ~= nil then
        res = res .. utils.from_length_coded_int(#field.default_value) .. field.default_value
    end
    return res
end

return _M