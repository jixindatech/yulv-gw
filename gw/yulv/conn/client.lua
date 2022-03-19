local setmetatable = setmetatable
local string = string
local strchar = string.char
local strsub = string.sub
local strrep = string.rep
local strbyte = string.byte
local strlen = string.len
local strfmt = string.format
local strlower = string.lower
local strupper = string.upper

local bit = bit
local lshift = bit.lshift
local bor = bit.bor
local rshift = bit.rshift
local band = bit.band
local bnot =bit.bnot

local math = math

local tabconcat = table.concat
local tabunpack = table.unpack

local cjson = require("cjson.safe")
local utils = require("gw.utils.util")
local const = require("gw.yulv.mysql.const")
local fingerprint = require("gw.yulv.hooks.fingerprint")
local errstate    = require("gw.yulv.mysql.errstate")
local errmsg      = require("gw.yulv.mysql.errmsg")
local errno       = require("gw.yulv.mysql.errno")
local io          = require("gw.yulv.mysql.io")
local charset     = require("gw.yulv.mysql.charset")
local client_const = require("gw.yulv.conn.const")
local pool  = require("gw.yulv.backend.pool")
local field       = require("gw.yulv.mysql.field")
local stmt = require("gw.yulv.conn.client_stmt")
local transaction = require("gw.yulv.conn.client_transaction")

local _M = {}
local mt = { __index = _M }

local SERVER_VERSION = 10
local SERVER_VERSION_STR = "5.7.31-log"
local MAX_PAYLOAD_LEN = lshift(1, 24) - 1
local LEN_NATIVE_SCRAMBLE = 20

local HEADER_LEN = 4
local HEADER_OK = 0x00

function _M.send_response(self, resp)
    self._packet_no = 0
    return self._sock:send(tabconcat(resp))
end

local function send_initial_handshake_packet(self)
    local server_ver = strchar(SERVER_VERSION)
    local server_version_str = SERVER_VERSION_STR
    local char_end = strchar(0)
    local connection_id = utils.set_byte4(1234)
    local salt1 = strsub(self._salt, 1, 8)
    local capabilities1 = utils.set_byte2(const.DEFAULT_CAPABILITY)
    local cset = strchar(charset.charset_id.utf8)
    local status = utils.set_byte2(self._status)
    local capabilities2 = utils.set_byte2(rshift(const.DEFAULT_CAPABILITY, 16))
    local padding1 = strchar(0x15)
    local padding2 = strrep(strchar(0), 10)
    local salt2 = strsub(self._salt, 9)

    local packet = server_ver ..
            server_version_str ..
            char_end ..
            connection_id ..
            salt1 ..
            char_end ..
            capabilities1 ..
            cset ..
            status ..
            capabilities2 ..
            padding1 ..
            padding2 ..
            salt2 ..
            char_end

    local err = io.send_packet(self, packet, #packet)
    if err ~= nil then
        return err
    end

    return nil
end

local function _set_nodes(obj, conf)
    local nodes = {}
    for _, item in ipairs(conf.database) do
        local options = {
            host = item.host,
            port = item.port,
            user = item.user,
            password = item.password,
            database = item.name,
            charset = "utf8"
        }
        nodes[item.name] = options
    end

    if nodes == nil then
        return "invalid node for user"
    end

    if obj._db == nil then
        obj._db = conf.database[1].name
    end

    obj._nodes = nodes
end


local function process_client_handshake(self)
    local resp, err = io.get_request(self)
    if err ~= nil then
        return err
    end

    local data = resp[2]

    local pos = 1
    local capabilities, max_packet_size, user

    capabilities, pos = utils.get_byte4(data, pos)
    self._capabilities = capabilities
    max_packet_size, pos = utils.get_byte4(data, pos)
    self._client_charset = strbyte(strsub(data, pos, pos+1))
    pos = pos + 1

    --skip reserved
    pos = pos +23

    user, pos = utils.from_cstring(data, pos)
    if not user then
        return "bad handshake initialization packet: bad user"
    end
    self._user = user

    local authlen = strbyte(data, pos)
    pos = pos + 1
    local auth = strsub(data, pos, authlen+pos-1)
    local user_conf = self._users[user]
    if user_conf == nil then
        return "invalid user"
    end

    err = _set_nodes(self, user_conf)
    if err ~= nil then
        return err
    end

    local password = user_conf.password
    local check_auth = utils.compute_token(password, self._salt, LEN_NATIVE_SCRAMBLE)
    if check_auth ~= auth then
        return "invalid password"
    end

    pos = pos + authlen

    if band(capabilities, const.client_capabilities.CLIENT_CONNECT_WITH_DB) > 0 then
        local db
        db, pos = utils.from_cstring(data, pos)
        if #db > 0 then
            self._db = db
        end
    end

    return nil
end

function _M.do_handshake(self)
    local err = send_initial_handshake_packet(self)
    if err ~= nil then
        return err
    end

    err = process_client_handshake(self)
    if err ~= nil then
        return err
    end

    err = io.send_ok_packet(self, nil)
    if err ~= nil then
        return err
    end

    return nil
end

local function  is_sql_sep(r)
    return r == ' ' or r == ',' or
            r == '\t' or r == '/' or
            r == '\n' or r == '\r'
end

local function get_sql_tokens(sql)
    local tokens = {}
    local index = 1

    local start = -1
    for i=1,strlen(sql) do
        local chr = strsub(sql, i, i)
        if is_sql_sep(chr) then
            if start ~= -1 then
                tokens[index] = strlower(strsub(sql, start, i - 1))
                index = index+1

                start = -1
            end
        else
            if start == -1 then
                start = i
            end
        end
    end

    if start >= 0 then
        tokens[index] = strlower(strsub(sql, start, #sql))
    end

    return tokens
end

local function write_field_list(obj, fields)
    local total = {}
    local index = 1
    local err
    for _, item in ipairs(fields) do
        local temp = field.dump(item)
        err = io.send_batch_packet(obj, temp, total, nil)
    end

    local eof = io.get_eof_packet(obj, 0)
    err = io.send_batch_packet(obj, eof, total, true)
    if err ~= nil then
        return err
    end
end

local function handle_field_list(obj, data)
    local pos = 1
    local table
    table, pos = utils.from_cstring(data, pos)
    local wildcard = strsub(data, pos)

    if obj._db == nil or obj._db == "" then
        return "database lost"
    end

    local node, err
    local fields
    node, err = pool.get_db(obj._db, obj._nodes[obj._db])
    if err ~= nil then
        return err
    end
    obj._node = node

    err = node:use_db(obj._db)
    if err ~= nil then
        return err
    end

    fields, err = node:field_list(table, wildcard)
    if err ~= nil then
        return err
    end

    pool.close_db(obj._node, obj._db)

    return write_field_list(obj, fields)
end

local function handle_use_db(obj, data)
    local node, err
    if #data == 0 then
        return "ER_UNKNOWN_ERROR", {"The length of database name is zero"}
    end

    if obj._nodes[data] == nil then
        return "ER_DBACCESS_DENIED_ERROR", {obj._user, ngx.var.hostname, data}
    end

    if obj._db == data then
        return io.send_ok_packet(obj, nil)
    end

    obj._db = data

    node, err = pool.get_db(obj._db, obj._nodes[obj._db])
    if err ~= nil then
        return err
    end
    obj._node = node

    err = node:use_db(obj._db)
    if err ~= nil then
        return err
    end

    pool.close_db(obj._node)

    return io.send_ok_packet(obj, nil)
end

local function hand_ping(obj)
    local err = io.send_ok_packet(obj, nil)
    if err ~= nil then
        return err
    end

    return nil
end

local function handle_set(obj, tokens)
    if tokens[2] ~= nil then
        local action = strupper(tokens[2])
        if action == "`AUTOCOMMIT`" or
                action == "AUTOCOMMIT" or
                action == "`@@AUTOCOMMIT`" or
                action == "@@AUTOCOMMIT" or
                action == "`@@SESSION.AUTOCOMMIT`" or
                action == "@@SESSION.AUTOCOMMIT" then
            --set autocommit = 0
            if tokens[4] == nil then
                return false, "invalid autocommit parameter"
            end

            local flag = tokens[4]
            if flag == "1" or flag == "on" then
                obj._status = bor(obj._status, client_const.SERVER_STATUS_AUTOCOMMIT)
                if transaction.is_in_transaction(obj) then
                    local node = obj._node
                    if node == nil then
                        return false, "invalid transaction node"
                    end

                    local result, err = io.exec(node, const.cmd.COM_QUERY, "set autocommit = 1")
                    if err ~= nil then
                        return false, err
                    end
                    err = io.send_ok_packet(obj, result)
                    if err ~= nil then
                        return false, err
                    end

                    return true, nil
                end
            elseif flag == "0" or flag == "off" then
                obj._status = band(obj._status, bnot(client_const.SERVER_STATUS_AUTOCOMMIT))
                local err = io.send_ok_packet(obj, nil)
                if err ~= nil then
                    return false, err
                end

                return true, nil
            end
        elseif action == "`NAMES`" or
                action == "NAMES" or
                action == "`CHARACTER_SET_RESULTS`" or
                action == "CHARACTER_SET_RESULTS" or
                action == "`@@CHARACTER_SET_RESULTS`" or
                action == "@@CHARACTER_SET_RESULTS" or
                action == "`@@SESSION.CHARACTER_SET_RESULTS`" or
                action == "@@SESSION.CHARACTER_SET_RESULTS" or
                action == "`CHARACTER_SET_CLIENT`" or
                action == "CHARACTER_SET_CLIENT" or
                action == "`@@CHARACTER_SET_CLIENT`" or
                action == "@@CHARACTER_SET_CLIENT" or
                action == "`@@SESSION.CHARACTER_SET_CLIENT`" or
                action == "@@SESSION.CHARACTER_SET_CLIENT" or
                action == "`CHARACTER_SET_CONNECTION`" or
                action == "CHARACTER_SET_CONNECTION" or
                action == "`@@CHARACTER_SET_CONNECTION`" or
                action == "@@CHARACTER_SET_CONNECTION" or
                action == "`@@SESSION.CHARACTER_SET_CONNECTION`" or
                action == "@@SESSION.CHARACTER_SET_CONNECTION"
        then
            if tokens[3] == nil then
                return false, "invalid charset"
            end

            if obj._charset ~= strlower(tokens[3]) then
                return false, "unsportted charset"
            end

            if tokens[4] ~= nil then
                if strupper(tokens[4]) ~= "COLLATE" then
                    return false, "invalid charset collate parameter"
                end
                if tokens[5] == nil or obj._collation ~= strlower(tokens[5]) then
                    return false, "invalid collate parameter"
                end
            end

            obj._status = band(obj._status, bnot(client_const.SERVER_STATUS_AUTOCOMMIT))
            local err = io.send_ok_packet(obj, nil)
            if err ~= nil then
                return false, err
            end

            return true, nil
        end
    end

    return false, nil
end

local function handle_query(obj, data)
    local node, err
    if transaction.is_in_transaction(obj) then
        node = obj._node
        if node == nil then
            return "invalid transaction node"
        end
    else
        node, err = pool.get_db(obj._db, obj._nodes[obj._db])
        if err ~= nil then
            return err
        end
        obj._node = node
    end

    local tokens = get_sql_tokens(data)
    local query_cmd = strlower(tokens[1])

    if query_cmd == "begin" then
        return transaction.handle_begin(obj, data)
    elseif query_cmd == "start" and strlower(tokens[2]) == "transaction" then
        return transaction.handle_begin(obj, data)
    elseif query_cmd == "commit" then
        return transaction.handle_commit(obj, data)
    elseif query_cmd == "rollback" then
        return transaction.handle_rollback(obj, data)
    elseif query_cmd == "set" then
        local ok
        ok, err = handle_set(obj, tokens)
        if err ~= nil then
            return err
        end

        if ok then
            return
        end
    end

    err = node:send_query(data)
    if err ~= nil then
        return err
    end

    local result
    result, err = io.read_result(node, false)
    if err ~= nil then
        return err
    end

    if transaction.is_in_transaction(obj) ~= true then
        pool.close_db(obj._node)
        obj._node = nil
    end

    if result.rows ~=nil and result.colums ~= nil then
        return io.write_rusult_set(obj, result)
    else
        return io.send_ok_packet(obj, result)
    end
end

local function handle_set_option(obj, data)
    local node, err
    node, err = pool.get_db(obj._db, obj._nodes[obj._db])
    if err ~= nil then
        return err
    end
    obj._node = node

    local eof = io.get_eof_packet(obj)
    return io.send_packet(obj, eof, #eof)
end

function _M.is_closed(self)
    return self._closed == true
end

function _M.dispatch(self, body, ctx)
    local cmd = strbyte(body, 1)
    local data = strsub(body, 2)

    local err, err_msg
    ctx.cmd = cmd
    ngx.log(ngx.ERR, "user:[" .. self._user .. "] db:[" .. self._db.. "] cmd no:" .. cmd)
    if cmd == const.cmd.COM_INIT_DB then
        err = handle_use_db(self, data)
    elseif cmd == const.cmd.COM_PING then
        err = hand_ping(self)
    elseif cmd == const.cmd.COM_QUERY then
        err = handle_query(self, data, ctx)
    elseif cmd == const.cmd.COM_FIELD_LIST then
        err = handle_field_list(self, data)
    elseif cmd == const.cmd.COM_STMT_PREPARE then
        ctx.data = data
        err = stmt.handle_prepare(self, data)
        if transaction.is_in_transaction(self) ~= true then
            pool.close_db(self._node)
            self._node = nil
        end
    elseif cmd == const.cmd.COM_STMT_EXECUTE then
        return stmt.handle_execute(self, data)
    elseif cmd == const.cmd.COM_STMT_CLOSE then
        err=  stmt.handle_close(self, data)
        if transaction.is_in_transaction(self) ~= true then
            pool.close_db(self._node)
            self._node = nil
        end
    elseif cmd == const.cmd.COM_STMT_SEND_LONG_DATA then
        return stmt.handle_long_data(self, data)
    elseif cmd == const.cmd.COM_STMT_RESET then
        return stmt.handle_reset(self, data)
    elseif cmd == const.cmd.COM_SET_OPTION then
        return handle_set_option(self, data)
    elseif cmd == const.cmd.COM_QUIT then
        transaction.handle_rollback(self, data)
        self._closed = true
        return nil
    else
        err = "ER_UNKNOWN_ERROR"
        err_msg = {strfmt("command %d not unsupported ", cmd)}
    end
    --[[
    if self._stmt == nil or transaction.is_in_transaction(self) ~= true then
        ngx.log(ngx.ERR,"close database handle")
        pool.close_db(self._node, self._db)
        self._node = nil
    else
        ngx.log(ngx.ERR,"in transaction")
    end
    ]]--
    return err, err_msg
end

function _M.new(opts)
    if #opts.users == nil then
        return nil, "invalid options"
    end

    local max_packet_size = opts.max_packet_size
    if not max_packet_size then
    max_packet_size = 1024 * 1024 -- default 1 MB
    end

    local salt = utils.get_random_buf(20)

    return setmetatable({
        _sock = opts.sock,
        _users = opts.users,
        _capabilities = nil,
        _max_packet_size = max_packet_size,
        _salt = salt,
        _charset = charset.DEFAULT_CHARSET,
        _collation = charset.DEFAULT_COLLATION_ID,
        _packet_no = -1,
        _status = client_const.SERVER_STATUS_AUTOCOMMIT,
        _user = nil,
        _nodes = nil,

        _stmt_id = 1,
        _stmts = {},
        _srv_capabilities = {},
        _node = nil,
        _db = nil,
        _affected_rows = 0,
        _insert_id = 0,
    }, mt), nil
end

return _M