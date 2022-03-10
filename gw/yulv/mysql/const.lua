local lshift = bit.lshift
local bor = bit.bor

local _M = {}

_M.cmd = {
	COM_SLEEP               = 0,
	COM_QUIT                = 1,
	COM_INIT_DB             = 2,
	COM_QUERY               = 3,
	COM_FIELD_LIST          = 4,
	COM_CREATE_DB           = 5,
	COM_DROP_DB             = 6,
	COM_REFRESH             = 7,
	COM_SHUTDOWN            = 8,
	COM_STATISTICS          = 9,
	COM_PROCESS_INFO        = 10,
	COM_CONNECT             = 11,
	COM_PROCESS_KILL        = 12,
	COM_DEBUG               = 13,
	COM_PING                = 14,
	COM_TIME                = 15,
	COM_DELAYED_INSERT      = 16,
	COM_CHANGE_USER         = 17,
	COM_BINLOG_DUMP         = 18,
	COM_TABLE_DUMP          = 19,
	COM_CONNECT_OUT         = 20,
	COM_REGISTER_SLAVE      = 21,
	COM_STMT_PREPARE        = 22,
	COM_STMT_EXECUTE        = 23,
	COM_STMT_SEND_LONG_DATA = 24,
	COM_STMT_CLOSE          = 25,
	COM_STMT_RESET          = 26,
	COM_SET_OPTION          = 27,
	COM_STMT_FETCH          = 28,
	COM_DAEMON              = 29,
	COM_BINLOG_DUMP_GTID    = 30,
	COM_RESET_CONNECTION    = 31,

}

_M.client_capabilities = {
    	CLIENT_LONG_PASSWORD                    = lshift(1, 0),
    	CLIENT_FOUND_ROWS                       = lshift(1, 1),
    	CLIENT_LONG_FLAG                        = lshift(1, 2),
    	CLIENT_CONNECT_WITH_DB                  = lshift(1, 3),
    	CLIENT_NO_SCHEMA                        = lshift(1, 4),
    	CLIENT_COMPRESS                         = lshift(1, 5),
    	CLIENT_ODBC                             = lshift(1, 6),
    	CLIENT_LOCAL_FILES                      = lshift(1, 7),
    	CLIENT_IGNORE_SPACE                     = lshift(1, 8),
    	CLIENT_PROTOCOL_41                      = lshift(1, 9),
    	CLIENT_INTERACTIVE                      = lshift(1, 10),
    	CLIENT_SSL                              = lshift(1, 11),
    	CLIENT_IGNORE_SIGPIPE                   = lshift(1, 12),
    	CLIENT_TRANSACTIONS                     = lshift(1, 13),
    	CLIENT_RESERVED                         = lshift(1, 14),
    	CLIENT_SECURE_CONNECTION                = lshift(1, 15),
    	CLIENT_MULTI_STATEMENTS                 = lshift(1, 16),
    	CLIENT_MULTI_RESULTS                    = lshift(1, 17),
    	CLIENT_PS_MULTI_RESULTS                 = lshift(1, 18),
    	CLIENT_PLUGIN_AUTH                      = lshift(1, 19),
    	CLIENT_CONNECT_ATTRS                    = lshift(1, 20),
    	CLIENT_PLUGIN_AUTH_LENENC_CLIENT_DATA   = lshift(1, 21),

}

_M.DEFAULT_CAPABILITY = bor(
        _M.client_capabilities.CLIENT_LONG_PASSWORD,
        _M.client_capabilities.CLIENT_LONG_FLAG,
        _M.client_capabilities.CLIENT_CONNECT_WITH_DB,
        _M.client_capabilities.CLIENT_PROTOCOL_41,
        _M.client_capabilities.CLIENT_TRANSACTIONS,
        _M.client_capabilities.CLIENT_SECURE_CONNECTION
)

_M.MAX_PAYLOAD_LEN = lshift(1, 24) - 1


_M.OK_HEADER             = 0x00
_M.ERR_HEADER            = 0xff
_M.EOF_HEADER            = 0xfe
_M.LOCAL_IN_FILE_HEADER  = 0xfb

_M.stmt = {
    MYSQL_TYPE_DECIMAL   = 0,
    MYSQL_TYPE_TINY      = 1,
    MYSQL_TYPE_SHORT     = 2,
    MYSQL_TYPE_LONG      = 3,
    MYSQL_TYPE_FLOAT     = 4,
    MYSQL_TYPE_DOUBLE    = 5,
    MYSQL_TYPE_NULL      = 6,
    MYSQL_TYPE_TIMESTAMP = 7,
    MYSQL_TYPE_LONGLONG  = 8,
    MYSQL_TYPE_INT24     = 9,
    MYSQL_TYPE_DATE      = 10,
    MYSQL_TYPE_TIME      = 11,
    MYSQL_TYPE_DATETIME  = 12,
    MYSQL_TYPE_YEAR      = 13,
    MYSQL_TYPE_NEWDATE   = 14,
    MYSQL_TYPE_VARCHAR   = 15,
    MYSQL_TYPE_BIT       = 16,

    MYSQL_TYPE_NEWDECIMAL    =  0xf6,
    MYSQL_TYPE_ENUM          =  0xf7,
    MYSQL_TYPE_SET           =  0xf8,
    MYSQL_TYPE_TINY_BLOB     =  0xf9,
    MYSQL_TYPE_MEDIUM_BLOB   =  0xfa,
    MYSQL_TYPE_LONG_BLOB     =  0xfb,
    MYSQL_TYPE_BLOB          =  0xfc,
    MYSQL_TYPE_VAR_STRING    =  0xfd,
    MYSQL_TYPE_STRING        =  0xfe,
    MYSQL_TYPE_GEOMETRY      =  0xff,
}

return _M