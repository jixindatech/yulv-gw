local lshift = bit.lshift

local _M = {}

_M.ERR_HEADER = 0xff

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

return _M