local config = require("gw.core.config")
local cjson = require("cjson.safe")
local producer = require("resty.kafka.producer")
local logger = require("gw.yulv.socket")
local syslog = require("gw.core.syslog")

local _M = {}

local logconf
local appname = "yulv"

local function rsyslog(name, msg, opts)
    if not logger.initted() then
        local ok, err = logger.init {
            host = opts.host,
            port = opts.port,
            sock_type = opts.type,
            flush_limit = 1,
            --drop_limit = 5678,
            timeout = 10000,
            pool_size = 100
        }
        if not ok then
            ngx.log(ngx.ERR, "failed to initialize the logger: ", err)
            return
        end
    end

    local logstr = syslog.encode("LOCAL0", "INFO", ngx.var.hostname, appname, ngx.worker.pid(), name, cjson.encode(msg))
    local bytes, err = logger.log(logstr)
    if err then
        ngx.log(ngx.ERR, "failed to log message: ", err)
        return
    end

end

local function kafkalog(msg, broker_list, topic)
    local message = cjson.encode(msg)
    local bp = producer:new(broker_list, { producer_type = "async" })
    local ok, err = bp:send(topic, nil, message)
    if not ok then
        ngx.log(ngx.ERR, "kafka send err:", err)
        return
    end
end

function _M.log(msg, type)
    if logconf == nil then
        logconf = config.get_config_log()
    end

    if type == "access" then
        if logconf.rsyslog and logconf.rsyslog.access_log then
            rsyslog("yulv_access", msg, logconf.rsyslog)
        end
        if logconf.kafka and logconf.kafka.access_toplic then
            kafkalog(msg, logconf.kafka.broker, logconf.kafka.access_toplic)
        end
        if logconf.file and logconf.file.access_log then
            ngx.log(ngx.ERR, cjson.encode(msg))
        end
    elseif type == "rule" then
        if logconf.rsyslog and logconf.rsyslog.access_log then
            rsyslog("yulv_rule", msg, logconf.rsyslog)
        end
        if logconf.kafka and logconf.kafka.access_toplic then
            kafkalog(msg, logconf.kafka.broker, logconf.kafka.rule_topic)
        end
        if logconf.file and logconf.file.access_log then
            ngx.log(ngx.ERR, cjson.encode(msg))
        end
    end
end

return _M