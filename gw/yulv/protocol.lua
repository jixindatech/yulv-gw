local strbyte = string.byte
local bor = bit.bor
local lshift = bit.lshift
local rshift = bit.rshift

local _M = {
    OK = 0,
    WaitForServerGreeting = 1,
    WaitForClientHello = 2,
    WaitForServerHello = 3,
    WaitForAuthSwitchRequest = 4,
    WaitForAuthSwitchResponse = 5,

    HeaderLen = 4,
}

local function _get_byte3(data, i)
    local a, b, c = strbyte(data, i, i + 2)
    return bor(a, lshift(b, 8), lshift(c, 16)), i + 3
end

local function get_sock_data(sock)
    local header, err = sock:receive(_M.HeaderLen) -- packet header
    if not header then
        return nil, nil, "failed to receive packet header: " .. err
    end
    local len, pos = _get_byte3(header, 1)
    --print("packet length: ", len)

    if len == 0 then
        return nil, nil, "empty packet"
    end

    local num = strbyte(header, pos)
    --print("recv packet: packet no: ", num)

    local packet_no = num
    local data, err = sock:receive(len)
    if not data then
        return nil, nil, "failed to read packet content: " .. err
    end

    return header, data, nil
end

function _M.get_server_greeting(sock)
    return get_sock_data(sock)
end

function _M.get_client_hello(sock)
    return get_sock_data(sock)
end

function _M.get_server_hello(sock)
    return get_sock_data(sock)
end

function _M.get_client_authrequest_data(sock)
    return get_sock_data(sock)
end

function _M.get_server_authresponse_data(sock)
    return get_sock_data(sock)
end

function _M.get_client_data(sock)
    return get_sock_data(sock)
end
return _M