--- Interface to handle socket timeouts and recoverable errors.
-- @module rethinkdb.internal.socket
-- @author Adam Grandquist
-- @license Apache
-- @copyright Adam Grandquist 2016

local errors = require'rethinkdb.errors'
local ssl = require('ssl')
local socket_sink = require('socket').sink
local socket_source = require('socket').source

local function settimeout(socket, ...)
  return pcall(socket.settimeout, socket, ...)
end

local function socket(r, host, port, ssl_params, timeout)
  local raw_socket, err = r.tcp()

  if not raw_socket then
    return nil, errors.ReQLDriverError(r, err .. ': opening socket')
  end

  local status = settimeout(raw_socket, timeout, 't') and
    settimeout(raw_socket, timeout, 'b') or
    settimeout(raw_socket, timeout)

  if not status then
    return nil, errors.ReQLDriverError(r, 'Failed to set timeout')
  end

  status, err = raw_socket:connect(host, port)

  if not status then
    return nil, errors.ReQLDriverError(r, err .. ': connecting socket')
  end

  if ssl_params then
    raw_socket, err = ssl.wrap(raw_socket, ssl_params)

    if not raw_socket then
      return nil, errors.ReQLDriverError(r, err .. ': wrapping socket in ssl')
    end

    status = false
    while not status do
      status, err = raw_socket:dohandshake()
      if err == 'closed' then
        return nil, errors.ReQLDriverError(
          r, 'socket closed durring ssl handshake')
      end
    end
  end

  local socket_inst = {}

  socket_inst.sink = socket_sink('keep-open', raw_socket)

  function socket_inst.source(length)
    return socket_source('by-length', raw_socket, length)
  end

  function socket_inst.close()
    if not ngx and not ssl_params then  --luacheck: globals ngx
      raw_socket:shutdown()
    end
    raw_socket:close()
  end

  return socket_inst
end

return socket
