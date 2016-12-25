--- Interface to handle default connection construction.
-- @module rethinkdb.connector
-- @author Adam Grandquist
-- @license Apache
-- @copyright Adam Grandquist 2016

local connection_instance = require'rethinkdb.connection_instance'
local current_handshake = require'rethinkdb.internal.current_handshake'

local m = {}

local DEFAULT_HOST = 'localhost'
local DEFAULT_PORT = 28015
local DEFAULT_USER = 'admin'
local DEFAULT_AUTH_KEY = ''
local DEFAULT_TIMEOUT = 20 -- In seconds

function m.init(r)
  function r.connector(connection_opts)
    local auth_key = connection_opts.password or connection_opts.auth_key or DEFAULT_AUTH_KEY
    local db = connection_opts.db -- left nil if this is not set
    local host = connection_opts.host or DEFAULT_HOST
    local port = connection_opts.port or DEFAULT_PORT
    local proto_version = connection_opts.proto_version or current_handshake
    local ssl_params = connection_opts.ssl
    local timeout = connection_opts.timeout or DEFAULT_TIMEOUT
    local user = connection_opts.user or DEFAULT_USER

    local function handshake_inst(_r, socket_inst)
      return proto_version(_r, socket_inst, auth_key, user)
    end

    local connector_inst_meta_table = {}

    function connector_inst_meta_table.__tostring()
      return 'rethinkdb connection to ' .. host .. ':' .. port
    end

    local connector_inst = setmetatable({r = r}, connector_inst_meta_table)

    function connector_inst.connect(callback)
      local connection = connection_instance(
        connector_inst.r,
        handshake_inst,
        host,
        port,
        ssl_params,
        timeout
      )
      if callback then
        local function cb(err, conn)
          if err then
            return callback(err)
          end
          conn.use(db)
          return callback(nil, conn)
        end
        return connection.connect(cb)
      end

      local conn, err = connection.connect()
      if err then
        return nil, err
      end
      conn.use(db)
      return conn
    end

    function connector_inst._start(reql_inst, options, callback)
      local function cb(err, conn)
        if err then
          if callback then
            return callback(err)
          end
          return nil, err
        end
        conn.use(db)
        return conn._start(reql_inst, options, callback)
      end
      return connector_inst.connect(cb)
    end

    function connector_inst.use(_db)
      db = _db
    end

    return connector_inst
  end
end

return m
