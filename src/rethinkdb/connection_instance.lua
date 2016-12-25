--- Interface for concrete connections.
-- @module rethinkdb.connection_instance
-- @author Adam Grandquist
-- @license Apache
-- @copyright Adam Grandquist 2016

local cursor = require'rethinkdb.cursor'
local errors = require'rethinkdb.errors'
local ltn12 = require('ltn12')
local protocol = require'rethinkdb.internal.protocol'
local protect = require'rethinkdb.internal.protect'
local socket = require'rethinkdb.internal.socket'

local function with(inline, ...)
  inline()
  return ...
end

local conn_inst_meta_table = {}

function conn_inst_meta_table.__tostring(conn_inst)
  return (
    conn_inst.is_open() and 'open' or 'closed'
  ) .. ' rethinkdb connection to ' .. conn_inst.host .. ':' .. conn_inst.port
end

local function connection_instance(r, handshake_inst, host, port, ssl_params, timeout)
  local db, outstanding_callbacks, protocol_inst, responses = nil, {}, nil, {}

  local function reset(err, callback)
    local function cb(...)
      if type(callback) == 'function' then
        return callback(...)
      end
      return nil, ...
    end
    for _, state in pairs(outstanding_callbacks) do
      state.open = nil
    end
    if protocol_inst then protocol_inst.close() end
    db, outstanding_callbacks, protocol_inst, responses = nil, {}, nil, {}
    if type(err) == 'string' then
      return cb(errors.ReQLDriverError(r, err))
    end
    return cb(err)
  end

  local conn_inst = setmetatable(
    {host = host, port = port, r = r}, conn_inst_meta_table)

  function conn_inst.is_open()
    return protocol_inst and true or false
  end

  --- Change the default database on this connection.
  function conn_inst.use(_db)
    if _db then
      db = conn_inst.r.reql.db(_db)
    else
      db = nil
    end
  end

  local function add_response(token, response, state)
    protocol_inst.continue_query(token)

    local err
    response, err = protect(conn_inst.r.decode, response)
    if not response then
      return reset(err)
    end

    state.add_response(response)
    return true
  end

  local function sink(chunk, err)
    if not chunk then
      return nil, err
    end
    local token, response = chunk[1], chunk[2]
    if token then
      local state = outstanding_callbacks[token]
      if not state then
        return true
      end
      if state.outstanding_callback then
        local success, result = add_response(token, response, state)
        if not success then
          return nil, result
        end
        return true
      end
      responses[token] = response
    end
    return true
  end

  local function make_cursor(token, options, reql_inst)
    local state = {open = true}

    function state.del_query()
      -- This query is done, delete this cursor
      outstanding_callbacks[token] = nil
      responses[token] = nil
      state.open = nil
    end

    function state.end_query()
      if protocol_inst then
        return protocol_inst.end_query(token)
      end
    end

    function state.maybe_response()
      if responses[token] then
        local response = nil
        response, responses[token] = responses[token], response

        return add_response(token, response, state)
      end
      return true
    end

    function state.step()
      -- Buffer data, execute return results if need be
      while not responses[token] do
        local success, err = ltn12.pump.step(protocol_inst.source(), sink)
        if not success then
          return reset(err)
        end
      end
      return state.maybe_response()
    end

    -- Save cursor shared state
    outstanding_callbacks[token] = state

    return cursor(conn_inst.r, state, options, reql_inst)
  end

  function conn_inst._start(reql_inst, options, callback)
    local function cb(err, cur)
      if type(callback) == 'function' then
        if cur then
          return with(cur.close, callback(err, cur))
        end
        return callback(err)
      end
      return cur, err
    end
    if not conn_inst.is_open() then return cb(errors.ReQLDriverError(r, 'Connection is closed.')) end

    -- Set global options
    local global_opts = {}

    for first, second in pairs(options) do
      local data, err = conn_inst.r.reql(second)
      if not data then
        return cb(err)
      end
      global_opts[first] = data
    end

    if options.db then
      global_opts.db = conn_inst.r.reql.db(global_opts.db)
    elseif db then
      global_opts.db = db
    end

    -- Construct query
    local token, err = protocol_inst.send_query(conn_inst.r, reql_inst, global_opts)

    if err then
      return reset(err, cb)
    end

    if options.noreply then
      return true
    end

    return cb(nil, make_cursor(token, options, reql_inst))
  end

  --- Close an open connection. Accepts the following options.
  -- - noreply_wait: whether to wait for noreply writes to complete before
  --   closing (default true). If this is set to false, some outstanding noreply
  --   writes may be aborted. Has no effect if connection is already closing.
  -- Closing a connection waits until all outstanding requests have finished and
  -- then frees any open resources associated with the connection. If
  -- noreply_wait is set to false, all outstanding requests are canceled
  -- immediately.
  function conn_inst.close(opts_or_callback, callback)
    local opts = {}
    if callback or type(opts_or_callback) == 'table' then
      opts = opts_or_callback
    elseif type(opts_or_callback) == 'function' then
      callback = opts_or_callback
    end

    local noreply_wait = (opts.noreply_wait ~= false) and conn_inst.is_open()

    if noreply_wait then
      conn_inst.noreply_wait()
    end

    return reset(nil, callback)
  end

  function conn_inst.connect(callback)
    local socket_inst, err = socket(conn_inst.r, conn_inst.host, conn_inst.port, ssl_params, timeout)

    if not socket_inst then
      return reset(err, callback)
    end

    local init_success

    init_success, err = handshake_inst(conn_inst.r, socket_inst)

    if not init_success then
      if type(err) == 'table' then
        if 10 <= err.error_code and err.error_code <= 20 then
          return reset(errors.ReQLAuthError(r, err.error), callback)
        end
        return reset(err.error, callback)
      end
      return reset(err, callback)
    end

    protocol_inst, err = protocol(socket_inst)

    if not protocol_inst then
      return reset(err, callback)
    end

    if callback then
      return with(reset, callback(nil, conn_inst))
    end

    return conn_inst
  end

  --- noreply_wait ensures that previous queries with the noreply flag have been
  -- processed by the server. Note that this guarantee only applies to queries
  -- run on the given connection.
  function conn_inst.noreply_wait(callback)
    local function cb(err)
      if err then
        return reset(err, callback)
      end
      if callback then
        return callback(nil, true)
      end
      return true
    end
    if not conn_inst.is_open() then return cb(errors.ReQLDriverError(r, 'Connection is closed.')) end

    -- Construct query
    local token, err = protocol_inst.noreply_wait()

    if not token then
      return cb(err)
    end

    local _, e = make_cursor(token, {}, {}).to_array()
    return cb(e)
  end

  --- Close and reopen a connection. Accepts the following options.
  -- - noreply_wait. whether to wait for noreply writes to complete before
  --   closing (default true). If this is set to false, some outstanding noreply
  --   writes may be aborted. Has no effect if connection is already closing.
  function conn_inst.reconnect(opts_or_callback, callback)
    local opts = {}
    if callback or not type(opts_or_callback) == 'function' then
      opts = opts_or_callback
    else
      callback = opts_or_callback
    end
    conn_inst.close(opts)
    return conn_inst.connect(callback)
  end

  function conn_inst.server(callback)
    local function cb(err, arr)
      if err then
        return reset(err, callback)
      end
      if callback then
        return callback(nil, arr)
      end
      return arr
    end
    if not conn_inst.is_open() then return cb(errors.ReQLDriverError(r, 'Connection is closed.')) end

    -- Construct query
    local token, err = protocol_inst.server_info()

    if not token then
      return cb(err)
    end

    local arr, e = make_cursor(token, {}, {}).to_array()
    return cb(e, arr)
  end

  return conn_inst
end

return connection_instance
