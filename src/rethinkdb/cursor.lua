--- Interface for cursors.
-- @module rethinkdb.cursor
-- @author Adam Grandquist
-- @license Apache
-- @copyright Adam Grandquist 2016

local convert_pseudotype = require'rethinkdb.internal.convert_pseudotype'
local errors = require'rethinkdb.errors'
local protodef = require'rethinkdb.internal.protodef'

local Response = protodef.Response
local ErrorType = protodef.ErrorType

local SUCCESS_PARTIAL = Response.SUCCESS_PARTIAL
local WAIT_COMPLETE = Response.WAIT_COMPLETE

local basic_responses = {
  [Response.SERVER_INFO] = true,
  [Response.SUCCESS_ATOM] = true,
  [Response.SUCCESS_PARTIAL] = true,
  [Response.SUCCESS_SEQUENCE] = true,
}

local error_types = {
  [Response.COMPILE_ERROR] = errors.ReQLCompileError,
  [Response.CLIENT_ERROR] = errors.ReQLClientError,
  [Response.RUNTIME_ERROR] = errors.ReQLRuntimeError,
}

local runtime_error_types = {
  [ErrorType.INTERNAL] = errors.ReQLInternalError,
  [ErrorType.NON_EXISTENCE] = errors.ReQLNonExistenceError,
  [ErrorType.OP_FAILED] = errors.ReQLOpFailedError,
  [ErrorType.OP_INDETERMINATE] = errors.ReQLOpIndeterminateError,
  [ErrorType.PERMISSION_ERROR] = errors.ReQLPermissionsError,
  [ErrorType.QUERY_LOGIC] = errors.ReQLQueryLogicError,
  [ErrorType.RESOURCE_LIMIT] = errors.ReQLResourceLimitError,
  [ErrorType.USER] = errors.ReQLUserError,
}

local function new_response(state, response, options, reql_inst)
  -- Behavior varies considerably based on response type
  local t = response.t
  if t ~= SUCCESS_PARTIAL then
    -- We got the final document for this cursor
    state.del_query()
  end
  local err = error_types[t]
  if err then
    local err_type = runtime_error_types[response.e]
    -- Error responses are not discarded, and the error will be sent to all future callbacks
    if err_type then
      local function it()
        return err_type(reql_inst.r, response.r[1], reql_inst, response.b)
      end
      return it
    end
    local function it()
      return err(reql_inst.r, response.r[1], reql_inst, response.b)
    end
    return it
  end
  if t == WAIT_COMPLETE then
    local function it()
    end
    return it
  end
  if basic_responses[t] then
    response.r, err = convert_pseudotype(reql_inst.r, response.r, options)
    if not response.r then
      local function it()
        return errors.ReQLDriverError(reql_inst.r, err, reql_inst)
      end
      return it
    end
    local ipairs_f, ipairs_s, ipairs_var = ipairs(response.r)
    local function it()
      local res
      ipairs_var, res = ipairs_f(ipairs_s, ipairs_var)
      if ipairs_var ~= nil then
        return res
      end
    end
    return it
  end
  local function it()
    return errors.ReQLDriverError(reql_inst.r, 'unknown response type from server [' .. t .. '].', reql_inst)
  end
  return it
end

local function each(state, var)
  if not state.it then
    if not state.open then
      return
    end
    local success, err = state.step()
    if not success then
      if type(err) == 'table' then
        return 0, err
      end
      return 0, errors.ReQLDriverError({}, err)
    end
  end
  local row = state.it()
  if row == nil then
    state.it = nil
    return each(state, var)
  end
  if type(row) == 'table' and row.ReQLError then
    return 0, row
  end
  return var + 1, row
end

local meta_table = {}

function meta_table.__tostring(cursor_inst)
  if cursor_inst.feed_type then
    return 'RethinkDB Cursor ' .. cursor_inst.feed_type
  end
  return 'RethinkDB Cursor'
end

function meta_table.__pairs(cursor_inst)
  return cursor_inst.each()
end

--- Object returned from a successful send to server. This is used to retrieve
-- results of a query.
local function cursor(r, state, options, reql_inst)
  local cursor_inst = setmetatable({r = r}, meta_table)

  function state.add_response(response)
    if not cursor_inst.feed_type then
      if response.n then
        for k, v in pairs(protodef.ResponseNote) do
          if v == response.n then
            cursor_inst.feed_type = k
          end
        end
      else
        cursor_inst.feed_type = 'finite'
      end
    end
    state.it = new_response(state, response, options, reql_inst)
    while state.outstanding_callback do
      local row = state.it()
      if not row then
        if not state.open then
          state.it = nil
          cursor_inst.set()
        end
      end
      if type(row) == 'table' and row.ReQLError then
        state.outstanding_callback(row)
        cursor_inst.set()
      end
      state.outstanding_callback(nil, row)
    end
  end

  --- Set or clear a callback for asynchronous processing of query results.
  -- This callback will only be called if another
  -- cursor from the same connection is retrieving rows synchronously. Calling
  -- without arguments clears any previous function set, and retains results for
  -- future retrieval.
  function cursor_inst.set(callback)
    state.outstanding_callback = callback
    if callback then
      state.maybe_response()
    end
  end

  --- Close a cursor. Closing a cursor cancels the corresponding query and frees
  -- the memory associated with the open request.
  function cursor_inst.close(callback)
    local function cb(err)
      if callback then return callback(err) end
      if err then
        return nil, err
      end
      return true
    end
    if state.open then
      local success, err = state.end_query()
      if not success then
        return cb(err)
      end
    end
    return cb()
  end

  --- Supports iteration with a for loop. First variable is an auto incrementing
  -- integer starting at 1. This integer does not correspond with any data on
  -- the server. If this is 0 then the row is an error. This error will be
  -- returned forever and the caller is responsible for breaking out of the
  -- loop. The second variable is a row result from the query or an error.
  -- Errors returned in a loop do not have valid links to the driver instance.
  function cursor_inst.each()
    cursor_inst.set()
    return each, state, 0
  end

  --- Collect all results from cursor and close. In the case of an error the
  -- first result is nil and the third result contains any results received.
  function cursor_inst.to_array()
    local arr = {}

    for i, v in cursor_inst.each() do
      if i == 0 then
        return nil, v, arr
      end
      arr[i] = v
    end

    return arr
  end

  return cursor_inst
end

return cursor
