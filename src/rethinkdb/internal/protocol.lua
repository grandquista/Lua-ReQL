--- Interface to handle single message protocol details.
-- @module rethinkdb.internal.protocol
-- @author Adam Grandquist
-- @license Apache
-- @copyright Adam Grandquist 2016

local errors = require'rethinkdb.errors'
local little_to_int = require'rethinkdb.internal.bytes_to_int'.little
local int_to_little = require'rethinkdb.internal.int_to_bytes'.little
local ltn12 = require('ltn12')
local protect = require'rethinkdb.internal.protect'
local protodef = require'rethinkdb.internal.protodef'

local Query = protodef.Query
local Term = protodef.Term

local datum = Term.datum
local make_obj = Term.make_obj

local CONTINUE = '[' .. Query.CONTINUE .. ']'
local NOREPLY_WAIT = '[' .. Query.NOREPLY_WAIT .. ']'
local SERVER_INFO = '[' .. Query.SERVER_INFO .. ']'
local STOP = '[' .. Query.STOP .. ']'

local START = Query.START

local nil_table = {}

local local_opts = {
  binary_format = true,
  format = true,
  group_format = true,
  time_format = true,
}

--- convert from internal represention to JSON
local function build(term)
  if type(term) ~= 'table' then return term end
  if term.tt == datum then
    return term.args[1]
  end
  if term.tt == make_obj then
    local res = {}
    for key, val in pairs(term.optargs) do
      res[key] = build(val)
    end
    return res
  end
  local res = {term.tt}
  if next(term.args) then
    local args = {}
    for i, arg in ipairs(term.args) do
      args[i] = build(arg)
    end
    res[2] = args
  end
  if next(term.optargs) then
    local opts = {}
    for key, val in pairs(term.optargs) do
      opts[key] = build(val)
    end
    res[3] = opts
  end
  return res
end

local function get_response(ctx)
  if ctx.response_length then
    if string.len(ctx.buffer) < ctx.response_length then
      return
    end
    local response = string.sub(ctx.buffer, 1, ctx.response_length)
    ctx.buffer = string.sub(ctx.buffer, ctx.response_length + 1)
    ctx.response_length = nil
    return {ctx.token, response}
  end
  if string.len(ctx.buffer) < 12 then
    return
  end
  ctx.token = little_to_int(string.sub(ctx.buffer, 1, 8))
  ctx.response_length = little_to_int(string.sub(ctx.buffer, 9, 12))
  ctx.buffer = string.sub(ctx.buffer, 13)
end

local function buffer_response(ctx, chunk)
  if chunk then
    ctx.buffer = ctx.buffer .. chunk
  else
    local expected_length = ctx.response_length or 12
    if string.len(ctx.buffer) < expected_length then
      ctx.buffer = ''
      ctx.response_length = nil
      return nil, ctx
    end
  end
  return get_response(ctx) or nil_table, ctx
end

local function new_token()
  local var = 0

  local function get_token()
    var = var + 1
    return var
  end

  return get_token
end

local function protocol(socket_inst)
  local ctx = {buffer = ''}
  local filter = ltn12.filter.cycle(buffer_response, ctx)

  local function write_socket(r, token, data)
    data = table.concat{int_to_little(token, 8), int_to_little(string.len(data), 4), data}
    local success, err = ltn12.pump.all(ltn12.source.string(data), socket_inst.sink)
    if not success then
      return nil, errors.ReQLDriverError(r, err .. ': writing socket')
    end
    return token
  end

  local get_token = new_token()

  local protocol_inst = {close = socket_inst.close}

  function protocol_inst.send_query(r, reql_inst, global_opts)
    local query = {START, build(reql_inst)}
    if global_opts and next(global_opts) then
      local optargs = {}
      for k, v in pairs(global_opts) do
        if not local_opts[k] then
          optargs[k] = build(v)
        end
      end
      if next(optargs) then query[3] = optargs end
    end

    -- Assign token
    local data, err = protect(r.encode, query)
    if not data then
      return nil, errors.ReQLDriverError(r, err .. ': encoding query')
    end
    return write_socket(r, get_token(), data)
  end

  function protocol_inst.continue_query(r, token)
    return write_socket(r, token, CONTINUE)
  end

  function protocol_inst.end_query(r, token)
    return write_socket(r, token, STOP)
  end

  function protocol_inst.noreply_wait(r)
    return write_socket(r, get_token(), NOREPLY_WAIT)
  end

  function protocol_inst.server_info(r)
    return write_socket(r, get_token(), SERVER_INFO)
  end

  function protocol_inst.source()
    return ltn12.source.chain(socket_inst.source(ctx.response_length or 12), filter)
  end

  return protocol_inst
end

return protocol
