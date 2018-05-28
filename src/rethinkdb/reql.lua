--- Interface
-- The terms accepted and their signatures are more dependent on the server that
-- the driver connects to. All reql terms are available as a top level property
-- and as a chained term. See RethinkDB api documentation for terms and their
-- arguments.
-- @module rethinkdb.reql
-- @author Adam Grandquist
-- @license Apache
-- @copyright Adam Grandquist 2016

local errors = require'rethinkdb.errors'
local protodef = require'rethinkdb.internal.protodef'

local unpack = _G.unpack or table.unpack  -- luacheck: read globals table.unpack

local Term = protodef.Term

--- meta table for reql
-- @func __index
-- @table meta_table
local meta_table = {}

--- printable represention
function meta_table.__tostring(term)
  return table.concat{'reql.', term.st, '(...)'}
end

--- get index query on server
function meta_table.__call(term, ...)
  return term.bracket(...)
end

--- get count on server
function meta_table.__len(term)
  return term.count()
end

--- reql math term
function meta_table.__add(term, ...)
  return term.add(...)
end

--- reql math term
function meta_table.__mul(term, ...)
  return term.mul(...)
end

--- reql math term
function meta_table.__mod(term, ...)
  return term.mod(...)
end

--- reql math term
function meta_table.__sub(term, ...)
  return term.sub(...)
end

--- reql math term
function meta_table.__div(term, ...)
  return term.div(...)
end

local function continue_boolean(_, reql, val)
  return reql.datum(val)
end

local function continue_function(_, reql, val)
  return reql.func(val)
end

local function continue_nil(_, reql)
  return reql.json'null'
end

local function continue_number(_, reql, val)
  return reql.datum(val)
end

local function continue_string(_, reql, val)
  return reql.datum(val)
end

local function continue_table(_, reql, val, nesting_depth)
  if getmetatable(val) == meta_table then
    return val
  end
  local result = {}
  local array = true
  for first, second in pairs(val) do
    local data, err = reql(second, nesting_depth - 1)
    if not data then
      return nil, err
    end
    if array then array = type(first) == 'number' end
    result[first] = data
  end
  if array then
    return reql.make_array(unpack(result))
  end
  return reql.make_obj(result)
end

local function continue_thread(r)
  return nil, errors.ReQLDriverError(r ,'Cannot insert thread object into query')
end

local function continue_userdata(r)
  return nil, errors.ReQLDriverError(r, 'Cannot insert userdata object into query')
end

local continue_reql = {
  boolean = continue_boolean,
  ['function'] = continue_function,
  ['nil'] = continue_nil,
  number = continue_number,
  string = continue_string,
  table = continue_table,
  thread = continue_thread,
  userdata = continue_userdata,
}

--- terms that take a variable number of arguments and an optional final argument that is a table of options
local function get_opts(...)
  local args = {...}
  local n = #args
  local opt = args[n]
  if (type(opt) == 'table') and (getmetatable(opt) ~= meta_table) then
    for k in pairs(opt) do
      if type(k) ~= 'string' then
        return args
      end
    end
    args[n] = nil
    return args, opt
  end
  return args
end

--- terms that take 0 arguments and an optional final argument that is a table of options
local function arity_0(opts)
  return {}, opts
end

--- terms that take 2 arguments and an optional final argument that is a table of options
local function arity_2(arg1, arg2, opts)
  return {arg1, arg2}, opts
end

--- terms that take 3 arguments and an optional final argument that is a table of options
local function arity_3(arg1, arg2, arg3, opts)
  return {arg1, arg2, arg3}, opts
end

--- terms that take 4 arguments and an optional final argument that is a table of options
local function arity_4(arg1, arg2, arg3, arg4, opts)
  return {arg1, arg2, arg3, arg4}, opts
end

--- mapping from reql term names to argument signatures
local arg_wrappers = {
  [Term.between] = arity_4,
  [Term.between_deprecated] = arity_4,
  [Term.changes] = get_opts,
  [Term.circle] = get_opts,
  [Term.delete] = get_opts,
  [Term.distance] = get_opts,
  [Term.distinct] = get_opts,
  [Term.during] = arity_4,
  [Term.eq_join] = get_opts,
  [Term.filter] = arity_3,
  [Term.fold] = get_opts,
  [Term.get_all] = get_opts,
  [Term.get_intersecting] = get_opts,
  [Term.get_nearest] = get_opts,
  [Term.group] = get_opts,
  [Term.http] = arity_3,
  [Term.index_create] = get_opts,
  [Term.index_rename] = get_opts,
  [Term.insert] = arity_3,
  [Term.iso8601] = get_opts,
  [Term.js] = get_opts,
  [Term.make_obj] = arity_0,
  [Term.max] = get_opts,
  [Term.min] = get_opts,
  [Term.order_by] = get_opts,
  [Term.random] = get_opts,
  [Term.reconfigure] = arity_2,
  [Term.reduce] = get_opts,
  [Term.replace] = arity_3,
  [Term.slice] = get_opts,
  [Term.table] = get_opts,
  [Term.table_create] = get_opts,
  [Term.union] = get_opts,
  [Term.update] = arity_3,
  [Term.wait] = arity_2
}

local function binary(r, args, optargs)
  local data = args[1]
  if type(data) == 'string' then
    return {
      {['$reql_type$'] = 'BINARY', data = r.b64(data)}
    }, optargs
  end
  return args, optargs
end

local function fold(r, args, optargs)
  if optargs then
    if type(optargs.emit) == 'function' then
      optargs.emit = r.reql.func(optargs.emit, {arity = 3})
    end
    if type(optargs.finalEmit) == 'function' then
      optargs.finalEmit = r.reql.func(optargs.finalEmit, {arity = 1})
    end
  end
  local n = #args
  if type(args[n]) == 'function' then
    args[n] = r.reql.func(args[n], {arity = 2})
  end
  return args, optargs
end

--- int incremented to keep reql function arguments unique
local next_var_id = 0

local function func(r, args, optargs)
  local __func = args[1]
  local anon_args = {}
  local arg_nums = {}
  local arity = nil
  if optargs then
    arity, optargs.arity = optargs.arity, arity
  end
  if not arity and debug.getinfo then
    local func_info = debug.getinfo(__func)
    if func_info.what == 'Lua' and func_info.nparams then
      arity = func_info.nparams
    end
  end
  for _=1, arity or 1 do
    table.insert(arg_nums, next_var_id)
    table.insert(anon_args, r.reql.var(next_var_id))
    next_var_id = next_var_id + 1
  end
  __func = __func(unpack(anon_args))
  if __func == nil then
    return nil, errors.ReQLDriverError(r, 'Anonymous function returned `nil`. Did you forget a `return`?')
  end
  return {arg_nums, __func}, optargs
end

local function call(r, args, optargs)
  local __func = table.remove(args)
  if type(__func) == 'function' then
    __func = r.reql.func(__func, {arity = #args})
  end
  return {__func, unpack(args)}, optargs
end

local function reduce(r, args, optargs)
  local n = #args
  if type(args[n]) == 'function' then
    args[n] = r.reql.func(args[n], {arity = 2})
  end
  return args, optargs
end

local mutate_table = {
  [Term.binary] = binary,
  [Term.call] = call,
  [Term.fold] = fold,
  [Term.func] = func,
  [Term.reduce] = reduce,
}

--- returns a chained term
-- @tab r
-- @string st reql term name
-- @treturn function @{reql_term}
-- @treturn nil if there is no known term
local function index(r, st)
  local tt = rawget(Term, st)
  if not tt then
    return nil
  end

  local wrap = arg_wrappers[tt]
  local mutate = mutate_table[tt]

  --- instantiates a chained term
  local function reql_term(...)
    local args, optargs
    if wrap then
      args, optargs = wrap(...)
    else
      args = {...}
    end

    if mutate then
      args, optargs = mutate(r, args, optargs)
      if not args then
        return nil, optargs
      end
    end

    local reql_inst = setmetatable({
      args = {}, optargs = {}, r = r, st = st, tt = tt}, meta_table)

    if st == 'datum' then
      reql_inst.args[1] = args[1]
    else
      for i, a in ipairs(args) do
        local data, err = r.reql(a)
        if not data then
          return nil, err
        end
        reql_inst.args[i] = data
      end

      if optargs then
        for k, v in pairs(optargs) do
          local data, err = r.reql(v)
          if not data then
            return nil, err
          end
          reql_inst.optargs[k] = data
        end
      end
    end

    --- Run a query on a connection and return a cursor or nil and an error
    -- @tab connection
    -- @tab[opt] options
    -- @func[opt] callback
    function reql_inst.run(connection, options, callback)
      -- Handle run(connection, callback)
      if type(options) == 'function' then
        callback, options = options, callback
      end
      -- else we suppose that we have run(connection[, options[, callback]])

      return connection._start(reql_inst, options or {}, callback)
    end

    return reql_inst
  end

  return reql_term
end

function meta_table.__index(cls, st)
  local wrap = index(cls.r, st)

  local function reql_term(...)
    return wrap(cls, ...)
  end

  return reql_term
end

local m = {}

function m.init(r)
  --- meta table driver module
  -- @func __call
  -- @func __index
  -- @table reql_meta_table
  local reql_meta_table = {}

  --- wrap lua value
  -- @tab reql driver ast module
  -- @param[opt] val lua value to wrap
  -- @int[opt=20] nesting_depth max depth of value recursion
  -- @treturn table reql
  -- @raise Cannot insert userdata object into query
  -- @raise Cannot insert thread object into query
  function reql_meta_table.__call(reql, val, nesting_depth)
    nesting_depth = nesting_depth or 20
    if nesting_depth <= 0 then
      return nil, errors.ReQLDriverError(r, 'Nesting depth limit exceeded')
    end
    local continue = continue_reql[type(val)]
    if not continue then
      return nil, errors.ReQLDriverError(r, 'Unknown Lua type ' .. type(val))
    end
    return continue(r, reql, val, nesting_depth)
  end

  --- creates a top level term
  -- @tab _ driver ast module
  -- @string st reql term name
  -- @treturn table reql
  function reql_meta_table.__index(_, st)
    return index(r, st)
  end

  --- module export
  -- @table reql
  r.reql = setmetatable({}, reql_meta_table)
end

return m
