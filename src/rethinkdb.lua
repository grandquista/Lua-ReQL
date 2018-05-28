--- Main interface combining public modules in an export table.
-- The api accepts a callback function as an optional last argument in several
-- places. Where it does, the normal return objects are sent to the callback
-- after an optional error object, and the callback results are returned.  All
-- calls that can error will return a nil or false first and an error second. No
-- results will be returned after an error. An error can be a string from other
-- Lua modules or a ReQLError.
-- @module rethinkdb
-- @author Adam Grandquist
-- @license Apache
-- @copyright Adam Grandquist 2016
-- @alias r

local connection = require'rethinkdb.connection'
local connector = require'rethinkdb.connector'
local current_handshake = require'rethinkdb.internal.current_handshake'
local depreciate = require'rethinkdb.depreciate'
local utilities = require'rethinkdb.internal.utilities'
local reql = require'rethinkdb.reql'
local rtype = require'rethinkdb.rtype'

local v = require('semver')

--- Creates an independent driver instance with the passed options.
local function new(driver_options)
  --- The top-level ReQL namespace. Connections, cursors, errors, queries, and
  -- driver instances have a property r that points to the driver instance they
  -- were created with.
  local r = {}

  r.new = new

  --- Implementation of RethinkDB handshake version 1. Supports server version
  -- 2.3+. Passed to proto_version connection option.
  r.proto_V1_0 = current_handshake

  --- The loaded luarocks version string as a semver.
  r.version = v'1.0.4'
  r._VERSION = r.version

  connection.init(r)
  connector.init(r)
  depreciate.init(r)
  reql.init(r)
  rtype.init(r)
  utilities.init(r, driver_options or {})

  return r
end

return new()
