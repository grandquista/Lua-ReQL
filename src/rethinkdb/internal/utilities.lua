--- Helpers to allow overriding driver internals.
-- @module rethinkdb.internal.utilities
-- @author Adam Grandquist
-- @license Apache
-- @copyright Adam Grandquist 2016

local mime = require('mime')

local json, socket

if ngx == nil then  --luacheck: globals ngx
  json = require('json')
  socket = require('socket')
else
  json = require('cjson')
  socket = ngx.socket  --luacheck: globals ngx
  socket.sink = require('socket').sink
  socket.source = require('socket').source
end

local m = {}

function m.init(r, driver_options)
  local _mime = driver_options.mime or mime
  local _json = driver_options.json or json

  r.r = r

  r.b64 = driver_options.b64 or _mime.b64
  r.unb64 = driver_options.unb64 or _mime.unb64

  r.decode = driver_options.decode or _json.decode
  r.encode = driver_options.encode or _json.encode

  r.socket = driver_options.socket or socket

  r.tcp = driver_options.tcp or r.socket.tcp

  if driver_options.grid then
    require'rethinkdb.grid'.init(r)
  end
end

return m
