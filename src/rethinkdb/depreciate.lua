--- Interface
-- @module rethinkdb.reql
-- @author Adam Grandquist
-- @license Apache
-- @copyright Adam Grandquist 2016

local int_to_little = require'rethinkdb.internal.int_to_bytes'.little
local ltn12 = require('ltn12')

local function proto_V0_x(socket_inst, auth_key, magic)
  -- Initialize connection with magic number to validate version

  local data = table.concat{
    magic,
    int_to_little(string.len(auth_key), 4),
    auth_key,
    '\199\112\105\126'
  }
  local success, err = ltn12.pump.all(ltn12.source.string(data), socket_inst.sink)
  if not success then
    return nil, err
  end

  local sink, buffer = ltn12.sink.table()

  -- Now we have to wait for a response from the server
  -- acknowledging the connection
  while true do
    if string.len(table.concat(buffer)) > 8 then
      success, err = ltn12.pump.all(socket_inst.source(1), sink)
      socket_inst.close()
      if not success then
        return nil, err
      end
      return nil, table.concat(buffer)
    end
    success, err = ltn12.pump.step(socket_inst.source(1), sink)
    if not success then
      return nil, err
    end
    if table.concat(buffer) == 'SUCCESS\0' then
      -- We're good, finish setting up the connection
      return true
    end
  end
end

local m = {}

function m.init(r)
  --- Depreciated implementation of RethinkDB handshake version 0.3. Supports
  -- server version 1.16+. Passed to proto_version connection option. Will be
  -- removed in driver version 2.
  function r.proto_V0_3(_, socket_inst, auth_key)
    return proto_V0_x(socket_inst, auth_key, '\62\232\117\95')
  end

  --- Depreciated implementation of RethinkDB handshake version 0.4. Supports
  -- server version 2.0+. Passed to proto_version connection option. Will be
  -- removed in driver version 2.
  function r.proto_V0_4(_, socket_inst, auth_key)
    return proto_V0_x(socket_inst, auth_key, '\32\45\12\64')
  end
end

return m
