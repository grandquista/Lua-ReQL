--- Helper for converting an int to a string of bytes.
-- @module rethinkdb.internal.int_to_bytes
-- @author Adam Grandquist
-- @license Apache
-- @copyright Adam Grandquist 2016

local int_to_bytes = {}

local pack = string.pack  -- luacheck: read globals string.pack

if not pack then
  local unpack = _G.unpack or table.unpack  -- luacheck: read globals table.unpack

  function int_to_bytes.little(num, bytes)
    local res = {}
    num = math.fmod(num, 2 ^ (8 * bytes))
    for k = bytes, 1, -1 do
      local den = 2 ^ (8 * (k - 1))
      res[k] = math.floor(num / den)
      num = math.fmod(num, den)
    end
    return string.char(unpack(res))
  end

  function int_to_bytes.big(num, bytes)
    local res = {}
    num = math.fmod(num, 2 ^ (8 * bytes))
    for k = 1, bytes do
      local den = 2 ^ (8 * (bytes - k))
      res[k] = math.floor(num / den)
      num = math.fmod(num, den)
    end
    return string.char(unpack(res))
  end

  return int_to_bytes
end

function int_to_bytes.little(num, bytes)
  num = math.fmod(num, 2 ^ (8 * bytes))
  return pack('!1<I' .. bytes, num)
end

function int_to_bytes.big(num, bytes)
  num = math.fmod(num, 2 ^ (8 * bytes))
  return pack('!1>I' .. bytes, num)
end

return int_to_bytes
