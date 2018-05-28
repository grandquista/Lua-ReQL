--- Helper for converting a string of bytes to an int.
-- @module rethinkdb.internal.bytes_to_int
-- @author Adam Grandquist
-- @license Apache
-- @copyright Adam Grandquist 2016

local bytes_to_int = {}

local unpack = string.unpack  -- luacheck: read globals string.unpack

if not unpack then
  function bytes_to_int.little(str)
    local n = 0
    for k=1, string.len(str) do
      n = n + string.byte(str, k) * 2 ^ ((k - 1) * 8)
    end
    return n
  end

  function bytes_to_int.big(str)
    local n = 0
    local bytes = string.len(str)
    for k=1, bytes do
      n = n + string.byte(str, k) * 2 ^ ((bytes - k) * 8)
    end
    return n
  end

  return bytes_to_int
end

function bytes_to_int.little(str)
  return (unpack('!1<I' .. string.len(str), str))
end

function bytes_to_int.big(str)
  return (unpack('!1>I' .. string.len(str), str))
end

return bytes_to_int
