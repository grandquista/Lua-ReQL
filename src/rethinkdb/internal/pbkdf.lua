--- pasword based key derivation function.
-- @module rethinkdb.internal.pbkdf
-- @author Adam Grandquist
-- @license Apache
-- @copyright Adam Grandquist 2016
-- implemented following (https://tools.ietf.org/html/rfc2898)

--- Helper for bitwise operations.
local function prequire(mod_name, ...)
  if not mod_name then return end

  local success, bits = pcall(require, mod_name)

  if success then
    return true, bits
  end

  return prequire(...)
end

local _, bits = prequire(
  'rethinkdb.internal.bits53', 'bit32', 'bit', 'rethinkdb.internal.bits51')

local crypto = require('crypto')

local bxor = bits.bxor

local unpack = _G.unpack or table.unpack

local function int_to_bytes(num, bytes)
  if string.pack then
    return string.pack('!1>I' .. bytes, num)
  end

  local res = {}
  num = math.fmod(num, 2 ^ (8 * bytes))
  for k = 1, bytes do
    local den = 2 ^ (8 * (bytes - k))
    res[k] = math.floor(num / den)
    num = math.fmod(num, den)
  end
  return string.char(unpack(res))
end

--- key derivation function
-- dtype
-- password an octet string
-- salt an octet string
-- iteration count a positive integer
-- dkLen length in octets of derived key, a positive integer
local function hmac_pbkdf2(dtype, password, salt, iteration, dkLen)
  -- length in octets of pseudorandom function output, a positive integer
  local hLen = string.len(crypto.hmac.digest(dtype, '', '', true))

  if dkLen > (2^32 - 1) * hLen then
    return nil, 'derived key too long'
  end

  --- length in blocks of derived key, a positive integer
  -- l = CEIL (dkLen / hLen) ,
  local l = math.ceil(dkLen / hLen)

  --- intermediate values, octet strings
  -- T_1 = F (P, S, c, 1) ,
  -- T_2 = F (P, S, c, 2) ,
  -- ...
  -- T_l = F (P, S, c, l) ,
  local T = {}

  --- underlying pseudorandom function
  -- local hmac = crypto.hmac.new(dtype, password)

  for i=1, l do
    --- intermediate values, octet strings
    -- F (P, S, c, i) = U_1 \xor U_2 \xor ... \xor U_c
    -- U_1 = PRF (P, S || INT (i)) ,
    -- U_2 = PRF (P, U_1) ,
    -- ...
    -- U_c = PRF (P, U_{c-1}) .
    local U = crypto.hmac.digest(dtype, salt .. int_to_bytes(i, 4), password, true)

    local t = {}

    for _=2, iteration do
      for j=1, string.len(U) do
        t[j] = bxor(t[j] or 0, string.byte(U, j) or 0)
      end

      U = crypto.hmac.digest(dtype, U, password, true)
    end

    for j=1, string.len(U) do
      t[j] = bxor(t[j] or 0, string.byte(U, j) or 0)
    end

    -- message authentication code, an octet string
    T[i] = string.char(unpack(t))
  end

  --- derived key, an octet string
  -- DK = T_1 || T_2 ||  ...  || T_l<0..r-1>
  return string.sub(table.concat(T), 1, dkLen)
end

return hmac_pbkdf2