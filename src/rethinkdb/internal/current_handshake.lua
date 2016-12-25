--- Handler implementing latest RethinkDB handshake.
-- @module rethinkdb.internal.current_handshake
-- @author Adam Grandquist
-- @license Apache
-- @copyright Adam Grandquist 2016

local crypto = require('crypto')
local errors = require'rethinkdb.errors'
local ltn12 = require('ltn12')
local pbkdf2 = require'rethinkdb.internal.pbkdf'
local protect = require'rethinkdb.internal.protect'

--- Helper for bitwise operations.
local function prequire(mod_name, ...)
  if not mod_name then return end

  local success, bits = pcall(require, mod_name)

  if success then
    return bits
  end

  return prequire(...)
end

local bits = prequire(
  'rethinkdb.internal.bits53', 'bit32', 'bit', 'rethinkdb.internal.bits51')

local bor = bits.bor
local bxor = bits.bxor
local rand_bytes = crypto.rand.bytes

local unpack = _G.unpack or table.unpack

local function bxor256(u, t)
  local res = {}
  for i=1, math.max(string.len(u), string.len(t)) do
    res[i] = bxor(string.byte(u, i) or 0, string.byte(t, i) or 0)
  end
  return string.char(unpack(res))
end

local function compare_digest(a, b)
  local result

  if string.len(a) == string.len(b) then
    result = 0
  end
  if string.len(a) ~= string.len(b) then
    result = 1
  end

  for i=1, math.max(string.len(a), string.len(b)) do
    result = bor(result, bxor(string.byte(a, i) or 0, string.byte(b, i) or 0))
  end

  return result ~= 0
end

local function maybe_auth_err(r, err, append)
  if 10 <= err.error_code and err.error_code <= 20 then
    return nil, errors.ReQLAuthError(r, err.error .. append)
  end
  return nil, errors.ReQLDriverError(r, err.error .. append)
end

local function current_handshake(r, socket_inst, auth_key, user)
  local function send(data)
    local success, err = ltn12.pump.all(ltn12.source.string(data), socket_inst.sink)
    if not success then
      socket_inst.close()
      return nil, err
    end
    return true
  end

  local buffer = ''

  local function sink(chunk, src_err)
    if src_err then
      return nil, src_err
    end
    if chunk == nil then
      return nil, 'closed'
    end
    buffer = buffer .. chunk
    return true
  end

  local function encode(object)
    local json, err = protect(r.encode, object)
    if not json then
      return nil, err
    end
    return send(table.concat{json, '\0'})
  end

  local function get_message()
    local i = string.find(buffer, '\0')
    while not i do
      local success, err = ltn12.pump.step(socket_inst.source(1), sink)
      if not success then
        return nil, err
      end
      i = string.find(buffer, '\0')
    end

    local message = string.sub(buffer, 1, i - 1)
    buffer = string.sub(buffer, i + 1)
    return message
  end

  local success, err = send'\195\189\194\52'
  if not success then
    return nil, errors.ReQLDriverError(r, err .. ': sending magic number')
  end

  -- Now we have to wait for a response from the server
  -- acknowledging the connection
  -- this will be a null terminated json document on success
  -- or a null terminated error string on failure
  local message
  message, err = get_message()
  if not message then
    return nil, errors.ReQLDriverError(r, err .. ': in first response')
  end

  local response = protect(r.decode, message)

  if not response then
    return nil, errors.ReQLDriverError(r, message .. ': in first response')
  end

  if not response.success then
    return maybe_auth_err(r, response, ': in first response')
  end

  local nonce = r.b64(rand_bytes(18))

  local client_first_message_bare = 'n=' .. user .. ',r=' .. nonce

  -- send the second client message
  -- {
  --   "protocol_version": <number>,
  --   "authentication_method": <method>,
  --   "authentication": "n,,n=<user>,r=<nonce>"
  -- }
  success, err = encode{
    protocol_version = response.min_protocol_version,
    authentication_method = 'SCRAM-SHA-256',
    authentication = 'n,,' .. client_first_message_bare
  }
  if not success then
    return nil, errors.ReQLDriverError(r, err .. ': encoding SCRAM challenge')
  end


  -- wait for the second server challenge
  -- this is always a json document
  -- {
  --   "success": <bool>,
  --   "authentication": "r=<nonce><server_nonce>,s=<salt>,i=<iteration>"
  -- }

  message, err = get_message()
  if not message then
    return nil, errors.ReQLDriverError(r, err .. ': in second response')
  end

  response, err = protect(r.decode, message)

  if not response then
    return nil, errors.ReQLDriverError(r, err .. ': decoding second response')
  end

  if not response.success then
    return maybe_auth_err(r, response, ': in second response')
  end

  -- the authentication property will need to be retained
  local authentication = {}
  local server_first_message = response.authentication
  local response_authentication = server_first_message .. ','
  for k, v in string.gmatch(response_authentication, '([rsi])=(.-),') do
    authentication[k] = v
  end

  if string.sub(authentication.r, 1, string.len(nonce)) ~= nonce then
    return nil, errors.ReQLDriverError(r, 'Invalid nonce')
  end

  authentication.i = tonumber(authentication.i)

  local client_final_message_without_proof = 'c=biws,r=' .. authentication.r

  local salt = r.unb64(authentication.s)

  -- SaltedPassword := Hi(Normalize(password), salt, i)
  local salted_password, str_err = pbkdf2('sha256', auth_key, salt, authentication.i, 32)

  if not salted_password then
    return nil, errors.ReQLDriverError(r, str_err)
  end

  -- ClientKey := HMAC(SaltedPassword, "Client Key")
  local client_key = crypto.hmac.digest('sha256', 'Client Key', salted_password, true)

  -- StoredKey := H(ClientKey)
  local stored_key = crypto.digest('sha256', client_key, true)

  -- AuthMessage := client-first-message-bare + "," +
  --                server-first-message + "," +
  --                client-final-message-without-proof
  local auth_message = table.concat({
      client_first_message_bare,
      server_first_message,
      client_final_message_without_proof}, ',')

  -- ClientSignature := HMAC(StoredKey, AuthMessage)
  local client_signature = crypto.hmac.digest('sha256', auth_message, stored_key, true)

  local client_proof = bxor256(client_key, client_signature)

  -- ServerKey := HMAC(SaltedPassword, "Server Key")
  local server_key = crypto.hmac.digest('sha256', 'Server Key', salted_password, true)

  -- ServerSignature := HMAC(ServerKey, AuthMessage)
  local server_signature = crypto.hmac.digest('sha256', auth_message, server_key, true)

  -- send the third client message
  -- {
  --   "authentication": "c=biws,r=<nonce><server_nonce>,p=<proof>"
  -- }
  success, err = encode{
    authentication =
    table.concat{client_final_message_without_proof, ',p=', r.b64(client_proof)}
  }
  if not success then
    return nil, errors.ReQLDriverError(r, err .. ': encoding SCRAM response')
  end

  -- wait for the third server challenge
  -- this is always a json document
  -- {
  --   "success": <bool>,
  --   "authentication": "v=<server_signature>"
  -- }
  message, err = get_message()
  if not message then
    return nil, errors.ReQLDriverError(r, err .. ': in third response')
  end

  response, err = protect(r.decode, message)

  if not response then
    return nil, errors.ReQLDriverError(r, err .. ': decoding third response')
  end

  if not response.success then
    return maybe_auth_err(r, response, ': in third response')
  end

  response_authentication = response.authentication .. ','
  for k, v in string.gmatch(response_authentication, '([v])=(.-),') do
    authentication[k] = v
  end

  if not authentication.v then
    return nil, errors.ReQLDriverError(
      r,
      message .. ': missing server signature'
    )
  end

  if compare_digest(authentication.v, server_signature) then
    return true
  end

  return nil, errors.ReQLAuthError(r, 'invalid server signature')
end

return current_handshake
