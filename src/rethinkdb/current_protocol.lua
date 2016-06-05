--- Handler implementing latest RethinkDB handshake.
-- @module rethinkdb.current_protocol
-- @author Adam Grandquist
-- @license Apache
-- @copyright Adam Grandquist 2016

local utilities = require'rethinkdb.utilities'

local bits = require'rethinkdb.bits'
local bytes_to_int = require'rethinkdb.bytes_to_int'
local crypto = require('crypto')
local errors = require'rethinkdb.errors'
local int_to_bytes = require'rethinkdb.int_to_bytes'

local unb64 = utilities.unb64
local b64 = utilities.b64
local encode = utilities.encode
local decode = utilities.decode

local bor = bits.bor
local bxor = bits.bxor
local tobit = bits.tobit
local rand_bytes = crypto.rand.bytes
local hmac = crypto.hmac

local function __compare_digest(a, b)
  local left, result
  local right = b

  if #a == #b then
    left = a
    result = 0
  end
  if #a ~= #b then
    left = b
    result = 1
  end

  for i=1, #left do
    result = bor(result, bxor(left[i], right[i]))
  end

  return tobit(result) ~= tobit(0)
end

local pbkdf2_cache = {}

local function __pbkdf2_hmac(hash_name, password, salt, iterations)
  local cache_string = password .. ',' .. salt .. ',' .. iterations

  if pbkdf2_cache[cache_string] then
    return pbkdf2_cache[cache_string]
  end

  local msg_buffer = ''

  local function digest(msg)
    msg_buffer = msg_buffer .. msg
    local mac = hmac.new(hash_name, password)
    mac:update(msg_buffer)
    return mac:final(nil, true)
  end

  local t = digest(salt .. '\0\0\0\1')
  local lo_u, hi_u =
    bytes_to_int(string.sub(t, 1, 16)),
    bytes_to_int(string.sub(t, 17))
  for _=1, iterations do
    t = digest(t)
    lo_u, hi_u =
      bits.bxor(lo_u, bytes_to_int(string.sub(t, 1, 16))),
      bits.bxor(hi_u, bytes_to_int(string.sub(t, 17)))
  end

  local u = int_to_bytes(lo_u, 8) .. int_to_bytes(hi_u, 8)
  pbkdf2_cache[cache_string] = u
  return u
end

local function current_protocol(r, raw_socket, auth_key, user)
  local nonce = b64(r, rand_bytes(18))

  local client_first_message_bare = 'n=' .. user .. ',r=' .. nonce

  local size, send_err = raw_socket.send(
    '\195\189\194\52{"protocol_version":0,',
    '"authentication_method":"SCRAM-SHA-256",',
    '"authentication":"n,,', client_first_message_bare, '"}\0'
  )
  if not size then
    return nil, send_err
  end
  if send_err ~= '' then
    size, send_err = raw_socket.send(send_err)
    if not size then
      return nil, send_err
    end
    if send_err ~= '' then
      return nil, errors.ReQLDriverError'Incomplete protocol sent'
    end
  end

  -- Now we have to wait for a response from the server
  -- acknowledging the connection
  -- this will be a null terminated json document on success
  -- or a null terminated error string on failure
  local message, buffer = raw_socket.get_message('')

  if not message then
    return nil, buffer
  end

  local response = decode(r, message)

  if not response then
    return nil, errors.ReQLDriverError(message)
  end

  if response.success ~= true then
    return nil, errors.ReQLDriverError(message)
  end

  -- when protocol versions are updated this is where we send the following
  -- for now it is sent above
  -- {
  --   "protocol_version": <number>,
  --   "authentication_method": <method>,
  --   "authentication": "n,,n=<user>,r=<nonce>"
  -- }

  -- wait for the second server challenge
  -- this is always a json document
  -- {
  --   "success": <bool>,
  --   "authentication": "r=<nonce><server_nonce>,s=<salt>,i=<iteration>"
  -- }
  -- the authentication property will need to be retained
  local authentication = {}
  local server_first_message

  response, buffer = raw_socket.decode_message(buffer)

  if not response then
    return nil, buffer
  end

  if not response.success then
    if 10 <= response.error_code and response.error_code <= 20 then
      return nil, errors.ReQLAuthError(response.error)
    end
    return nil, errors.ReQLDriverError(response.error)
  end
  server_first_message = response.authentication
  local response_authentication = server_first_message .. ','
  for k, v in string.gmatch(response_authentication, '([rsi])=(.-),') do
    authentication[k] = v
  end

  if string.sub(authentication.r, 1, #nonce) ~= nonce then
    return nil, errors.ReQLDriverError'Invalid nonce'
  end

  authentication.i = tonumber(authentication.i)

  local client_final_message_without_proof = 'c=biws,r=' .. authentication.r

  local salt = unb64(r, authentication.s)

  -- SaltedPassword := Hi(Normalize(password), salt, i)
  local salted_password = __pbkdf2_hmac('sha256', auth_key, salt, authentication.i)

  -- ClientKey := HMAC(SaltedPassword, "Client Key")
  local client_key = hmac.digest('sha256', salted_password, 'Client Key', true)

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
  local client_signature = hmac.digest('sha256', stored_key, auth_message, true)

  local client_proof = int_to_bytes(bxor(bytes_to_int(client_key), bytes_to_int(client_signature)), 4)

  -- ServerKey := HMAC(SaltedPassword, "Server Key")
  local server_key = hmac.digest('sha256', salted_password, 'Server Key', true)

  -- ServerSignature := HMAC(ServerKey, AuthMessage)
  local server_signature = hmac.digest('sha256', server_key, auth_message, true)

  -- send the third client message
  -- {
  --   "authentication": "c=biws,r=<nonce><server_nonce>,p=<proof>"
  -- }
  size, send_err = raw_socket.send(encode(r, {
    authentication =
    table.concat({client_final_message_without_proof, b64(r, client_proof)}, ',p=')
  }), '\0')
  if not size then
    return nil, send_err
  end
  if send_err ~= '' then
    size, send_err = raw_socket.send(send_err)
    if not size then
      return nil, send_err
    end
    if send_err ~= '' then
      return nil, errors.ReQLDriverError'Incomplete protocol sent'
    end
  end

  -- wait for the third server challenge
  -- this is always a json document
  -- {
  --   "success": <bool>,
  --   "authentication": "v=<server_signature>"
  -- }
  response, buffer = raw_socket.decode_message(buffer)

  if not response then
    return nil, buffer
  end

  if not response.success then
    if 10 <= response.error_code and response.error_code <= 20 then
      return nil, errors.ReQLAuthError(response.error)
    end
    return nil, errors.ReQLDriverError(response.error)
  end

  if not __compare_digest(response.v, server_signature) then
    return nil, errors.ReQLDriverError(response)
  end

  return buffer
end

return current_protocol
