rockspec_format = '1.1'
package = 'reql'
version = '1.0.4-0'
source = {
  url = 'git://github.com/RebirthDB/rebirthdb-lua',
  branch = 'v1.0.4',
}
description = {
  summary = 'A Lua driver for RebirthDB.',
  detailed = [===[
# RebirthDB-Lua

RebirthDB driver in Lua

## Installing
- _IF USING LUA 5.1_ `luarocks install luabitop`
- `luarocks install lua-reql`

## Dev Dependencies
- Lua >= 5.1
- Luarocks
  - busted
  - luacheck
  - luacov
  - _IF USING LUA 5.1_ luabitop
- RebirthDB

## Testing
- `luacheck .`
- `busted -c`
- `luacov`

## Installing from source
- `luarocks make`

[![Build Status](https://travis-ci.org/RebirthDB/rebirthdb-lua.svg?branch=master)](https://travis-ci.org/RebirthDB/rebirthdb-lua)

[![Coverage Status](https://coveralls.io/repos/github/RebirthDB/rebirthdb-lua/badge.svg?branch=master)](https://coveralls.io/github/RebirthDB/rebirthdb-lua?branch=master)
  ]===],
  homepage = 'https://github.com/RebirthDB/rebirthdb-lua/wiki',
  license = 'Apache',
}
dependencies = {
  'lua >= 5.1, < 5.4',
  'luacrypto ~> 0.3',
  'luajson ~> 1.3',
  'luasec ~> 0.6',
  'luasocket ~> 3.0',
  'semver ~> 1.2',
}
build = {
  type = 'builtin',
  modules = {
    reql = 'src/rethinkdb.lua',
    ['reql.connection_instance'] = 'src/rethinkdb/connection_instance.lua',
    ['reql.connection'] = 'src/rethinkdb/connection.lua',
    ['reql.connector'] = 'src/rethinkdb/connector.lua',
    ['reql.cursor'] = 'src/rethinkdb/cursor.lua',
    ['reql.depreciate'] = 'src/rethinkdb/depreciate.lua',
    ['reql.errors'] = 'src/rethinkdb/errors.lua',
    ['reql.reql'] = 'src/rethinkdb/reql.lua',
    ['reql.rtype'] = 'src/rethinkdb/rtype.lua',
    ['reql.internal.bits51'] = 'src/rethinkdb/internal/bits51.lua',
    ['reql.internal.bits53'] = 'src/rethinkdb/internal/bits53.lua',
    ['reql.internal.bytes_to_int'] = 'src/rethinkdb/internal/bytes_to_int.lua',
    ['reql.internal.convert_pseudotype'] = 'src/rethinkdb/internal/convert_pseudotype.lua',
    ['reql.internal.current_handshake'] = 'src/rethinkdb/internal/current_handshake.lua',
    ['reql.internal.int_to_bytes'] = 'src/rethinkdb/internal/int_to_bytes.lua',
    ['reql.internal.pbkdf'] = 'src/rethinkdb/internal/pbkdf.lua',
    ['reql.internal.protect'] = 'src/rethinkdb/internal/protect.lua',
    ['reql.internal.protocol'] = 'src/rethinkdb/internal/protocol.lua',
    ['reql.internal.protodef'] = 'src/rethinkdb/internal/protodef.lua',
    ['reql.internal.socket'] = 'src/rethinkdb/internal/socket.lua',
    ['reql.internal.utilities'] = 'src/rethinkdb/internal/utilities.lua',
  }
}
