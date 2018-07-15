# RebirthDB-Lua

ReQL driver for Lua
[![Build Status](https://travis-ci.org/RebirthDB/rebirthdb-lua.svg?branch=master)](https://travis-ci.org/RebirthDB/rebirthdb-lua)
[![Coverage Status](https://coveralls.io/repos/github/RebirthDB/rebirthdb-lua/badge.svg?branch=master)](https://coveralls.io/github/RebirthDB/rebirthdb-lua?branch=master)

## Installing
- _IF USING LUA 5.1_ `luarocks install luabitop`
- `luarocks install reql`

See [Wiki](https://github.com/RebirthDB/rebirthdb-lua/wiki) for documentation.

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
