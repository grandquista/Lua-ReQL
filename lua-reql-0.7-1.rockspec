package = 'Lua-ReQL'
version = '0.7-1'
source = {
  url = 'git://github.com/grandquista/Lua-ReQL',
  tag = 'v0.7.1',
}
description = {
  summary = 'A Lua driver for RethinkDB.',
  homepage = 'https://github.com/grandquista/Lua-ReQL/wiki',
  license = 'Apache',
}
dependencies = {
  'lua >= 5.1',
  'luasocket ~> 3',
  'luajson ~> 1',
}
build = {
  type = 'builtin',
  modules = {
    rethinkdb = 'src/rethinkdb.lua'
  }
}
