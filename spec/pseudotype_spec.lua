local function reql_error_formatter(err)
  if type(err) ~= 'table' then return end
  if err.ReQLError then
    return err.message()
  end
end

describe('pseudotype', function()
  local r

  setup(function()
    assert:add_formatter(reql_error_formatter)
    r = require('rethinkdb')

    function r.run(query, ...)
      assert.is_table(query, ...)
      return assert.is_table(query.run(query.r.c))
    end

    r.c = assert.is_table(r.connect())
  end)

  teardown(function()
    r = nil
    assert:remove_formatter(reql_error_formatter)
  end)

  it('binary', function()
    local binary = 'Hello World'
    local query = assert.is_table(r.reql.binary(binary))
    local raw = assert.is_table(r.run(query).to_array())[1]
    assert.are.equal('BINARY', raw['$reql_type$'])
    assert.are.equal(binary, r.unb64(raw.data))
    local native = assert.is_table(query.run(r.c, {binary_format = 'native'}).to_array())[1]
    assert.are.equal(binary, native)
  end)

  it('group', function()
    local data = {{x = 1}, {x = 2}, {x = 2}, {x = 3}}
    local query = assert.is_table(r.reql(data).group'x')
    local raw = assert.is_table(r.run(query).to_array())[1]
    local native = assert.is_table(query.run(r.c, {group_format = 'native'}).to_array())[1]
    assert.are_not_same(native, raw)
    for i = 1, #raw.data do
      assert.are_same(native[i].group, raw.data[i][1])
      assert.are_same(native[i].reduction, raw.data[i][2])
    end
  end)

  it('time', function()
    r.run(r.reql.db_create'time').to_array()
    r.run(r.reql.db'time'.table_create'now').to_array()
    assert.is_table(r.run(r.reql.db'time'.table'now'.insert{id = 0, time = r.reql.now()}).to_array())
    local query = assert.is_table(r.reql.db'time'.table'now'.get(0)'time')
    local raw = assert.is_table(r.run(query).to_array())[1]
    local native = assert.is_table(query.run(r.c, {time_format = 'native'}).to_array())[1]
    assert.is_table(r.run(r.reql.db'time'.table'now'.delete()).to_array())
    assert.are_not_same(native, raw)
    assert.is_number(raw.epoch_time)
    assert.is_number(native.hour)
  end)
end)
