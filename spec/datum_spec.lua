local function reql_error_formatter(err)
  if type(err) ~= 'table' then return end
  if err.ReQLError then
    return err.message()
  end
end

describe('datum', function()
  local r

  setup(function()
    assert:add_formatter(reql_error_formatter)
    r = require('rethinkdb')

    function r.run(query, ...)
      assert.is_table(query, ...)
      assert.are.equal('reql', r.type(query))
      return assert.is_table(query.run(query.r.c))
    end

    r.c = assert.is_table(r.connect())
  end)

  teardown(function()
    r = nil
    assert:remove_formatter(reql_error_formatter)
  end)

  it('false', function()
    local var = false
    local cur = r.run(r.reql(var))
    assert.same({var}, cur.to_array())
  end)

  it('true', function()
    local var = true
    local cur = r.run(r.reql(var))
    assert.same({var}, cur.to_array())
  end)

  it('nil', function()
    local var = nil
    local cur = r.run(r.reql(var))
    assert.same({r.decode'null'}, cur.to_array())
  end)

  it('string', function()
    local var = 'not yap wa\' Hol'
    local cur = r.run(r.reql(var))
    assert.same({var}, cur.to_array())
  end)

  it('0', function()
    local var = 0
    local cur = r.run(r.reql(var))
    assert.same({var}, cur.to_array())
  end)

  it('1', function()
    local var = 1
    local cur = r.run(r.reql(var))
    assert.same({var}, cur.to_array())
  end)

  it('-1', function()
    local var = -1
    local cur = r.run(r.reql(var))
    assert.same({var}, cur.to_array())
  end)

  it('τ', function()
    local var = 6.28
    local cur = r.run(r.reql(var))
    assert.same({var}, cur.to_array())
  end)

  it('𝑒', function()
    local var = 2.2
    local cur = r.run(r.reql(var))
    assert.same({var}, cur.to_array())
  end)

  it('α', function()
    local var = 0.00001
    local cur = r.run(r.reql(var))
    assert.same({var}, cur.to_array())
  end)

  it('array', function()
    local var = {[1] = 1, [2] = 2}
    local cur = r.run(r.reql(var))
    assert.same({var}, cur.to_array())
  end)

  it('table', function()
    local var = {first = 1, second = 2}
    local cur = r.run(r.reql(var))
    assert.same({var}, cur.to_array())
  end)
end)
