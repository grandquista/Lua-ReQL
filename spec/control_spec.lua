local function reql_error_formatter(err)
  if type(err) ~= 'table' then return end
  if err.ReQLError then
    return err.message()
  end
end

local version = require('rethinkdb.internal.semver')(string.match(_VERSION, '%d+%.%d+'))

describe('control', function()
  local r

  setup(function()
    assert:add_formatter(reql_error_formatter)
    r = require('rethinkdb')

    function r.run(query, ...)
      assert.is_table(query, ...)
      return assert.is_table(query.run(query.r.c))
    end

    local reql_db = 'control'
    local reql_table = 'func'
    r.reql_db = r.reql.db(reql_db)
    r.reql_table = r.reql.table(reql_table)

    r.c = assert.is_table(r.connect())

    r.run(r.reql.db_create(reql_db)).to_array()
    r.c.use(reql_db)
    r.run(r.reql.table_create(reql_table)).to_array()
  end)

  teardown(function()
    if r.c then
      r.run(r.reql_table.delete()).to_array()
    end
    r = nil
    assert:remove_formatter(reql_error_formatter)
  end)

  it('branch db', function()
    local cur = r.run(r.reql_db.branch(1, 2))
    assert.is_nil(cur.to_array())
  end)

  it('branch error', function()
    local cur = r.run(r.reql.branch(r.reql.error'a', 1, 2))
    assert.is_nil(cur.to_array())
  end)

  it('branch false', function()
    local cur = r.run(r.reql.branch(false, 1, 2))
    assert.same({2}, assert.is_table(cur.to_array()))
  end)

  it('branch nil', function()
    local cur = r.run(assert.is_table(r.reql().branch(1, 2)))
    assert.same({2}, assert.is_table(cur.to_array()))
  end)

  it('branch num', function()
    local cur = r.run(r.reql.branch(1, 'c', false))
    assert.same({'c'}, assert.is_table(cur.to_array()))
  end)

  it('branch table', function()
    local cur = r.run(r.reql_table.branch(1, 2))
    assert.is_nil(cur.to_array())
  end)

  it('branch true', function()
    local cur = r.run(r.reql.branch(true, 1, 2))
    assert.same({1}, assert.is_table(cur.to_array()))
  end)

  it('do append', function()
    local cur = r.run(r.reql{0, 1, 2}.call(function(v) return v.append(3) end))
    assert.same({{0, 1, 2, 3}}, assert.is_table(cur.to_array()))
  end)

  if version.major >= 5 and version.minor > 1 then
    it('do', function()
      local cur = r.run(r.reql.call(function() return 1 end))
      assert.same({1}, assert.is_table(cur.to_array()))
    end)

    it('do add', function()
      local cur = r.run(r.reql.call(1, 2, function(x, y) return x.add(y) end))
      assert.same({3}, assert.is_table(cur.to_array()))
    end)

    it('do extra arg', function()
      local cur = r.run(r.reql.call(1, function(x, y) return x + y end))
      assert.is_nil(cur.to_array())
    end)

    it('do missing arg', function()
      local cur = r.run(r.reql.call(1, 2, function(x) return x end))
      assert.is_nil(cur.to_array())
    end)
  end

  it('do mul', function()
    local cur = r.run(r.reql(1).call(function(v) return v.mul(2) end))
    assert.same({2}, assert.is_table(cur.to_array()))
  end)

  it('do no args', function()
    local cur = r.run(r.reql.call())
    assert.is_nil(cur.to_array())
  end)

  it('do no func', function()
    local cur = r.run(r.reql.call(1))
    assert.same({1}, assert.is_table(cur.to_array()))
  end)

  it('do no return', function()
    local cur = r.run(r.reql.call(1, function() end))
    assert.is_not_nil(cur)
  end)

  it('do return nil', function()
    local cur = r.run(r.reql.call(1, function() return nil end))
    assert.is_not_nil(cur)
  end)

  it('do str add num', function()
    local cur = r.run(r.reql'abc'.call(function(v) return v.add(3) end))
    assert.is_nil(cur.to_array())
  end)

  it('do str add str add num', function()
    local cur = r.run(r.reql'abc'.call(function(v) return v.add'def' end).add(3))
    assert.is_nil(cur.to_array())
  end)

  it('do str append', function()
    local cur = r.run(r.reql'abc'.call(function(v) return v.append(3) end))
    assert.is_nil(cur.to_array())
  end)

  it('error', function()
    local cur = r.run(r.reql.error'Hello World')
    assert.is_nil(cur.to_array())
  end)

  it('js', function()
    local cur = r.run(r.reql.js'1 + 1')
    assert.same({2}, assert.is_table(cur.to_array()))
  end)

  it('js add add', function()
    local cur = r.run(r.reql.js'1 + 1; 2 + 2')
    assert.same({4}, assert.is_table(cur.to_array()))
  end)

  it('do js function add', function()
    local cur = r.run(r.reql.call(1, 2, r.reql.js'(function(a, b) { return a + b; })'))
    assert.same({3}, assert.is_table(cur.to_array()))
  end)

  it('do js function', function()
    local cur = r.run(r.reql.call(1, r.reql.js'(function(x) { return x + 1; })'))
    assert.same({2}, assert.is_table(cur.to_array()))
  end)
  it('do js function add str', function()
    local cur = r.run(r.reql.call('foo', r.reql.js'(function(x) { return x + "bar"; })'))
    assert.same({'foobar'}, assert.is_table(cur.to_array()))
  end)

  it('do js no timeout', function()
    local cur = r.run(r.reql.js('1 + 2', {timeout = 1.2}))
    assert.same({3}, assert.is_table(cur.to_array()))
  end)

  it('js function result', function()
    local cur = r.run(r.reql.js'(function() { return 1; })')
    assert.is_nil(cur.to_array())
  end)

  it('js function no wrap', function()
    local cur = r.run(r.reql.js'function() { return 1; }')
    assert.is_nil(cur.to_array())
  end)

  it('do js function missing arg', function()
    local cur = r.run(r.reql.call(1, 2, r.reql.js'(function(a) { return a; })'))
    assert.same({1}, assert.is_table(cur.to_array()))
  end)

  it('do js function extra arg', function()
    local cur = r.run(r.reql.call(1, 2, r.reql.js'(function(a, b, c) { return a; })'))
    assert.same({1}, assert.is_table(cur.to_array()))
  end)

  it('do js function return undefined', function()
    local cur = r.run(r.reql.call(1, 2, r.reql.js'(function(a, b, c) { return c; })'))
    assert.is_nil(cur.to_array())
  end)

  it('filter js', function()
    local cur = r.run(r.reql.filter({1, 2, 3}, r.reql.js'(function(a) { return a >= 2; })'))
    assert.same({{2, 3}}, assert.is_table(cur.to_array()))
  end)

  it('map js', function()
    local cur = r.run(r.reql.map({1, 2, 3}, r.reql.js'(function(a) { return a + 1; })'))
    assert.same({{2, 3, 4}}, assert.is_table(cur.to_array()))
  end)

  it('map js constant', function()
    local cur = r.run(r.reql.map({1, 2, 3}, r.reql.js'1'))
    assert.is_nil(cur.to_array())
  end)

  it('filter js undefined', function()
    local cur = r.run(r.reql.filter({1, 2, 3}, r.reql.js'(function(a) {})'))
    assert.is_nil(cur.to_array())
  end)

  it('map constant', function()
    local cur = r.run(r.reql.map({1, 2, 3}, 1))
    assert.is_nil(cur.to_array())
  end)

  it('filter constant str', function()
    local cur = r.run(r.reql.filter({1, 2, 3}, 'foo'))
    assert.same({{1, 2, 3}}, assert.is_table(cur.to_array()))
  end)

  it('filter constant obj', function()
    local cur = r.run(r.reql.filter({1, 2, 3}, {}))
    assert.same({{1, 2, 3}}, assert.is_table(cur.to_array()))
  end)

  it('filter nil', function()
    local cur = r.run(r.reql.filter({1, 2, 3}, r.reql()))
    assert.same({{}}, assert.is_table(cur.to_array()))
  end)

  it('filter false', function()
    local cur = r.run(r.reql.filter({1, 2, 3}, false))
    assert.same({{}}, assert.is_table(cur.to_array()))
  end)

  it('for each insert', function()
    local cur = r.run(r.reql.for_each({1, 2, 3}, function(row) return r.reql_table.insert({id = row}) end))
    assert.same({{deleted = 0, replaced = 0, unchanged = 0, errors = 0, skipped = 0, inserted = 3}}, assert.is_table(cur.to_array()))
  end)

  it('count for each insert', function()
    local cur = r.run(r.reql.for_each({1, 2, 3}, function(row) return r.reql_table.insert({id = row}) end))
    assert.is_table(cur.to_array())
    cur = r.run(r.reql_table.count())
    assert.same({3}, assert.is_table(cur.to_array()))
  end)

  it('for each update', function()
    local cur = r.run(r.reql.for_each({1, 2, 3}, function(row) return r.reql_table.insert({id = row}) end))
    assert.is_table(cur.to_array())
    cur = r.run(r.reql.for_each({1, 2, 3}, function(row) return r.reql_table.update({foo = row}) end))
    assert.same({{deleted = 0, replaced = 9, unchanged = 0, errors = 0, skipped = 0, inserted = 0}}, assert.is_table(cur.to_array()))
  end)

  it('for each insert with duplicates', function()
    local cur = r.run(r.reql.for_each({1, 2, 3}, function(row) return r.reql_table.insert({id = row}) end))
    assert.is_table(cur.to_array())
    cur = r.run(r.reql.for_each({1, 2, 3}, function(row) return r.reql_table.update({foo = row}) end))
    assert.is_table(cur.to_array())
    cur = r.run(r.reql.for_each({1, 2, 3}, function(row) return {r.reql_table.insert({id = row}), r.reql_table.insert({id = row * 10})} end))
    local res = assert.is_table(cur.to_array())
    assert.are.equal(3, res[1].errors)
    assert.are.equal(3, res[1].inserted)
  end)

  it('for each update many', function()
    local cur = r.run(r.reql.for_each({1, 2, 3}, function(row) return r.reql_table.insert({id = row}) end))
    assert.is_table(cur.to_array())
    cur = r.run(r.reql.for_each({1, 2, 3}, function(row) return r.reql_table.update({foo = row}) end))
    assert.is_table(cur.to_array())
    cur = r.run(r.reql.for_each({1, 2, 3}, function(row) return {r.reql_table.insert({id = row}), r.reql_table.insert({id = row * 10})} end))
    assert.is_table(cur.to_array())
    cur = r.run(r.reql.for_each({1, 2, 3}, function(row) return r.reql_table.update({foo = row}) end))
    assert.same({{deleted = 0, replaced = 18, unchanged = 0, errors = 0, skipped = 0, inserted = 0}}, assert.is_table(cur.to_array()))
  end)
end)
