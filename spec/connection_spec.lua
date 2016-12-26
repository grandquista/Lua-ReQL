local function reql_error_formatter(err)
  if type(err) ~= 'table' then return end
  if err.ReQLError then
    return err.message()
  end
end

describe('connection', function()
  local r

  setup(function()
    assert:add_formatter(reql_error_formatter)
    r = require('rethinkdb')
  end)

  teardown(function()
    r = nil
    assert:remove_formatter(reql_error_formatter)
  end)

  it('basic', function()
    r.connect(function(err, c)
      assert.is_nil(err)
      assert.is_table(c)
      assert.are.equal('connection', r.type(c))
    end)
  end)

  it('depreciated v0.3', function()
    r.connect({proto_version = r.proto_V0_3}, function(err, c)
      assert.is_nil(err)
      assert.is_table(c)
      assert.is_true(c.is_open())
    end)
  end)

  it('depreciated v0.4', function()
    r.connect({proto_version = r.proto_V0_4}, function(err, c)
      assert.is_nil(err)
      assert.is_table(c)
      assert.is_true(c.is_open())
    end)
  end)

  it('refused', function()
    local conn, err = r.connect'172.0.0.254'
    assert.is_nil(conn)
    assert.is_table(err)
    assert.is_table(err.ReQLDriverError)
    assert.is_nil(err.ReQLAuthError)
  end)

  it('bad password', function()
    local err = r.connect({password = '0xDEADBEEF'}, function(err, c)
      assert.is_nil(c)
      return err
    end)
    assert.is_table(err)
    assert.is_table(err.ReQLDriverError)
    assert.is_table(err.ReQLAuthError)
  end)

  it('good password', function()
    local key = 'Is_this_an_improved_authentication_key?'

    local conn, err = r.connect()
    assert.is_nil(err)
    assert.is_table(conn)
    assert.is_true(conn.is_open())

    local users = r.reql.db'rethinkdb'.table'users'

    assert.is_table(
      assert.is_table(
        users.insert{
          {id = 'devops', password = key},
          {id = 'dev', password = {password = key, iterations = 64}}
        }.run(conn)).to_array())

    assert.is_true(assert.is_table(r.connect{user = 'devops', password = key}).is_open())
    assert.is_true(assert.is_table(r.connect{user = 'dev', password = key}).is_open())

    assert.is_true(conn.is_open())
    assert.is_table(users.get('dev', 'devops').delete().run(conn)).to_array()
  end)

  it('return conn', function()
    local conn, err = r.connect()
    assert.is_nil(err)
    assert.is_table(conn)
    assert.is_true(conn.is_open())
    err = conn.close{noreply_wait = false}
    assert.is_nil(err)
    assert.is_false(conn.is_open())
  end)

  it('noreply wait', function()
    local conn, err = r.connect()
    assert.is_nil(err)
    assert.is_table(conn)
    assert.is_true(conn.is_open())
    err = conn.close{noreply_wait = true}
    assert.is_nil(err)
    assert.is_false(conn.is_open())
  end)

  it('fails to insert eventually #expensive', function()
    local reql_db = 'connection'
    local reql_table = 'tests'

    local c = assert.is_table(r.connect())

    assert.is_table(r.reql.db_create(reql_db).run(c)).to_array()
    c.use(reql_db)
    assert.is_table(r.reql.table_create(reql_table).run(c)).to_array()

    for id=1, 500000 do
      assert.is_true(r.reql.table(reql_table).insert{id=id}.run(c, {noreply = true}))
    end
    assert.is_true(c.noreply_wait())
    assert.is_true(
      assert.is_table(
        assert.is_table(
          r.reql.table(reql_table).get(500000)'id'.eq(500000).run(c)
        ).to_array()
      )[1]
    )

    c.reconnect(function(err, conn)
      assert.is_table(conn, err)
      r.reql.table(reql_table).delete().run(conn).to_array()
    end)
  end)
end)
