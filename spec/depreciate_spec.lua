local function reql_error_formatter(err)
  if type(err) ~= 'table' then return end
  if err.ReQLError then
    return err.message()
  end
end

describe('connection with depreciated protocol v0.3', function()
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
    r.connect({proto_version = r.proto_V0_3}, function(err, c)
      assert.is_nil(err)
      assert.is_table(c)
    end)
  end)

  it('refused', function()
    local conn, err = r.connect{host = '172.0.0.254', proto_version = r.proto_V0_3}
    assert.is_nil(conn)
    assert.is_table(err)
    assert.is_table(err.ReQLDriverError)
  end)

  it('bad password', function()
    local err = r.connect({password = '0xDEADBEEF', proto_version = r.proto_V0_3}, function(err, c)
      assert.is_nil(c)
      return err
    end)
    assert.is_table(err)
    assert.is_table(err.ReQLDriverError)
  end)

  it('return conn', function()
    local conn, err = r.connect{proto_version = r.proto_V0_3}
    assert.is_nil(err)
    assert.is_table(conn)
    assert.is_true(conn.is_open())
    err = conn.close{noreply_wait = false}
    assert.is_nil(err)
    assert.is_false(conn.is_open())
  end)

  it('noreply wait', function()
    local conn, err = r.connect{proto_version = r.proto_V0_3}
    assert.is_nil(err)
    assert.is_table(conn)
    assert.is_true(conn.is_open())
    err = conn.close{noreply_wait = true}
    assert.is_nil(err)
    assert.is_false(conn.is_open())
  end)
end)

describe('connection with depreciated protocol v0.4', function()
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
    r.connect({proto_version = r.proto_V0_4}, function(err, c)
      assert.is_nil(err)
      assert.is_table(c)
    end)
  end)

  it('refused', function()
    local conn, err = r.connect{host = '172.0.0.254', proto_version = r.proto_V0_4}
    assert.is_nil(conn)
    assert.is_table(err)
    assert.is_table(err.ReQLDriverError)
  end)

  it('bad password', function()
    local err = r.connect({password = '0xDEADBEEF', proto_version = r.proto_V0_4}, function(err, c)
      assert.is_nil(c)
      return err
    end)
    assert.is_table(err)
    assert.is_table(err.ReQLDriverError)
  end)

  it('return conn', function()
    local conn, err = r.connect{proto_version = r.proto_V0_4}
    assert.is_nil(err)
    assert.is_table(conn)
    assert.is_true(conn.is_open())
    err = conn.close{noreply_wait = false}
    assert.is_nil(err)
    assert.is_false(conn.is_open())
  end)

  it('noreply wait', function()
    local conn, err = r.connect{proto_version = r.proto_V0_4}
    assert.is_nil(err)
    assert.is_table(conn)
    assert.is_true(conn.is_open())
    err = conn.close{noreply_wait = true}
    assert.is_nil(err)
    assert.is_false(conn.is_open())
  end)
end)
