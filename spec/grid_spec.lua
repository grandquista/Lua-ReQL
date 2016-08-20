local function reql_error_formatter(err)
  if type(err) ~= 'table' then return end
  if err.ReQLError then
    return err.message()
  end
end

describe('regrid', function()
  local r

  setup(function()
    assert:add_formatter(reql_error_formatter)
    r = require('rethinkdb').new{grid = true}
    assert.is_function(r.re_grid)
  end)

  teardown(function()
    r = nil
    assert:remove_formatter(reql_error_formatter)
  end)

  it('test', function()
    local bucket = assert.is_table(r.re_grid())
    assert.is_true(bucket.init_bucket())
  end)
end)
