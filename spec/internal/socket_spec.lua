local ltn12 = require('ltn12')

local function reql_error_formatter(err)
  if type(err) ~= 'table' then return end
  if err.ReQLError then
    return err.message()
  end
end

describe('socket', function()
  local r, socket

  setup(function()
    assert:add_formatter(reql_error_formatter)
    r = require('rethinkdb')
    socket = require('rethinkdb.internal.socket')
  end)

  teardown(function()
    socket = nil
    r = nil
    assert:remove_formatter(reql_error_formatter)
  end)

  it('closes repeatedly', function()
    local client = assert.is_table(socket(r, 'localhost', 28015, nil, 1))
    client.close()
    client.close()
  end)

  it('connects', function()
    local client = assert.is_table(socket(r, 'localhost', 28015, nil, 1))
    finally(client.close)

    assert.is_truthy(ltn12.pump.all(ltn12.source.string'\0\0\0\0\0\0\0\0\0\0\0\0', client.sink))

    local sink, buffer = ltn12.sink.table()

    local expected = table.concat{
      'ERROR: Received an unsupported protocol version. This port is for ',
      'RethinkDB queries. Does your client driver version not match the ',
      'server?\n\0',
    }

    assert.is_truthy(ltn12.pump.all(client.source(string.len(expected)), sink))

    local message = table.concat(buffer)

    assert.are_equal(expected, message)
  end)
end)
