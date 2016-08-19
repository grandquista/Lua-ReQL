--- Interface
-- When a file is written to ReGrid, a files record is written to a files table.
-- Then the file is broken up into chunks which are written as separate records
-- in a chunks table. Once all the chunks are written, the files record is
-- updated to show that the file is Complete. The file is now ready for read
-- operations.
-- Files record
--
-- {
--   "id" : "<String>",
--   "length" : "<Number>",
--   "chunkSizeBytes" : "<Number>",
--   "finishedAt" : "<Time>",
--   "startedAt" : "<Time>",
--   "deletedAt" : "<Time>",
--   "sha256" : "<String>",
--   "filename" : "<String>",
--   "status" : "<String>",
--   "metadata" : "<Object>"
-- }
-- Key	Description
-- id	a unique ID for this document.
-- length	the length of this stored file, in bytes.
-- chunkSizeBytes	the size, in bytes, of each data chunk of this file. This value is configurable by file. The default is 255KB (1024 * 255).
-- finishedAt	the date and time this file finished writing to ReGrid. The value of this field MUST be the datetime when the upload completed, not the datetime when it was begun.
-- startedAt	the date and time this file started writing to ReGrid. The value of this field MUST be the datetime when the upload started, not the datetime when it was finished.
-- deletedAt	the date and time this files status was set to Deleted. The value of this field MUST be the datetime when file was marked Deleted.
-- sha256	SHA256 checksum for this user file, computed from the file’s data, stored as a hex string (lowercase).
-- filename	the name of this stored file; this does not need to be unique.
-- status	Status may be "Complete" or "Incomplete" or "Deleted".
-- metadata	any additional application data the user wishes to store.
--
-- Chunks record
--
-- {
--   "id": "<String>",
--   "file_id": "<String>",
--   "num": "<Number>",
--   "data": "<Binary>"
-- }
-- Key	Description
-- id	a unique ID for this document.
-- file_id	the id for this file (the id from the files table document).
-- num	the index number of this chunk, zero-based
-- data	a chunk of data from the user file
-- @module rethinkdb.grid
-- @author Adam Grandquist
-- @license Apache
-- @copyright Adam Grandquist 2016

local ltn12 = require('ltn12')
local protect = require'rethinkdb.internal.protect'

local m = {}

local DEFAULT_BUCKET_NAME = 'fs'
local DEFAULT_CHUNK_SIZE_BYTES = 1024 * 255
local DEFAULT_CONCURRENCY = 10

local function filter_chunk_low(chunk, data, chunk_size_bytes)
  return protect(function()
    if not chunk then
      if string.len(data) < chunk_size_bytes then
        return data
      end
    else
      data = data .. chunk
    end
    if string.len(data) >= chunk_size_bytes then
      chunk = string.sub(data, 1, chunk_size_bytes)
      data = string.sub(data, chunk_size_bytes)
    else
      chunk = ''
    end
    return chunk, data
  end)
end

local function filter_chunk(chunk_size_bytes)
  return ltn12.filter.cycle(filter_chunk_low, '', chunk_size_bytes)
end

local function sink_chunk(file_id, chunk_size_bytes, filest, chunkst)
  local state = {
    file_id = file_id,
    num = 0,
  }

  local function sink(chunk, err)
    return protect(function()
      if not chunk then
        if err then
          return nil, err
        end
        return filest.get(file_id).update().run()
      end
      if chunk == '' then return true end

      state.data = chunk

      chunkst.insert(state).run()

      state.num = state.num + 1

      return true
    end)
  end

  return ltn12.sink.chain(filter_chunk(chunk_size_bytes), sink)
end

function m.init(r)
  --- new ReGrid(connectionOptions, bucketOptions)
  --
  -- ReGrid drivers MUST provide a constructor to return a new Bucket instance,
  -- which exposes all the public API methods.
  --
  -- connectionOptions MAY be an existing connection, if that is more suitable
  -- to your chosen language.
  --
  -- Code Example
  --
  -- var connectionOptions = {
  --   // required connection options. Adapt to your chosen language.
  -- }
  --
  -- var bucketOptions = {
  --   bucketName: 'fs',
  --   chunkSizeBytes: 1024 * 255, // 255KB SHOULD be the default chunk size.
  --   concurrency: 10 // OPTIONAL - useful if you are writing files asynchronously
  -- }
  --
  -- var bucket = new ReGrid(connectionOptions, bucketOptions)
  --
  -- bucket // a new bucket instance
  function r.re_grid(connection_options, bucket_options)
    return protect(function()
      local bucket_name = bucket_options.bucket_name or DEFAULT_BUCKET_NAME
      local chunk_size_bytes = bucket_options.chunk_size_bytes or DEFAULT_CHUNK_SIZE_BYTES
      local concurrency = bucket_options.concurrency or DEFAULT_CONCURRENCY

      if not r.type(connection_options) then
        local err
        connection_options, err = r.connector(connection_options)
        connection_options.concurrency = concurrency  -- @todo
        if not connection_options then
          return nil, err
        end
      end

      local filesn = r.reql.add(bucket_name, '_files')
      local filest = filesn.table()
      local chunksn = r.reql.add(bucket_name, '_chunks')
      local chunkst = chunksn.table()

      local bucket = {r = r}

      --- bucket.initBucket()
      --
      -- ReGrid drivers MUST provied a method to create required tables and
      -- indexes.
      --
      -- Table Names
      --
      -- Two tables MUST be created for ReGrid to function, the 'files' table and
      -- the 'chunks' table. Tables MUST be a combination of the bucketName
      -- followed by an underscore and the table type. Given the default
      -- bucketName of 'fs' the files table MUST be named fs_files and the chunks
      -- table MUST be named fs_chunks
      --
      -- The driver MUST check whether the tables already exist before creating
      -- them. If creating the tables fails the driver MUST return an error.
      --
      -- Indexes
      --
      -- For efficient retrieval of files and chunks, a few indexes are required
      -- by ReGrid. Indexes MUST be named as shown below.
      --
      -- r.table('<FilesTable>').indexCreate('file_ix', [r.row('status'),
      -- r.row('filename'), r.row('finishedAt')])
      --
      -- r.table('<ChunksTable>').indexCreate('chunk_ix', [r.row('file_id'),
      -- r.row('num')])
      -- The driver MUST check whether the indexes already exist before creating
      -- them. If creating the indexes fails the driver MUST return an error.
      function bucket.init_bucket()
        return protect(function()
          assert(filesn.table_create().run(connection_options, {noreply = true}))
          assert(chunksn.table_create().run(connection_options, {noreply = true}))
          assert(filest.wait()'ready'.eq(1).branch(
            filest.index_create(
              'file_ix',
              function(row)
                return {row'status', row'filename', row'finishedAt'}
              end
            )
          ).run(connection_options, {noreply = true}))
          assert(chunkst.wait()'ready'.eq(1).branch(
            chunkst.index_create(
              'chunk_ix',
              function(row)
                return {row'file_id', row'num'}
              end
            )
          ).run(connection_options, {noreply = true}))
          local cur = assert(filest.index_wait'file_ix''ready'.run(connection_options))
          local arr = assert(cur.to_array())
          assert(arr[1], 'file index not ready.')
          cur = assert(chunkst.index_wait'chunk_ix''ready'.run(connection_options))
          arr = assert(cur.to_array())
          return assert(arr[1], 'chunk index not ready.')
        end)
      end

      --- bucket.createWriteStream(filename, options)
      --
      -- Drivers SHOULD use their languages built-in stream abstraction.
      -- Otherwise, they MUST provide their own.
      --
      -- Code Example
      --
      -- // An options object MAY be passed in. All fields are optional.
      -- var options = {
      --   chunkSizeBytes: 1024 * 255,
      --   metadata: null
      -- }
      --
      -- bucket.createWriteStream(filename, options) // returns a stream
      function bucket.create_write_stream(filename, options)
        return protect(function()
          local cur = assert(filest.insert{
            chunkSizeBytes = options.chunk_size_bytes or chunk_size_bytes,
            startedAt = r.reql.now(),
            filename = filename,
            status = 'Incomplete',
            metadata = options.metadata
          }.run(connection_options))
          local arr = assert(cur.to_array())

          local file_id = arr[1].generated_keys[1]

          return sink_chunk(file_id, chunk_size_bytes, filest, chunkst)
        end)
      end

      --- createReadStreamById(file_id)
      --
      -- Get a readStream by id
      function bucket.create_read_stream_by_id(file_id)
        return protect(function()
          local cur = assert(filest.get(file_id).run(connection_options))
          local arr = assert(cur.to_array())

          assert(arr[1].status == 'Complete', 'Attempted to read ' .. string.lower(arr[1].status) .. ' file.')

          local function source()
          end

          return source()
        end)
      end

      --- createReadStreamByFilename(filename, options)
      --
      -- Get a readStream by filename. Since filenames are not unique, there can
      -- be multiple "revisions" of a file. A user may optionally specify a
      -- revision in the options object.
      function bucket.create_read_stream_by_filename(filename, options)
        return protect(function()
          local function source()
          end

          return source()
        end)
      end

      return bucket
    end)
  end
end

return m
