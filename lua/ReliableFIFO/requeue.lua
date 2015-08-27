-- requeue
-- # KEYS[1] source_queue name (e.g. foo_working)
-- # KEYS[2] unprocessed_queue name (e.g. foo_unprocessed)
-- # KEYS[3] failed_queue name (e.g. foo_failed)
-- # ARGV[1] item_key
-- # ARGV[2] max_process_count
-- # ARGV[3] place to requeue in dest_queue:
--      0: at queue tail, 1: at queue head
-- # ARGV[4] error string
-- # ARGV[5] increment_process_count
--
if #KEYS ~= 3 then error('requeue_failed_ng2000 requires 3 keys') end

local source_queue = assert(KEYS[1], 'source_queue missing')
local unprocessed_queue = assert(KEYS[2], 'unprocessed_queue missing')
local failed_queue = assert(KEYS[3], 'failed_queue missing')
local item_key     = assert(ARGV[1], 'item_key missing')
local max_process_count = tonumber(ARGV[2])
local place        = tonumber(ARGV[3])
local error        = assert(ARGV[4])
local increment_process_count = tonumber(ARGV[5])

assert(place == 0 or place == 1, 'requeue place should be 0 or 1')

local n = redis.call('lrem', source_queue, 1, item_key)

local payload_key = 'item-' .. item_key
local meta_key    = 'meta-' .. item_key

if n > 0 then
    if string.len(error) > 0 then
        redis.call('hmset', meta_key, 'last_error', error)
    end

    local process_count = redis.call('hincrby', meta_key, 'process_count', increment_process_count)
    local dest_queue = unprocessed_queue

    if process_count > max_process_count then
        dest_queue = failed_queue
        redis.call('hincrby', meta_key, 'bail_count', 1)
        redis.call('hset', meta_key, 'process_count', 0)
    end

    if place == 0 then
        redis.call('lpush', dest_queue, item_key)
    else
        redis.call('rpush', dest_queue, item_key)
    end
end

return n
