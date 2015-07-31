-- requeue_busy_ng2000
-- # KEYS[1] source_queue name (e.g. foo_working)
-- # KEYS[2] dest_queue name (e.g. foo_unprocessed)
-- # ARGV[1] item_key
-- # ARGV[2] place to requeue in dest_queue:
--      0: at queue tail, 1: at queue head
--
if #KEYS ~= 2 then error('requeue_busy_ng2000 requires 3 keys') end

local source_queue = assert(KEYS[1], 'source_queue missing')
local dest_queue   = assert(KEYS[2], 'dest_queue missing')
local item_key     = assert(ARGV[1], 'item_key missing')
local place        = tonumber(ARGV[2])
local error        = assert(ARGV[3])

assert(place == 0 or place == 1, 'requeue place should be 0 or 1')

local n = redis.call('lrem', source_queue, 1, item_key)

if n > 0 then
    if place == 0 then
        redis.call('lpush', dest_queue, item_key)
    else
        redis.call('rpush', dest_queue, item_key)
    end

    if string.len(error) > 0 then
        local meta_key = 'meta-' .. item_key
        redis.call('hmset', meta_key, 'last_error', error)
    end
end

return n
