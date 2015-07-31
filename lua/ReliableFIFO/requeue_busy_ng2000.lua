-- requeue_busy_ng2000
-- # KEYS[1] from queue name (e.g. working queue)
-- # KEYS[2] dest queue name (e.g. unprocessed queue)
-- # ARGV[1] item
-- # ARGV[2] place to requeue in dest-queue:
--      0: at queue tail, 1: at queue head
--
if #KEYS ~= 2 then error('requeue_busy_ng2000 requires 3 keys') end

local from  = assert(KEYS[1], 'busy queue name missing')
local dest  = assert(KEYS[2], 'dest queue name missing')
local item  = assert(ARGV[1], 'item missing')
local place = tonumber(ARGV[2])
local error = assert(ARGV[3])

assert(place == 0 or place == 1, 'requeue place should be 0 or 1')

local n = redis.call('lrem', from, 1, item)
if n > 0 then
    if place == 0 then
        redis.call('lpush', dest, item)
    else
        redis.call('rpush', dest, item)
    end
end

return n
