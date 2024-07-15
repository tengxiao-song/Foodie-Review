local voucherId = ARGV[1]
local userId = ARGV[2]
local orderId = ARGV[3]

local stockKey = 'seckill:stock:' .. voucherId
local orderKey = 'seckill:order:' .. voucherId

local stock = tonumber(redis.call('get', stockKey))
if stock <= 0 then
    return 1
end

local isMember = redis.call('sismember', orderKey, userId)
if isMember == 1 then
    return 2
end

redis.call('incrby', stockKey, -1)
redis.call('sadd', orderKey, userId)

redis.call("xadd", "stream.orders", "*", "userId", userId, "voucherId", voucherId, "id", orderId)  -- 发送到消息队列
return 0