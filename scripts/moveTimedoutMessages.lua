-- Move timed-out messages back to active queue

local rawData = assert(KEYS[1], 'data is missing')

local data = cjson.decode(rawData)
local currTime = data.now
local processingQueue = 'p:queue'

local total = redis.call('ZCOUNT', processingQueue, 0, '+inf')

if total > 0 then
	local vals = redis.call('ZRANGEBYSCORE', processingQueue, 0, currTime)

	for i = 1, #vals do
		local queueName = string.match(vals[i], '([%w%-_]+):')
		local id = string.match(vals[i], ':([%w%-_]+)')
		local activeQueue = 'q:' .. queueName .. ':act'

		redis.call('RPUSH', activeQueue, id)
	end

	redis.call('ZREMRANGEBYSCORE', processingQueue, 0, currTime)

	return total - #vals
else
	return total
end
