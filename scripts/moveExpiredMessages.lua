-- Move timed-out messages back to active queue

local rawData = assert(KEYS[1], 'data is missing')

local data = cjson.decode(rawData)
local currTime = data.now
local scheduledPrefix = 's:'
local scheduledQueue = scheduledPrefix .. 'queue'
local scheduledData = scheduledPrefix .. 'data'

local total = redis.call('ZCOUNT', scheduledQueue, 0, '+inf')

if total > 0 then
	local vals = redis.call('ZRANGEBYSCORE', scheduledQueue, 0, currTime)

	for i = 1, #vals do
		local queueName = string.match(vals[i], '([%w%-_]+):')
		local id = string.match(vals[i], ':([%w%-_]+)')

		local activeQueue = 'q:' .. queueName .. ':act'
		local datahash = 'q:' .. queueName .. ':data:' .. id

		redis.call('RPUSH', activeQueue, id)
		redis.call('RENAME', scheduledData .. ':' .. id, datahash)
	end

	redis.call('ZREMRANGEBYSCORE', scheduledQueue, 0, currTime)

	return total - #vals
else
	return total
end