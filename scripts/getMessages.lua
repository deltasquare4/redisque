-- Gets specified number of messages from specified queue, while moving them to a temporary sorted queue for timeouts

local rawData = assert(KEYS[1], 'data is missing')

local data = cjson.decode(rawData)
local count = data.count - 1
local activeQueue = data.queue
local currTime = data.now
local queueName = string.match(activeQueue, 'q:([%w%-_]+):act')
local prefix = 'q:' .. queueName .. ':'
local processingQueue = 'p:queue'
local dataHash = prefix .. 'data:'
local messages = {}

local ids = redis.call('LRANGE', activeQueue, 0, count) -- Get message IDs

local hgetall = function (key)
	local bulk = redis.call('HGETALL', key)
	local result = {}
	local nextkey
	for i, v in ipairs(bulk) do
		if i % 2 == 1 then
			nextkey = v
		else
			result[nextkey] = v
		end
	end
	return result
end

-- Remove these messages from active queue
redis.call('LTRIM', activeQueue, count + 1, -1)

-- Restructure messages into JSON array
for i = 1, #ids do
	local message = hgetall(dataHash .. ids[i])
	local expireTS = currTime + message['timeout']
	-- Add id to processing queue for expiration
	redis.call('ZADD', processingQueue, expireTS, queueName .. ':' .. ids[i])
	table.insert(messages, message)
end

return cjson.encode(messages)
