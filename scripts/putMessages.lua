-- Puts multiple messages into specified queue

local activeQueue = assert(KEYS[1], 'activeQueue is missing')
local rawMessages = assert(KEYS[2], 'Messages are missing')

local messages = cjson.decode(rawMessages)
-- local messages = rawMessages
local queueName = string.match(activeQueue, 'q:([%w%-_]+):act')
local prefix = 'q:' .. queueName .. ':'
local scheduledQueue = 's:queue'
local dataHash = prefix .. 'data:'
local scheduledData = 's:data'
local idKey = 'msgId'
-- local messages = {}

local hmset = function (key, dict)
  if next(dict) == nil then return nil end
	local bulk = {}
	for k, v in pairs(dict) do
		table.insert(bulk, k)
		table.insert(bulk, v)
	end
	return redis.call('HMSET', key, unpack(bulk))
end

for i = 1, #messages do
	local message = messages[i]
	local id = message.id or redis.call('INCR', idKey)
	message.id = id

	if message.delay then
		-- Save message data
		hmset(scheduledData .. ':' .. id, message)

		-- Add message to scheduled list
		redis.call('ZADD', scheduledQueue, message.delay, queueName .. ':' .. id)
	else
		-- Save message data
		hmset(dataHash .. id, message)

		-- Add message id to active queue
		redis.call('RPUSH', activeQueue, id)
	end
end

return cjson.encode(messages)
