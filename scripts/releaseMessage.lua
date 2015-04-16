-- Releases the message from processing list into active list

local rawData = assert(KEYS[1], 'data is missing')

local data = cjson.decode(rawData)
local id = data.id
local activeQueue = data.queue
local queueName = string.match(activeQueue, 'q:([%w%-_]+):act')
local prefix = 'q:' .. queueName .. ':'
local processingQueue = 'p:queue'

-- Remove it from processing list if exists
local removed = redis.call('ZREM', processingQueue, queueName .. ':' .. id)

if (removed > 0) then
	-- Add it to active list
	redis.call('RPUSH', activeQueue, id)
end

return true
