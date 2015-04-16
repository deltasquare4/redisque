-- Removes the message from active/processing lists and clears the data

local rawData = assert(KEYS[1], 'data is missing')

local data = cjson.decode(rawData)
local id = data.id
local activeQueue = data.queue
local queueName = string.match(activeQueue, 'q:([%w%-_]+):act')
local prefix = 'q:' .. queueName .. ':'
local processingQueue = 'p:queue'
local dataHash = prefix .. 'data:' .. id

-- Remove it from processing list if exists
redis.call('ZREM', processingQueue, queueName .. ':' .. id)

-- Remove it from active list if exists
redis.call('LREM', activeQueue, 0, id)

-- Remove data
redis.call('DEL', dataHash)

return true
