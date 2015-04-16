-- Acquires or extends a lock

local key = KEYS[1]
local ttl = KEYS[2]
local value = KEYS[3]

local lockSet = redis.call('SETNX', key, value)
local currValue = redis.call('GET', key)
local lock = 0

if lockSet == 1 or value == currValue then
	redis.call('PEXPIRE', key, ttl)
	lock = 1
end

return lock
