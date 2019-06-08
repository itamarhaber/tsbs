--[[
Aggregate a single timeseries in a zset of a device/metric
KEYS[1] - the zset
ARGV[1] - start timestamp
ARGV[2] - end timestamp
return: int (max of values)
]]--

local sub = string.sub
local remove = table.remove
local agg = 0
local data = redis.call('ZRANGEBYSCORE', KEYS[1], ARGV[1], ARGV[2])
while #data > 0 do
    local point, fields = remove(data), {}
    point:gsub("([^:]+)", function(c) fields[#fields+1] = c end)
    local datum = tonumber(fields[2])  -- user_usage, 1st field
    if datum > agg then
        agg = datum
    end
end
return agg