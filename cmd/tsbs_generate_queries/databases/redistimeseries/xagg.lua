--[[
Aggregate a timeseries in a stream
KEYS[1] - the stream
ARGV[1] - start timestamp
ARGV[2] - end timestamp
ARGV[3] - field name
return: int (max of values)
]]--

local remove = table.remove
local agg = 0
local data = redis.call('XRANGE', KEYS[1], ARGV[1], ARGV[2])
while #data > 0 do
    local msg = remove(data)
    local fv = msg[2]
    local datum = tonumber(fv[2])  -- user_usage
    if datum > agg then
        agg = datum
    end
end
return agg