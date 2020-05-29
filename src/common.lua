local function slice(array, start, finish)
    local t = {}
    for k = start, finish do
        t[#t + 1] = array[k]
    end
    return t
end

local function retrieveEntry(id, feature_keys)
    local features = {}

    while #feature_keys > 0 do
        local key = table.remove(feature_keys, 1)
        features[#features + 1] = redis.pcall("ZSCORE", key, id)
    end

    return features
end

local function retrieveEntries(path, is_low_to_high, feature_keys, low, high)
    local ids = redis.pcall((is_low_to_high == "true") and "zrange" or "zrevrange", path, low, high)
    local features = {}

    while #feature_keys > 0 do
        local key = table.remove(feature_keys, 1)

        local scores = {}
        for n = 1, #ids, 1 do
            table.insert(scores, redis.pcall("ZSCORE", key, ids[n]))
        end
        features[#features + 1] = scores
    end

    -- [
    --   ['foo', 'bar', 'baz'],
    --   [ [1, 2, 3], [4, 5, 6] ]
    -- ]
    return {ids, features}
end

local function aroundRange(path, is_low_to_high, id, distance, fill_borders)
    local r = redis.pcall((is_low_to_high == "true") and "zrank" or "zrevrank", path, id)
    if r == false or r == nil then
        return {-1, -1}
    end
    local c = redis.pcall("zcard", path)
    local l = math.max(0, r - distance)
    local h = 0
    if fill_borders == "true" then
        h = l + 2 * distance
        if h >= c then
            h = math.min(c, r + distance)
            l = math.max(0, h - 2 * distance - 1)
        end
    else
        h = math.min(c, r + distance)
    end
    return {l, h, c, r}
end

-- %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

local function get(path)
    local res = redis.pcall("get", path)
    if res == false then
        return nil
    end
    return res
end

local function set(path, value)
    return redis.pcall("set", path, value)
end

local function del(path)
    return redis.pcall("del", path)
end

local function zadd(path, id, score)
    return redis.pcall("zadd", path, score, id)
end

local function improve(path, id, score, lowToHigh)
    local ps = redis.pcall("zscore", path, id)
    if lowToHigh == "true" then
        if (not ps) or tonumber(score) < tonumber(ps) then
            redis.pcall("zadd", path, score, id)
            return 1
        end
    else
        if (not ps) or tonumber(score) > tonumber(ps) then
            redis.pcall("zadd", path, score, id)
            return 1
        end
    end
    return 0
end

local function zincrby(path, id, amount)
    return redis.pcall("zincrby", path, amount, id)
end

local function zrem(path, id)
    return redis.pcall("zrem", path, id)
end

--  %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

local function split(str, delim)
    local gen = string.gmatch(str, "[^" .. delim .. "]+")
    local res = {}
    for ii in gen do
        res[#res + 1] = ii
    end
    return res
end

--  %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
local getLastTimestampedId

local function id2PathedId(path, id)
    local pathedId = path .. "/ids/" .. id
    return pathedId
end

local function id2CurrentTimestampedId(timestamp, id)
    local timestampedId = tostring(timestamp) .. ":" .. id -- 13 digits is enough for more than 315 years
    return timestampedId
end

local function timestampedId2Id(timestampedId)
    local splittedId = split(timestampedId, ":")
    local id = table.concat(slice(splittedId, 2, #splittedId), "")
    return id
end

local function updateTimeStampedId(path, timestamp, id, timestampedId)
    local pathedId = id2PathedId(path, id)
    if timestampedId == nil then
        timestampedId = id2CurrentTimestampedId(timestamp, id)
    end
    local lastTimestampedId = getLastTimestampedId(id, pathedId, "false")

    set(pathedId, timestampedId)
    if lastTimestampedId ~= nil then
        zrem(path, lastTimestampedId)
    end

    return timestampedId
end

getLastTimestampedId = function(path, timestamp, id, createIfNotExists)
    local pathedId = id2PathedId(path, id)
    local timestampedId = get(pathedId)
    if (createIfNotExists == "true") and timestampedId == nil then
        timestampedId = updateTimeStampedId(path, timestamp, id, pathedId)
    end
    return timestampedId
end

local function deleteTimestampedId(path, timestamp, id)
    local pathedId = id2PathedId(path, id)
    local oldTimestampedId = get(pathedId)
    local exists = oldTimestampedId == nil
    if exists then
        del(pathedId)
    end
    return oldTimestampedId
end

local function timestampedAdd(path, timestamp, id, score)
    local timestampedId = updateTimeStampedId(path, timestamp, id)
    zadd(path, timestampedId, score)
end

local function timestampedRemove(path, timestamp, id)
    local lastTimestampedId = deleteTimestampedId(path, timestamp, id)
    if lastTimestampedId ~= nil then
        zrem(path, lastTimestampedId)
    end
end

local function timestampedImprove(path, timestamp, lowToHigh, id, score)
    local lastTimestampedId = getLastTimestampedId(path, timestamp, id, "false")
    local currentTimestampedId = id2CurrentTimestampedId(timestamp, id)
    local updated
    if lastTimestampedId == nil then
        updated = 1
        zadd(path, currentTimestampedId, score)
        updateTimeStampedId(path, timestamp, id, currentTimestampedId)
    else
        updated = improve(path, lastTimestampedId, score, lowToHigh)
        if updated == 1 then
            updateTimeStampedId(path, timestamp, id, currentTimestampedId)
            zadd(path, currentTimestampedId, score)
        end
    end
    return updated
end

local function timestampedIncr(path, timestamp, id, amount)
    local lastTimestampedId = getLastTimestampedId(path, timestamp, id, "false")
    local newScore
    if lastTimestampedId == nil then
        newScore = tostring(amount)
    else
        newScore = zincrby(path, lastTimestampedId, amount)
    end
    local timestampedId = updateTimeStampedId(path, timestamp, id)
    zadd(path, timestampedId, amount)
    return newScore
end

local function timestampedClear(path)
    local allTimestampedIds = redis.pcall("zrange", path, 0, -1)
    for i = 1, table.getn(allTimestampedIds) do
        local id = timestampedId2Id(allTimestampedIds[i])
        redis.pcall("del", id2PathedId(path, id))
    end
    redis.pcall("del", path)
end

local function retrieveMultimetricEntry(id, feature_keys)
    local features = {}

    while #feature_keys > 0 do
        local key = table.remove(feature_keys, 1)
        local timestampedId = getLastTimestampedId(key, nil, id, "false")
        features[#features + 1] = redis.pcall("ZSCORE", key, timestampedId)
    end

    return features
end

local function retrieveMultimetricEntries(path, is_low_to_high, feature_keys, low, high)
    local ids = redis.pcall((is_low_to_high == "true") and "zrange" or "zrevrange", path, low, high)
    local features = {}

    while #feature_keys > 0 do
        local key = table.remove(feature_keys, 1)

        local scores = {}
        for n = 1, #ids, 1 do
            local id = timestampedId2Id(ids[n])
            local timestampedId = getLastTimestampedId(key, nil, id, "false")
            table.insert(scores, redis.pcall("ZSCORE", key, timestampedId))
        end
        features[#features + 1] = scores
    end

    -- [
    --   ['foo', 'bar', 'baz'],
    --   [ [1, 2, 3], [4, 5, 6] ]
    -- ]
    return {ids, features}
end
