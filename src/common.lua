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
        features[#features + 1] = redis.call("ZSCORE", key, id)
    end

    return features
end

local function retrieveEntries(path, is_low_to_high, feature_keys, low, high)
    local ids = redis.call((is_low_to_high == "true") and "zrange" or "zrevrange", path, low, high)
    local features = {}

    while #feature_keys > 0 do
        local key = table.remove(feature_keys, 1)

        local scores = {}
        for n = 1, #ids, 1 do
            table.insert(scores, redis.call("ZSCORE", key, ids[n]))
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
    local r = redis.call((is_low_to_high == "true") and "zrank" or "zrevrank", path, id)
    if r == false or r == nil then
        return {-1, -1}
    end
    local c = redis.call("zcard", path)
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
    return redis.call("get", path)
end

local function set(path, value)
    return redis.call("set", path, value)
end

local function del(path)
    return redis.call("del", path)
end

local function zadd(path, id, score)
    return redis.call("zadd", path, score, id)
end

local function Improve(path, id, score, lowToHigh)
    local ps = redis.call("zscore", path, id)
    if lowToHigh == 'true' then
        if not ps or tonumber(score) < tonumber(ps) then
            redis.call("zadd", path, score, id)
            return 1
        end
    else
        if not ps or tonumber(score) > tonumber(ps) then
            redis.call("zadd", path, score, id)
            return 1
        end
    end
    return 0
end

local function zincrby(path, id, amount)
    return redis.call("zincrby", path, amount, id)
end

local function zrem(path, id)
    return redis.call("zrem", path, id)
end

--  %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
-- local getLastTimeStampedId

local function id2PathedId(path, id)
    local pathedId = path + "/ids/" + id
    return pathedId
end

local function id2CurrentTimestampedId(timestamp, id)
    local timestampedId = tostring(timestamp) + ":" + id -- 13 digits is enough for more than 315 years
    return timestampedId
end

local function timestampedId2Id(timestampedId)
    local splittedId = split(timestampedId, ":")
    local id = table.concat(tail(splittedId), "")
    return id
end

local function updateTimeStampedId(path, timestamp, id, timestampedId)
    local pathedId = id2PathedId(id)
    if timestampedId == nil then
        timestampedId = id2CurrentTimestampedId(timestamp, id)
    end
    local lastTimestampedId = getLastTimeStampedId(id, pathedId, false)

    set(pathedId, timestampedId)
    if lastTimestampedId ~= nil then
        zrem(path, lastTimestampedId)
    end

    return timestampedId
end

local function getLastTimeStampedId(path, timestamp, id, createIfNotExists)
    local pathedId = id2PathedId(id)
    local timestampedId = get(pathedId)
    if createIfNotExists and timestampedId == nil then
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
    local lastTimestampedId = getLastTimeStampedId(path, timestamp, id, false)
    local currentTimestampedId = this.id2CurrentTimestampedId(timestamp, id)
    local updated
    if lastTimestampedId == nil then
        updated = 1
        zadd(path, currentTimestampedId, score)
        updateTimeStampedId(path, timestamp, id, currentTimestampedId)
    else
        updated = improve(path, lastTimestampedId, score, lowToHigh)
        if updated then
            updateTimeStampedId(path, timestamp, id, currentTimestampedId)
            zadd(path, currentTimestampedId, score)
        end
    end
    return updated
end

local function timestampedIncr(path, timestamp, id, amount)
    local lastTimestampedId = getLastTimestampedId(path, timestamp, id, false)
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
