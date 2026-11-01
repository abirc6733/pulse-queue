package queue

import "github.com/redis/go-redis/v9"

// luaEnqueue handles atomic task insertion with deduplication and scheduling.
// KEYS[1]: task key
// KEYS[2]: target priority queue key
// KEYS[3]: delayed sorted set key
// KEYS[4]: unique key (optional)
// ARGV[1]: task ID
// ARGV[2]: task JSON payload
// ARGV[3]: scheduled timestamp in ms
// ARGV[4]: current timestamp in ms
// ARGV[5]: unique TTL in seconds
var luaEnqueue = redis.NewScript(`
if KEYS[4] ~= "" then
    if redis.call("EXISTS", KEYS[4]) == 1 then
        return 0 -- duplicate task rejected
    end
    local ttl = tonumber(ARGV[5])
    if ttl and ttl > 0 then
        redis.call("SET", KEYS[4], ARGV[1], "EX", ttl)
    end
end

redis.call("SET", KEYS[1], ARGV[2])

local scheduled_at = tonumber(ARGV[3])
local now = tonumber(ARGV[4])

if scheduled_at > now then
    redis.call("ZADD", KEYS[3], scheduled_at, ARGV[1])
else
    redis.call("LPUSH", KEYS[2], ARGV[1])
end

return 1
`)

// luaDequeue atomically retrieves the highest priority task and puts it in the processing set with a deadline.
// KEYS[1..4]: critical, high, default, low queue keys
// KEYS[5]: processing sorted set key
// ARGV[1]: worker ID
// ARGV[2]: deadline timestamp in ms (now + visibility timeout)
var luaDequeue = redis.NewScript(`
for i = 1, 4 do
    local q_key = KEYS[i]
    local task_id = redis.call("RPOP", q_key)
    if task_id then
        redis.call("ZADD", KEYS[5], ARGV[2], task_id)
        return task_id
    end
end
return nil
`)

// luaAck marks a task as successfully completed and removes it from processing.
// KEYS[1]: processing sorted set key
// KEYS[2]: task key
// KEYS[3]: completed sorted set key
// ARGV[1]: task ID
// ARGV[2]: updated task JSON
// ARGV[3]: completed timestamp in ms
// ARGV[4]: retention TTL in seconds
var luaAck = redis.NewScript(`
redis.call("ZREM", KEYS[1], ARGV[1])
local ttl = tonumber(ARGV[4])
if ttl and ttl > 0 then
    redis.call("SET", KEYS[2], ARGV[2], "EX", ttl)
else
    redis.call("SET", KEYS[2], ARGV[2])
end
redis.call("ZADD", KEYS[3], ARGV[3], ARGV[1])
return 1
`)

// luaNack handles failed tasks by re-scheduling them for retry or pushing to DLQ.
// KEYS[1]: processing sorted set key
// KEYS[2]: task key
// KEYS[3]: delayed sorted set key
// KEYS[4]: DLQ list key
// ARGV[1]: task ID
// ARGV[2]: updated task JSON
// ARGV[3]: is_dlq ("1" = dead-letter, "0" = retry)
// ARGV[4]: next_run_time_ms (when retrying)
var luaNack = redis.NewScript(`
redis.call("ZREM", KEYS[1], ARGV[1])
redis.call("SET", KEYS[2], ARGV[2])

if ARGV[3] == "1" then
    redis.call("LPUSH", KEYS[4], ARGV[1])
else
    redis.call("ZADD", KEYS[3], tonumber(ARGV[4]), ARGV[1])
end

return 1
`)

// luaMigrateDelayed atomically moves scheduled tasks whose run_at <= now to their active queue.
// KEYS[1]: delayed sorted set key
// ARGV[1]: current timestamp in ms
// ARGV[2]: batch size limit (e.g. 100)
// Returns list of [task_id, task_json] to allow routing to priority queue.
var luaMigrateDelayed = redis.NewScript(`
local tasks = redis.call("ZRANGEBYSCORE", KEYS[1], 0, ARGV[1], "LIMIT", 0, tonumber(ARGV[2]))
local ready = {}
for i, task_id in ipairs(tasks) do
    redis.call("ZREM", KEYS[1], task_id)
    table.insert(ready, task_id)
end
return ready
`)

// luaRequeueOrphans identifies in-flight tasks that exceeded their visibility timeout.
// KEYS[1]: processing sorted set key
// ARGV[1]: current timestamp in ms
// ARGV[2]: batch limit
var luaRequeueOrphans = redis.NewScript(`
local expired = redis.call("ZRANGEBYSCORE", KEYS[1], 0, ARGV[1], "LIMIT", 0, tonumber(ARGV[2]))
for i, task_id in ipairs(expired) do
    redis.call("ZREM", KEYS[1], task_id)
end
return expired
`)
