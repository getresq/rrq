-- KEYS: [1] = job_key, [2] = events_key, [3] = dlq_key, [4] = unique_lock_key (full key or empty string)
-- ARGV: [1] = job_id, [2] = error_message, [3] = completion_time (RFC3339),
--        [4] = dlq_result_ttl_seconds
--
-- Returns: new current_retries count on success, negative value on any error (check Redis logs)
-- Error handling: outer pcall returns -1 on any error inside run() (Lua error or failing redis.call).
-- Commands executed before the failure point have taken effect (no full script rollback when caught).
-- The root cause appears in the Redis server log. Callers must treat -1 as a potential inconsistency signal.

local function run()
    local job_key = KEYS[1]
    local events_key = KEYS[2]
    local dlq_key = KEYS[3]
    local unique_lock_key = KEYS[4]

    local job_id = ARGV[1]
    local error_message = ARGV[2]
    local completion_time = ARGV[3]
    local dlq_ttl = tonumber(ARGV[4]) or 0

    -- Increment retries as part of the terminal atomic transition (matches happy-path retry.lua)
    local new_retry_count = redis.call('HINCRBY', job_key, 'current_retries', 1)

    -- Terminal state
    redis.call('HMSET', job_key,
        'status', 'FAILED',
        'last_error', error_message,
        'completion_time', completion_time
    )

    -- Record failure event (exact shape used by existing move_job_to_dlq + save_job_result)
    redis.call('XADD', events_key, '*',
        'event', 'failed',
        'job_id', job_id,
        'status', 'FAILED'
    )

    -- DLQ membership
    redis.call('LPUSH', dlq_key, job_id)

    -- TTLs (callers normally pass DEFAULT_DLQ_RESULT_TTL_SECONDS (>0) to preserve the prior always-expire behavior)
    if dlq_ttl > 0 then
        redis.call('EXPIRE', job_key, dlq_ttl)
        redis.call('EXPIRE', events_key, dlq_ttl)
    end

    -- Optional unique lock release (done inside script for atomicity with state change)
    if unique_lock_key and unique_lock_key ~= '' then
        redis.call('DEL', unique_lock_key)
    end

    return new_retry_count
end

local ok, result = pcall(run)
if not ok then
    -- Sentinel negative value tells caller a script failure occurred.
    -- The actual error is in the Redis server log for the script invocation.
    return -1
end
return result
