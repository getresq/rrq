-- KEYS: [1] = job_key, [2] = queue_key, [3] = active_key (worker-specific or empty string),
--        [4] = lock_key (full rrq:lock:job:* key)
-- ARGV: [1] = job_id, [2] = score (f64 millis for ZADD), [3] = requeue_message (for last_error),
--        [4] = releasing_owner (worker_id or synthetic owner for lock check),
--        [5] = next_scheduled_run_time (RFC3339 or empty string — only for orphan/cron path)
--
-- Returns:
--   0 = job missing (no-op)
--   1 = requeued (ZADD performed, status fixed if needed, lock released if owner matched)
--   2 = already queued (cleaned up active/lock, no ZADD)
--  -1 = script error (check Redis logs). Commands before the failure executed; no full rollback when caught by pcall.
--
-- This script is the atomic version of the multi-command walks in drain_tasks and
-- recover_orphaned_jobs. It preserves the owner-checked lock release pattern.

local function run()
    local job_key = KEYS[1]
    local queue_key = KEYS[2]
    local active_key = KEYS[3]
    local lock_key = KEYS[4]

    local job_id = ARGV[1]
    local score = ARGV[2]
    local requeue_message = ARGV[3]
    local releasing_owner = ARGV[4]
    local next_scheduled = ARGV[5]

    -- Job must exist
    if redis.call('EXISTS', job_key) == 0 then
        return 0
    end

    -- Already visible in the target queue? Clean up our tracking only.
    if redis.call('ZSCORE', queue_key, job_id) then
        if active_key and active_key ~= '' then
            redis.call('ZREM', active_key, job_id)
        end
        -- Only release lock if we are (still) the owner
        if releasing_owner and releasing_owner ~= '' then
            if redis.call('GET', lock_key) == releasing_owner then
                redis.call('DEL', lock_key)
            end
        end
        return 2
    end

    -- Not queued — perform the requeue.
    -- If it was ACTIVE, demote to PENDING and clear transient fields (matches mark_job_pending).
    local current_status = redis.call('HGET', job_key, 'status')
    if current_status == 'ACTIVE' then
        redis.call('HMSET', job_key, 'status', 'PENDING', 'last_error', requeue_message)
        redis.call('HDEL', job_key, 'start_time', 'worker_id')
    else
        -- Still record the requeue reason for diagnostics
        if requeue_message and requeue_message ~= '' then
            redis.call('HSET', job_key, 'last_error', requeue_message)
        end
    end

    -- Put it back in the time-sorted queue (score may be "now" or a future scheduled time)
    redis.call('ZADD', queue_key, score, job_id)

    -- Remove from this worker's active set if present
    if active_key and active_key ~= '' then
        redis.call('ZREM', active_key, job_id)
    end

    -- Optional: restore next_scheduled_run_time for cron/deferred/orphan cases.
    -- Only the orphan recovery path passes a non-empty value here.
    if next_scheduled and next_scheduled ~= '' then
        redis.call('HSET', job_key, 'next_scheduled_run_time', next_scheduled)
    end

    -- Owner-checked lock release (same pattern as release_lock_if_owner.lua)
    if releasing_owner and releasing_owner ~= '' then
        if redis.call('GET', lock_key) == releasing_owner then
            redis.call('DEL', lock_key)
        end
    end

    return 1
end

local ok, result = pcall(run)
if not ok then
    -- Sentinel; see header comment for pcall + rollback semantics.
    return -1
end
return result
