use super::*;
use crate::test_support::RedisTestContext;
use chrono::Utc;
use serde_json::json;
use std::collections::HashMap;

fn build_job(queue_name: &str, dlq_name: &str) -> Job {
    Job {
        id: Job::new_id(),
        function_name: "do_work".to_string(),
        job_params: serde_json::Map::new(),
        enqueue_time: Utc::now(),
        start_time: None,
        status: JobStatus::Pending,
        current_retries: 0,
        next_scheduled_run_time: None,
        max_retries: 3,
        job_timeout_seconds: Some(30),
        result_ttl_seconds: Some(60),
        job_unique_key: None,
        completion_time: None,
        result: None,
        last_error: None,
        queue_name: Some(queue_name.to_string()),
        dlq_name: Some(dlq_name.to_string()),
        worker_id: None,
        trace_context: None,
        correlation_context: None,
    }
}

#[tokio::test]
async fn lua_scripts_compile_in_redis() {
    let mut ctx = RedisTestContext::new().await.unwrap();
    for script in [
        LOCK_AND_START_LUA,
        CLAIM_READY_LUA,
        REFRESH_LOCK_LUA,
        RELEASE_LOCK_IF_OWNER_LUA,
        RETRY_LUA,
        ENQUEUE_LUA,
        MOVE_TO_DLQ_LUA,
        REQUEUE_JOB_LUA,
    ] {
        let sha: String = redis::cmd("SCRIPT")
            .arg("LOAD")
            .arg(script)
            .query_async(&mut ctx.store.conn)
            .await
            .unwrap();
        assert_eq!(sha.len(), 40);
    }
}

#[tokio::test]
async fn job_store_queue_and_lock_flow() {
    let mut ctx = RedisTestContext::new().await.unwrap();
    let queue_name = ctx.settings.default_queue_name.clone();
    let dlq_name = ctx.settings.default_dlq_name.clone();
    let job = build_job(&queue_name, &dlq_name);

    ctx.store.save_job_definition(&job).await.unwrap();
    let loaded = ctx
        .store
        .get_job_definition(&job.id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(loaded.function_name, job.function_name);

    let score = Utc::now().timestamp_millis() as f64;
    ctx.store
        .add_job_to_queue(&queue_name, &job.id, score)
        .await
        .unwrap();
    assert!(ctx.store.queue_exists(&queue_name).await.unwrap());
    assert_eq!(ctx.store.queue_size(&queue_name).await.unwrap(), 1);
    assert!(ctx.store.is_job_queued(&queue_name, &job.id).await.unwrap());

    let ready = ctx.store.get_ready_job_ids(&queue_name, 10).await.unwrap();
    assert!(ready.contains(&job.id));

    let start_time = Utc::now();
    let (locked, removed) = ctx
        .store
        .atomic_lock_and_start_job(&job.id, &queue_name, "worker-1", 1000, start_time)
        .await
        .unwrap();
    assert!(locked);
    assert_eq!(removed, 1);
    assert_eq!(
        ctx.store.get_job_lock_owner(&job.id).await.unwrap(),
        Some("worker-1".to_string())
    );
    let started = ctx
        .store
        .get_job_definition(&job.id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(started.status, JobStatus::Active);
    assert_eq!(started.worker_id.as_deref(), Some("worker-1"));
    assert_eq!(
        started.start_time.unwrap().timestamp(),
        start_time.timestamp()
    );
    let active = ctx.store.get_active_job_ids("worker-1").await.unwrap();
    assert!(active.contains(&job.id));

    ctx.store
        .remove_active_job("worker-1", &job.id)
        .await
        .unwrap();
    ctx.store
        .mark_job_pending(&job.id, Some("reset"))
        .await
        .unwrap();
    ctx.store.release_job_lock(&job.id).await.unwrap();
    assert_eq!(ctx.store.get_job_lock_owner(&job.id).await.unwrap(), None);

    let mut fields = HashMap::new();
    fields.insert("last_error".to_string(), "boom".to_string());
    ctx.store.update_job_fields(&job.id, &fields).await.unwrap();
    ctx.store
        .update_job_status(&job.id, JobStatus::Active)
        .await
        .unwrap();
    let updated = ctx
        .store
        .get_job_definition(&job.id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(updated.status, JobStatus::Active);
    assert_eq!(updated.last_error.as_deref(), Some("boom"));
}

#[tokio::test]
async fn job_store_atomic_claim_ready_jobs_claims_batch() {
    let mut ctx = RedisTestContext::new().await.unwrap();
    let queue_name = ctx.settings.default_queue_name.clone();
    let dlq_name = ctx.settings.default_dlq_name.clone();
    let worker_id = "worker-batch";
    let base_score = Utc::now().timestamp_millis() - 1_000;

    let mut job_ids = Vec::new();
    for index in 0..3 {
        let mut job = build_job(&queue_name, &dlq_name);
        job.id = format!("batch-job-{index}");
        ctx.store.save_job_definition(&job).await.unwrap();
        ctx.store
            .add_job_to_queue(&queue_name, &job.id, (base_score + index as i64) as f64)
            .await
            .unwrap();
        job_ids.push(job.id);
    }

    let start_time = Utc::now();
    let claimed = ctx
        .store
        .atomic_claim_ready_jobs(&queue_name, worker_id, 10_000, 0, 2, start_time)
        .await
        .unwrap();
    assert_eq!(claimed.len(), 2);
    assert_eq!(ctx.store.queue_size(&queue_name).await.unwrap(), 1);

    let active = ctx.store.get_active_job_ids(worker_id).await.unwrap();
    for claimed_id in &claimed {
        assert!(active.contains(claimed_id));
        assert_eq!(
            ctx.store.get_job_lock_owner(claimed_id).await.unwrap(),
            Some(worker_id.to_string())
        );
        let job = ctx
            .store
            .get_job_definition(claimed_id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(job.status, JobStatus::Active);
        assert_eq!(job.worker_id.as_deref(), Some(worker_id));
    }

    for job_id in job_ids {
        let _ = ctx.store.release_job_lock(&job_id).await;
        let _ = ctx.store.remove_active_job(worker_id, &job_id).await;
    }
}

#[tokio::test]
async fn job_store_atomic_claim_ready_jobs_skips_locked_candidates() {
    let mut ctx = RedisTestContext::new().await.unwrap();
    let queue_name = ctx.settings.default_queue_name.clone();
    let dlq_name = ctx.settings.default_dlq_name.clone();
    let worker_id = "worker-claim";
    let locked_job_id = "batch-skip-job-0";
    let base_score = Utc::now().timestamp_millis() - 1_000;

    for index in 0..3 {
        let mut job = build_job(&queue_name, &dlq_name);
        job.id = format!("batch-skip-job-{index}");
        ctx.store.save_job_definition(&job).await.unwrap();
        ctx.store
            .add_job_to_queue(&queue_name, &job.id, (base_score + index as i64) as f64)
            .await
            .unwrap();
    }

    let locked = ctx
        .store
        .try_lock_job(locked_job_id, "other-worker", 10_000)
        .await
        .unwrap();
    assert!(locked);

    let claimed = ctx
        .store
        .atomic_claim_ready_jobs(&queue_name, worker_id, 10_000, 0, 2, Utc::now())
        .await
        .unwrap();
    assert_eq!(claimed.len(), 2);
    assert!(!claimed.iter().any(|id| id == locked_job_id));
    assert_eq!(
        ctx.store.get_job_lock_owner(locked_job_id).await.unwrap(),
        Some("other-worker".to_string())
    );
    assert!(
        ctx.store
            .is_job_queued(&queue_name, locked_job_id)
            .await
            .unwrap()
    );
    assert_eq!(ctx.store.queue_size(&queue_name).await.unwrap(), 1);

    for job_id in claimed {
        let _ = ctx.store.remove_active_job(worker_id, &job_id).await;
        let _ = ctx.store.release_job_lock(&job_id).await;
    }
    let _ = ctx.store.release_job_lock(locked_job_id).await;
}

#[tokio::test]
async fn job_store_atomic_claim_ready_jobs_uses_job_timeout_for_provisional_lock_ttl() {
    let mut ctx = RedisTestContext::new().await.unwrap();
    let queue_name = ctx.settings.default_queue_name.clone();
    let dlq_name = ctx.settings.default_dlq_name.clone();
    let worker_id = "worker-timeout-ttl";

    let mut job = build_job(&queue_name, &dlq_name);
    job.id = "claim-timeout-ttl-job".to_string();
    job.job_timeout_seconds = Some(4);
    ctx.store.save_job_definition(&job).await.unwrap();
    ctx.store
        .add_job_to_queue(
            &queue_name,
            &job.id,
            (Utc::now().timestamp_millis() - 100) as f64,
        )
        .await
        .unwrap();

    let default_lock_timeout_ms = 30_000;
    let lock_timeout_extension_seconds = 3;
    let claimed = ctx
        .store
        .atomic_claim_ready_jobs(
            &queue_name,
            worker_id,
            default_lock_timeout_ms,
            lock_timeout_extension_seconds,
            1,
            Utc::now(),
        )
        .await
        .unwrap();
    assert_eq!(claimed, vec![job.id.clone()]);

    let lock_key = format!("{LOCK_KEY_PREFIX}{}", job.id);
    let lock_ttl_ms: i64 = redis::cmd("PTTL")
        .arg(&lock_key)
        .query_async(&mut ctx.store.conn)
        .await
        .unwrap();

    let expected_lock_timeout_ms =
        (job.job_timeout_seconds.unwrap() + lock_timeout_extension_seconds) * 1_000;
    assert!(lock_ttl_ms > 0);
    assert!(
        lock_ttl_ms <= expected_lock_timeout_ms && lock_ttl_ms >= expected_lock_timeout_ms - 3_000,
        "expected claim TTL near {expected_lock_timeout_ms}ms, got {lock_ttl_ms}ms"
    );
    assert!(
        lock_ttl_ms < default_lock_timeout_ms - 5_000,
        "expected claim TTL to be derived from job timeout, got {lock_ttl_ms}ms"
    );

    let _ = ctx.store.remove_active_job(worker_id, &job.id).await;
    let _ = ctx.store.release_job_lock(&job.id).await;
}

#[tokio::test]
async fn job_store_refresh_job_lock_timeout_requires_owner() {
    let mut ctx = RedisTestContext::new().await.unwrap();
    let queue_name = ctx.settings.default_queue_name.clone();
    let dlq_name = ctx.settings.default_dlq_name.clone();
    let mut job = build_job(&queue_name, &dlq_name);
    job.id = "refresh-lock-job".to_string();
    ctx.store.save_job_definition(&job).await.unwrap();

    let locked = ctx
        .store
        .try_lock_job(&job.id, "worker-owner", 1_000)
        .await
        .unwrap();
    assert!(locked);

    let refreshed = ctx
        .store
        .refresh_job_lock_timeout(&job.id, "other-worker", 5_000)
        .await
        .unwrap();
    assert!(!refreshed);
    assert_eq!(
        ctx.store.get_job_lock_owner(&job.id).await.unwrap(),
        Some("worker-owner".to_string())
    );

    let refreshed = ctx
        .store
        .refresh_job_lock_timeout(&job.id, "worker-owner", 5_000)
        .await
        .unwrap();
    assert!(refreshed);

    let missing_lock_refreshed = ctx
        .store
        .refresh_job_lock_timeout("missing-job", "worker-owner", 5_000)
        .await
        .unwrap();
    assert!(!missing_lock_refreshed);

    let _ = ctx.store.release_job_lock(&job.id).await;
}

#[tokio::test]
async fn job_store_release_job_lock_if_owner_requires_owner() {
    let mut ctx = RedisTestContext::new().await.unwrap();
    let queue_name = ctx.settings.default_queue_name.clone();
    let dlq_name = ctx.settings.default_dlq_name.clone();
    let mut job = build_job(&queue_name, &dlq_name);
    job.id = "release-lock-owner-job".to_string();
    ctx.store.save_job_definition(&job).await.unwrap();

    let locked = ctx
        .store
        .try_lock_job(&job.id, "worker-owner", 1_000)
        .await
        .unwrap();
    assert!(locked);

    let released = ctx
        .store
        .release_job_lock_if_owner(&job.id, "other-worker")
        .await
        .unwrap();
    assert!(!released);
    assert_eq!(
        ctx.store.get_job_lock_owner(&job.id).await.unwrap(),
        Some("worker-owner".to_string())
    );

    let released = ctx
        .store
        .release_job_lock_if_owner(&job.id, "worker-owner")
        .await
        .unwrap();
    assert!(released);
    assert_eq!(ctx.store.get_job_lock_owner(&job.id).await.unwrap(), None);
}

#[tokio::test]
async fn job_store_atomic_claim_ready_jobs_skips_stale_queue_entries_without_creating_hash() {
    let mut ctx = RedisTestContext::new().await.unwrap();
    let queue_name = ctx.settings.default_queue_name.clone();
    let worker_id = "worker-stale";
    let stale_job_id = "stale-queue-job";
    let score = (Utc::now().timestamp_millis() - 1_000) as f64;

    ctx.store
        .add_job_to_queue(&queue_name, stale_job_id, score)
        .await
        .unwrap();

    let claimed = ctx
        .store
        .atomic_claim_ready_jobs(&queue_name, worker_id, 10_000, 0, 1, Utc::now())
        .await
        .unwrap();
    assert!(claimed.is_empty());
    assert!(
        !ctx.store
            .is_job_queued(&queue_name, stale_job_id)
            .await
            .unwrap()
    );
    assert!(
        ctx.store
            .get_job_definition(stale_job_id)
            .await
            .unwrap()
            .is_none()
    );
    assert_eq!(
        ctx.store.get_job_lock_owner(stale_job_id).await.unwrap(),
        None
    );
    let active = ctx.store.get_active_job_ids(worker_id).await.unwrap();
    assert!(!active.contains(&stale_job_id.to_string()));
}

#[tokio::test]
async fn job_store_atomic_claim_ready_jobs_claims_existing_hash_missing_enqueue_time() {
    let mut ctx = RedisTestContext::new().await.unwrap();
    let queue_name = ctx.settings.default_queue_name.clone();
    let dlq_name = ctx.settings.default_dlq_name.clone();
    let worker_id = "worker-missing-enqueue-time";
    let mut job = build_job(&queue_name, &dlq_name);
    job.id = "missing-enqueue-time-job".to_string();
    ctx.store.save_job_definition(&job).await.unwrap();
    ctx.store
        .add_job_to_queue(
            &queue_name,
            &job.id,
            (Utc::now().timestamp_millis() - 1_000) as f64,
        )
        .await
        .unwrap();

    let job_key = format!("{JOB_KEY_PREFIX}{}", job.id);
    let removed: i64 = redis::cmd("HDEL")
        .arg(&job_key)
        .arg("enqueue_time")
        .query_async(&mut ctx.store.conn)
        .await
        .unwrap();
    assert_eq!(removed, 1);

    let claimed = ctx
        .store
        .atomic_claim_ready_jobs(&queue_name, worker_id, 10_000, 0, 1, Utc::now())
        .await
        .unwrap();
    assert_eq!(claimed, vec![job.id.clone()]);
    assert!(!ctx.store.is_job_queued(&queue_name, &job.id).await.unwrap());
    assert_eq!(
        ctx.store.get_job_lock_owner(&job.id).await.unwrap(),
        Some(worker_id.to_string())
    );
    let active = ctx.store.get_active_job_ids(worker_id).await.unwrap();
    assert!(active.contains(&job.id));

    let _ = ctx.store.remove_active_job(worker_id, &job.id).await;
    let _ = ctx.store.release_job_lock(&job.id).await;
}

#[tokio::test]
async fn job_store_get_job_definitions_skips_malformed_hash_entries() {
    let mut ctx = RedisTestContext::new().await.unwrap();
    let malformed_job_id = "malformed-job";

    ctx.store
        .update_job_status(malformed_job_id, JobStatus::Active)
        .await
        .unwrap();
    let jobs = ctx
        .store
        .get_job_definitions(&[malformed_job_id.to_string()])
        .await
        .unwrap();
    assert_eq!(jobs.len(), 1);
    assert!(jobs[0].is_none());
}

#[tokio::test]
async fn job_store_retry_and_dlq_flow() {
    let mut ctx = RedisTestContext::new().await.unwrap();
    let queue_name = ctx.settings.default_queue_name.clone();
    let dlq_name = ctx.settings.default_dlq_name.clone();
    let job = build_job(&queue_name, &dlq_name);

    ctx.store.save_job_definition(&job).await.unwrap();
    let retry_score = (Utc::now().timestamp_millis() - 1000) as f64;
    let retry_count = ctx
        .store
        .atomic_retry_job(
            &job.id,
            &queue_name,
            retry_score,
            "retry",
            JobStatus::Retrying,
        )
        .await
        .unwrap();
    assert_eq!(retry_count, 1);
    let loaded = ctx
        .store
        .get_job_definition(&job.id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(loaded.status, JobStatus::Retrying);
    assert_eq!(loaded.last_error.as_deref(), Some("retry"));

    let new_retry = ctx.store.increment_job_retries(&job.id).await.unwrap();
    assert_eq!(new_retry, 2);

    ctx.store
        .move_job_to_dlq(&job.id, &dlq_name, "failed", Utc::now())
        .await
        .unwrap();
    let events_key = format_job_events_key(&job.id);
    let event_count: i64 = redis::cmd("XLEN")
        .arg(&events_key)
        .query_async(&mut ctx.store.conn)
        .await
        .unwrap();
    assert_eq!(event_count, 1);
    let events_ttl: i64 = redis::cmd("TTL")
        .arg(&events_key)
        .query_async(&mut ctx.store.conn)
        .await
        .unwrap();
    assert!(events_ttl > 0);
    assert_eq!(ctx.store.dlq_len(&dlq_name).await.unwrap(), 1);
    let ids = ctx.store.get_dlq_job_ids(&dlq_name).await.unwrap();
    assert!(ids.contains(&job.id));
    assert_eq!(
        ctx.store.dlq_remove_job(&dlq_name, &job.id).await.unwrap(),
        1
    );
    assert_eq!(ctx.store.dlq_len(&dlq_name).await.unwrap(), 0);
}

#[tokio::test]
async fn job_store_save_job_result_emits_event_stream_and_applies_ttl() {
    let mut ctx = RedisTestContext::new().await.unwrap();
    let queue_name = ctx.settings.default_queue_name.clone();
    let dlq_name = ctx.settings.default_dlq_name.clone();
    let job = build_job(&queue_name, &dlq_name);
    ctx.store.save_job_definition(&job).await.unwrap();

    ctx.store
        .save_job_result(&job.id, &json!({"ok": true}), 30)
        .await
        .unwrap();

    let events_key = format_job_events_key(&job.id);
    let event_count: i64 = redis::cmd("XLEN")
        .arg(&events_key)
        .query_async(&mut ctx.store.conn)
        .await
        .unwrap();
    assert_eq!(event_count, 1);
    let events_ttl: i64 = redis::cmd("TTL")
        .arg(&events_key)
        .query_async(&mut ctx.store.conn)
        .await
        .unwrap();
    assert!(events_ttl > 0);
    assert!(events_ttl <= 30);
}

#[tokio::test]
async fn atomic_requeue_already_queued_demotes_active_job() {
    let mut ctx = RedisTestContext::new().await.unwrap();
    let queue_name = ctx.settings.default_queue_name.clone();
    let dlq_name = ctx.settings.default_dlq_name.clone();
    let mut job = build_job(&queue_name, &dlq_name);
    let worker_id = format!("worker-{}", job.id);
    let start_time = Utc::now();
    job.status = JobStatus::Active;
    job.start_time = Some(start_time);
    job.worker_id = Some(worker_id.clone());
    ctx.store.save_job_definition(&job).await.unwrap();
    ctx.store
        .add_job_to_queue(&queue_name, &job.id, start_time.timestamp_millis() as f64)
        .await
        .unwrap();
    ctx.store
        .track_active_job(&worker_id, &job.id, start_time)
        .await
        .unwrap();
    assert!(
        ctx.store
            .try_lock_job(&job.id, &worker_id, 30_000)
            .await
            .unwrap()
    );

    let result = ctx
        .store
        .atomic_requeue_job(
            &job.id,
            &queue_name,
            start_time.timestamp_millis() as f64,
            "Recovered after lock expiry or worker crash.",
            &worker_id,
            Some(&worker_id),
            None,
        )
        .await
        .unwrap();

    assert_eq!(result, 2);
    let loaded = ctx
        .store
        .get_job_definition(&job.id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(loaded.status, JobStatus::Pending);
    assert_eq!(
        loaded.last_error.as_deref(),
        Some("Recovered after lock expiry or worker crash.")
    );
    assert!(loaded.start_time.is_none());
    assert!(loaded.worker_id.is_none());
    assert!(
        ctx.store
            .get_active_job_ids(&worker_id)
            .await
            .unwrap()
            .is_empty()
    );
    assert!(
        ctx.store
            .get_job_lock_owner(&job.id)
            .await
            .unwrap()
            .is_none()
    );
    assert_eq!(ctx.store.queue_size(&queue_name).await.unwrap(), 1);
}

#[tokio::test]
async fn job_store_unique_locks_and_health() {
    let mut ctx = RedisTestContext::new().await.unwrap();
    let queue_name = ctx.settings.default_queue_name.clone();
    let dlq_name = ctx.settings.default_dlq_name.clone();
    let job = build_job(&queue_name, &dlq_name);

    ctx.store.save_job_definition(&job).await.unwrap();
    let unique_key = format!("unique-{}", job.id);
    let acquired = ctx
        .store
        .acquire_unique_job_lock(&unique_key, &job.id, 5)
        .await
        .unwrap();
    assert!(acquired);
    assert!(ctx.store.get_lock_ttl(&unique_key).await.unwrap() > 0);
    ctx.store
        .release_unique_job_lock(&unique_key)
        .await
        .unwrap();
    assert_eq!(ctx.store.get_lock_ttl(&unique_key).await.unwrap(), 0);

    let mut health = serde_json::Map::new();
    health.insert("worker_id".to_string(), json!("worker-1"));
    health.insert("status".to_string(), json!("running"));
    ctx.store
        .set_worker_health("worker-1", &health, 60)
        .await
        .unwrap();
    let (payload, ttl) = ctx.store.get_worker_health("worker-1").await.unwrap();
    assert!(payload.is_some());
    assert!(ttl.unwrap_or(0) > 0);

    let (_, health_keys) = ctx.store.scan_worker_health_keys(0, 10).await.unwrap();
    assert!(
        health_keys
            .iter()
            .any(|key| key.contains("rrq:health:worker:"))
    );

    let score = Utc::now().timestamp_millis() as f64;
    ctx.store
        .add_job_to_queue(&queue_name, &job.id, score)
        .await
        .unwrap();
    let (_, queue_keys) = ctx.store.scan_queue_keys(0, 10).await.unwrap();
    assert!(queue_keys.iter().any(|key| key.contains(&queue_name)));

    let (_, job_keys) = ctx.store.scan_job_keys(0, 10).await.unwrap();
    assert!(job_keys.iter().any(|key| key.contains(&job.id)));

    ctx.store
        .track_active_job("worker-1", &job.id, Utc::now())
        .await
        .unwrap();
    let (_, active_keys) = ctx.store.scan_active_job_keys(0, 10).await.unwrap();
    assert!(
        active_keys
            .iter()
            .any(|key| key.contains(crate::constants::ACTIVE_JOBS_PREFIX))
    );
}
