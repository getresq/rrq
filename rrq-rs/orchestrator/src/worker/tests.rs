use super::*;
use crate::constants::{DLQ_KEY_PREFIX, JOB_KEY_PREFIX, LOCK_KEY_PREFIX};
use crate::test_support::RedisTestContext;
use redis::AsyncCommands;
use serde_json::json;
use std::collections::HashSet;
use tokio::sync::Mutex as TokioMutex;
use tokio::sync::Notify;
use tokio::time::timeout;
use uuid::Uuid;

#[derive(Clone)]
enum TestOutcome {
    Success(Value),
    Retry,
}

#[derive(Clone)]
struct StaticRunner {
    outcome: TestOutcome,
    delay: Duration,
    last_request_id: Arc<TokioMutex<Option<String>>>,
    cancelled: Arc<TokioMutex<Vec<String>>>,
}

#[derive(Clone)]
struct BlockingRunner {
    gate: Arc<Notify>,
    started_queues: Arc<TokioMutex<Vec<String>>>,
}

#[derive(Clone)]
struct CloseAwareRunner {
    execute_started: Arc<Notify>,
    execute_gate: Arc<Notify>,
    close_started: Arc<Notify>,
    close_gate: Arc<Notify>,
    close_called: Arc<AtomicBool>,
}

#[derive(Clone)]
struct CloseInterruptsExecuteRunner {
    execute_started: Arc<Notify>,
    execute_gate: Arc<Notify>,
    close_started: Arc<Notify>,
    close_gate: Arc<Notify>,
}

impl BlockingRunner {
    fn new() -> Self {
        Self {
            gate: Arc::new(Notify::new()),
            started_queues: Arc::new(TokioMutex::new(Vec::new())),
        }
    }
}

impl CloseAwareRunner {
    fn new() -> Self {
        Self {
            execute_started: Arc::new(Notify::new()),
            execute_gate: Arc::new(Notify::new()),
            close_started: Arc::new(Notify::new()),
            close_gate: Arc::new(Notify::new()),
            close_called: Arc::new(AtomicBool::new(false)),
        }
    }
}

impl CloseInterruptsExecuteRunner {
    fn new() -> Self {
        Self {
            execute_started: Arc::new(Notify::new()),
            execute_gate: Arc::new(Notify::new()),
            close_started: Arc::new(Notify::new()),
            close_gate: Arc::new(Notify::new()),
        }
    }
}

#[async_trait::async_trait]
impl Runner for BlockingRunner {
    async fn execute(&self, request: ExecutionRequest) -> Result<ExecutionOutcome> {
        {
            let mut guard = self.started_queues.lock().await;
            guard.push(request.context.queue_name.clone());
        }
        self.gate.notified().await;
        Ok(ExecutionOutcome::success(
            request.job_id.clone(),
            request.request_id.clone(),
            json!({"ok": true}),
        ))
    }

    async fn cancel(&self, _job_id: &str, _request_id: Option<&str>) -> Result<()> {
        Ok(())
    }
}

#[async_trait::async_trait]
impl Runner for CloseAwareRunner {
    async fn execute(&self, _request: ExecutionRequest) -> Result<ExecutionOutcome> {
        self.execute_started.notify_waiters();
        self.execute_gate.notified().await;
        Err(anyhow::anyhow!("runner stopped"))
    }

    async fn close(&self) -> Result<()> {
        self.close_called.store(true, Ordering::SeqCst);
        self.close_started.notify_waiters();
        self.close_gate.notified().await;
        self.execute_gate.notify_waiters();
        Ok(())
    }
}

#[async_trait::async_trait]
impl Runner for CloseInterruptsExecuteRunner {
    async fn execute(&self, _request: ExecutionRequest) -> Result<ExecutionOutcome> {
        self.execute_started.notify_waiters();
        self.execute_gate.notified().await;
        Err(anyhow::anyhow!("runner stopped"))
    }

    async fn close(&self) -> Result<()> {
        self.close_started.notify_waiters();
        // Simulate connection teardown interrupting in-flight execute while shutdown is ongoing.
        self.execute_gate.notify_waiters();
        self.close_gate.notified().await;
        Ok(())
    }
}

impl StaticRunner {
    fn new(outcome: TestOutcome, delay: Duration) -> Self {
        Self {
            outcome,
            delay,
            last_request_id: Arc::new(TokioMutex::new(None)),
            cancelled: Arc::new(TokioMutex::new(Vec::new())),
        }
    }
}

#[async_trait::async_trait]
impl Runner for StaticRunner {
    async fn execute(&self, request: ExecutionRequest) -> Result<ExecutionOutcome> {
        {
            let mut guard = self.last_request_id.lock().await;
            *guard = Some(request.request_id.clone());
        }
        if !self.delay.is_zero() {
            tokio::time::sleep(self.delay).await;
        }
        let outcome = match &self.outcome {
            TestOutcome::Success(value) => ExecutionOutcome::success(
                request.job_id.clone(),
                request.request_id.clone(),
                value.clone(),
            ),
            TestOutcome::Retry => ExecutionOutcome::retry(
                request.job_id.clone(),
                request.request_id.clone(),
                "retry",
                Some(30.0),
            ),
        };
        Ok(outcome)
    }

    async fn cancel(&self, job_id: &str, request_id: Option<&str>) -> Result<()> {
        let mut cancelled = self.cancelled.lock().await;
        cancelled.push(request_id.unwrap_or(job_id).to_string());
        Ok(())
    }
}

fn build_job(queue_name: &str, dlq_name: &str, unique_key: Option<String>) -> Job {
    Job {
        id: Job::new_id(),
        function_name: "task".to_string(),
        job_params: serde_json::Map::new(),
        enqueue_time: Utc::now(),
        start_time: None,
        status: JobStatus::Pending,
        current_retries: 0,
        next_scheduled_run_time: None,
        max_retries: 3,
        job_timeout_seconds: Some(1),
        result_ttl_seconds: Some(30),
        job_unique_key: unique_key,
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

#[test]
fn split_runner_name_variants() {
    let (exec, handler) = split_runner_name("exec#handler");
    assert_eq!(exec, Some("exec".to_string()));
    assert_eq!(handler, "handler");

    let (exec, handler) = split_runner_name("#handler");
    assert_eq!(exec, None);
    assert_eq!(handler, "handler");

    let (exec, handler) = split_runner_name("exec#");
    assert_eq!(exec, Some("exec".to_string()));
    assert!(handler.is_empty());

    let (exec, handler) = split_runner_name("plain");
    assert_eq!(exec, None);
    assert_eq!(handler, "plain");
}

#[tokio::test]
async fn cleanup_running_clears_in_memory_state_when_lock_release_fails() {
    let mut ctx = RedisTestContext::new().await.unwrap();
    let worker_id = "worker-1";
    let job_id = format!("job-{}", Uuid::new_v4());
    let queue_name = normalize_queue_name(&ctx.settings.default_queue_name);
    let lock_key = format!("{LOCK_KEY_PREFIX}{job_id}");

    ctx.store
        .track_active_job(worker_id, &job_id, Utc::now())
        .await
        .unwrap();

    let running_jobs = Arc::new(Mutex::new(HashMap::new()));
    {
        let mut running = running_jobs.lock().await;
        running.insert(
            job_id.clone(),
            RunningJobInfo {
                queue_name,
                runner_name: Some("test".to_string()),
                request_id: Some("req-1".to_string()),
            },
        );
    }
    let running_task = tokio::spawn(async {});
    let abort_handle = running_task.abort_handle();
    running_task.abort();
    let running_aborts = Arc::new(Mutex::new(HashMap::new()));
    {
        let mut aborts = running_aborts.lock().await;
        aborts.insert(job_id.clone(), abort_handle);
    }

    let redis = redis::Client::open(ctx.settings.redis_dsn.as_str()).unwrap();
    let mut conn = redis.get_multiplexed_async_connection().await.unwrap();
    conn.lpush::<_, _, ()>(&lock_key, "wrongtype-owner")
        .await
        .unwrap();

    let cleanup_err = cleanup_running(
        &job_id,
        &mut ctx.store,
        worker_id,
        running_jobs.clone(),
        running_aborts.clone(),
    )
    .await
    .expect_err("cleanup should fail when lock key has wrong type");
    assert!(
        cleanup_err
            .to_string()
            .to_ascii_lowercase()
            .contains("wrongtype")
    );

    let running = running_jobs.lock().await;
    assert!(
        !running.contains_key(&job_id),
        "running_jobs must be cleared even when lock release fails"
    );
    drop(running);

    let aborts = running_aborts.lock().await;
    assert!(
        !aborts.contains_key(&job_id),
        "running_aborts must be cleared even when lock release fails"
    );
    drop(aborts);

    let active = ctx.store.get_active_job_ids(worker_id).await.unwrap();
    assert!(
        !active.contains(&job_id),
        "redis active-jobs set should still be cleaned up"
    );
}

#[tokio::test]
async fn handle_execution_outcome_success_releases_lock() {
    let mut ctx = RedisTestContext::new().await.unwrap();
    let queue_name = ctx.settings.default_queue_name.clone();
    let dlq_name = ctx.settings.default_dlq_name.clone();
    let job = build_job(&queue_name, &dlq_name, Some("unique-1".to_string()));
    ctx.store.save_job_definition(&job).await.unwrap();
    let acquired = ctx
        .store
        .acquire_unique_job_lock("unique-1", &job.id, 10)
        .await
        .unwrap();
    assert!(acquired);

    let outcome = ExecutionOutcome::success(&job.id, "req-1", json!({"ok": true}));
    handle_execution_outcome(
        &job,
        &queue_name,
        &ctx.settings,
        &mut ctx.store,
        outcome,
        0.0,
    )
    .await
    .unwrap();

    let loaded = ctx
        .store
        .get_job_definition(&job.id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(loaded.status, JobStatus::Completed);
    assert_eq!(loaded.result, Some(json!({"ok": true})));
    let ttl = ctx.store.get_lock_ttl("unique-1").await.unwrap();
    assert_eq!(ttl, 0);
}

#[tokio::test]
async fn handle_execution_outcome_retry_after_sets_schedule() {
    let mut ctx = RedisTestContext::new().await.unwrap();
    let queue_name = ctx.settings.default_queue_name.clone();
    let dlq_name = ctx.settings.default_dlq_name.clone();
    let job = build_job(&queue_name, &dlq_name, None);
    ctx.store.save_job_definition(&job).await.unwrap();

    let outcome = ExecutionOutcome::retry(&job.id, "req-1", "retry", Some(0.01));
    handle_execution_outcome(
        &job,
        &queue_name,
        &ctx.settings,
        &mut ctx.store,
        outcome,
        0.0,
    )
    .await
    .unwrap();

    let loaded = ctx
        .store
        .get_job_definition(&job.id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(loaded.status, JobStatus::Retrying);
    assert!(loaded.current_retries >= 1);
    assert!(loaded.next_scheduled_run_time.is_some());
    assert!(ctx.store.is_job_queued(&queue_name, &job.id).await.unwrap());
}

#[tokio::test]
async fn handle_execution_outcome_timeout_moves_to_dlq() {
    let mut ctx = RedisTestContext::new().await.unwrap();
    let queue_name = ctx.settings.default_queue_name.clone();
    let dlq_name = ctx.settings.default_dlq_name.clone();
    let job = build_job(&queue_name, &dlq_name, Some("unique-timeout".to_string()));
    ctx.store.save_job_definition(&job).await.unwrap();
    let acquired = ctx
        .store
        .acquire_unique_job_lock("unique-timeout", &job.id, 10)
        .await
        .unwrap();
    assert!(acquired);

    let outcome = ExecutionOutcome::timeout(&job.id, "req-1", "timeout");
    handle_execution_outcome(
        &job,
        &queue_name,
        &ctx.settings,
        &mut ctx.store,
        outcome,
        0.0,
    )
    .await
    .unwrap();

    let loaded = ctx
        .store
        .get_job_definition(&job.id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(loaded.status, JobStatus::Failed);
    assert!(ctx.store.dlq_len(&dlq_name).await.unwrap() >= 1);
    let ttl = ctx.store.get_lock_ttl("unique-timeout").await.unwrap();
    assert_eq!(ttl, 0);
}

#[tokio::test]
async fn handle_execution_outcome_handler_not_found_moves_to_dlq() {
    let mut ctx = RedisTestContext::new().await.unwrap();
    let queue_name = ctx.settings.default_queue_name.clone();
    let dlq_name = ctx.settings.default_dlq_name.clone();
    let job = build_job(&queue_name, &dlq_name, None);
    ctx.store.save_job_definition(&job).await.unwrap();

    let outcome = ExecutionOutcome::handler_not_found(&job.id, "req-1", "missing");
    handle_execution_outcome(
        &job,
        &queue_name,
        &ctx.settings,
        &mut ctx.store,
        outcome,
        0.0,
    )
    .await
    .unwrap();

    let loaded = ctx
        .store
        .get_job_definition(&job.id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(loaded.status, JobStatus::Failed);
    assert!(ctx.store.dlq_len(&dlq_name).await.unwrap() >= 1);
}

#[tokio::test]
async fn handle_execution_outcome_error_exceeds_retries() {
    let mut ctx = RedisTestContext::new().await.unwrap();
    let queue_name = ctx.settings.default_queue_name.clone();
    let dlq_name = ctx.settings.default_dlq_name.clone();
    let mut job = build_job(&queue_name, &dlq_name, None);
    job.max_retries = 1;
    job.current_retries = 0;
    ctx.store.save_job_definition(&job).await.unwrap();

    let outcome = ExecutionOutcome::error(&job.id, "req-1", "failed");
    handle_execution_outcome(
        &job,
        &queue_name,
        &ctx.settings,
        &mut ctx.store,
        outcome,
        0.0,
    )
    .await
    .unwrap();

    let loaded = ctx
        .store
        .get_job_definition(&job.id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(loaded.status, JobStatus::Failed);
    assert!(ctx.store.dlq_len(&dlq_name).await.unwrap() >= 1);
}

#[tokio::test]
async fn recover_orphaned_jobs_requeues_and_marks_pending() {
    let mut ctx = RedisTestContext::new().await.unwrap();
    let queue_name = ctx.settings.default_queue_name.clone();
    let dlq_name = ctx.settings.default_dlq_name.clone();
    let mut job = build_job(&queue_name, &dlq_name, None);
    job.status = JobStatus::Active;
    job.next_scheduled_run_time = Some(Utc::now());
    ctx.store.save_job_definition(&job).await.unwrap();
    let worker_id = format!("worker-{}", Uuid::new_v4());
    ctx.store
        .track_active_job(&worker_id, &job.id, Utc::now())
        .await
        .unwrap();
    let shutdown = Arc::new(AtomicBool::new(false));

    recover_orphaned_jobs(&mut ctx.store, &ctx.settings, &shutdown)
        .await
        .unwrap();

    let loaded = ctx
        .store
        .get_job_definition(&job.id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(loaded.status, JobStatus::Pending);
    assert!(ctx.store.is_job_queued(&queue_name, &job.id).await.unwrap());
    let active = ctx.store.get_active_job_ids(&worker_id).await.unwrap();
    assert!(!active.contains(&job.id));
}

#[tokio::test]
async fn recover_orphaned_jobs_skips_locked_job() {
    let mut ctx = RedisTestContext::new().await.unwrap();
    let queue_name = ctx.settings.default_queue_name.clone();
    let dlq_name = ctx.settings.default_dlq_name.clone();
    let mut job = build_job(&queue_name, &dlq_name, None);
    job.status = JobStatus::Active;
    ctx.store.save_job_definition(&job).await.unwrap();
    let worker_id = format!("worker-{}", Uuid::new_v4());
    ctx.store
        .track_active_job(&worker_id, &job.id, Utc::now())
        .await
        .unwrap();
    let locked = ctx
        .store
        .try_lock_job(&job.id, "other-worker", 1000)
        .await
        .unwrap();
    assert!(locked);
    let shutdown = Arc::new(AtomicBool::new(false));

    recover_orphaned_jobs(&mut ctx.store, &ctx.settings, &shutdown)
        .await
        .unwrap();

    assert!(!ctx.store.is_job_queued(&queue_name, &job.id).await.unwrap());
    let active = ctx.store.get_active_job_ids(&worker_id).await.unwrap();
    assert!(active.contains(&job.id));
}

#[tokio::test]
async fn recover_orphaned_jobs_skips_healthy_worker() {
    let mut ctx = RedisTestContext::new().await.unwrap();
    let queue_name = ctx.settings.default_queue_name.clone();
    let dlq_name = ctx.settings.default_dlq_name.clone();
    let mut job = build_job(&queue_name, &dlq_name, None);
    job.status = JobStatus::Active;
    ctx.store.save_job_definition(&job).await.unwrap();
    let worker_id = format!("worker-{}", Uuid::new_v4());
    ctx.store
        .track_active_job(&worker_id, &job.id, Utc::now())
        .await
        .unwrap();
    let mut health = serde_json::Map::new();
    health.insert("worker_id".to_string(), json!(worker_id));
    health.insert("status".to_string(), json!("running"));
    ctx.store
        .set_worker_health(&worker_id, &health, 60)
        .await
        .unwrap();
    let shutdown = Arc::new(AtomicBool::new(false));

    recover_orphaned_jobs(&mut ctx.store, &ctx.settings, &shutdown)
        .await
        .unwrap();

    assert!(!ctx.store.is_job_queued(&queue_name, &job.id).await.unwrap());
    let active = ctx.store.get_active_job_ids(&worker_id).await.unwrap();
    assert!(active.contains(&job.id));
}

#[tokio::test]
async fn recover_orphaned_jobs_respects_limit() {
    let mut ctx = RedisTestContext::new().await.unwrap();
    let queue_name = ctx.settings.default_queue_name.clone();
    let dlq_name = ctx.settings.default_dlq_name.clone();
    let worker_id = format!("worker-{}", Uuid::new_v4());
    let total_jobs = 101;

    for index in 0..total_jobs {
        let mut job = build_job(&queue_name, &dlq_name, None);
        job.id = format!("job-{index}");
        job.status = JobStatus::Active;
        job.next_scheduled_run_time = Some(Utc::now());
        ctx.store.save_job_definition(&job).await.unwrap();
        ctx.store
            .track_active_job(&worker_id, &job.id, Utc::now())
            .await
            .unwrap();
    }
    let shutdown = Arc::new(AtomicBool::new(false));

    recover_orphaned_jobs(&mut ctx.store, &ctx.settings, &shutdown)
        .await
        .unwrap();

    let queue_size = ctx.store.queue_size(&queue_name).await.unwrap();
    assert!(queue_size <= 100);
    let active = ctx.store.get_active_job_ids(&worker_id).await.unwrap();
    assert!(!active.is_empty());
}

#[test]
fn orphan_requeue_post_action_preserves_active_on_errors() {
    assert_eq!(
        orphan_requeue_post_action(0),
        OrphanRequeuePostAction::CleanupOnly
    );
    assert_eq!(
        orphan_requeue_post_action(1),
        OrphanRequeuePostAction::CleanupAndCount
    );
    assert_eq!(
        orphan_requeue_post_action(2),
        OrphanRequeuePostAction::CleanupAndCount
    );
    assert_eq!(
        orphan_requeue_post_action(-1),
        OrphanRequeuePostAction::PreserveActive
    );
    assert_eq!(
        orphan_requeue_post_action(3),
        OrphanRequeuePostAction::PreserveActive
    );
}

#[tokio::test]
async fn poll_for_jobs_distributes_across_queues() {
    let mut ctx = RedisTestContext::new().await.unwrap();
    ctx.settings.default_runner_name = "test".to_string();
    let runner = Arc::new(BlockingRunner::new());
    let gate = runner.gate.clone();
    let started = runner.started_queues.clone();
    let mut runners: HashMap<String, Arc<dyn Runner>> = HashMap::new();
    runners.insert("test".to_string(), runner);
    let mut client = RRQClient::new(ctx.settings.clone(), ctx.store.clone());
    let queue_a = "queue-a".to_string();
    let queue_b = "queue-b".to_string();

    for i in 0..2 {
        let options = EnqueueOptions {
            queue_name: Some(queue_a.clone()),
            job_id: Some(format!("qa-{i}")),
            ..Default::default()
        };
        let _ = client
            .enqueue("task", serde_json::Map::new(), options)
            .await
            .unwrap();
    }
    for i in 0..2 {
        let options = EnqueueOptions {
            queue_name: Some(queue_b.clone()),
            job_id: Some(format!("qb-{i}")),
            ..Default::default()
        };
        let _ = client
            .enqueue("task", serde_json::Map::new(), options)
            .await
            .unwrap();
    }

    let mut worker = RRQWorker::new(
        ctx.settings.clone(),
        Some(vec![queue_a.clone(), queue_b.clone()]),
        Some("worker-1".to_string()),
        runners,
        true,
        2,
    )
    .await
    .unwrap();

    let fetched = worker.poll_for_jobs(2).await.unwrap();
    assert_eq!(fetched, 2);

    async fn wait_for_started(started: &Arc<TokioMutex<Vec<String>>>) -> Result<Vec<String>> {
        for _ in 0..50 {
            let guard = started.lock().await;
            if guard.len() == 2 {
                return Ok(guard.clone());
            }
            drop(guard);
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        Err(anyhow::anyhow!("runner did not start expected jobs"))
    }

    let started_queues = wait_for_started(&started).await.unwrap();
    let mut unique = started_queues.iter().cloned().collect::<HashSet<_>>();
    assert!(unique.remove(&normalize_queue_name(&queue_a)));
    assert!(unique.remove(&normalize_queue_name(&queue_b)));

    gate.notify_waiters();
    timeout(Duration::from_secs(2), worker.drain_tasks())
        .await
        .unwrap()
        .unwrap();
}

#[tokio::test]
async fn drain_tasks_waits_for_runner_close_before_requeue_when_cancel_hints_disabled() {
    let mut ctx = RedisTestContext::new().await.unwrap();
    ctx.settings.default_runner_name = "test".to_string();
    ctx.settings.runner_enable_inflight_cancel_hints = false;
    ctx.settings.worker_shutdown_grace_period_seconds = 0.01;
    let runner = Arc::new(CloseAwareRunner::new());
    let mut runners: HashMap<String, Arc<dyn Runner>> = HashMap::new();
    runners.insert("test".to_string(), runner.clone());
    let mut client = RRQClient::new(ctx.settings.clone(), ctx.store.clone());
    let job = client
        .enqueue("task", serde_json::Map::new(), EnqueueOptions::default())
        .await
        .unwrap();
    let queue_name = normalize_queue_name(&ctx.settings.default_queue_name);

    let mut worker = RRQWorker::new(
        ctx.settings.clone(),
        None,
        Some("worker-close-order".to_string()),
        runners,
        true,
        1,
    )
    .await
    .unwrap();

    let fetched = worker.poll_for_jobs(1).await.unwrap();
    assert_eq!(fetched, 1);
    tokio::time::timeout(Duration::from_secs(1), runner.execute_started.notified())
        .await
        .expect("runner should start execution");

    let mut inspect_store = worker.job_store.clone();
    let mut drain = Box::pin(worker.drain_tasks());
    tokio::select! {
        _ = runner.close_started.notified() => {}
        result = &mut drain => {
            panic!("drain_tasks completed before runner close started: {result:?}");
        }
        _ = tokio::time::sleep(Duration::from_secs(1)) => {
            panic!("runner close was not invoked during forced drain");
        }
    }

    // Requeue must not happen until close() has completed.
    assert!(
        !inspect_store
            .is_job_queued(&queue_name, &job.id)
            .await
            .unwrap()
    );
    assert!(runner.close_called.load(Ordering::SeqCst));

    runner.close_gate.notify_waiters();
    tokio::time::timeout(Duration::from_secs(2), &mut drain)
        .await
        .expect("drain_tasks should complete after close unblocks")
        .unwrap();

    assert!(
        inspect_store
            .is_job_queued(&queue_name, &job.id)
            .await
            .unwrap()
    );
}

#[tokio::test]
async fn drain_tasks_aborts_inflight_before_runner_close_to_avoid_retry_or_dlq() {
    let mut ctx = RedisTestContext::new().await.unwrap();
    ctx.settings.default_runner_name = "test".to_string();
    ctx.settings.runner_enable_inflight_cancel_hints = false;
    ctx.settings.worker_shutdown_grace_period_seconds = 0.01;
    let runner = Arc::new(CloseInterruptsExecuteRunner::new());
    let mut runners: HashMap<String, Arc<dyn Runner>> = HashMap::new();
    runners.insert("test".to_string(), runner.clone());
    let mut client = RRQClient::new(ctx.settings.clone(), ctx.store.clone());
    let job = client
        .enqueue(
            "task",
            serde_json::Map::new(),
            EnqueueOptions {
                max_retries: Some(1),
                ..Default::default()
            },
        )
        .await
        .unwrap();
    let queue_name = normalize_queue_name(&ctx.settings.default_queue_name);
    let dlq_name = ctx.settings.default_dlq_name.clone();

    let mut worker = RRQWorker::new(
        ctx.settings.clone(),
        None,
        Some("worker-abort-before-close".to_string()),
        runners,
        true,
        1,
    )
    .await
    .unwrap();

    let fetched = worker.poll_for_jobs(1).await.unwrap();
    assert_eq!(fetched, 1);
    tokio::time::timeout(Duration::from_secs(1), runner.execute_started.notified())
        .await
        .expect("runner should start execution");

    let mut inspect_store = worker.job_store.clone();
    let mut drain = Box::pin(worker.drain_tasks());
    tokio::select! {
        _ = runner.close_started.notified() => {}
        result = &mut drain => {
            panic!("drain_tasks completed before runner close started: {result:?}");
        }
        _ = tokio::time::sleep(Duration::from_secs(1)) => {
            panic!("runner close was not invoked during forced drain");
        }
    }

    // While close() is blocked, in-flight execution should already be aborted and must not
    // consume retry budget or move job to DLQ.
    tokio::time::sleep(Duration::from_millis(50)).await;
    let loaded = inspect_store
        .get_job_definition(&job.id)
        .await
        .unwrap()
        .expect("job definition should exist");
    assert_eq!(loaded.current_retries, 0);
    assert!(
        !inspect_store
            .get_dlq_job_ids(&dlq_name)
            .await
            .unwrap()
            .iter()
            .any(|id| id == &job.id)
    );

    runner.close_gate.notify_waiters();
    tokio::time::timeout(Duration::from_secs(2), &mut drain)
        .await
        .expect("drain_tasks should complete after close unblocks")
        .unwrap();

    let loaded = inspect_store
        .get_job_definition(&job.id)
        .await
        .unwrap()
        .expect("job definition should exist after drain");
    assert_eq!(loaded.current_retries, 0);
    assert!(
        inspect_store
            .is_job_queued(&queue_name, &job.id)
            .await
            .unwrap()
    );
    assert!(
        !inspect_store
            .get_dlq_job_ids(&dlq_name)
            .await
            .unwrap()
            .iter()
            .any(|id| id == &job.id)
    );
}

#[tokio::test]
async fn release_claimed_job_requeues_and_marks_pending() {
    let mut ctx = RedisTestContext::new().await.unwrap();
    ctx.settings.default_runner_name = "test".to_string();
    let runner = Arc::new(StaticRunner::new(
        TestOutcome::Success(json!({"ok": true})),
        Duration::from_millis(0),
    ));
    let mut runners: HashMap<String, Arc<dyn Runner>> = HashMap::new();
    runners.insert("test".to_string(), runner);
    let mut client = RRQClient::new(ctx.settings.clone(), ctx.store.clone());
    let job = client
        .enqueue("task", serde_json::Map::new(), EnqueueOptions::default())
        .await
        .unwrap();

    let mut worker = RRQWorker::new(
        ctx.settings.clone(),
        None,
        Some("worker-1".to_string()),
        runners,
        true,
        1,
    )
    .await
    .unwrap();
    let queue_name = normalize_queue_name(&ctx.settings.default_queue_name);
    let claimed = worker
        .job_store
        .atomic_claim_ready_jobs(&queue_name, &worker.worker_id, 10_000, 0, 1, Utc::now())
        .await
        .unwrap();
    assert_eq!(claimed, vec![job.id.clone()]);
    let claimed_job = worker
        .job_store
        .get_job_definition(&job.id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(claimed_job.status, JobStatus::Active);

    worker
        .release_claimed_job(
            &queue_name,
            &job.id,
            Some(&claimed_job),
            Some("Failed to refresh claimed job lock before dispatch. Re-queued."),
        )
        .await;

    let reloaded = worker
        .job_store
        .get_job_definition(&job.id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(reloaded.status, JobStatus::Pending);
    assert!(
        worker
            .job_store
            .is_job_queued(&queue_name, &job.id)
            .await
            .unwrap()
    );
    assert_eq!(
        worker.job_store.get_job_lock_owner(&job.id).await.unwrap(),
        None
    );
    let active = worker
        .job_store
        .get_active_job_ids(&worker.worker_id)
        .await
        .unwrap();
    assert!(!active.contains(&job.id));
}

#[tokio::test]
async fn release_claimed_job_does_not_clear_other_worker_lock() {
    let mut ctx = RedisTestContext::new().await.unwrap();
    ctx.settings.default_runner_name = "test".to_string();
    let runner = Arc::new(StaticRunner::new(
        TestOutcome::Success(json!({"ok": true})),
        Duration::from_millis(0),
    ));
    let mut runners: HashMap<String, Arc<dyn Runner>> = HashMap::new();
    runners.insert("test".to_string(), runner);
    let mut client = RRQClient::new(ctx.settings.clone(), ctx.store.clone());
    let job = client
        .enqueue("task", serde_json::Map::new(), EnqueueOptions::default())
        .await
        .unwrap();

    let mut worker = RRQWorker::new(
        ctx.settings.clone(),
        None,
        Some("worker-1".to_string()),
        runners,
        true,
        1,
    )
    .await
    .unwrap();
    let queue_name = normalize_queue_name(&ctx.settings.default_queue_name);
    let claimed = worker
        .job_store
        .atomic_claim_ready_jobs(&queue_name, &worker.worker_id, 10_000, 0, 1, Utc::now())
        .await
        .unwrap();
    assert_eq!(claimed, vec![job.id.clone()]);
    let claimed_job = worker
        .job_store
        .get_job_definition(&job.id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(claimed_job.status, JobStatus::Active);

    let _ = worker.job_store.release_job_lock(&job.id).await;
    let relocked = worker
        .job_store
        .try_lock_job(&job.id, "worker-2", 10_000)
        .await
        .unwrap();
    assert!(relocked);

    worker
        .release_claimed_job(
            &queue_name,
            &job.id,
            Some(&claimed_job),
            Some("Failed to refresh claimed job lock before dispatch. Re-queued."),
        )
        .await;

    assert_eq!(
        worker.job_store.get_job_lock_owner(&job.id).await.unwrap(),
        Some("worker-2".to_string())
    );
    assert!(
        !worker
            .job_store
            .is_job_queued(&queue_name, &job.id)
            .await
            .unwrap()
    );
    let reloaded = worker
        .job_store
        .get_job_definition(&job.id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(reloaded.status, JobStatus::Active);
    let active = worker
        .job_store
        .get_active_job_ids(&worker.worker_id)
        .await
        .unwrap();
    assert!(!active.contains(&job.id));
}

#[tokio::test]
async fn release_claimed_job_lock_owner_lookup_error_requeues_and_cleans_up_claim_state() {
    let mut ctx = RedisTestContext::new().await.unwrap();
    ctx.settings.default_runner_name = "test".to_string();
    let runner = Arc::new(StaticRunner::new(
        TestOutcome::Success(json!({"ok": true})),
        Duration::from_millis(0),
    ));
    let mut runners: HashMap<String, Arc<dyn Runner>> = HashMap::new();
    runners.insert("test".to_string(), runner);
    let mut client = RRQClient::new(ctx.settings.clone(), ctx.store.clone());
    let job = client
        .enqueue("task", serde_json::Map::new(), EnqueueOptions::default())
        .await
        .unwrap();

    let mut worker = RRQWorker::new(
        ctx.settings.clone(),
        None,
        Some("worker-1".to_string()),
        runners,
        true,
        1,
    )
    .await
    .unwrap();
    let queue_name = normalize_queue_name(&ctx.settings.default_queue_name);
    let claimed = worker
        .job_store
        .atomic_claim_ready_jobs(&queue_name, &worker.worker_id, 10_000, 0, 1, Utc::now())
        .await
        .unwrap();
    assert_eq!(claimed, vec![job.id.clone()]);
    let claimed_job = worker
        .job_store
        .get_job_definition(&job.id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(claimed_job.status, JobStatus::Active);

    let _ = worker.job_store.release_job_lock(&job.id).await;
    let redis = redis::Client::open(ctx.settings.redis_dsn.as_str()).unwrap();
    let mut conn = redis.get_multiplexed_async_connection().await.unwrap();
    let lock_key = format!("{LOCK_KEY_PREFIX}{}", job.id);
    conn.lpush::<_, _, ()>(&lock_key, "other-worker")
        .await
        .unwrap();
    assert!(worker.job_store.get_job_lock_owner(&job.id).await.is_err());

    worker
        .release_claimed_job(
            &queue_name,
            &job.id,
            Some(&claimed_job),
            Some("Failed to refresh claimed job lock before dispatch. Re-queued."),
        )
        .await;

    let reloaded = worker
        .job_store
        .get_job_definition(&job.id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        reloaded.status,
        JobStatus::Pending,
        "job should be marked pending by fallback release when lock ownership is unknown"
    );
    assert!(
        worker
            .job_store
            .is_job_queued(&queue_name, &job.id)
            .await
            .unwrap(),
        "job should be requeued by fallback release when lock-owner lookup fails"
    );
    let active = worker
        .job_store
        .get_active_job_ids(&worker.worker_id)
        .await
        .unwrap();
    assert!(
        !active.contains(&job.id),
        "active tracking should be cleaned up when ownership lookup fails"
    );
    assert_eq!(
        worker.job_store.get_job_lock_owner(&job.id).await.unwrap(),
        None
    );

    let reclaimed = worker
        .job_store
        .atomic_claim_ready_jobs(&queue_name, &worker.worker_id, 10_000, 0, 1, Utc::now())
        .await
        .unwrap();
    assert_eq!(reclaimed, vec![job.id.clone()]);

    let _ = worker
        .job_store
        .remove_active_job(&worker.worker_id, &job.id)
        .await;
    let _ = worker.job_store.release_job_lock(&job.id).await;
}

#[tokio::test]
async fn release_claimed_job_without_definition_requeues_and_releases_lock() {
    let mut ctx = RedisTestContext::new().await.unwrap();
    ctx.settings.default_runner_name = "test".to_string();
    let runner = Arc::new(StaticRunner::new(
        TestOutcome::Success(json!({"ok": true})),
        Duration::from_millis(0),
    ));
    let mut runners: HashMap<String, Arc<dyn Runner>> = HashMap::new();
    runners.insert("test".to_string(), runner);
    let mut client = RRQClient::new(ctx.settings.clone(), ctx.store.clone());
    let job = client
        .enqueue("task", serde_json::Map::new(), EnqueueOptions::default())
        .await
        .unwrap();

    let mut worker = RRQWorker::new(
        ctx.settings.clone(),
        None,
        Some("worker-1".to_string()),
        runners,
        true,
        1,
    )
    .await
    .unwrap();
    let queue_name = normalize_queue_name(&ctx.settings.default_queue_name);
    let claimed = worker
        .job_store
        .atomic_claim_ready_jobs(&queue_name, &worker.worker_id, 10_000, 0, 1, Utc::now())
        .await
        .unwrap();
    assert_eq!(claimed, vec![job.id.clone()]);
    assert!(
        !worker
            .job_store
            .is_job_queued(&queue_name, &job.id)
            .await
            .unwrap()
    );

    worker
        .release_claimed_job_without_definition(
            &queue_name,
            &job.id,
            Some("Failed to parse claimed job definition. Re-queued."),
        )
        .await;

    let reloaded = worker
        .job_store
        .get_job_definition(&job.id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(reloaded.status, JobStatus::Pending);

    assert!(
        worker
            .job_store
            .is_job_queued(&queue_name, &job.id)
            .await
            .unwrap()
    );
    assert_eq!(
        worker.job_store.get_job_lock_owner(&job.id).await.unwrap(),
        None
    );
    let active = worker
        .job_store
        .get_active_job_ids(&worker.worker_id)
        .await
        .unwrap();
    assert!(!active.contains(&job.id));
}

#[tokio::test]
async fn release_claimed_job_without_definition_requeue_write_failure_preserves_claim_state() {
    let mut ctx = RedisTestContext::new().await.unwrap();
    ctx.settings.default_runner_name = "test".to_string();
    let runner = Arc::new(StaticRunner::new(
        TestOutcome::Success(json!({"ok": true})),
        Duration::from_millis(0),
    ));
    let mut runners: HashMap<String, Arc<dyn Runner>> = HashMap::new();
    runners.insert("test".to_string(), runner);
    let mut client = RRQClient::new(ctx.settings.clone(), ctx.store.clone());
    let job = client
        .enqueue("task", serde_json::Map::new(), EnqueueOptions::default())
        .await
        .unwrap();

    let mut worker = RRQWorker::new(
        ctx.settings.clone(),
        None,
        Some("worker-1".to_string()),
        runners,
        true,
        1,
    )
    .await
    .unwrap();
    let queue_name = normalize_queue_name(&ctx.settings.default_queue_name);
    let claimed = worker
        .job_store
        .atomic_claim_ready_jobs(&queue_name, &worker.worker_id, 10_000, 0, 1, Utc::now())
        .await
        .unwrap();
    assert_eq!(claimed, vec![job.id.clone()]);

    let redis = redis::Client::open(ctx.settings.redis_dsn.as_str()).unwrap();
    let mut conn = redis.get_multiplexed_async_connection().await.unwrap();
    let queue_key = queue_name.clone();
    conn.set::<_, _, ()>(&queue_key, "wrongtype").await.unwrap();

    worker
        .release_claimed_job_without_definition(
            &queue_name,
            &job.id,
            Some("Failed to parse claimed job definition. Re-queued."),
        )
        .await;

    let reloaded = worker
        .job_store
        .get_job_definition(&job.id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(reloaded.status, JobStatus::Pending);
    let active = worker
        .job_store
        .get_active_job_ids(&worker.worker_id)
        .await
        .unwrap();
    assert!(
        active.contains(&job.id),
        "claim state should be preserved when requeue writes fail"
    );
    assert_eq!(
        worker.job_store.get_job_lock_owner(&job.id).await.unwrap(),
        Some(worker.worker_id.clone())
    );

    conn.del::<_, ()>(&queue_key).await.unwrap();

    worker
        .release_claimed_job_without_definition(&queue_name, &job.id, Some("cleanup"))
        .await;

    assert!(
        worker
            .job_store
            .is_job_queued(&queue_name, &job.id)
            .await
            .unwrap()
    );
    let active_after = worker
        .job_store
        .get_active_job_ids(&worker.worker_id)
        .await
        .unwrap();
    assert!(!active_after.contains(&job.id));
    assert_eq!(
        worker.job_store.get_job_lock_owner(&job.id).await.unwrap(),
        None
    );
}

#[tokio::test]
async fn release_claimed_job_without_definition_lookup_error_preserves_claim_state() {
    let mut ctx = RedisTestContext::new().await.unwrap();
    ctx.settings.default_runner_name = "test".to_string();
    let runner = Arc::new(StaticRunner::new(
        TestOutcome::Success(json!({"ok": true})),
        Duration::from_millis(0),
    ));
    let mut runners: HashMap<String, Arc<dyn Runner>> = HashMap::new();
    runners.insert("test".to_string(), runner);
    let mut client = RRQClient::new(ctx.settings.clone(), ctx.store.clone());
    let job = client
        .enqueue("task", serde_json::Map::new(), EnqueueOptions::default())
        .await
        .unwrap();

    let mut worker = RRQWorker::new(
        ctx.settings.clone(),
        None,
        Some("worker-1".to_string()),
        runners,
        true,
        1,
    )
    .await
    .unwrap();
    let queue_name = normalize_queue_name(&ctx.settings.default_queue_name);
    let claimed = worker
        .job_store
        .atomic_claim_ready_jobs(&queue_name, &worker.worker_id, 10_000, 0, 1, Utc::now())
        .await
        .unwrap();
    assert_eq!(claimed, vec![job.id.clone()]);

    let _ = worker.job_store.release_job_lock(&job.id).await;
    let redis = redis::Client::open(ctx.settings.redis_dsn.as_str()).unwrap();
    let mut conn = redis.get_multiplexed_async_connection().await.unwrap();
    let lock_key = format!("{LOCK_KEY_PREFIX}{}", job.id);
    conn.lpush::<_, _, ()>(&lock_key, "other-worker")
        .await
        .unwrap();
    assert!(worker.job_store.get_job_lock_owner(&job.id).await.is_err());

    worker
        .release_claimed_job_without_definition(
            &queue_name,
            &job.id,
            Some("Failed to parse claimed job definition. Re-queued."),
        )
        .await;

    let reloaded = worker
        .job_store
        .get_job_definition(&job.id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        reloaded.status,
        JobStatus::Active,
        "job should not be marked pending when lock ownership is uncertain"
    );
    assert!(
        !worker
            .job_store
            .is_job_queued(&queue_name, &job.id)
            .await
            .unwrap(),
        "job should not be requeued when lock-owner lookup fails"
    );
    let active = worker
        .job_store
        .get_active_job_ids(&worker.worker_id)
        .await
        .unwrap();
    assert!(
        active.contains(&job.id),
        "active tracking should remain unchanged when ownership lookup fails"
    );
}

#[tokio::test]
async fn quarantine_claimed_job_without_definition_preserves_claim_when_dlq_and_requeue_fail() {
    let mut ctx = RedisTestContext::new().await.unwrap();
    ctx.settings.default_runner_name = "test".to_string();
    let runner = Arc::new(StaticRunner::new(
        TestOutcome::Success(json!({"ok": true})),
        Duration::from_millis(0),
    ));
    let mut runners: HashMap<String, Arc<dyn Runner>> = HashMap::new();
    runners.insert("test".to_string(), runner);
    let mut client = RRQClient::new(ctx.settings.clone(), ctx.store.clone());
    let job = client
        .enqueue("task", serde_json::Map::new(), EnqueueOptions::default())
        .await
        .unwrap();

    let mut worker = RRQWorker::new(
        ctx.settings.clone(),
        None,
        Some("worker-1".to_string()),
        runners,
        true,
        1,
    )
    .await
    .unwrap();
    let queue_name = normalize_queue_name(&ctx.settings.default_queue_name);
    let dlq_name = ctx.settings.default_dlq_name.clone();
    let claimed = worker
        .job_store
        .atomic_claim_ready_jobs(&queue_name, &worker.worker_id, 10_000, 0, 1, Utc::now())
        .await
        .unwrap();
    assert_eq!(claimed, vec![job.id.clone()]);

    let redis = redis::Client::open(ctx.settings.redis_dsn.as_str()).unwrap();
    let mut conn = redis.get_multiplexed_async_connection().await.unwrap();
    let queue_key = queue_name.clone();
    let dlq_key = if dlq_name.starts_with(DLQ_KEY_PREFIX) {
        dlq_name.clone()
    } else {
        format!("{DLQ_KEY_PREFIX}{dlq_name}")
    };
    conn.set::<_, _, ()>(&queue_key, "wrongtype").await.unwrap();
    conn.set::<_, _, ()>(&dlq_key, "wrongtype").await.unwrap();

    worker
        .quarantine_claimed_job_without_definition(
            &queue_name,
            &job.id,
            "Failed to parse claimed job definition. Moved to DLQ.",
        )
        .await;

    let active = worker
        .job_store
        .get_active_job_ids(&worker.worker_id)
        .await
        .unwrap();
    assert!(
        active.contains(&job.id),
        "claim state should be preserved when DLQ persistence and fallback requeue both fail"
    );
    assert_eq!(
        worker.job_store.get_job_lock_owner(&job.id).await.unwrap(),
        Some(worker.worker_id.clone())
    );

    conn.del::<_, ()>(&queue_key).await.unwrap();
    conn.del::<_, ()>(&dlq_key).await.unwrap();

    worker
        .quarantine_claimed_job_without_definition(
            &queue_name,
            &job.id,
            "Failed to parse claimed job definition. Moved to DLQ.",
        )
        .await;

    let active_after = worker
        .job_store
        .get_active_job_ids(&worker.worker_id)
        .await
        .unwrap();
    assert!(!active_after.contains(&job.id));
    assert_eq!(
        worker.job_store.get_job_lock_owner(&job.id).await.unwrap(),
        None
    );
    let dlq_ids = worker.job_store.get_dlq_job_ids(&dlq_name).await.unwrap();
    assert!(dlq_ids.iter().any(|id| id == &job.id));
}

#[tokio::test]
async fn release_remaining_claimed_jobs_moves_none_job_defs_to_dlq() {
    let mut ctx = RedisTestContext::new().await.unwrap();
    ctx.settings.default_runner_name = "test".to_string();
    let runner = Arc::new(StaticRunner::new(
        TestOutcome::Success(json!({"ok": true})),
        Duration::from_millis(0),
    ));
    let mut runners: HashMap<String, Arc<dyn Runner>> = HashMap::new();
    runners.insert("test".to_string(), runner);
    let mut client = RRQClient::new(ctx.settings.clone(), ctx.store.clone());
    let job = client
        .enqueue("task", serde_json::Map::new(), EnqueueOptions::default())
        .await
        .unwrap();

    let mut worker = RRQWorker::new(
        ctx.settings.clone(),
        None,
        Some("worker-1".to_string()),
        runners,
        true,
        1,
    )
    .await
    .unwrap();
    let queue_name = normalize_queue_name(&ctx.settings.default_queue_name);
    let dlq_name = ctx.settings.default_dlq_name.clone();
    let claimed = worker
        .job_store
        .atomic_claim_ready_jobs(&queue_name, &worker.worker_id, 10_000, 0, 1, Utc::now())
        .await
        .unwrap();
    assert_eq!(claimed, vec![job.id.clone()]);
    assert!(
        !worker
            .job_store
            .is_job_queued(&queue_name, &job.id)
            .await
            .unwrap()
    );

    worker
        .release_remaining_claimed_jobs(
            &queue_name,
            &claimed,
            &[None],
            Some("Job execution interrupted by worker shutdown. Re-queued."),
        )
        .await;

    let reloaded = worker
        .job_store
        .get_job_definition(&job.id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(reloaded.status, JobStatus::Failed);
    assert!(
        !worker
            .job_store
            .is_job_queued(&queue_name, &job.id)
            .await
            .unwrap()
    );
    let dlq_ids = worker.job_store.get_dlq_job_ids(&dlq_name).await.unwrap();
    assert!(dlq_ids.iter().any(|id| id == &job.id));
    assert_eq!(
        worker.job_store.get_job_lock_owner(&job.id).await.unwrap(),
        None
    );
    let active = worker
        .job_store
        .get_active_job_ids(&worker.worker_id)
        .await
        .unwrap();
    assert!(!active.contains(&job.id));
}

#[tokio::test]
async fn poll_for_jobs_moves_claimed_job_missing_enqueue_time_to_dlq() {
    let mut ctx = RedisTestContext::new().await.unwrap();
    ctx.settings.default_runner_name = "test".to_string();
    let runner = Arc::new(StaticRunner::new(
        TestOutcome::Success(json!({"ok": true})),
        Duration::from_millis(0),
    ));
    let mut runners: HashMap<String, Arc<dyn Runner>> = HashMap::new();
    runners.insert("test".to_string(), runner);
    let mut client = RRQClient::new(ctx.settings.clone(), ctx.store.clone());
    let job = client
        .enqueue("task", serde_json::Map::new(), EnqueueOptions::default())
        .await
        .unwrap();

    let mut worker = RRQWorker::new(
        ctx.settings.clone(),
        None,
        Some("worker-1".to_string()),
        runners,
        true,
        1,
    )
    .await
    .unwrap();
    let queue_name = normalize_queue_name(&ctx.settings.default_queue_name);
    let dlq_name = ctx.settings.default_dlq_name.clone();

    let redis = redis::Client::open(ctx.settings.redis_dsn.as_str()).unwrap();
    let mut conn = redis.get_multiplexed_async_connection().await.unwrap();
    let job_key = format!("{JOB_KEY_PREFIX}{}", job.id);
    let removed: i64 = conn.hdel(&job_key, "enqueue_time").await.unwrap();
    assert_eq!(removed, 1);

    let fetched = worker.poll_for_jobs(1).await.unwrap();
    assert_eq!(fetched, 0);
    assert!(
        !worker
            .job_store
            .is_job_queued(&queue_name, &job.id)
            .await
            .unwrap()
    );
    let fetched_again = worker.poll_for_jobs(1).await.unwrap();
    assert_eq!(fetched_again, 0);
    let dlq_ids = worker.job_store.get_dlq_job_ids(&dlq_name).await.unwrap();
    assert!(dlq_ids.iter().any(|id| id == &job.id));
    assert_eq!(
        worker.job_store.get_job_lock_owner(&job.id).await.unwrap(),
        None
    );
    let active = worker
        .job_store
        .get_active_job_ids(&worker.worker_id)
        .await
        .unwrap();
    assert!(!active.contains(&job.id));

    let status: String = conn.hget(&job_key, "status").await.unwrap();
    assert_eq!(status, JobStatus::Failed.as_str());
    let last_error: String = conn.hget(&job_key, "last_error").await.unwrap();
    assert_eq!(
        last_error,
        "Failed to parse claimed job definition. Moved to DLQ."
    );
}

#[tokio::test]
async fn poll_for_jobs_moves_malformed_claimed_job_definition_to_dlq() {
    let mut ctx = RedisTestContext::new().await.unwrap();
    ctx.settings.default_runner_name = "test".to_string();
    let runner = Arc::new(StaticRunner::new(
        TestOutcome::Success(json!({"ok": true})),
        Duration::from_millis(0),
    ));
    let mut runners: HashMap<String, Arc<dyn Runner>> = HashMap::new();
    runners.insert("test".to_string(), runner);
    let mut client = RRQClient::new(ctx.settings.clone(), ctx.store.clone());
    let job = client
        .enqueue("task", serde_json::Map::new(), EnqueueOptions::default())
        .await
        .unwrap();

    let mut worker = RRQWorker::new(
        ctx.settings.clone(),
        None,
        Some("worker-1".to_string()),
        runners,
        true,
        1,
    )
    .await
    .unwrap();
    let queue_name = normalize_queue_name(&ctx.settings.default_queue_name);
    let dlq_name = ctx.settings.default_dlq_name.clone();

    let redis = redis::Client::open(ctx.settings.redis_dsn.as_str()).unwrap();
    let mut conn = redis.get_multiplexed_async_connection().await.unwrap();
    let job_key = format!("{JOB_KEY_PREFIX}{}", job.id);
    conn.hset::<_, _, _, ()>(&job_key, "enqueue_time", "not-a-timestamp")
        .await
        .unwrap();

    let fetched = worker.poll_for_jobs(1).await.unwrap();
    assert_eq!(fetched, 0);
    assert!(
        !worker
            .job_store
            .is_job_queued(&queue_name, &job.id)
            .await
            .unwrap()
    );
    let fetched_again = worker.poll_for_jobs(1).await.unwrap();
    assert_eq!(fetched_again, 0);
    let dlq_ids = worker.job_store.get_dlq_job_ids(&dlq_name).await.unwrap();
    assert!(dlq_ids.iter().any(|id| id == &job.id));
    assert_eq!(
        worker.job_store.get_job_lock_owner(&job.id).await.unwrap(),
        None
    );
    let active = worker
        .job_store
        .get_active_job_ids(&worker.worker_id)
        .await
        .unwrap();
    assert!(!active.contains(&job.id));

    let status: String = conn.hget(&job_key, "status").await.unwrap();
    assert_eq!(status, JobStatus::Failed.as_str());
    let last_error: String = conn.hget(&job_key, "last_error").await.unwrap();
    assert_eq!(
        last_error,
        "Failed to parse claimed job definition. Moved to DLQ."
    );
}

#[tokio::test]
async fn poll_for_jobs_malformed_claimed_job_definition_releases_unique_lock() {
    let mut ctx = RedisTestContext::new().await.unwrap();
    ctx.settings.default_runner_name = "test".to_string();
    let runner = Arc::new(StaticRunner::new(
        TestOutcome::Success(json!({"ok": true})),
        Duration::from_millis(0),
    ));
    let mut runners: HashMap<String, Arc<dyn Runner>> = HashMap::new();
    runners.insert("test".to_string(), runner);
    let mut client = RRQClient::new(ctx.settings.clone(), ctx.store.clone());
    let unique_key = format!("malformed-unique-{}", Uuid::new_v4());
    let job = client
        .enqueue(
            "task",
            serde_json::Map::new(),
            EnqueueOptions {
                unique_key: Some(unique_key.clone()),
                ..EnqueueOptions::default()
            },
        )
        .await
        .unwrap();

    let mut worker = RRQWorker::new(
        ctx.settings.clone(),
        None,
        Some("worker-1".to_string()),
        runners,
        true,
        1,
    )
    .await
    .unwrap();
    let queue_name = normalize_queue_name(&ctx.settings.default_queue_name);
    let dlq_name = ctx.settings.default_dlq_name.clone();
    assert!(worker.job_store.get_lock_ttl(&unique_key).await.unwrap() > 0);

    let redis = redis::Client::open(ctx.settings.redis_dsn.as_str()).unwrap();
    let mut conn = redis.get_multiplexed_async_connection().await.unwrap();
    let job_key = format!("{JOB_KEY_PREFIX}{}", job.id);
    conn.hset::<_, _, _, ()>(&job_key, "enqueue_time", "not-a-timestamp")
        .await
        .unwrap();

    let fetched = worker.poll_for_jobs(1).await.unwrap();
    assert_eq!(fetched, 0);
    assert_eq!(worker.job_store.get_lock_ttl(&unique_key).await.unwrap(), 0);
    assert!(
        !worker
            .job_store
            .is_job_queued(&queue_name, &job.id)
            .await
            .unwrap()
    );
    let dlq_ids = worker.job_store.get_dlq_job_ids(&dlq_name).await.unwrap();
    assert!(dlq_ids.iter().any(|id| id == &job.id));
}

#[tokio::test]
async fn poll_for_jobs_requeues_claimed_jobs_on_lock_timeout_overflow() {
    let mut ctx = RedisTestContext::new().await.unwrap();
    ctx.settings.default_runner_name = "test".to_string();
    let runner = Arc::new(StaticRunner::new(
        TestOutcome::Success(json!({"ok": true})),
        Duration::from_millis(0),
    ));
    let mut runners: HashMap<String, Arc<dyn Runner>> = HashMap::new();
    runners.insert("test".to_string(), runner);
    let mut worker = RRQWorker::new(
        ctx.settings.clone(),
        None,
        Some("worker-1".to_string()),
        runners,
        true,
        1,
    )
    .await
    .unwrap();
    let queue_name = ctx.settings.default_queue_name.clone();
    let normalized_queue_name = normalize_queue_name(&queue_name);
    let dlq_name = ctx.settings.default_dlq_name.clone();
    let mut job = build_job(&queue_name, &dlq_name, None);
    job.job_timeout_seconds = Some(i64::MAX);
    ctx.store.save_job_definition(&job).await.unwrap();
    ctx.store
        .add_job_to_queue(&queue_name, &job.id, Utc::now().timestamp_millis() as f64)
        .await
        .unwrap();

    let err = worker.poll_for_jobs(1).await.unwrap_err();
    assert!(err.to_string().contains("lock_timeout_ms overflow"));

    let reloaded = worker
        .job_store
        .get_job_definition(&job.id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(reloaded.status, JobStatus::Pending);
    assert!(
        worker
            .job_store
            .is_job_queued(&normalized_queue_name, &job.id)
            .await
            .unwrap()
    );
    assert_eq!(
        worker.job_store.get_job_lock_owner(&job.id).await.unwrap(),
        None
    );
    let active = worker
        .job_store
        .get_active_job_ids(&worker.worker_id)
        .await
        .unwrap();
    assert!(!active.contains(&job.id));
}

#[tokio::test]
async fn poll_for_jobs_requeues_claimed_jobs_when_permit_acquire_fails() {
    let mut ctx = RedisTestContext::new().await.unwrap();
    ctx.settings.default_runner_name = "test".to_string();
    let runner = Arc::new(StaticRunner::new(
        TestOutcome::Success(json!({"ok": true})),
        Duration::from_millis(0),
    ));
    let mut runners: HashMap<String, Arc<dyn Runner>> = HashMap::new();
    runners.insert("test".to_string(), runner);
    let mut client = RRQClient::new(ctx.settings.clone(), ctx.store.clone());
    let job = client
        .enqueue("task", serde_json::Map::new(), EnqueueOptions::default())
        .await
        .unwrap();
    let mut worker = RRQWorker::new(
        ctx.settings.clone(),
        None,
        Some("worker-1".to_string()),
        runners,
        true,
        1,
    )
    .await
    .unwrap();
    let queue_name = normalize_queue_name(&ctx.settings.default_queue_name);
    worker.semaphore.close();

    let err = worker.poll_for_jobs(1).await.unwrap_err();
    assert!(
        err.to_string()
            .contains("Worker concurrency permit acquisition failed before dispatch")
            || err.to_string().contains("semaphore")
    );

    let reloaded = worker
        .job_store
        .get_job_definition(&job.id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(reloaded.status, JobStatus::Pending);
    assert_eq!(
        reloaded.last_error.as_deref(),
        Some("Worker concurrency permit acquisition failed before dispatch. Re-queued.")
    );
    assert!(
        worker
            .job_store
            .is_job_queued(&queue_name, &job.id)
            .await
            .unwrap()
    );
    assert_eq!(
        worker.job_store.get_job_lock_owner(&job.id).await.unwrap(),
        None
    );
    let active = worker
        .job_store
        .get_active_job_ids(&worker.worker_id)
        .await
        .unwrap();
    assert!(!active.contains(&job.id));
}

#[tokio::test]
async fn calculate_jittered_delay_handles_non_positive_base() {
    let mut ctx = RedisTestContext::new().await.unwrap();
    ctx.settings.default_runner_name = "test".to_string();
    let runner = Arc::new(StaticRunner::new(
        TestOutcome::Success(json!({"ok": true})),
        Duration::from_millis(0),
    ));
    let mut runners: HashMap<String, Arc<dyn Runner>> = HashMap::new();
    runners.insert("test".to_string(), runner);
    let worker = RRQWorker::new(
        ctx.settings.clone(),
        None,
        Some("worker-1".to_string()),
        runners,
        true,
        1,
    )
    .await
    .unwrap();

    assert!(worker.calculate_jittered_delay(0.0, 0.5).is_zero());
    assert!(worker.calculate_jittered_delay(-1.0, 0.5).is_zero());
}

#[tokio::test]
async fn calculate_backoff_respects_max_delay() {
    let settings = RRQSettings {
        base_retry_delay_seconds: 2.0,
        max_retry_delay_seconds: 5.0,
        ..Default::default()
    };
    assert_eq!(calculate_backoff_seconds(&settings, 1), 2.0);
    assert_eq!(calculate_backoff_seconds(&settings, 2), 4.0);
    assert_eq!(calculate_backoff_seconds(&settings, 3), 5.0);
}

#[tokio::test]
async fn worker_new_allows_missing_default_runner_for_fully_routed_selected_queues() {
    let mut ctx = RedisTestContext::new().await.unwrap();
    ctx.settings.default_runner_name = "agent_main".to_string();
    let aux_queue = normalize_queue_name("rrq:queue:agent:aux");
    ctx.settings
        .runner_routes
        .insert(aux_queue.clone(), "agent_aux".to_string());

    let runner = Arc::new(StaticRunner::new(
        TestOutcome::Success(json!({"ok": true})),
        Duration::from_millis(0),
    ));
    let mut runners: HashMap<String, Arc<dyn Runner>> = HashMap::new();
    runners.insert("agent_aux".to_string(), runner);

    let worker = RRQWorker::new(
        ctx.settings.clone(),
        Some(vec![aux_queue.clone()]),
        Some("worker-1".to_string()),
        runners,
        true,
        1,
    )
    .await
    .unwrap();

    assert_eq!(worker.queues, vec![aux_queue]);
}

#[tokio::test]
async fn worker_new_rejects_missing_default_runner_for_unrouted_selected_queues() {
    let mut ctx = RedisTestContext::new().await.unwrap();
    ctx.settings.default_runner_name = "agent_main".to_string();
    let aux_queue = normalize_queue_name("rrq:queue:agent:aux");

    let runner = Arc::new(StaticRunner::new(
        TestOutcome::Success(json!({"ok": true})),
        Duration::from_millis(0),
    ));
    let mut runners: HashMap<String, Arc<dyn Runner>> = HashMap::new();
    runners.insert("agent_aux".to_string(), runner);

    let err = match RRQWorker::new(
        ctx.settings.clone(),
        Some(vec![aux_queue]),
        Some("worker-1".to_string()),
        runners,
        true,
        1,
    )
    .await
    {
        Ok(_) => panic!("worker creation should fail when default runner is required"),
        Err(err) => err,
    };

    assert!(
        err.to_string()
            .contains("default runner 'agent_main' is not configured")
    );
}

#[tokio::test]
async fn worker_new_reports_missing_default_runner_before_empty_runner_map() {
    let mut ctx = RedisTestContext::new().await.unwrap();
    ctx.settings.default_runner_name = "agent_main".to_string();
    let aux_queue = normalize_queue_name("rrq:queue:agent:aux");

    let runners: HashMap<String, Arc<dyn Runner>> = HashMap::new();

    let err = match RRQWorker::new(
        ctx.settings.clone(),
        Some(vec![aux_queue]),
        Some("worker-1".to_string()),
        runners,
        true,
        1,
    )
    .await
    {
        Ok(_) => panic!("worker creation should fail when default runner is required"),
        Err(err) => err,
    };

    assert!(
        err.to_string()
            .contains("default runner 'agent_main' is not configured")
    );
}

#[tokio::test]
async fn worker_new_rejects_missing_routed_runner_when_all_selected_queues_are_routed() {
    let mut ctx = RedisTestContext::new().await.unwrap();
    ctx.settings.default_runner_name = "agent_main".to_string();
    let aux_queue = normalize_queue_name("rrq:queue:agent:aux");
    let mail_queue = normalize_queue_name("rrq:queue:agent:mail");
    ctx.settings
        .runner_routes
        .insert(aux_queue.clone(), "agent_aux".to_string());
    ctx.settings
        .runner_routes
        .insert(mail_queue.clone(), "agent_mail_typo".to_string());

    let runner = Arc::new(StaticRunner::new(
        TestOutcome::Success(json!({"ok": true})),
        Duration::from_millis(0),
    ));
    let mut runners: HashMap<String, Arc<dyn Runner>> = HashMap::new();
    runners.insert("agent_aux".to_string(), runner);

    let err = match RRQWorker::new(
        ctx.settings.clone(),
        Some(vec![aux_queue, mail_queue.clone()]),
        Some("worker-1".to_string()),
        runners,
        true,
        1,
    )
    .await
    {
        Ok(_) => panic!("worker creation should fail for missing routed runner"),
        Err(err) => err,
    };

    let message = err.to_string();
    assert!(message.contains(mail_queue.as_str()));
    assert!(message.contains("agent_mail_typo"));
}

#[tokio::test]
async fn worker_new_rejects_non_positive_default_job_timeout() {
    let mut ctx = RedisTestContext::new().await.unwrap();
    ctx.settings.default_runner_name = "test".to_string();
    ctx.settings.default_job_timeout_seconds = 0;
    let runner = Arc::new(StaticRunner::new(
        TestOutcome::Success(json!({"ok": true})),
        Duration::from_millis(0),
    ));
    let mut runners: HashMap<String, Arc<dyn Runner>> = HashMap::new();
    runners.insert("test".to_string(), runner);

    let err = match RRQWorker::new(
        ctx.settings.clone(),
        None,
        Some("worker-1".to_string()),
        runners,
        true,
        1,
    )
    .await
    {
        Ok(_) => panic!("worker creation should fail"),
        Err(err) => err,
    };
    assert!(
        err.to_string()
            .contains("default_job_timeout_seconds must be positive")
    );
}

#[tokio::test]
async fn worker_new_rejects_overflowing_provisional_claim_lock_timeout() {
    let mut ctx = RedisTestContext::new().await.unwrap();
    ctx.settings.default_runner_name = "test".to_string();
    ctx.settings.default_job_timeout_seconds = i64::MAX;
    ctx.settings.default_lock_timeout_extension_seconds = 1;
    let runner = Arc::new(StaticRunner::new(
        TestOutcome::Success(json!({"ok": true})),
        Duration::from_millis(0),
    ));
    let mut runners: HashMap<String, Arc<dyn Runner>> = HashMap::new();
    runners.insert("test".to_string(), runner);

    let err = match RRQWorker::new(
        ctx.settings.clone(),
        None,
        Some("worker-1".to_string()),
        runners,
        true,
        1,
    )
    .await
    {
        Ok(_) => panic!("worker creation should fail"),
        Err(err) => err,
    };
    assert!(
        err.to_string()
            .contains("provisional claim lock timeout overflow")
    );
}

#[tokio::test]
async fn worker_processes_success_job() {
    let mut ctx = RedisTestContext::new().await.unwrap();
    ctx.settings.default_runner_name = "test".to_string();
    let runner = Arc::new(StaticRunner::new(
        TestOutcome::Success(json!({"ok": true})),
        Duration::from_millis(0),
    ));
    let mut runners: HashMap<String, Arc<dyn Runner>> = HashMap::new();
    runners.insert("test".to_string(), runner);
    let mut client = RRQClient::new(ctx.settings.clone(), ctx.store.clone());
    let job = client
        .enqueue("success", serde_json::Map::new(), EnqueueOptions::default())
        .await
        .unwrap();
    let mut worker = RRQWorker::new(
        ctx.settings.clone(),
        None,
        Some("worker-1".to_string()),
        runners,
        true,
        1,
    )
    .await
    .unwrap();
    timeout(Duration::from_secs(5), worker.run())
        .await
        .unwrap()
        .unwrap();
    let loaded = ctx
        .store
        .get_job_definition(&job.id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(loaded.status, JobStatus::Completed);
    assert_eq!(loaded.result, Some(json!({"ok": true})));
}

#[tokio::test]
async fn worker_processes_retry_job() {
    let mut ctx = RedisTestContext::new().await.unwrap();
    ctx.settings.default_runner_name = "test".to_string();
    let runner = Arc::new(StaticRunner::new(
        TestOutcome::Retry,
        Duration::from_millis(0),
    ));
    let mut runners: HashMap<String, Arc<dyn Runner>> = HashMap::new();
    runners.insert("test".to_string(), runner);
    let mut client = RRQClient::new(ctx.settings.clone(), ctx.store.clone());
    let job = client
        .enqueue("retry", serde_json::Map::new(), EnqueueOptions::default())
        .await
        .unwrap();
    let mut worker = RRQWorker::new(
        ctx.settings.clone(),
        None,
        Some("worker-1".to_string()),
        runners,
        true,
        1,
    )
    .await
    .unwrap();
    timeout(Duration::from_secs(5), worker.run())
        .await
        .unwrap()
        .unwrap();
    let loaded = ctx
        .store
        .get_job_definition(&job.id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(loaded.status, JobStatus::Retrying);
    assert_eq!(loaded.current_retries, 1);
    assert!(loaded.next_scheduled_run_time.is_some());
}

#[tokio::test]
async fn worker_timeout_skips_cancel_hint_by_default() {
    let mut ctx = RedisTestContext::new().await.unwrap();
    ctx.settings.default_runner_name = "test".to_string();
    let runner = Arc::new(StaticRunner::new(
        TestOutcome::Success(json!({"ok": true})),
        Duration::from_millis(1500),
    ));
    let last_request_id = runner.last_request_id.clone();
    let cancelled = runner.cancelled.clone();
    let mut runners: HashMap<String, Arc<dyn Runner>> = HashMap::new();
    runners.insert("test".to_string(), runner);
    let mut client = RRQClient::new(ctx.settings.clone(), ctx.store.clone());
    let options = EnqueueOptions {
        job_timeout_seconds: Some(1),
        ..Default::default()
    };
    let job = client
        .enqueue("timeout", serde_json::Map::new(), options)
        .await
        .unwrap();
    let mut worker = RRQWorker::new(
        ctx.settings.clone(),
        None,
        Some("worker-1".to_string()),
        runners,
        true,
        1,
    )
    .await
    .unwrap();
    timeout(Duration::from_secs(5), worker.run())
        .await
        .unwrap()
        .unwrap();
    let request_id = last_request_id.lock().await.clone().unwrap();
    let cancelled = cancelled.lock().await;
    assert!(!cancelled.contains(&request_id));
    let loaded = ctx
        .store
        .get_job_definition(&job.id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(loaded.status, JobStatus::Failed);
}

#[tokio::test]
async fn worker_timeout_sends_cancel_hint_when_enabled() {
    let mut ctx = RedisTestContext::new().await.unwrap();
    ctx.settings.default_runner_name = "test".to_string();
    ctx.settings.runner_enable_inflight_cancel_hints = true;
    let runner = Arc::new(StaticRunner::new(
        TestOutcome::Success(json!({"ok": true})),
        Duration::from_millis(1500),
    ));
    let last_request_id = runner.last_request_id.clone();
    let cancelled = runner.cancelled.clone();
    let mut runners: HashMap<String, Arc<dyn Runner>> = HashMap::new();
    runners.insert("test".to_string(), runner);
    let mut client = RRQClient::new(ctx.settings.clone(), ctx.store.clone());
    let options = EnqueueOptions {
        job_timeout_seconds: Some(1),
        ..Default::default()
    };
    let job = client
        .enqueue("timeout", serde_json::Map::new(), options)
        .await
        .unwrap();
    let mut worker = RRQWorker::new(
        ctx.settings.clone(),
        None,
        Some("worker-1".to_string()),
        runners,
        true,
        1,
    )
    .await
    .unwrap();
    timeout(Duration::from_secs(5), worker.run())
        .await
        .unwrap()
        .unwrap();
    let request_id = last_request_id.lock().await.clone().unwrap();
    let cancelled = cancelled.lock().await;
    assert!(cancelled.contains(&request_id));
    let loaded = ctx
        .store
        .get_job_definition(&job.id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(loaded.status, JobStatus::Failed);
}
