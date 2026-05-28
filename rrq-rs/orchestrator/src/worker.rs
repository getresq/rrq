use std::collections::HashMap;
use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};
use std::time::Instant;

use anyhow::Result;
use chrono::{TimeZone, Utc};
use rand::Rng;
use rrq_protocol::{ExecutionContext, ExecutionOutcome, ExecutionRequest, OutcomeStatus};
use serde_json::Value;
use tokio::sync::{Mutex, Semaphore};
use tokio::time::{Duration, sleep};
use tracing::{Instrument, field::Empty};
use uuid::Uuid;

use crate::client::{EnqueueOptions, RRQClient};
use crate::constants::{
    DEFAULT_DLQ_RESULT_TTL_SECONDS, DEFAULT_WORKER_ID_PREFIX, UNIQUE_JOB_LOCK_PREFIX,
};
use crate::job::{Job, JobStatus};
use crate::runner::{Runner, RunnerExecutionResult};
use crate::store::JobStore;
use crate::telemetry;
use rrq_config::CronJob;
use rrq_config::{QUEUE_KEY_PREFIX, RRQSettings, normalize_queue_name};

#[derive(Debug, Clone)]
struct RunningJobInfo {
    queue_name: String,
    runner_name: Option<String>,
    request_id: Option<String>,
}

pub struct RRQWorker {
    settings: RRQSettings,
    queues: Vec<String>,
    worker_id: String,
    job_store: JobStore,
    client: RRQClient,
    runners: HashMap<String, Arc<dyn Runner>>,
    default_runner_name: String,
    runner_routes: HashMap<String, String>,
    worker_concurrency: usize,
    semaphore: Arc<Semaphore>,
    running_jobs: Arc<Mutex<HashMap<String, RunningJobInfo>>>,
    running_aborts: Arc<Mutex<HashMap<String, tokio::task::AbortHandle>>>,
    status: Arc<Mutex<String>>,
    cron_jobs: Arc<Mutex<Vec<CronJob>>>,
    burst: bool,
    shutdown: Arc<AtomicBool>,
    queue_cursor: usize,
    provisional_claim_lock_timeout_ms: i64,
}

impl RRQWorker {
    pub async fn new(
        settings: RRQSettings,
        queues: Option<Vec<String>>,
        worker_id: Option<String>,
        runners: HashMap<String, Arc<dyn Runner>>,
        burst: bool,
        worker_concurrency: usize,
    ) -> Result<Self> {
        let mut settings = settings;
        settings.default_queue_name = normalize_queue_name(&settings.default_queue_name);
        settings.runner_routes = settings
            .runner_routes
            .into_iter()
            .map(|(queue_name, runner_name)| (normalize_queue_name(&queue_name), runner_name))
            .collect();

        let mut queues = queues
            .unwrap_or_else(|| vec![settings.default_queue_name.clone()])
            .into_iter()
            .map(|queue_name| normalize_queue_name(&queue_name))
            .collect::<Vec<_>>();
        queues.sort();
        queues.dedup();
        if queues.is_empty() {
            return Err(anyhow::anyhow!(
                "worker must be configured with at least one queue"
            ));
        }

        let default_runner_name = settings.default_runner_name.clone();
        let default_runner_required = queues
            .iter()
            .any(|queue_name| resolve_routed_runner(&settings.runner_routes, queue_name).is_none());
        if default_runner_required && !runners.contains_key(&default_runner_name) {
            return Err(anyhow::anyhow!(
                "default runner '{}' is not configured",
                default_runner_name
            ));
        }
        for queue_name in &queues {
            if let Some(routed_runner_name) =
                resolve_routed_runner(&settings.runner_routes, queue_name)
                && !runners.contains_key(&routed_runner_name)
            {
                return Err(anyhow::anyhow!(
                    "queue '{}' routes to runner '{}' but that runner is not configured",
                    queue_name,
                    routed_runner_name
                ));
            }
        }
        if runners.is_empty() {
            return Err(anyhow::anyhow!("RRQWorker requires at least one runner"));
        }
        let worker_concurrency = if worker_concurrency == 0 {
            return Err(anyhow::anyhow!("worker_concurrency must be positive"));
        } else {
            worker_concurrency
        };
        let runner_routes = settings.runner_routes.clone();
        let provisional_claim_lock_timeout_ms = provisional_claim_lock_timeout_ms(&settings)?;
        let job_store = JobStore::new(settings.clone()).await?;
        let client = RRQClient::new(settings.clone(), job_store.clone());
        let worker_id = worker_id.unwrap_or_else(|| {
            let short_id = Uuid::new_v4().to_string();
            let suffix = &short_id[..6];
            format!(
                "{DEFAULT_WORKER_ID_PREFIX}{}_{}",
                std::process::id(),
                suffix
            )
        });
        Ok(Self {
            settings,
            queues,
            worker_id,
            job_store,
            client,
            runners,
            default_runner_name,
            runner_routes,
            worker_concurrency,
            semaphore: Arc::new(Semaphore::new(worker_concurrency)),
            running_jobs: Arc::new(Mutex::new(HashMap::new())),
            running_aborts: Arc::new(Mutex::new(HashMap::new())),
            status: Arc::new(Mutex::new("initializing".to_string())),
            cron_jobs: Arc::new(Mutex::new(Vec::new())),
            burst,
            shutdown: Arc::new(AtomicBool::new(false)),
            queue_cursor: 0,
            provisional_claim_lock_timeout_ms,
        })
    }

    pub fn worker_id(&self) -> &str {
        &self.worker_id
    }

    pub fn settings(&self) -> &RRQSettings {
        &self.settings
    }

    pub fn request_shutdown(&self) {
        self.shutdown.store(true, Ordering::SeqCst);
    }

    pub fn shutdown_handle(&self) -> Arc<AtomicBool> {
        self.shutdown.clone()
    }

    pub async fn close_runners(&self) {
        for runner in self.runners.values() {
            if let Err(err) = runner.close().await {
                tracing::debug!("runner close error: {err}");
            }
        }
    }

    async fn set_status(&self, value: &str) {
        let mut status = self.status.lock().await;
        *status = value.to_string();
    }

    fn calculate_jittered_delay(&self, base_delay: f64, jitter_factor: f64) -> Duration {
        if base_delay <= 0.0 {
            return Duration::ZERO;
        }
        let jitter = jitter_factor.clamp(0.0, 0.99);
        let min_delay = (base_delay * (1.0 - jitter)).max(0.0);
        let mut max_delay = base_delay * (1.0 + jitter);
        if max_delay < min_delay {
            max_delay = min_delay;
        }
        let mut rng = rand::rng();
        let delay = rng.random_range(min_delay..=max_delay);
        Duration::from_secs_f64(delay)
    }

    pub async fn run(&mut self) -> Result<()> {
        tracing::info!(worker_id = %self.worker_id, "worker started");
        self.set_status("running").await;
        {
            let mut cron_jobs = self.cron_jobs.lock().await;
            cron_jobs.extend(self.settings.cron_jobs.clone());
            let now = Utc::now();
            for job in cron_jobs.iter_mut() {
                let _ = job.schedule_next(now);
            }
        }

        let heartbeat_handle = {
            let context = HeartbeatContext {
                shutdown: self.shutdown.clone(),
                job_store: self.job_store.clone(),
                worker_id: self.worker_id.clone(),
                queues: self.queues.clone(),
                status: self.status.clone(),
                running_jobs: self.running_jobs.clone(),
                worker_concurrency: self.worker_concurrency,
                settings: self.settings.clone(),
            };
            tokio::spawn(async move {
                heartbeat_loop(context).await;
            })
        };

        let cron_handle = {
            let shutdown = self.shutdown.clone();
            let job_store = self.job_store.clone();
            let client = self.client.clone();
            let cron_jobs = self.cron_jobs.clone();
            tokio::spawn(async move {
                cron_loop(shutdown, cron_jobs, client, job_store).await;
            })
        };

        while !self.shutdown.load(Ordering::SeqCst) {
            let running = self
                .worker_concurrency
                .saturating_sub(self.semaphore.available_permits());
            let fetch_count = self.worker_concurrency.saturating_sub(running);
            if fetch_count == 0 {
                telemetry::record_poll_cycle("no_capacity");
                self.set_status("idle (concurrency limit)").await;
                let delay =
                    self.calculate_jittered_delay(self.settings.default_poll_delay_seconds, 0.5);
                sleep_with_shutdown(&self.shutdown, delay).await;
                continue;
            }

            self.set_status("polling").await;
            let fetched = self.poll_for_jobs(fetch_count).await?;
            if fetched == 0 {
                telemetry::record_poll_cycle("no_jobs");
                let running_jobs = self.running_jobs.lock().await.len();
                if self.burst && running_jobs == 0 {
                    break;
                }
                self.set_status("idle (no jobs)").await;
                let delay =
                    self.calculate_jittered_delay(self.settings.default_poll_delay_seconds, 0.5);
                sleep_with_shutdown(&self.shutdown, delay).await;
            } else {
                telemetry::record_poll_cycle("fetched");
            }
        }

        self.shutdown.store(true, Ordering::SeqCst);
        self.drain_tasks().await?;
        let _ = heartbeat_handle.await;
        let _ = cron_handle.await;
        tracing::info!(worker_id = %self.worker_id, "worker stopped");
        Ok(())
    }

    async fn poll_for_jobs(&mut self, count: usize) -> Result<usize> {
        let mut fetched = 0;
        let total_queues = self.queues.len();
        let start_index = self.queue_cursor % total_queues;
        self.queue_cursor = (start_index + 1) % total_queues;
        let fair_share = count.div_ceil(total_queues).max(1);
        let poll_span = tracing::debug_span!(
            "rrq.poll_cycle",
            "rrq.worker_id" = %self.worker_id,
            "rrq.requested" = count,
            "rrq.fetched" = Empty
        );
        async {
            for pass in 0..2 {
                for offset in 0..total_queues {
                    if fetched >= count || self.shutdown.load(Ordering::SeqCst) {
                        poll_span.record("rrq.fetched", fetched as i64);
                        return Ok(fetched);
                    }
                    let queue_index = (start_index + offset) % total_queues;
                    let queue_name = self.queues[queue_index].clone();
                    let remaining = count - fetched;
                    let request_count = if pass == 0 {
                        remaining.min(fair_share)
                    } else {
                        remaining
                    };
                    if request_count == 0 {
                        continue;
                    }
                    let fetch_span = tracing::debug_span!(
                        "rrq.claim",
                        "rrq.queue" = %queue_name,
                        "rrq.pass" = pass,
                        "rrq.requested" = request_count,
                        "rrq.claimed" = Empty,
                        "rrq.claim_ms" = Empty
                    );
                    let fetch_started = Instant::now();
                    let claim_start_time = Utc::now();
                    telemetry::record_claim_attempt(&queue_name);
                    let claimed = self
                        .job_store
                        .atomic_claim_ready_jobs(
                            &queue_name,
                            &self.worker_id,
                            self.provisional_claim_lock_timeout_ms,
                            self.settings.default_lock_timeout_extension_seconds,
                            request_count,
                            claim_start_time,
                        )
                        .instrument(fetch_span.clone())
                        .await?;
                    fetch_span.record(
                        "rrq.claim_ms",
                        fetch_started.elapsed().as_secs_f64() * 1000.0,
                    );
                    fetch_span.record("rrq.claimed", claimed.len() as i64);
                    if claimed.is_empty() {
                        telemetry::record_claim_miss(&queue_name, "none_ready");
                        continue;
                    }
                    telemetry::record_jobs_fetched(&queue_name, claimed.len() as u64);
                    for _ in 0..claimed.len() {
                        telemetry::record_lock_acquire(&queue_name, "acquired");
                    }
                    let job_defs = match self
                        .job_store
                        .get_job_definitions(&claimed)
                        .instrument(fetch_span.clone())
                        .await
                    {
                        Ok(job_defs) => job_defs,
                        Err(err) => {
                            tracing::warn!(
                                "rrq.queue" = %queue_name,
                                error = %err,
                                "failed to load claimed job definitions; releasing claims"
                            );
                            self.release_remaining_claimed_jobs_without_definitions(
                                &queue_name,
                                &claimed,
                            )
                            .await;
                            return Err(err);
                        }
                    };

                    for index in 0..claimed.len() {
                        let job_id = claimed[index].as_str();
                        let job_opt = job_defs.get(index).and_then(Option::as_ref);
                        if fetched >= count || self.shutdown.load(Ordering::SeqCst) {
                            tracing::warn!(
                                "rrq.queue" = %queue_name,
                                "rrq.job_id" = %job_id,
                                "worker shutdown or fetch cap reached during claimed batch; releasing job"
                            );
                            self.release_remaining_claimed_jobs(
                                &queue_name,
                                &claimed[index..],
                                &job_defs[index..],
                                Some("Job execution interrupted by worker shutdown. Re-queued."),
                            )
                            .await;
                            poll_span.record("rrq.fetched", fetched as i64);
                            return Ok(fetched);
                        }
                        let job = match job_opt {
                            Some(job) => job,
                            None => {
                                tracing::warn!(
                                    "rrq.queue" = %queue_name,
                                    "rrq.job_id" = %job_id,
                                    "claimed job definition missing/malformed; moving to DLQ"
                                );
                                self.quarantine_claimed_job_without_definition(
                                    &queue_name,
                                    job_id,
                                    "Failed to parse claimed job definition. Moved to DLQ.",
                                )
                                .await;
                                continue;
                            }
                        };

                        let job_timeout = job
                            .job_timeout_seconds
                            .unwrap_or(self.settings.default_job_timeout_seconds);
                        let lock_timeout_ms = match job_timeout
                            .checked_add(self.settings.default_lock_timeout_extension_seconds)
                            .and_then(|sum| sum.checked_mul(1000))
                        {
                            Some(lock_timeout_ms) => lock_timeout_ms,
                            None => {
                                tracing::warn!(
                                    "rrq.queue" = %queue_name,
                                    "rrq.job_id" = %job.id,
                                    "lock timeout overflow while preparing claimed job; releasing claimed remainder"
                                );
                                self.release_claimed_jobs_from_index(
                                    &queue_name,
                                    &claimed,
                                    &job_defs,
                                    index,
                                    Some("Invalid lock timeout while preparing execution. Re-queued."),
                                )
                                .await;
                                return Err(anyhow::anyhow!("lock_timeout_ms overflow"));
                            }
                        };
                        if lock_timeout_ms <= 0 {
                            tracing::warn!(
                                "rrq.queue" = %queue_name,
                                "rrq.job_id" = %job.id,
                                "invalid non-positive lock timeout while preparing claimed job; releasing claimed remainder"
                            );
                            self.release_claimed_jobs_from_index(
                                &queue_name,
                                &claimed,
                                &job_defs,
                                index,
                                Some("Invalid lock timeout while preparing execution. Re-queued."),
                            )
                            .await;
                            return Err(anyhow::anyhow!("lock_timeout_ms must be positive"));
                        }
                        let lock_refreshed = match self
                            .job_store
                            .refresh_job_lock_timeout(&job.id, &self.worker_id, lock_timeout_ms)
                            .await
                        {
                            Ok(lock_refreshed) => lock_refreshed,
                            Err(err) => {
                                tracing::warn!(
                                    "rrq.queue" = %queue_name,
                                    "rrq.job_id" = %job.id,
                                    error = %err,
                                    "failed to refresh claimed job lock; releasing claimed remainder"
                                );
                                self.release_claimed_jobs_from_index(
                                    &queue_name,
                                    &claimed,
                                    &job_defs,
                                    index,
                                    Some(
                                        "Failed to refresh claimed job lock before dispatch. Re-queued.",
                                    ),
                                )
                                .await;
                                return Err(err);
                            }
                        };
                        if !lock_refreshed {
                            telemetry::record_claim_miss(&queue_name, "lock_refresh_lost");
                            tracing::warn!(
                                "rrq.queue" = %queue_name,
                                "rrq.job_id" = %job.id,
                                "lost claimed job lock ownership before dispatch; re-queueing"
                            );
                            self.release_claimed_job(
                                &queue_name,
                                &job.id,
                                Some(job),
                                Some("Failed to refresh claimed job lock before dispatch. Re-queued."),
                            )
                            .await;
                            continue;
                        }
                        let start_time = job.start_time.unwrap_or(claim_start_time);

                        let permit = match self.semaphore.clone().acquire_owned().await {
                            Ok(permit) => permit,
                            Err(err) => {
                                tracing::error!(
                                    "rrq.queue" = %queue_name,
                                    "rrq.job_id" = %job.id,
                                    error = %err,
                                    "failed to acquire worker permit for claimed job; releasing claimed remainder"
                                );
                                self.release_claimed_jobs_from_index(
                                    &queue_name,
                                    &claimed,
                                    &job_defs,
                                    index,
                                    Some(
                                        "Worker concurrency permit acquisition failed before dispatch. Re-queued.",
                                    ),
                                )
                                .await;
                                return Err(err.into());
                            }
                        };
                        let job_store = self.job_store.clone();
                        let runners = self.runners.clone();
                        let runner_routes = self.runner_routes.clone();
                        let default_runner_name = self.default_runner_name.clone();
                        let settings = self.settings.clone();
                        let worker_id = self.worker_id.clone();
                        let running_jobs = self.running_jobs.clone();
                        let running_aborts = self.running_aborts.clone();
                        let queue_name = queue_name.clone();
                        let mut job_for_task = job.clone();
                        job_for_task.start_time = Some(start_time);
                        {
                            let mut running = running_jobs.lock().await;
                            running.insert(
                                job.id.clone(),
                                RunningJobInfo {
                                    queue_name: queue_name.clone(),
                                    runner_name: None,
                                    request_id: None,
                                },
                            );
                        }

                        let handle = tokio::spawn(async move {
                            let _permit = permit;
                            let context = ExecuteJobContext {
                                settings,
                                job_store,
                                runners,
                                default_runner_name,
                                runner_routes,
                                worker_id,
                                running_jobs,
                                running_aborts,
                            };
                            if let Err(err) = execute_job(job_for_task, queue_name, context).await {
                                tracing::error!("job execution error: {err}");
                            }
                        });
                        {
                            let mut aborts = self.running_aborts.lock().await;
                            aborts.insert(job.id.clone(), handle.abort_handle());
                        }

                        fetched += 1;
                    }
                }
            }
            poll_span.record("rrq.fetched", fetched as i64);
            Ok(fetched)
        }
        .instrument(poll_span.clone())
        .await
    }

    async fn lock_owned_by_other(&mut self, job_id: &str) -> Option<bool> {
        for attempt in 0..2 {
            match self.job_store.get_job_lock_owner(job_id).await {
                Ok(owner) => {
                    return Some(
                        owner
                            .as_deref()
                            .is_some_and(|owner| owner != self.worker_id.as_str()),
                    );
                }
                Err(err) => {
                    tracing::warn!(
                        "rrq.job_id" = %job_id,
                        attempt = attempt + 1,
                        error = %err,
                        "failed to read job lock owner while releasing claimed job"
                    );
                }
            }
        }
        None
    }

    fn is_wrongtype_error(err: &anyhow::Error) -> bool {
        err.to_string().to_ascii_lowercase().contains("wrongtype")
    }

    async fn requeue_claimed_job_if_needed(
        &mut self,
        queue_name: &str,
        job_id: &str,
        job: &Job,
        pending_error: Option<&str>,
    ) -> bool {
        let should_requeue = matches!(
            job.status,
            JobStatus::Active | JobStatus::Pending | JobStatus::Retrying
        );
        if should_requeue {
            if job.status == JobStatus::Active
                && let Err(err) = self.job_store.mark_job_pending(job_id, pending_error).await
            {
                tracing::warn!(
                    "rrq.queue" = %queue_name,
                    "rrq.job_id" = %job_id,
                    error = %err,
                    "failed to mark claimed job pending before requeue"
                );
                return false;
            }
            let already_queued = match self.job_store.is_job_queued(queue_name, job_id).await {
                Ok(value) => value,
                Err(err) => {
                    tracing::warn!(
                        "rrq.queue" = %queue_name,
                        "rrq.job_id" = %job_id,
                        error = %err,
                        "failed to verify claimed job queue membership before requeue"
                    );
                    return false;
                }
            };
            if !already_queued
                && let Err(err) = self
                    .job_store
                    .add_job_to_queue(queue_name, job_id, Utc::now().timestamp_millis() as f64)
                    .await
            {
                tracing::warn!(
                    "rrq.queue" = %queue_name,
                    "rrq.job_id" = %job_id,
                    error = %err,
                    "failed to requeue claimed job"
                );
                return false;
            }
        }
        true
    }

    async fn requeue_claimed_job_without_definition(
        &mut self,
        queue_name: &str,
        job_id: &str,
        pending_error: Option<&str>,
    ) -> bool {
        if let Err(err) = self.job_store.mark_job_pending(job_id, pending_error).await {
            tracing::warn!(
                "rrq.queue" = %queue_name,
                "rrq.job_id" = %job_id,
                error = %err,
                "failed to mark claimed job without definition pending before requeue"
            );
            return false;
        }
        let already_queued = match self.job_store.is_job_queued(queue_name, job_id).await {
            Ok(value) => value,
            Err(err) => {
                tracing::warn!(
                    "rrq.queue" = %queue_name,
                    "rrq.job_id" = %job_id,
                    error = %err,
                    "failed to verify claimed job without definition queue membership before requeue"
                );
                return false;
            }
        };
        if !already_queued
            && let Err(err) = self
                .job_store
                .add_job_to_queue(queue_name, job_id, Utc::now().timestamp_millis() as f64)
                .await
        {
            tracing::warn!(
                "rrq.queue" = %queue_name,
                "rrq.job_id" = %job_id,
                error = %err,
                "failed to requeue claimed job without definition"
            );
            return false;
        }
        true
    }

    async fn cleanup_claimed_job_state(&mut self, job_id: &str) {
        let _ = self
            .job_store
            .remove_active_job(&self.worker_id, job_id)
            .await;
        if let Err(err) = self
            .job_store
            .release_job_lock_if_owner(job_id, &self.worker_id)
            .await
        {
            tracing::warn!(
                "rrq.job_id" = %job_id,
                error = %err,
                "failed to release claimed job lock if owned by current worker"
            );
            if Self::is_wrongtype_error(&err)
                && let Err(force_err) = self.job_store.release_job_lock(job_id).await
            {
                tracing::warn!(
                    "rrq.job_id" = %job_id,
                    error = %force_err,
                    "failed to force-release malformed claimed job lock"
                );
            }
        }
    }

    async fn release_claimed_job(
        &mut self,
        queue_name: &str,
        job_id: &str,
        job: Option<&Job>,
        pending_error: Option<&str>,
    ) {
        let Some(lock_owned_by_other) = self.lock_owned_by_other(job_id).await else {
            tracing::warn!(
                "rrq.queue" = %queue_name,
                "rrq.job_id" = %job_id,
                "lock ownership unknown while releasing claimed job; applying fallback cleanup"
            );
            if let Some(job) = job {
                if job.worker_id.as_deref() == Some(self.worker_id.as_str()) {
                    if !self
                        .requeue_claimed_job_if_needed(queue_name, job_id, job, pending_error)
                        .await
                    {
                        tracing::warn!(
                            "rrq.queue" = %queue_name,
                            "rrq.job_id" = %job_id,
                            "preserving claimed job because fallback requeue writes failed"
                        );
                        return;
                    }
                } else {
                    tracing::warn!(
                        "rrq.queue" = %queue_name,
                        "rrq.job_id" = %job_id,
                        "skipping fallback requeue for unknown lock ownership because claimed job is assigned to another worker"
                    );
                }
            }
            self.cleanup_claimed_job_state(job_id).await;
            return;
        };
        if !lock_owned_by_other
            && let Some(job) = job
            && job.worker_id.as_deref() == Some(self.worker_id.as_str())
            && !self
                .requeue_claimed_job_if_needed(queue_name, job_id, job, pending_error)
                .await
        {
            tracing::warn!(
                "rrq.queue" = %queue_name,
                "rrq.job_id" = %job_id,
                "preserving claimed job because requeue writes failed"
            );
            return;
        }
        self.cleanup_claimed_job_state(job_id).await;
    }

    async fn release_claimed_jobs_from_index(
        &mut self,
        queue_name: &str,
        claimed: &[String],
        job_defs: &[Option<Job>],
        index: usize,
        pending_error: Option<&str>,
    ) {
        if index >= claimed.len() {
            return;
        }
        let job_id = claimed[index].as_str();
        match job_defs.get(index).and_then(Option::as_ref) {
            Some(job) => {
                self.release_claimed_job(queue_name, job_id, Some(job), pending_error)
                    .await;
            }
            None => {
                self.quarantine_claimed_job_without_definition(
                    queue_name,
                    job_id,
                    "Failed to parse claimed job definition. Moved to DLQ.",
                )
                .await;
            }
        }
        let remaining_claimed = claimed.get(index + 1..).unwrap_or(&[]);
        let remaining_defs = job_defs.get(index + 1..).unwrap_or(&[]);
        self.release_remaining_claimed_jobs(
            queue_name,
            remaining_claimed,
            remaining_defs,
            pending_error,
        )
        .await;
    }

    async fn release_remaining_claimed_jobs(
        &mut self,
        queue_name: &str,
        claimed: &[String],
        job_defs: &[Option<Job>],
        pending_error: Option<&str>,
    ) {
        for (index, job_id) in claimed.iter().enumerate() {
            match job_defs.get(index).and_then(Option::as_ref) {
                Some(job) => {
                    self.release_claimed_job(queue_name, job_id, Some(job), pending_error)
                        .await;
                }
                None => {
                    self.quarantine_claimed_job_without_definition(
                        queue_name,
                        job_id,
                        "Failed to parse claimed job definition. Moved to DLQ.",
                    )
                    .await;
                }
            }
        }
    }

    async fn quarantine_claimed_job_without_definition(
        &mut self,
        queue_name: &str,
        job_id: &str,
        error_message: &str,
    ) {
        let Some(lock_owned_by_other) = self.lock_owned_by_other(job_id).await else {
            tracing::warn!(
                "rrq.queue" = %queue_name,
                "rrq.job_id" = %job_id,
                "preserving claimed malformed job because lock ownership is unknown"
            );
            return;
        };
        if !lock_owned_by_other {
            // Claimed jobs should already be removed from the hot queue, but remove defensively.
            let _ = self
                .job_store
                .remove_job_from_queue(queue_name, job_id)
                .await;

            let default_dlq_name = self.settings.default_dlq_name.clone();
            match self.job_store.get_job_data_map(job_id).await {
                Ok(Some(job_map)) => {
                    let dlq_name = job_map
                        .get("dlq_name")
                        .filter(|name| !name.is_empty())
                        .cloned()
                        .unwrap_or(default_dlq_name);
                    let unique_key = job_map
                        .get("job_unique_key")
                        .filter(|key| !key.is_empty())
                        .cloned();
                    if let Err(err) = self
                        .job_store
                        .move_job_to_dlq(job_id, &dlq_name, error_message, Utc::now())
                        .await
                    {
                        tracing::warn!(
                            "rrq.queue" = %queue_name,
                            "rrq.job_id" = %job_id,
                            "rrq.dlq" = %dlq_name,
                            error = %err,
                            "failed to move malformed claimed job to DLQ"
                        );
                        if !self
                            .requeue_claimed_job_without_definition(
                                queue_name,
                                job_id,
                                Some("Failed to move malformed claimed job to DLQ. Re-queued."),
                            )
                            .await
                        {
                            tracing::warn!(
                                "rrq.queue" = %queue_name,
                                "rrq.job_id" = %job_id,
                                "preserving claimed malformed job because DLQ persistence and fallback requeue both failed"
                            );
                            return;
                        }
                    } else if let Some(unique_key) = unique_key.as_deref()
                        && let Err(err) = self.job_store.release_unique_job_lock(unique_key).await
                    {
                        tracing::warn!(
                            "rrq.queue" = %queue_name,
                            "rrq.job_id" = %job_id,
                            "rrq.dlq" = %dlq_name,
                            "rrq.unique_key" = %unique_key,
                            error = %err,
                            "failed to release unique lock for malformed claimed job moved to DLQ"
                        );
                    }
                }
                Ok(None) => {
                    tracing::warn!(
                        "rrq.queue" = %queue_name,
                        "rrq.job_id" = %job_id,
                        "claimed job definition missing while attempting malformed-job quarantine"
                    );
                }
                Err(err) => {
                    tracing::warn!(
                        "rrq.queue" = %queue_name,
                        "rrq.job_id" = %job_id,
                        error = %err,
                        "failed to read claimed job hash while attempting malformed-job quarantine"
                    );
                    if !self
                        .requeue_claimed_job_without_definition(
                            queue_name,
                            job_id,
                            Some("Failed to load malformed claimed job for DLQ move. Re-queued."),
                        )
                        .await
                    {
                        tracing::warn!(
                            "rrq.queue" = %queue_name,
                            "rrq.job_id" = %job_id,
                            "preserving claimed malformed job because fallback requeue writes failed"
                        );
                        return;
                    }
                }
            }
        }
        self.cleanup_claimed_job_state(job_id).await;
    }

    async fn release_claimed_job_without_definition(
        &mut self,
        queue_name: &str,
        job_id: &str,
        pending_error: Option<&str>,
    ) {
        let Some(lock_owned_by_other) = self.lock_owned_by_other(job_id).await else {
            tracing::warn!(
                "rrq.queue" = %queue_name,
                "rrq.job_id" = %job_id,
                "preserving claimed job without definition because lock ownership is unknown"
            );
            return;
        };
        if !lock_owned_by_other
            && !self
                .requeue_claimed_job_without_definition(queue_name, job_id, pending_error)
                .await
        {
            tracing::warn!(
                "rrq.queue" = %queue_name,
                "rrq.job_id" = %job_id,
                "preserving claimed job without definition because requeue writes failed"
            );
            return;
        }
        self.cleanup_claimed_job_state(job_id).await;
    }
    async fn release_remaining_claimed_jobs_without_definitions(
        &mut self,
        queue_name: &str,
        claimed: &[String],
    ) {
        for job_id in claimed {
            self.release_claimed_job_without_definition(
                queue_name,
                job_id,
                Some("Failed to load claimed job definition. Re-queued."),
            )
            .await;
        }
    }

    async fn drain_tasks(&self) -> Result<()> {
        let grace = Duration::from_secs_f64(self.settings.worker_shutdown_grace_period_seconds);
        let deadline = tokio::time::Instant::now() + grace;

        loop {
            let remaining = {
                let running = self.running_jobs.lock().await;
                running.len()
            };
            if remaining == 0 {
                return Ok(());
            }
            if tokio::time::Instant::now() >= deadline {
                break;
            }
            sleep(Duration::from_millis(50)).await;
        }

        let running = self.running_jobs.lock().await.clone();
        let aborts = self.running_aborts.lock().await.clone();
        if self.settings.runner_enable_inflight_cancel_hints {
            for (job_id, info) in &running {
                let resolved_runner = info.runner_name.clone().unwrap_or_else(|| {
                    resolve_routed_runner(&self.runner_routes, &info.queue_name)
                        .unwrap_or_else(|| self.default_runner_name.clone())
                });
                if let Some(runner) = self.runners.get(&resolved_runner) {
                    let _ = runner.cancel(job_id, info.request_id.as_deref()).await;
                }
            }
        }
        for (_job_id, abort) in aborts {
            abort.abort();
        }
        // Ensure runner processes are shut down before re-queueing interrupted jobs,
        // but only after aborting execution tasks so transport teardown doesn't
        // get misclassified as a real job failure (retry/DLQ).
        self.close_runners().await;
        for (job_id, info) in running {
            let mut store = self.job_store.clone();
            let job_opt = store.get_job_definition(&job_id).await?;
            let status = job_opt.as_ref().map(|job| job.status);
            let should_requeue = matches!(
                status,
                Some(JobStatus::Active | JobStatus::Pending | JobStatus::Retrying)
            );
            if should_requeue {
                let score = Utc::now().timestamp_millis() as f64;
                let requeue_message = if status == Some(JobStatus::Active) {
                    "Job execution interrupted by worker shutdown. Re-queued."
                } else {
                    ""
                };

                let result = store
                    .atomic_requeue_job(
                        &job_id,
                        &info.queue_name,
                        score,
                        requeue_message,
                        &self.worker_id,
                        Some(&self.worker_id),
                        None, // shutdown always forces "now"; no next_scheduled preservation
                    )
                    .await
                    .unwrap_or(-1);

                if result == 1 {
                    tracing::warn!("re-queueing job {} after shutdown", job_id);
                } else if result < 0 {
                    tracing::warn!(
                        "atomic requeue returned error sentinel for job {} after shutdown (check Redis logs)",
                        job_id
                    );
                }
                // result == 0 (missing) or 2 (already queued) are silent no-ops, matching prior best-effort behavior
            }

            // Defensive cleanup for every job (harmless no-op for successful requeues, which already did
            // ZREM + owner-checked lock release inside atomic_requeue_job).
            if let Err(err) = store.remove_active_job(&self.worker_id, &job_id).await {
                tracing::warn!("failed to remove active job {job_id}: {err}");
            }
            if let Err(err) = store
                .release_job_lock_if_owner(&job_id, &self.worker_id)
                .await
            {
                tracing::warn!("failed to release job lock {job_id}: {err}");
            }
        }

        Ok(())
    }
}

fn provisional_claim_lock_timeout_ms(settings: &RRQSettings) -> Result<i64> {
    if settings.default_job_timeout_seconds <= 0 {
        return Err(anyhow::anyhow!(
            "default_job_timeout_seconds must be positive"
        ));
    }
    if settings.default_lock_timeout_extension_seconds < 0 {
        return Err(anyhow::anyhow!(
            "default_lock_timeout_extension_seconds must be >= 0"
        ));
    }
    let lock_timeout_ms = settings
        .default_job_timeout_seconds
        .checked_add(settings.default_lock_timeout_extension_seconds)
        .and_then(|sum| sum.checked_mul(1000))
        .ok_or_else(|| anyhow::anyhow!("provisional claim lock timeout overflow"))?;
    if lock_timeout_ms <= 0 {
        return Err(anyhow::anyhow!(
            "provisional claim lock timeout must be positive"
        ));
    }
    Ok(lock_timeout_ms)
}

fn resolve_routed_runner(
    runner_routes: &HashMap<String, String>,
    queue_name: &str,
) -> Option<String> {
    let normalized = normalize_queue_name(queue_name);
    runner_routes
        .get(&normalized)
        .or_else(|| runner_routes.get(queue_name))
        .or_else(|| {
            normalized
                .strip_prefix(QUEUE_KEY_PREFIX)
                .and_then(|bare| runner_routes.get(bare))
        })
        .cloned()
}

fn split_runner_name(function_name: &str) -> (Option<String>, String) {
    if let Some((prefix, handler)) = function_name.split_once('#') {
        if handler.is_empty() {
            return (Some(prefix.to_string()), String::new());
        }
        let runner = if prefix.is_empty() {
            None
        } else {
            Some(prefix.to_string())
        };
        return (runner, handler.to_string());
    }
    (None, function_name.to_string())
}

struct ExecuteJobContext {
    settings: RRQSettings,
    job_store: JobStore,
    runners: HashMap<String, Arc<dyn Runner>>,
    default_runner_name: String,
    runner_routes: HashMap<String, String>,
    worker_id: String,
    running_jobs: Arc<Mutex<HashMap<String, RunningJobInfo>>>,
    running_aborts: Arc<Mutex<HashMap<String, tokio::task::AbortHandle>>>,
}

struct HeartbeatContext {
    shutdown: Arc<AtomicBool>,
    job_store: JobStore,
    worker_id: String,
    queues: Vec<String>,
    status: Arc<Mutex<String>>,
    running_jobs: Arc<Mutex<HashMap<String, RunningJobInfo>>>,
    worker_concurrency: usize,
    settings: RRQSettings,
}

async fn execute_job(job: Job, queue_name: String, context: ExecuteJobContext) -> Result<()> {
    let ExecuteJobContext {
        settings,
        mut job_store,
        runners,
        default_runner_name,
        runner_routes,
        worker_id,
        running_jobs,
        running_aborts,
    } = context;
    let attempt = job.current_retries + 1;
    let started_at = Instant::now();
    let start_time_utc = job.start_time.unwrap_or_else(Utc::now);
    let queue_wait_ms = start_time_utc
        .signed_duration_since(job.enqueue_time)
        .num_milliseconds()
        .max(0) as f64;
    telemetry::record_queue_wait_ms(&queue_name, queue_wait_ms);
    let span = tracing::info_span!(
        "rrq.job",
        "span.kind" = "consumer",
        "messaging.system" = "redis",
        "messaging.destination.name" = %queue_name,
        "messaging.operation" = "process",
        "rrq.job_id" = %job.id,
        "rrq.function" = %job.function_name,
        "rrq.queue" = %queue_name,
        "rrq.attempt" = attempt,
        "rrq.worker_id" = %worker_id,
        "rrq.runner" = Empty,
        "rrq.queue_wait_ms" = queue_wait_ms,
        "rrq.outcome" = Empty,
        "rrq.duration_ms" = Empty,
        "rrq.retry_delay_ms" = Empty,
        "rrq.error_message" = Empty,
        "rrq.error_type" = Empty
    );
    if let Some(trace_context) = job.trace_context.as_ref() {
        telemetry::set_parent_from_trace_context(&span, trace_context);
    }
    if let Some(correlation_context) = job.correlation_context.as_ref() {
        telemetry::apply_correlation_context_to_span(&span, correlation_context);
    }
    let _enter = span.enter();
    let job_timeout = job
        .job_timeout_seconds
        .unwrap_or(settings.default_job_timeout_seconds);
    if job_timeout <= 0 {
        let message = format!("Invalid job timeout: {job_timeout}. Must be positive.");
        let duration_ms = started_at.elapsed().as_secs_f64() * 1000.0;
        span.record("rrq.outcome", "fatal");
        span.record("rrq.error_message", message.as_str());
        span.record("rrq.duration_ms", duration_ms);
        telemetry::record_job_processed(&queue_name, "unknown", "fatal", duration_ms);
        handle_fatal_job_error(&job, &queue_name, &message, &mut job_store).await?;
        cleanup_running(
            &job.id,
            &mut job_store,
            &worker_id,
            running_jobs,
            running_aborts,
        )
        .await?;
        return Ok(());
    }
    let deadline = Utc::now() + chrono::Duration::seconds(job_timeout);

    let (runner_name, handler_name) = split_runner_name(&job.function_name);
    if handler_name.is_empty() {
        let duration_ms = started_at.elapsed().as_secs_f64() * 1000.0;
        span.record("rrq.outcome", "fatal");
        span.record("rrq.error_message", "Handler name is missing");
        span.record("rrq.duration_ms", duration_ms);
        telemetry::record_job_processed(&queue_name, "unknown", "fatal", duration_ms);
        handle_fatal_job_error(&job, &queue_name, "Handler name is missing", &mut job_store)
            .await?;
        cleanup_running(
            &job.id,
            &mut job_store,
            &worker_id,
            running_jobs,
            running_aborts,
        )
        .await?;
        return Ok(());
    }

    let resolved_runner = match runner_name {
        Some(name) => name,
        None => resolve_routed_runner(&runner_routes, &queue_name)
            .unwrap_or(default_runner_name.clone()),
    };
    span.record("rrq.runner", resolved_runner.as_str());
    {
        let mut running = running_jobs.lock().await;
        if let Some(info) = running.get_mut(&job.id) {
            info.runner_name = Some(resolved_runner.clone());
        }
    }

    let runner = runners.get(&resolved_runner).cloned();
    let runner = match runner {
        Some(exec) => exec,
        None => {
            let message = format!("No runner configured for '{resolved_runner}'.");
            let duration_ms = started_at.elapsed().as_secs_f64() * 1000.0;
            span.record("rrq.outcome", "fatal");
            span.record("rrq.error_message", message.as_str());
            span.record("rrq.duration_ms", duration_ms);
            telemetry::record_job_processed(&queue_name, &resolved_runner, "fatal", duration_ms);
            handle_fatal_job_error(&job, &queue_name, &message, &mut job_store).await?;
            cleanup_running(
                &job.id,
                &mut job_store,
                &worker_id,
                running_jobs,
                running_aborts,
            )
            .await?;
            return Ok(());
        }
    };

    let params = job
        .job_params
        .iter()
        .map(|(key, value)| (key.clone(), value.clone()))
        .collect();
    let request = ExecutionRequest {
        protocol_version: rrq_protocol::PROTOCOL_VERSION.to_string(),
        request_id: Uuid::new_v4().to_string(),
        job_id: job.id.clone(),
        function_name: handler_name,
        params,
        context: ExecutionContext {
            job_id: job.id.clone(),
            attempt: attempt as u32,
            enqueue_time: job.enqueue_time,
            queue_name: queue_name.clone(),
            deadline: Some(deadline),
            trace_context: job.trace_context.clone(),
            correlation_context: job.correlation_context.clone(),
            worker_id: Some(worker_id.clone()),
        },
    };
    let request_id = request.request_id.clone();
    {
        let mut running = running_jobs.lock().await;
        if let Some(info) = running.get_mut(&job.id) {
            info.request_id = Some(request_id.clone());
        }
    }

    let dispatch_span = tracing::debug_span!(
        "rrq.dispatch",
        "rrq.job_id" = %job.id,
        "rrq.request_id" = %request_id,
        "rrq.runner" = %resolved_runner,
        "rrq.queue" = %queue_name,
        "rrq.timeout_seconds" = job_timeout
    );
    if let Some(correlation_context) = job.correlation_context.as_ref() {
        telemetry::apply_correlation_context_to_span(&dispatch_span, correlation_context);
    }
    let exec_result = runner
        .execute_with_timeout(
            request,
            Duration::from_secs(job_timeout as u64),
            settings.runner_enable_inflight_cancel_hints,
        )
        .instrument(dispatch_span)
        .await;

    let duration_ms = started_at.elapsed().as_secs_f64() * 1000.0;
    let (outcome_result, outcome_label) = match exec_result {
        RunnerExecutionResult::Completed(result) => match *result {
            Ok(outcome) => {
                let outcome_label = classify_outcome(&outcome);
                let outcome_result = handle_execution_outcome(
                    &job,
                    &queue_name,
                    &settings,
                    &mut job_store,
                    outcome,
                    duration_ms,
                )
                .await;
                (outcome_result, outcome_label)
            }
            Err(err) => {
                let message = format!("Runner transport error: {err}");
                span.record("rrq.outcome", "transport_error");
                span.record("rrq.error_message", message.as_str());
                span.record("rrq.duration_ms", duration_ms);
                tracing::error!(
                    outcome = "transport_error",
                    duration_ms,
                    error_message = %message,
                    "job failed before runner response"
                );
                let outcome_result =
                    process_failure_job(&job, &queue_name, &settings, &mut job_store, &message)
                        .await;
                (outcome_result, "transport_error")
            }
        },
        RunnerExecutionResult::TimedOut => {
            let message = format!("Job timed out after {}s.", job_timeout);
            span.record("rrq.outcome", "timeout");
            span.record("rrq.error_message", message.as_str());
            span.record("rrq.duration_ms", duration_ms);
            tracing::warn!(
                outcome = "timeout",
                duration_ms,
                error_message = %message,
                "job timed out before runner response"
            );
            let outcome_result =
                handle_job_timeout(&job, &queue_name, &mut job_store, &message).await;
            (outcome_result, "timeout")
        }
    };
    telemetry::record_job_processed(&queue_name, &resolved_runner, outcome_label, duration_ms);

    let cleanup_result = cleanup_running(
        &job.id,
        &mut job_store,
        &worker_id,
        running_jobs,
        running_aborts,
    )
    .await;

    if let Err(err) = outcome_result {
        if let Err(cleanup_err) = cleanup_result {
            tracing::error!("cleanup failed after outcome error: {cleanup_err}");
        }
        return Err(err);
    }

    cleanup_result?;
    Ok(())
}

fn classify_outcome(outcome: &ExecutionOutcome) -> &'static str {
    match outcome.status {
        OutcomeStatus::Success => "success",
        OutcomeStatus::Retry => "retry",
        OutcomeStatus::Timeout => "timeout",
        OutcomeStatus::Error => {
            let error_type = outcome
                .error
                .as_ref()
                .and_then(|error| error.error_type.as_deref());
            if error_type == Some("handler_not_found") {
                "fatal"
            } else {
                "error"
            }
        }
    }
}

async fn cleanup_running(
    job_id: &str,
    job_store: &mut JobStore,
    worker_id: &str,
    running_jobs: Arc<Mutex<HashMap<String, RunningJobInfo>>>,
    running_aborts: Arc<Mutex<HashMap<String, tokio::task::AbortHandle>>>,
) -> Result<()> {
    let remove_active_result = job_store.remove_active_job(worker_id, job_id).await;
    let release_lock_result = job_store.release_job_lock_if_owner(job_id, worker_id).await;

    {
        let mut running = running_jobs.lock().await;
        running.remove(job_id);
    }
    {
        let mut aborts = running_aborts.lock().await;
        aborts.remove(job_id);
    }

    if let Err(err) = remove_active_result {
        if let Err(lock_err) = release_lock_result {
            tracing::warn!(
                "rrq.job_id" = %job_id,
                error = %lock_err,
                "failed to release job lock while cleaning up after active-job removal failure"
            );
        }
        return Err(err);
    }

    release_lock_result.map(|_| ())
}

async fn handle_execution_outcome(
    job: &Job,
    queue_name: &str,
    settings: &RRQSettings,
    job_store: &mut JobStore,
    outcome: ExecutionOutcome,
    duration_ms: f64,
) -> Result<()> {
    let span = tracing::Span::current();
    span.record("rrq.duration_ms", duration_ms);
    if let Some(error) = outcome.error.as_ref() {
        span.record("rrq.error_message", error.message.as_str());
        if let Some(error_type) = error.error_type.as_deref() {
            span.record("rrq.error_type", error_type);
        }
    }

    match outcome.status {
        OutcomeStatus::Success => {
            let result = outcome.result.unwrap_or(Value::Null);
            let ttl = job
                .result_ttl_seconds
                .unwrap_or(settings.default_result_ttl_seconds);
            job_store.save_job_result(&job.id, &result, ttl).await?;
            if let Some(unique_key) = job.job_unique_key.as_ref() {
                job_store.release_unique_job_lock(unique_key).await?;
            }
            span.record("rrq.outcome", "success");
            tracing::info!(outcome = "success", duration_ms, "job completed");
        }
        OutcomeStatus::Retry => {
            let message = outcome
                .error
                .as_ref()
                .map(|error| error.message.clone())
                .unwrap_or_else(|| "Job requested retry".to_string());
            process_retry_job(
                job,
                queue_name,
                settings,
                job_store,
                &message,
                outcome.retry_after_seconds,
            )
            .await?;
            span.record("rrq.outcome", "retry");
            if let Some(delay) = outcome.retry_after_seconds {
                span.record("rrq.retry_delay_ms", delay * 1000.0);
            }
            tracing::warn!(
                "rrq.job_id" = %job.id,
                outcome = "retry",
                duration_ms,
                retry_after_seconds = outcome.retry_after_seconds,
                error_message = %message,
                "job retry requested"
            );
        }
        OutcomeStatus::Timeout => {
            let message = outcome
                .error
                .as_ref()
                .map(|error| error.message.clone())
                .unwrap_or_else(|| {
                    format!(
                        "Job timed out after {}s.",
                        job.job_timeout_seconds
                            .unwrap_or(settings.default_job_timeout_seconds)
                    )
                });
            handle_job_timeout(job, queue_name, job_store, &message).await?;
            span.record("rrq.outcome", "timeout");
            tracing::warn!(
                "rrq.job_id" = %job.id,
                outcome = "timeout",
                duration_ms,
                error_message = %message,
                "job timeout"
            );
        }
        OutcomeStatus::Error => {
            let error_type = outcome
                .error
                .as_ref()
                .and_then(|error| error.error_type.as_deref());
            if error_type == Some("handler_not_found") {
                let message = outcome
                    .error
                    .as_ref()
                    .map(|error| error.message.clone())
                    .unwrap_or_else(|| "Handler not found".to_string());
                handle_fatal_job_error(job, queue_name, &message, job_store).await?;
                span.record("rrq.outcome", "fatal");
                tracing::error!(
                    "rrq.job_id" = %job.id,
                    outcome = "fatal",
                    duration_ms,
                    error_type = "handler_not_found",
                    error_message = %message,
                    "job fatal error"
                );
            } else {
                let message = outcome
                    .error
                    .as_ref()
                    .map(|error| error.message.clone())
                    .unwrap_or_else(|| "Job failed".to_string());
                process_failure_job(job, queue_name, settings, job_store, &message).await?;
                span.record("rrq.outcome", "error");
                tracing::error!(
                    "rrq.job_id" = %job.id,
                    outcome = "error",
                    duration_ms,
                    error_type = error_type.unwrap_or("unknown"),
                    error_message = %message,
                    "job failed"
                );
            }
        }
    }

    Ok(())
}

async fn handle_job_timeout(
    job: &Job,
    queue_name: &str,
    job_store: &mut JobStore,
    error_message: &str,
) -> Result<()> {
    move_to_dlq(job, queue_name, job_store, error_message).await?;
    Ok(())
}

async fn handle_fatal_job_error(
    job: &Job,
    queue_name: &str,
    error_message: &str,
    job_store: &mut JobStore,
) -> Result<()> {
    move_to_dlq(job, queue_name, job_store, error_message).await?;
    Ok(())
}

async fn move_to_dlq(
    job: &Job,
    _queue_name: &str,
    job_store: &mut JobStore,
    error_message: &str,
) -> Result<()> {
    let dlq_name = job
        .dlq_name
        .clone()
        .unwrap_or_else(|| job_store.settings().default_dlq_name.clone());

    let unique_lock_key = job
        .job_unique_key
        .as_ref()
        .map(|k| format!("{UNIQUE_JOB_LOCK_PREFIX}{k}"));

    let result = job_store
        .atomic_move_job_to_dlq(
            &job.id,
            &dlq_name,
            error_message,
            Utc::now(),
            DEFAULT_DLQ_RESULT_TTL_SECONDS,
            unique_lock_key.as_deref(),
        )
        .await?;

    if result < 0 {
        tracing::error!(
            "rrq.job_id" = %job.id,
            "rrq.dlq" = %dlq_name,
            "atomic move to dlq script returned error sentinel; check Redis logs for details"
        );
        anyhow::bail!("atomic move to dlq failed for job {}", job.id);
    }

    tracing::warn!(
        "rrq.job_id" = %job.id,
        "rrq.dlq" = %dlq_name,
        error_message = %error_message,
        "job moved to dlq"
    );
    Ok(())
}

async fn process_retry_job(
    job: &Job,
    queue_name: &str,
    settings: &RRQSettings,
    job_store: &mut JobStore,
    error_message: &str,
    retry_after_seconds: Option<f64>,
) -> Result<()> {
    let anticipated_retry = job.current_retries + 1;
    if anticipated_retry >= job.max_retries {
        tracing::warn!(
            "rrq.job_id" = %job.id,
            retries = anticipated_retry,
            max_retries = job.max_retries,
            "retry budget exhausted; moving job to dlq"
        );
        move_to_dlq(job, queue_name, job_store, error_message).await?;
        return Ok(());
    }

    let delay_seconds = match retry_after_seconds {
        Some(delay) => delay,
        None => calculate_backoff_seconds(settings, anticipated_retry),
    };

    let retry_at_score = (Utc::now().timestamp_millis() as f64) + delay_seconds * 1000.0;
    let target_queue = job
        .queue_name
        .as_deref()
        .unwrap_or(&settings.default_queue_name);
    let target_queue = normalize_queue_name(target_queue);
    let new_retry = job_store
        .atomic_retry_job(
            &job.id,
            &target_queue,
            retry_at_score,
            error_message,
            JobStatus::Retrying,
        )
        .await?;

    let next_run_time = Utc
        .timestamp_millis_opt(retry_at_score as i64)
        .single()
        .unwrap_or_else(Utc::now);
    let _ = job_store
        .update_job_next_scheduled_run_time(&job.id, next_run_time)
        .await;

    tracing::info!(
        "rrq.job_id" = %job.id,
        retries = new_retry,
        max_retries = job.max_retries,
        retry_after_seconds = delay_seconds,
        retry_at = %next_run_time.to_rfc3339(),
        "retrying job"
    );
    Ok(())
}

async fn process_failure_job(
    job: &Job,
    queue_name: &str,
    settings: &RRQSettings,
    job_store: &mut JobStore,
    error_message: &str,
) -> Result<()> {
    let anticipated_retry = job.current_retries + 1;
    if anticipated_retry >= job.max_retries {
        tracing::warn!(
            "rrq.job_id" = %job.id,
            retries = anticipated_retry,
            max_retries = job.max_retries,
            "failure exhausted retry budget; moving job to dlq"
        );
        move_to_dlq(job, queue_name, job_store, error_message).await?;
        return Ok(());
    }

    let delay_seconds = calculate_backoff_seconds(settings, anticipated_retry);
    let retry_at_score = (Utc::now().timestamp_millis() as f64) + delay_seconds * 1000.0;
    let target_queue = job
        .queue_name
        .as_deref()
        .unwrap_or(&settings.default_queue_name);
    let target_queue = normalize_queue_name(target_queue);

    let new_retry = job_store
        .atomic_retry_job(
            &job.id,
            &target_queue,
            retry_at_score,
            error_message,
            JobStatus::Retrying,
        )
        .await?;

    let next_run_time = Utc
        .timestamp_millis_opt(retry_at_score as i64)
        .single()
        .unwrap_or_else(Utc::now);
    let _ = job_store
        .update_job_next_scheduled_run_time(&job.id, next_run_time)
        .await;

    tracing::info!(
        "rrq.job_id" = %job.id,
        retries = new_retry,
        max_retries = job.max_retries,
        retry_after_seconds = delay_seconds,
        retry_at = %next_run_time.to_rfc3339(),
        "retrying failed job"
    );
    Ok(())
}

fn calculate_backoff_seconds(settings: &RRQSettings, retry_attempt: i64) -> f64 {
    let attempt = if retry_attempt <= 0 { 1 } else { retry_attempt } as u32;
    let exponent = attempt.saturating_sub(1).min(30);
    let delay = settings.base_retry_delay_seconds * (2u64.pow(exponent) as f64);
    delay.min(settings.max_retry_delay_seconds)
}

async fn heartbeat_loop(context: HeartbeatContext) {
    let HeartbeatContext {
        shutdown,
        mut job_store,
        worker_id,
        queues,
        status,
        running_jobs,
        worker_concurrency,
        settings,
    } = context;
    while !shutdown.load(Ordering::SeqCst) {
        let status_value = { status.lock().await.clone() };
        let active_jobs = running_jobs.lock().await.len();
        let mut health_data = serde_json::Map::new();
        health_data.insert("worker_id".to_string(), Value::String(worker_id.clone()));
        health_data.insert(
            "timestamp".to_string(),
            Value::String(Utc::now().to_rfc3339()),
        );
        health_data.insert("status".to_string(), Value::String(status_value));
        health_data.insert(
            "active_jobs".to_string(),
            Value::Number((active_jobs as i64).into()),
        );
        health_data.insert(
            "concurrency_limit".to_string(),
            Value::Number((worker_concurrency as i64).into()),
        );
        health_data.insert(
            "queues".to_string(),
            Value::Array(queues.iter().map(|q| Value::String(q.clone())).collect()),
        );
        let ttl = settings.worker_health_check_interval_seconds
            + settings.worker_health_check_ttl_buffer_seconds;
        if let Err(err) = job_store
            .set_worker_health(&worker_id, &health_data, ttl as i64)
            .await
        {
            tracing::error!("failed to update worker health: {err}");
        }
        if let Err(err) = recover_orphaned_jobs(&mut job_store, &settings, &shutdown).await {
            tracing::error!("failed to recover orphaned jobs: {err}");
        }

        let sleep_duration =
            Duration::from_secs_f64(settings.worker_health_check_interval_seconds.min(60.0));
        sleep_with_shutdown(&shutdown, sleep_duration).await;
    }
}

async fn recover_orphaned_jobs(
    job_store: &mut JobStore,
    settings: &RRQSettings,
    shutdown: &Arc<AtomicBool>,
) -> Result<()> {
    const MAX_RECOVERIES_PER_TICK: u64 = 100;
    let mut cursor: u64 = 0;
    let mut recovered = 0u64;
    let mut recovery_limited = false;
    'scan: loop {
        let (next, keys) = job_store.scan_active_job_keys(cursor, 100).await?;
        for key in keys {
            if !key.starts_with(crate::constants::ACTIVE_JOBS_PREFIX) {
                continue;
            }
            let worker_id = key.trim_start_matches(crate::constants::ACTIVE_JOBS_PREFIX);
            if worker_id.is_empty() {
                continue;
            }
            let (_, health_ttl) = job_store.get_worker_health(worker_id).await?;
            if let Some(ttl) = health_ttl
                && ttl > 0
            {
                continue;
            }
            let job_ids = job_store.get_active_job_ids(worker_id).await?;
            for job_id in job_ids {
                if shutdown.load(Ordering::SeqCst) {
                    return Ok(());
                }
                if job_store.get_job_lock_owner(&job_id).await?.is_some() {
                    continue;
                }
                let job_opt = job_store.get_job_definition(&job_id).await?;
                let job = match job_opt {
                    Some(job) => job,
                    None => {
                        let _ = job_store.remove_active_job(worker_id, &job_id).await;
                        continue;
                    }
                };
                let queue_name = job
                    .queue_name
                    .clone()
                    .unwrap_or_else(|| settings.default_queue_name.clone());
                let queue_name = normalize_queue_name(&queue_name);
                let lock_timeout_ms = job
                    .job_timeout_seconds
                    .unwrap_or(settings.default_job_timeout_seconds)
                    .checked_add(settings.default_lock_timeout_extension_seconds)
                    .and_then(|sum| sum.checked_mul(1000))
                    .ok_or_else(|| anyhow::anyhow!("lock_timeout_ms overflow"))?;
                if lock_timeout_ms <= 0 {
                    continue;
                }
                let lock_owner = format!("orphan-recovery-{worker_id}");
                if !job_store
                    .try_lock_job(&job.id, &lock_owner, lock_timeout_ms)
                    .await?
                {
                    continue;
                }
                if job_store.is_job_queued(&queue_name, &job_id).await? {
                    if job.status == JobStatus::Active {
                        let _ = job_store.mark_job_pending(&job_id, None).await;
                    }
                    let _ = job_store.remove_active_job(worker_id, &job_id).await;
                    let _ = job_store
                        .release_job_lock_if_owner(&job.id, &lock_owner)
                        .await;
                    continue;
                }
                if matches!(
                    job.status,
                    JobStatus::Active | JobStatus::Pending | JobStatus::Retrying
                ) {
                    let requeue_time = job.next_scheduled_run_time.unwrap_or_else(Utc::now);
                    let score = requeue_time.timestamp_millis() as f64;
                    let requeue_message = if job.status == JobStatus::Active {
                        "Recovered after lock expiry or worker crash."
                    } else {
                        ""
                    };
                    let next_scheduled_str = requeue_time.to_rfc3339();

                    let result = match job_store
                        .atomic_requeue_job(
                            &job.id,
                            &queue_name,
                            score,
                            requeue_message,
                            &lock_owner,
                            Some(worker_id),
                            Some(&next_scheduled_str),
                        )
                        .await
                    {
                        Ok(result) => result,
                        Err(err) => {
                            tracing::warn!(
                                "atomic requeue failed for orphaned job {}: {err}",
                                job.id
                            );
                            let _ = job_store
                                .release_job_lock_if_owner(&job.id, &lock_owner)
                                .await;
                            continue;
                        }
                    };

                    if result == 1 {
                        tracing::warn!(
                            event = "rrq.orphan_requeued",
                            job_id = %job.id,
                            "atomically re-queued orphaned job after health TTL expiry"
                        );
                    }
                    let post_action = orphan_requeue_post_action(result);
                    if post_action == OrphanRequeuePostAction::PreserveActive {
                        tracing::warn!(
                            "atomic requeue returned non-cleanup result {result} for orphaned job {} (check Redis logs)",
                            job.id
                        );
                        let _ = job_store
                            .release_job_lock_if_owner(&job.id, &lock_owner)
                            .await;
                        continue;
                    }
                    // result == 0 (job gone), 1 (requeued), or 2 (became queued between checks):
                    // ensure synthetic active/lock tracking we created is cleaned.
                    // For 1/2 the atomic already performed the ZREM + owner-checked DEL.
                    let _ = job_store.remove_active_job(worker_id, &job.id).await;
                    let _ = job_store
                        .release_job_lock_if_owner(&job.id, &lock_owner)
                        .await;

                    if post_action == OrphanRequeuePostAction::CleanupAndCount {
                        recovered += 1;
                        if recovered >= MAX_RECOVERIES_PER_TICK {
                            recovery_limited = true;
                            break 'scan;
                        }
                    }
                } else {
                    let _ = job_store.remove_active_job(worker_id, &job.id).await;
                    let _ = job_store
                        .release_job_lock_if_owner(&job.id, &lock_owner)
                        .await;
                }
            }
        }
        if next == 0 {
            break;
        }
        cursor = next;
    }
    if recovered > 0 {
        telemetry::record_orphan_recovered(recovered);
        tracing::warn!(
            event = "rrq.orphan_recovery",
            recovered_jobs = recovered,
            "re-queued orphaned jobs"
        );
    }
    if recovery_limited {
        tracing::warn!(
            event = "rrq.orphan_recovery_limited",
            per_tick_limit = MAX_RECOVERIES_PER_TICK,
            "orphan recovery hit per-tick limit"
        );
    }
    Ok(())
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum OrphanRequeuePostAction {
    CleanupOnly,
    CleanupAndCount,
    PreserveActive,
}

fn orphan_requeue_post_action(result: i64) -> OrphanRequeuePostAction {
    match result {
        0 => OrphanRequeuePostAction::CleanupOnly,
        1 | 2 => OrphanRequeuePostAction::CleanupAndCount,
        _ => OrphanRequeuePostAction::PreserveActive,
    }
}

async fn cron_loop(
    shutdown: Arc<AtomicBool>,
    cron_jobs: Arc<Mutex<Vec<CronJob>>>,
    mut client: RRQClient,
    mut job_store: JobStore,
) {
    while !shutdown.load(Ordering::SeqCst) {
        let now = Utc::now();
        struct DueCronJob {
            index: usize,
            function_name: String,
            params: serde_json::Map<String, Value>,
            queue_name: Option<String>,
            unique_key: Option<String>,
        }

        let due_jobs: Vec<DueCronJob> = {
            let mut jobs = cron_jobs.lock().await;
            let mut due = Vec::new();
            for (index, job) in jobs.iter_mut().enumerate() {
                if shutdown.load(Ordering::SeqCst) {
                    return;
                }
                let due_now = match job.due(now) {
                    Ok(value) => value,
                    Err(err) => {
                        tracing::error!("cron job schedule error: {err}");
                        continue;
                    }
                };
                if !due_now {
                    continue;
                }
                let unique_key = if job.unique {
                    Some(format!("cron:{}", job.function_name))
                } else {
                    None
                };
                due.push(DueCronJob {
                    index,
                    function_name: job.function_name.clone(),
                    params: job.params.clone(),
                    queue_name: job.queue_name.clone(),
                    unique_key,
                });
            }
            due
        };

        for due in &due_jobs {
            if shutdown.load(Ordering::SeqCst) {
                return;
            }
            let mut should_enqueue = true;
            if let Some(ref key) = due.unique_key
                && let Ok(ttl) = job_store.get_lock_ttl(key).await
                && ttl > 0
            {
                should_enqueue = false;
            }
            if should_enqueue {
                let options = EnqueueOptions {
                    queue_name: due.queue_name.clone(),
                    unique_key: due.unique_key.clone(),
                    max_retries: None,
                    job_timeout_seconds: None,
                    defer_until: None,
                    defer_by: None,
                    result_ttl_seconds: None,
                    trace_context: None,
                    job_id: None,
                };
                if let Err(err) = client
                    .enqueue(&due.function_name, due.params.clone(), options)
                    .await
                {
                    tracing::error!("cron enqueue failed for {}: {err}", due.function_name);
                }
            }
        }

        if !due_jobs.is_empty() {
            let mut jobs = cron_jobs.lock().await;
            for due in due_jobs {
                if let Some(job) = jobs.get_mut(due.index) {
                    let _ = job.schedule_next(now);
                }
            }
        }
        let delay = Duration::from_secs(30);
        sleep_with_shutdown(&shutdown, delay).await;
    }
}

async fn sleep_with_shutdown(shutdown: &Arc<AtomicBool>, duration: Duration) {
    let mut remaining = duration;
    let step = Duration::from_millis(100);
    while remaining > Duration::ZERO && !shutdown.load(Ordering::SeqCst) {
        let next = if remaining > step { step } else { remaining };
        sleep(next).await;
        remaining = remaining.saturating_sub(next);
    }
}

#[cfg(test)]
mod tests;
