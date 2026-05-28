use std::collections::HashMap;

use anyhow::{Context, Result};
use chrono::{DateTime, Utc};
use redis::AsyncCommands;
use redis::Script;
use serde_json::Value;

use crate::constants::{
    ACTIVE_JOBS_PREFIX, DEFAULT_DLQ_RESULT_TTL_SECONDS, DLQ_KEY_PREFIX, HEALTH_KEY_PREFIX,
    JOB_EVENTS_KEY_PREFIX, JOB_KEY_PREFIX, LOCK_KEY_PREFIX, QUEUE_KEY_PREFIX,
    UNIQUE_JOB_LOCK_PREFIX,
};
use crate::job::{Job, JobStatus};
use rrq_config::RRQSettings;

const LOCK_AND_START_LUA: &str = include_str!("lua/lock_and_start.lua");
const CLAIM_READY_LUA: &str = include_str!("lua/claim_ready.lua");
const REFRESH_LOCK_LUA: &str = include_str!("lua/refresh_lock.lua");
const RELEASE_LOCK_IF_OWNER_LUA: &str = include_str!("lua/release_lock_if_owner.lua");
const RETRY_LUA: &str = include_str!("lua/retry.lua");
const ENQUEUE_LUA: &str = include_str!("lua/enqueue.lua");
const MOVE_TO_DLQ_LUA: &str = include_str!("lua/move_to_dlq.lua");
const REQUEUE_JOB_LUA: &str = include_str!("lua/requeue_job.lua");

fn summarize_redis_dsn(dsn: &str) -> String {
    let (scheme, rest) = dsn.split_once("://").unwrap_or(("", dsn));
    let without_auth = rest.rsplit('@').next().unwrap_or(rest);
    let host = without_auth
        .split(['/', '?', '#'])
        .next()
        .unwrap_or(without_auth);

    if scheme.is_empty() {
        host.to_string()
    } else if host.is_empty() {
        format!("{scheme}://")
    } else {
        format!("{scheme}://{host}")
    }
}

fn format_job_events_key(job_id: &str) -> String {
    format!("{JOB_EVENTS_KEY_PREFIX}{job_id}")
}

fn is_tls_handshake_error(err: &redis::RedisError) -> bool {
    let message = err.to_string().to_ascii_lowercase();
    message.contains("tls handshake") || message.contains("handshake eof")
}

fn redis_connect_context(dsn: &str, err: &redis::RedisError) -> String {
    let summary = summarize_redis_dsn(dsn);
    let mut context = if summary.is_empty() {
        "failed to connect to Redis".to_string()
    } else {
        format!("failed to connect to Redis ({summary})")
    };

    if is_tls_handshake_error(err) {
        if dsn.starts_with("rediss://") || dsn.starts_with("valkeys://") {
            context.push_str(
                "; TLS handshake failed - verify the endpoint supports TLS and the port/certs are correct",
            );
        } else {
            context.push_str(
                "; TLS handshake failed - if Redis requires TLS, use rediss:// for the DSN",
            );
        }
    }

    context
}

#[derive(Clone)]
pub struct JobStore {
    settings: RRQSettings,
    conn: redis::aio::MultiplexedConnection,
    lock_and_start_script: Script,
    claim_ready_script: Script,
    refresh_lock_script: Script,
    release_lock_if_owner_script: Script,
    retry_script: Script,
    enqueue_script: Script,
    move_to_dlq_script: Script,
    requeue_job_script: Script,
}

impl JobStore {
    pub async fn new(settings: RRQSettings) -> Result<Self> {
        let client = redis::Client::open(settings.redis_dsn.as_str())
            .with_context(|| "failed to create Redis client")?;
        let conn = client
            .get_multiplexed_async_connection()
            .await
            .map_err(|err| {
                let context = redis_connect_context(&settings.redis_dsn, &err);
                anyhow::Error::new(err).context(context)
            })?;
        Ok(Self::with_connection(settings, conn))
    }

    pub fn with_connection(settings: RRQSettings, conn: redis::aio::MultiplexedConnection) -> Self {
        let lock_and_start_script = Script::new(LOCK_AND_START_LUA);
        let claim_ready_script = Script::new(CLAIM_READY_LUA);
        let refresh_lock_script = Script::new(REFRESH_LOCK_LUA);
        let release_lock_if_owner_script = Script::new(RELEASE_LOCK_IF_OWNER_LUA);
        let retry_script = Script::new(RETRY_LUA);
        let enqueue_script = Script::new(ENQUEUE_LUA);
        let move_to_dlq_script = Script::new(MOVE_TO_DLQ_LUA);
        let requeue_job_script = Script::new(REQUEUE_JOB_LUA);

        Self {
            settings,
            conn,
            lock_and_start_script,
            claim_ready_script,
            refresh_lock_script,
            release_lock_if_owner_script,
            retry_script,
            enqueue_script,
            move_to_dlq_script,
            requeue_job_script,
        }
    }

    pub fn settings(&self) -> &RRQSettings {
        &self.settings
    }

    pub fn clone_settings(&self) -> RRQSettings {
        self.settings.clone()
    }

    fn format_queue_key(&self, queue_name: &str) -> String {
        if queue_name.starts_with(QUEUE_KEY_PREFIX) {
            queue_name.to_string()
        } else {
            format!("{QUEUE_KEY_PREFIX}{queue_name}")
        }
    }

    fn format_dlq_key(&self, dlq_name: &str) -> String {
        if dlq_name.starts_with(DLQ_KEY_PREFIX) {
            dlq_name.to_string()
        } else {
            format!("{DLQ_KEY_PREFIX}{dlq_name}")
        }
    }

    fn active_jobs_key(worker_id: &str) -> String {
        format!("{ACTIVE_JOBS_PREFIX}{worker_id}")
    }

    fn parse_datetime(raw: &str) -> Option<DateTime<Utc>> {
        DateTime::parse_from_rfc3339(raw)
            .ok()
            .map(|dt| dt.with_timezone(&Utc))
    }

    fn parse_json(raw: &str) -> Option<Value> {
        if raw.is_empty() || raw.eq_ignore_ascii_case("null") {
            return None;
        }
        serde_json::from_str(raw).ok()
    }

    fn build_job_mapping(job: &Job) -> Result<Vec<(String, String)>> {
        let job_params_json = serde_json::to_string(&job.job_params)?;
        let result_json = serde_json::to_string(&job.result)?;

        let mut mapping: Vec<(String, String)> = vec![
            ("id".to_string(), job.id.clone()),
            ("function_name".to_string(), job.function_name.clone()),
            ("job_params".to_string(), job_params_json),
            ("enqueue_time".to_string(), job.enqueue_time.to_rfc3339()),
            ("status".to_string(), job.status.as_str().to_string()),
            (
                "current_retries".to_string(),
                job.current_retries.to_string(),
            ),
            ("max_retries".to_string(), job.max_retries.to_string()),
            ("result".to_string(), result_json),
        ];

        if let Some(value) = job.queue_name.as_ref() {
            mapping.push(("queue_name".to_string(), value.clone()));
        }
        if let Some(value) = job.next_scheduled_run_time {
            mapping.push(("next_scheduled_run_time".to_string(), value.to_rfc3339()));
        }
        if let Some(value) = job.start_time {
            mapping.push(("start_time".to_string(), value.to_rfc3339()));
        }
        if let Some(value) = job.job_timeout_seconds {
            mapping.push(("job_timeout_seconds".to_string(), value.to_string()));
        }
        if let Some(value) = job.result_ttl_seconds {
            mapping.push(("result_ttl_seconds".to_string(), value.to_string()));
        }
        if let Some(value) = job.job_unique_key.as_ref() {
            mapping.push(("job_unique_key".to_string(), value.clone()));
        }
        if let Some(value) = job.completion_time {
            mapping.push(("completion_time".to_string(), value.to_rfc3339()));
        }
        if let Some(value) = job.last_error.as_ref() {
            mapping.push(("last_error".to_string(), value.clone()));
        }
        if let Some(value) = job.dlq_name.as_ref() {
            mapping.push(("dlq_name".to_string(), value.clone()));
        }
        if let Some(value) = job.worker_id.as_ref() {
            mapping.push(("worker_id".to_string(), value.clone()));
        }
        if let Some(value) = job.trace_context.as_ref() {
            let trace_json = serde_json::to_string(value)?;
            mapping.push(("trace_context".to_string(), trace_json));
        }
        if let Some(value) = job.correlation_context.as_ref() {
            let correlation_json = serde_json::to_string(value)?;
            mapping.push(("correlation_context".to_string(), correlation_json));
        }

        Ok(mapping)
    }

    pub async fn save_job_definition(&mut self, job: &Job) -> Result<()> {
        let job_key = format!("{JOB_KEY_PREFIX}{}", job.id);
        let mapping = Self::build_job_mapping(job)?;

        let mapping_ref: Vec<(&str, &str)> = mapping
            .iter()
            .map(|(key, value)| (key.as_str(), value.as_str()))
            .collect();

        self.conn
            .hset_multiple::<_, _, _, ()>(&job_key, &mapping_ref)
            .await?;

        Ok(())
    }

    pub async fn atomic_enqueue_job(
        &mut self,
        job: &Job,
        queue_name: &str,
        score_ms: f64,
    ) -> Result<bool> {
        let job_key = format!("{JOB_KEY_PREFIX}{}", job.id);
        let queue_key = self.format_queue_key(queue_name);
        let mapping = Self::build_job_mapping(job)?;
        let mut args: Vec<String> = Vec::with_capacity(mapping.len() * 2 + 2);
        for (key, value) in mapping {
            args.push(key);
            args.push(value);
        }
        args.push(score_ms.to_string());
        args.push(job.id.clone());
        let script = self.enqueue_script.clone();
        let mut invocation = script.key(job_key);
        invocation.key(queue_key);
        for arg in &args {
            invocation.arg(arg);
        }
        let created: i64 = invocation.invoke_async(&mut self.conn).await?;
        Ok(created == 1)
    }

    pub async fn get_job_definition(&mut self, job_id: &str) -> Result<Option<Job>> {
        let job_key = format!("{JOB_KEY_PREFIX}{job_id}");
        let raw: HashMap<String, String> = self.conn.hgetall(job_key).await?;
        if raw.is_empty() {
            return Ok(None);
        }
        Ok(Some(Self::parse_job_map(
            raw,
            job_id,
            self.settings.default_max_retries,
        )?))
    }

    pub async fn get_job_definitions(&mut self, job_ids: &[String]) -> Result<Vec<Option<Job>>> {
        let maps = self.get_job_data_maps(job_ids).await?;
        let mut jobs = Vec::with_capacity(maps.len());
        for (index, map) in maps.into_iter().enumerate() {
            let job = match map {
                Some(map) => {
                    let fallback_id = job_ids
                        .get(index)
                        .cloned()
                        .unwrap_or_else(|| "unknown".to_string());
                    Self::parse_job_map(map, &fallback_id, self.settings.default_max_retries).ok()
                }
                None => None,
            };
            jobs.push(job);
        }
        Ok(jobs)
    }

    fn parse_job_map(
        raw: HashMap<String, String>,
        fallback_id: &str,
        default_max_retries: i64,
    ) -> Result<Job> {
        let job_params = raw
            .get("job_params")
            .and_then(|value| serde_json::from_str(value).ok())
            .unwrap_or_default();
        let result = raw.get("result").and_then(|value| Self::parse_json(value));
        let trace_context = raw.get("trace_context").and_then(|value| {
            if value.eq_ignore_ascii_case("null") {
                return None;
            }
            serde_json::from_str(value).ok()
        });
        let correlation_context = raw.get("correlation_context").and_then(|value| {
            if value.eq_ignore_ascii_case("null") {
                return None;
            }
            serde_json::from_str(value).ok()
        });

        let status = raw
            .get("status")
            .and_then(|value| JobStatus::parse(value))
            .ok_or_else(|| anyhow::anyhow!("invalid job status"))?;
        let enqueue_time = raw
            .get("enqueue_time")
            .and_then(|value| Self::parse_datetime(value))
            .ok_or_else(|| anyhow::anyhow!("missing enqueue_time"))?;
        let current_retries = raw
            .get("current_retries")
            .and_then(|value| value.parse::<i64>().ok())
            .unwrap_or(0);
        let max_retries = raw
            .get("max_retries")
            .and_then(|value| value.parse::<i64>().ok())
            .unwrap_or(default_max_retries);

        Ok(Job {
            id: raw
                .get("id")
                .cloned()
                .unwrap_or_else(|| fallback_id.to_string()),
            function_name: raw.get("function_name").cloned().unwrap_or_default(),
            job_params,
            enqueue_time,
            start_time: raw
                .get("start_time")
                .and_then(|value| Self::parse_datetime(value)),
            status,
            current_retries,
            next_scheduled_run_time: raw
                .get("next_scheduled_run_time")
                .and_then(|value| Self::parse_datetime(value)),
            max_retries,
            job_timeout_seconds: raw
                .get("job_timeout_seconds")
                .and_then(|value| value.parse::<i64>().ok()),
            result_ttl_seconds: raw
                .get("result_ttl_seconds")
                .and_then(|value| value.parse::<i64>().ok()),
            job_unique_key: raw.get("job_unique_key").cloned(),
            completion_time: raw
                .get("completion_time")
                .and_then(|value| Self::parse_datetime(value)),
            result,
            last_error: raw.get("last_error").cloned(),
            queue_name: raw.get("queue_name").cloned(),
            dlq_name: raw.get("dlq_name").cloned(),
            worker_id: raw.get("worker_id").cloned(),
            trace_context,
            correlation_context,
        })
    }

    pub async fn add_job_to_queue(
        &mut self,
        queue_name: &str,
        job_id: &str,
        score_ms: f64,
    ) -> Result<()> {
        let queue_key = self.format_queue_key(queue_name);
        self.conn
            .zadd::<_, _, _, ()>(&queue_key, job_id, score_ms)
            .await?;
        Ok(())
    }

    pub async fn get_job_data_map(
        &mut self,
        job_id: &str,
    ) -> Result<Option<HashMap<String, String>>> {
        let job_key = format!("{JOB_KEY_PREFIX}{job_id}");
        self.get_job_data_map_by_key(&job_key).await
    }

    pub async fn get_job_data_map_by_key(
        &mut self,
        job_key: &str,
    ) -> Result<Option<HashMap<String, String>>> {
        let raw: HashMap<String, String> = self.conn.hgetall(job_key).await?;
        if raw.is_empty() {
            return Ok(None);
        }
        Ok(Some(raw))
    }

    pub async fn scan_job_keys(&mut self, cursor: u64, count: usize) -> Result<(u64, Vec<String>)> {
        let pattern = format!("{JOB_KEY_PREFIX}*");
        let (next, keys): (u64, Vec<String>) = redis::cmd("SCAN")
            .arg(cursor)
            .arg("MATCH")
            .arg(pattern)
            .arg("COUNT")
            .arg(count)
            .query_async(&mut self.conn)
            .await?;
        Ok((next, keys))
    }

    pub async fn get_job_data_maps(
        &mut self,
        job_ids: &[String],
    ) -> Result<Vec<Option<HashMap<String, String>>>> {
        if job_ids.is_empty() {
            return Ok(Vec::new());
        }
        let mut pipe = redis::pipe();
        for job_id in job_ids {
            let job_key = format!("{JOB_KEY_PREFIX}{job_id}");
            pipe.hgetall(job_key);
        }
        let results: Vec<HashMap<String, String>> = pipe.query_async(&mut self.conn).await?;
        Ok(results
            .into_iter()
            .map(|map| if map.is_empty() { None } else { Some(map) })
            .collect())
    }

    pub async fn queue_exists(&mut self, queue_name: &str) -> Result<bool> {
        let queue_key = self.format_queue_key(queue_name);
        let exists: bool = self.conn.exists(queue_key).await?;
        Ok(exists)
    }

    pub async fn delete_keys_by_pattern(&mut self, pattern: &str) -> Result<usize> {
        let mut cursor = 0u64;
        let mut deleted = 0usize;
        loop {
            let (next, keys): (u64, Vec<String>) = redis::cmd("SCAN")
                .arg(cursor)
                .arg("MATCH")
                .arg(pattern)
                .arg("COUNT")
                .arg(200)
                .query_async(&mut self.conn)
                .await?;
            if !keys.is_empty() {
                let _: i64 = redis::cmd("DEL")
                    .arg(keys.clone())
                    .query_async(&mut self.conn)
                    .await?;
                deleted += keys.len();
            }
            if next == 0 {
                break;
            }
            cursor = next;
        }
        Ok(deleted)
    }

    pub async fn scan_keys_by_pattern(&mut self, pattern: &str) -> Result<Vec<String>> {
        let mut cursor = 0u64;
        let mut keys = Vec::new();
        loop {
            let (next, batch): (u64, Vec<String>) = redis::cmd("SCAN")
                .arg(cursor)
                .arg("MATCH")
                .arg(pattern)
                .arg("COUNT")
                .arg(200)
                .query_async(&mut self.conn)
                .await?;
            keys.extend(batch);
            if next == 0 {
                break;
            }
            cursor = next;
        }
        Ok(keys)
    }

    pub async fn update_job_status(&mut self, job_id: &str, status: JobStatus) -> Result<()> {
        let job_key = format!("{JOB_KEY_PREFIX}{job_id}");
        self.conn
            .hset::<_, _, _, ()>(job_key, "status", status.as_str())
            .await?;
        Ok(())
    }

    pub async fn update_job_fields(
        &mut self,
        job_id: &str,
        fields: &HashMap<String, String>,
    ) -> Result<()> {
        if fields.is_empty() {
            return Ok(());
        }
        let job_key = format!("{JOB_KEY_PREFIX}{job_id}");
        let mapping_ref: Vec<(&str, &str)> = fields
            .iter()
            .map(|(key, value)| (key.as_str(), value.as_str()))
            .collect();
        self.conn
            .hset_multiple::<_, _, _, ()>(&job_key, &mapping_ref)
            .await?;
        Ok(())
    }

    pub async fn dlq_len(&mut self, dlq_name: &str) -> Result<i64> {
        let key = self.format_dlq_key(dlq_name);
        let len: i64 = self.conn.llen(key).await?;
        Ok(len)
    }

    pub async fn dlq_remove_job(&mut self, dlq_name: &str, job_id: &str) -> Result<i64> {
        let key = self.format_dlq_key(dlq_name);
        let removed: i64 = self.conn.lrem(key, 1, job_id).await?;
        Ok(removed)
    }

    pub async fn get_ready_job_ids(
        &mut self,
        queue_name: &str,
        count: usize,
    ) -> Result<Vec<String>> {
        if count == 0 {
            return Ok(Vec::new());
        }
        let queue_key = self.format_queue_key(queue_name);
        let now_ms = Utc::now().timestamp_millis();
        let ids: Vec<String> = redis::cmd("ZRANGEBYSCORE")
            .arg(&queue_key)
            .arg("-inf")
            .arg(now_ms)
            .arg("LIMIT")
            .arg(0)
            .arg(count)
            .query_async(&mut self.conn)
            .await?;
        Ok(ids)
    }

    pub async fn remove_job_from_queue(&mut self, queue_name: &str, job_id: &str) -> Result<i64> {
        let queue_key = self.format_queue_key(queue_name);
        let removed: i64 = self.conn.zrem(queue_key, job_id).await?;
        Ok(removed)
    }

    pub async fn atomic_lock_and_start_job(
        &mut self,
        job_id: &str,
        queue_name: &str,
        worker_id: &str,
        lock_timeout_ms: i64,
        start_time: DateTime<Utc>,
    ) -> Result<(bool, i64)> {
        let lock_key = format!("{LOCK_KEY_PREFIX}{job_id}");
        let queue_key = self.format_queue_key(queue_name);
        let job_key = format!("{JOB_KEY_PREFIX}{job_id}");
        let active_key = Self::active_jobs_key(worker_id);
        let start_time_str = start_time.to_rfc3339();
        let active_score = start_time.timestamp() as f64;
        let result: (i64, i64) = self
            .lock_and_start_script
            .key(lock_key)
            .key(queue_key)
            .key(job_key)
            .key(active_key)
            .arg(worker_id)
            .arg(lock_timeout_ms)
            .arg(job_id)
            .arg(start_time_str)
            .arg(active_score)
            .invoke_async(&mut self.conn)
            .await?;
        Ok((result.0 != 0, result.1))
    }

    pub async fn atomic_claim_ready_jobs(
        &mut self,
        queue_name: &str,
        worker_id: &str,
        default_lock_timeout_ms: i64,
        lock_timeout_extension_seconds: i64,
        max_claims: usize,
        start_time: DateTime<Utc>,
    ) -> Result<Vec<String>> {
        if max_claims == 0 {
            return Ok(Vec::new());
        }
        if default_lock_timeout_ms <= 0 {
            return Err(anyhow::anyhow!("lock_timeout_ms must be positive"));
        }
        if lock_timeout_extension_seconds < 0 {
            return Err(anyhow::anyhow!(
                "lock_timeout_extension_seconds must be >= 0"
            ));
        }
        let queue_key = self.format_queue_key(queue_name);
        let active_key = Self::active_jobs_key(worker_id);
        let now_ms = Utc::now().timestamp_millis();
        let start_time_str = start_time.to_rfc3339();
        let active_score = start_time.timestamp() as f64;
        let claimed: Vec<String> = self
            .claim_ready_script
            .key(queue_key)
            .key(active_key)
            .arg(worker_id)
            .arg(now_ms)
            .arg(max_claims)
            .arg(default_lock_timeout_ms)
            .arg(lock_timeout_extension_seconds)
            .arg(start_time_str)
            .arg(active_score)
            .invoke_async(&mut self.conn)
            .await?;
        Ok(claimed)
    }

    pub async fn refresh_job_lock_timeout(
        &mut self,
        job_id: &str,
        worker_id: &str,
        lock_timeout_ms: i64,
    ) -> Result<bool> {
        if lock_timeout_ms <= 0 {
            return Err(anyhow::anyhow!("lock_timeout_ms must be positive"));
        }
        let lock_key = format!("{LOCK_KEY_PREFIX}{job_id}");
        let refreshed: i64 = self
            .refresh_lock_script
            .key(lock_key)
            .arg(worker_id)
            .arg(lock_timeout_ms)
            .invoke_async(&mut self.conn)
            .await?;
        Ok(refreshed != 0)
    }

    pub async fn release_job_lock(&mut self, job_id: &str) -> Result<()> {
        let lock_key = format!("{LOCK_KEY_PREFIX}{job_id}");
        let _: i64 = self.conn.del(lock_key).await?;
        Ok(())
    }

    pub async fn release_job_lock_if_owner(
        &mut self,
        job_id: &str,
        worker_id: &str,
    ) -> Result<bool> {
        let lock_key = format!("{LOCK_KEY_PREFIX}{job_id}");
        let released: i64 = self
            .release_lock_if_owner_script
            .key(lock_key)
            .arg(worker_id)
            .invoke_async(&mut self.conn)
            .await?;
        Ok(released != 0)
    }

    pub async fn try_lock_job(
        &mut self,
        job_id: &str,
        worker_id: &str,
        lock_timeout_ms: i64,
    ) -> Result<bool> {
        let lock_key = format!("{LOCK_KEY_PREFIX}{job_id}");
        let result: Option<String> = redis::cmd("SET")
            .arg(lock_key)
            .arg(worker_id)
            .arg("NX")
            .arg("PX")
            .arg(lock_timeout_ms)
            .query_async(&mut self.conn)
            .await?;
        Ok(result.is_some())
    }

    pub async fn get_job_lock_owner(&mut self, job_id: &str) -> Result<Option<String>> {
        let lock_key = format!("{LOCK_KEY_PREFIX}{job_id}");
        let owner: Option<String> = self.conn.get(lock_key).await?;
        Ok(owner)
    }

    pub async fn mark_job_started(
        &mut self,
        job_id: &str,
        worker_id: &str,
        start_time: DateTime<Utc>,
    ) -> Result<()> {
        let job_key = format!("{JOB_KEY_PREFIX}{job_id}");
        let mapping: Vec<(&str, String)> = vec![
            ("status", JobStatus::Active.as_str().to_string()),
            ("start_time", start_time.to_rfc3339()),
            ("worker_id", worker_id.to_string()),
        ];
        let mapping_ref: Vec<(&str, &str)> = mapping
            .iter()
            .map(|(key, value)| (*key, value.as_str()))
            .collect();
        self.conn
            .hset_multiple::<_, _, _, ()>(job_key, &mapping_ref)
            .await?;
        self.track_active_job(worker_id, job_id, start_time).await?;
        Ok(())
    }

    pub async fn mark_job_pending(&mut self, job_id: &str, last_error: Option<&str>) -> Result<()> {
        let job_key = format!("{JOB_KEY_PREFIX}{job_id}");
        let mut pipe = redis::pipe();
        pipe.atomic();
        pipe.hset(&job_key, "status", JobStatus::Pending.as_str());
        if let Some(error) = last_error {
            pipe.hset(&job_key, "last_error", error);
        }
        pipe.hdel(&job_key, "start_time");
        pipe.hdel(&job_key, "worker_id");
        pipe.query_async::<()>(&mut self.conn).await?;
        Ok(())
    }

    pub async fn update_job_next_scheduled_run_time(
        &mut self,
        job_id: &str,
        run_time: DateTime<Utc>,
    ) -> Result<()> {
        let job_key = format!("{JOB_KEY_PREFIX}{job_id}");
        self.conn
            .hset::<_, _, _, ()>(job_key, "next_scheduled_run_time", run_time.to_rfc3339())
            .await?;
        Ok(())
    }

    pub async fn atomic_retry_job(
        &mut self,
        job_id: &str,
        queue_name: &str,
        retry_at_score: f64,
        error_message: &str,
        status: JobStatus,
    ) -> Result<i64> {
        let job_key = format!("{JOB_KEY_PREFIX}{job_id}");
        let queue_key = self.format_queue_key(queue_name);
        let new_retry_count: i64 = self
            .retry_script
            .key(job_key)
            .key(queue_key)
            .arg(job_id)
            .arg(retry_at_score)
            .arg(error_message)
            .arg(status.as_str())
            .invoke_async(&mut self.conn)
            .await?;
        Ok(new_retry_count)
    }

    /// Atomically increment retries, mark FAILED, record failure event, push to DLQ,
    /// set TTLs, and optionally release a unique lock key. Replaces the previous split
    /// `increment_job_retries` + `move_job_to_dlq` + conditional release pattern.
    ///
    /// All keys touched by the script (including the optional unique lock) are declared
    /// in the KEYS array for Redis Cluster compatibility.
    ///
    /// Returns the new retry count on success, or -1 on script error (pcall sentinel per move_to_dlq.lua header; check Redis server logs).
    pub async fn atomic_move_job_to_dlq(
        &mut self,
        job_id: &str,
        dlq_name: &str,
        error_message: &str,
        completion_time: DateTime<Utc>,
        dlq_result_ttl_seconds: i64,
        unique_lock_key: Option<&str>,
    ) -> Result<i64> {
        let job_key = format!("{JOB_KEY_PREFIX}{job_id}");
        let events_key = format_job_events_key(job_id);
        let dlq_key = self.format_dlq_key(dlq_name);
        let unique = unique_lock_key.unwrap_or("");

        let retry_count: i64 = self
            .move_to_dlq_script
            .key(job_key)
            .key(events_key)
            .key(dlq_key)
            .key(unique)
            .arg(job_id)
            .arg(error_message)
            .arg(completion_time.to_rfc3339())
            .arg(dlq_result_ttl_seconds)
            .invoke_async(&mut self.conn)
            .await?;

        Ok(retry_count)
    }

    /// Atomically re-queue a job (ZADD with provided score), optionally demote ACTIVE→PENDING,
    /// clean up active set + owner-checked lock release, and (for orphan/cron paths) restore
    /// next_scheduled_run_time. This is the single-roundtrip replacement for the multi-command
    /// walks in drain_tasks and recover_orphaned_jobs.
    ///
    /// Returns:
    ///   0 = job did not exist (no-op)
    ///   1 = successfully re-queued
    ///   2 = already present in target queue (cleaned tracking only)
    ///  -1 = script error (check Redis logs; partial commands before failure may have executed)
    #[allow(clippy::too_many_arguments)] // Mirrors the 9-value Lua script contract (4 keys + 5 args); callers supply distinct semantic values.
    pub async fn atomic_requeue_job(
        &mut self,
        job_id: &str,
        queue_name: &str,
        score: f64,
        requeue_message: &str,
        releasing_owner: &str,
        active_worker_id: Option<&str>,
        next_scheduled_run_time: Option<&str>,
    ) -> Result<i64> {
        let job_key = format!("{JOB_KEY_PREFIX}{job_id}");
        let queue_key = self.format_queue_key(queue_name);
        let active_key = active_worker_id
            .map(|w| format!("{ACTIVE_JOBS_PREFIX}{w}"))
            .unwrap_or_default();
        let lock_key = format!("{LOCK_KEY_PREFIX}{job_id}");
        let next_sched = next_scheduled_run_time.unwrap_or("");

        let result: i64 = self
            .requeue_job_script
            .key(job_key)
            .key(queue_key)
            .key(active_key)
            .key(lock_key)
            .arg(job_id)
            .arg(score)
            .arg(requeue_message)
            .arg(releasing_owner)
            .arg(next_sched)
            .invoke_async(&mut self.conn)
            .await?;

        Ok(result)
    }

    pub async fn increment_job_retries(&mut self, job_id: &str) -> Result<i64> {
        let job_key = format!("{JOB_KEY_PREFIX}{job_id}");
        let new_retry_count: i64 = self.conn.hincr(job_key, "current_retries", 1).await?;
        Ok(new_retry_count)
    }

    pub async fn move_job_to_dlq(
        &mut self,
        job_id: &str,
        dlq_name: &str,
        error_message: &str,
        completion_time: DateTime<Utc>,
    ) -> Result<()> {
        let job_key = format!("{JOB_KEY_PREFIX}{job_id}");
        let events_key = format_job_events_key(job_id);
        let dlq_key = self.format_dlq_key(dlq_name);
        let mut pipe = redis::pipe();
        pipe.atomic();
        pipe.hset(&job_key, "status", JobStatus::Failed.as_str());
        pipe.hset(&job_key, "last_error", error_message);
        pipe.hset(&job_key, "completion_time", completion_time.to_rfc3339());
        pipe.cmd("XADD")
            .arg(&events_key)
            .arg("*")
            .arg("event")
            .arg("failed")
            .arg("job_id")
            .arg(job_id)
            .arg("status")
            .arg(JobStatus::Failed.as_str());
        pipe.lpush(&dlq_key, job_id);
        pipe.expire(&job_key, DEFAULT_DLQ_RESULT_TTL_SECONDS);
        pipe.expire(&events_key, DEFAULT_DLQ_RESULT_TTL_SECONDS);
        pipe.query_async::<()>(&mut self.conn).await?;
        Ok(())
    }

    pub async fn save_job_result(
        &mut self,
        job_id: &str,
        result: &Value,
        ttl_seconds: i64,
    ) -> Result<()> {
        let job_key = format!("{JOB_KEY_PREFIX}{job_id}");
        let events_key = format_job_events_key(job_id);
        let completion_time = Utc::now();
        let result_json = serde_json::to_string(result)?;
        let mut pipe = redis::pipe();
        pipe.atomic();
        pipe.hset(&job_key, "result", result_json);
        pipe.hset(&job_key, "completion_time", completion_time.to_rfc3339());
        pipe.hset(&job_key, "status", JobStatus::Completed.as_str());
        pipe.cmd("XADD")
            .arg(&events_key)
            .arg("*")
            .arg("event")
            .arg("completed")
            .arg("job_id")
            .arg(job_id)
            .arg("status")
            .arg(JobStatus::Completed.as_str());
        if ttl_seconds > 0 {
            pipe.expire(&job_key, ttl_seconds);
            pipe.expire(&events_key, ttl_seconds);
        } else if ttl_seconds == 0 {
            pipe.persist(&job_key);
            pipe.persist(&events_key);
        }
        pipe.query_async::<()>(&mut self.conn).await?;
        Ok(())
    }

    pub async fn track_active_job(
        &mut self,
        worker_id: &str,
        job_id: &str,
        start_time: DateTime<Utc>,
    ) -> Result<()> {
        let active_key = Self::active_jobs_key(worker_id);
        let score = start_time.timestamp() as f64;
        self.conn
            .zadd::<_, _, _, ()>(active_key, job_id, score)
            .await?;
        Ok(())
    }

    pub async fn remove_active_job(&mut self, worker_id: &str, job_id: &str) -> Result<()> {
        let active_key = Self::active_jobs_key(worker_id);
        let _: i64 = self.conn.zrem(active_key, job_id).await?;
        Ok(())
    }

    pub async fn acquire_unique_job_lock(
        &mut self,
        unique_key: &str,
        job_id: &str,
        lock_ttl_seconds: i64,
    ) -> Result<bool> {
        let lock_key = format!("{UNIQUE_JOB_LOCK_PREFIX}{unique_key}");
        let result: Option<String> = redis::cmd("SET")
            .arg(&lock_key)
            .arg(job_id)
            .arg("NX")
            .arg("EX")
            .arg(lock_ttl_seconds)
            .query_async(&mut self.conn)
            .await?;
        Ok(result.is_some())
    }

    pub async fn release_unique_job_lock(&mut self, unique_key: &str) -> Result<()> {
        let lock_key = format!("{UNIQUE_JOB_LOCK_PREFIX}{unique_key}");
        let _: i64 = self.conn.del(lock_key).await?;
        Ok(())
    }

    pub async fn get_lock_ttl(&mut self, unique_key: &str) -> Result<i64> {
        let lock_key = format!("{UNIQUE_JOB_LOCK_PREFIX}{unique_key}");
        let ttl: i64 = self.conn.ttl(lock_key).await?;
        Ok(if ttl > 0 { ttl } else { 0 })
    }

    pub async fn set_worker_health(
        &mut self,
        worker_id: &str,
        data: &serde_json::Map<String, Value>,
        ttl_seconds: i64,
    ) -> Result<()> {
        let key = format!("rrq:health:worker:{worker_id}");
        let payload = serde_json::to_string(data)?;
        redis::cmd("SET")
            .arg(key)
            .arg(payload)
            .arg("EX")
            .arg(ttl_seconds)
            .query_async::<()>(&mut self.conn)
            .await?;
        Ok(())
    }

    pub async fn get_worker_health(
        &mut self,
        worker_id: &str,
    ) -> Result<(Option<serde_json::Map<String, Value>>, Option<i64>)> {
        let key = format!("rrq:health:worker:{worker_id}");
        let mut pipe = redis::pipe();
        pipe.atomic();
        pipe.get(&key);
        pipe.ttl(&key);
        let (payload, ttl): (Option<String>, i64) = pipe.query_async(&mut self.conn).await?;
        let payload = match payload {
            Some(value) => value,
            None => return Ok((None, None)),
        };
        let parsed: serde_json::Map<String, Value> =
            serde_json::from_str(&payload).unwrap_or_default();
        let ttl = if ttl >= 0 { Some(ttl) } else { None };
        Ok((Some(parsed), ttl))
    }

    pub async fn scan_active_job_keys(
        &mut self,
        cursor: u64,
        count: usize,
    ) -> Result<(u64, Vec<String>)> {
        let pattern = format!("{ACTIVE_JOBS_PREFIX}*");
        let (next, keys): (u64, Vec<String>) = redis::cmd("SCAN")
            .arg(cursor)
            .arg("MATCH")
            .arg(pattern)
            .arg("COUNT")
            .arg(count)
            .query_async(&mut self.conn)
            .await?;
        Ok((next, keys))
    }

    pub async fn scan_queue_keys(
        &mut self,
        cursor: u64,
        count: usize,
    ) -> Result<(u64, Vec<String>)> {
        let pattern = format!("{QUEUE_KEY_PREFIX}*");
        let (next, keys): (u64, Vec<String>) = redis::cmd("SCAN")
            .arg(cursor)
            .arg("MATCH")
            .arg(pattern)
            .arg("COUNT")
            .arg(count)
            .query_async(&mut self.conn)
            .await?;
        Ok((next, keys))
    }

    pub async fn queue_size(&mut self, queue_name: &str) -> Result<i64> {
        let key = self.format_queue_key(queue_name);
        let size: i64 = self.conn.zcard(key).await?;
        Ok(size)
    }

    pub async fn queue_range_with_scores(
        &mut self,
        queue_name: &str,
        start: isize,
        stop: isize,
    ) -> Result<Vec<(String, f64)>> {
        let key = self.format_queue_key(queue_name);
        let entries: Vec<(String, f64)> = self.conn.zrange_withscores(key, start, stop).await?;
        Ok(entries)
    }

    pub async fn scan_worker_health_keys(
        &mut self,
        cursor: u64,
        count: usize,
    ) -> Result<(u64, Vec<String>)> {
        let pattern = format!("{HEALTH_KEY_PREFIX}*");
        let (next, keys): (u64, Vec<String>) = redis::cmd("SCAN")
            .arg(cursor)
            .arg("MATCH")
            .arg(pattern)
            .arg("COUNT")
            .arg(count)
            .query_async(&mut self.conn)
            .await?;
        Ok((next, keys))
    }

    pub async fn get_active_job_ids(&mut self, worker_id: &str) -> Result<Vec<String>> {
        let key = Self::active_jobs_key(worker_id);
        let ids: Vec<String> = self.conn.zrange(key, 0, -1).await?;
        Ok(ids)
    }

    pub async fn is_job_queued(&mut self, queue_name: &str, job_id: &str) -> Result<bool> {
        let key = self.format_queue_key(queue_name);
        let score: Option<f64> = self.conn.zscore(key, job_id).await?;
        Ok(score.is_some())
    }

    pub async fn get_dlq_job_ids(&mut self, dlq_name: &str) -> Result<Vec<String>> {
        let key = self.format_dlq_key(dlq_name);
        let ids: Vec<String> = self.conn.lrange(key, 0, -1).await?;
        Ok(ids)
    }

    pub async fn flushdb(&mut self) -> Result<()> {
        redis::cmd("FLUSHDB")
            .query_async::<()>(&mut self.conn)
            .await?;
        Ok(())
    }
}

#[cfg(test)]
mod tests;
