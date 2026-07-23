use std::str::FromStr;

use anyhow::{Context, Result};
use chrono::{DateTime, Utc};
use serde_json::{Value, json};
use sqlx::sqlite::{SqliteConnectOptions, SqliteJournalMode, SqlitePoolOptions};
use sqlx::{Row, SqlitePool, Transaction};
use thieving_eyes_protocol::{
    ArtifactRef, Attempt, AttemptState, Blocker, ErrorDetail, EventEnvelope, OutputValue,
    ResourceRef, RuntimeRef, SubmissionAccepted, SubmissionCreate, SubmissionResult,
    SubmissionState, SubmissionStatus,
};
use thiserror::Error;
use tokio::sync::broadcast;
use ulid::Ulid;

#[derive(Debug, Error)]
pub enum StoreError {
    #[error("resource not found")]
    NotFound,
    #[error("idempotency key was already used for a different request")]
    IdempotencyConflict,
    #[error("resource revision no longer matches")]
    RevisionConflict,
    #[error("submission is not queued")]
    NotQueued,
    #[error("workspace has an active writable attempt")]
    WorkspaceBusy,
    #[error(transparent)]
    Internal(#[from] anyhow::Error),
}

#[derive(Debug, Clone)]
pub struct AcceptedRecord {
    pub response: SubmissionAccepted,
    pub replay: bool,
}

#[derive(Debug, Clone)]
pub struct QueuedSubmission {
    pub submission_id: String,
    pub request: SubmissionCreate,
    pub priority: u8,
    pub created_at: DateTime<Utc>,
    pub not_before: Option<DateTime<Utc>>,
    pub start_deadline: Option<DateTime<Utc>>,
    pub workspace_key: Option<String>,
    pub workspace_access: Option<String>,
}

#[derive(Debug, Clone)]
pub struct ClaimSpec {
    pub submission_id: String,
    pub attempt_id: String,
    pub route_id: String,
    pub adapter: String,
    pub model: String,
    pub target_id: String,
    pub source_id: String,
    pub source_label: String,
    pub sandbox_profile: String,
    pub runtime: RuntimeRef,
}

#[derive(Debug, Clone)]
pub struct AcceptSpec {
    pub client_id: String,
    pub idempotency_key: String,
    pub request: SubmissionCreate,
    pub request_digest: String,
    pub profile: ResourceRef,
    pub policy: ResourceRef,
    pub workspace_key: Option<String>,
    pub workspace_access: Option<String>,
}

#[derive(Debug, Clone)]
pub struct FinishSpec {
    pub submission_id: String,
    pub attempt_id: String,
    pub state: SubmissionState,
    pub output: Option<String>,
    pub truncated: bool,
    pub error: Option<ErrorDetail>,
    pub agent_version: Option<String>,
}

#[derive(Debug, Clone)]
pub struct Store {
    pool: SqlitePool,
    events_tx: broadcast::Sender<EventEnvelope>,
}

impl Store {
    pub async fn open(database_url: &str) -> Result<Self> {
        let options = SqliteConnectOptions::from_str(database_url)
            .context("parse SQLite connection string")?
            .create_if_missing(true)
            .journal_mode(SqliteJournalMode::Wal)
            .foreign_keys(true);
        let pool = SqlitePoolOptions::new()
            .max_connections(8)
            .connect_with(options)
            .await
            .context("open SQLite database")?;
        sqlx::migrate!("./migrations")
            .run(&pool)
            .await
            .context("run SQLite migrations")?;
        let (events_tx, _) = broadcast::channel(1_024);
        Ok(Self { pool, events_tx })
    }

    #[must_use]
    pub fn subscribe(&self) -> broadcast::Receiver<EventEnvelope> {
        self.events_tx.subscribe()
    }

    pub async fn accept(&self, spec: AcceptSpec) -> Result<AcceptedRecord, StoreError> {
        let AcceptSpec {
            client_id,
            idempotency_key,
            request,
            request_digest,
            profile,
            policy,
            workspace_key,
            workspace_access,
        } = spec;
        let mut tx = self.pool.begin().await.map_err(internal)?;
        if let Some(row) = sqlx::query(
            "SELECT submission_id, request_digest, revision FROM submissions WHERE client_id = ? AND idempotency_key = ?",
        )
        .bind(&client_id)
        .bind(&idempotency_key)
        .fetch_optional(&mut *tx)
        .await
        .map_err(internal)?
        {
            let digest: String = row.try_get("request_digest").map_err(internal)?;
            if digest != request_digest {
                return Err(StoreError::IdempotencyConflict);
            }
            let submission_id: String = row.try_get("submission_id").map_err(internal)?;
            let revision = u64::try_from(row.try_get::<i64, _>("revision").map_err(internal)?)
                .map_err(|error| internal(anyhow::Error::from(error)))?;
            tx.commit().await.map_err(internal)?;
            return Ok(AcceptedRecord {
                response: accepted_response(
                    submission_id,
                    revision,
                    request_digest,
                    profile,
                    policy,
                    true,
                ),
                replay: true,
            });
        }

        let submission_id = format!("sub_{}", Ulid::new());
        let now = Utc::now();
        let priority = request
            .scheduling
            .as_ref()
            .and_then(|value| value.priority)
            .unwrap_or(50);
        let not_before = request
            .scheduling
            .as_ref()
            .and_then(|value| value.not_before);
        let start_deadline = request
            .scheduling
            .as_ref()
            .and_then(|value| value.start_deadline);
        sqlx::query(
            r#"INSERT INTO submissions (
                submission_id, client_id, idempotency_key, request_digest, request_json,
                resolved_profile_json, resolved_policy_json, state, revision, mode, priority,
                not_before, start_deadline, workspace_key, workspace_access, created_at, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, 'queued', 1, ?, ?, ?, ?, ?, ?, ?, ?)"#,
        )
        .bind(&submission_id)
        .bind(&client_id)
        .bind(&idempotency_key)
        .bind(&request_digest)
        .bind(to_json(&request)?)
        .bind(to_json(&profile)?)
        .bind(to_json(&policy)?)
        .bind(request.mode.name())
        .bind(i64::from(priority))
        .bind(not_before.map(|value| value.to_rfc3339()))
        .bind(start_deadline.map(|value| value.to_rfc3339()))
        .bind(workspace_key.as_deref())
        .bind(workspace_access.as_deref())
        .bind(now.to_rfc3339())
        .bind(now.to_rfc3339())
        .execute(&mut *tx)
        .await
        .map_err(internal)?;
        let event = append_event_tx(
            &mut tx,
            &submission_id,
            None,
            "submission.created",
            json!({"state": "queued"}),
        )
        .await?;
        tx.commit().await.map_err(internal)?;
        let _ = self.events_tx.send(event);

        Ok(AcceptedRecord {
            response: accepted_response(submission_id, 1, request_digest, profile, policy, false),
            replay: false,
        })
    }

    pub async fn status(&self, submission_id: &str) -> Result<SubmissionStatus, StoreError> {
        let row = sqlx::query("SELECT * FROM submissions WHERE submission_id = ?")
            .bind(submission_id)
            .fetch_optional(&self.pool)
            .await
            .map_err(internal)?
            .ok_or(StoreError::NotFound)?;
        let attempts = self.attempts(submission_id).await?;
        decode_status(&row, attempts)
    }

    pub async fn list_statuses(&self, limit: u32) -> Result<Vec<SubmissionStatus>, StoreError> {
        let rows =
            sqlx::query("SELECT submission_id FROM submissions ORDER BY created_at DESC LIMIT ?")
                .bind(i64::from(limit))
                .fetch_all(&self.pool)
                .await
                .map_err(internal)?;
        let mut statuses = Vec::with_capacity(rows.len());
        for row in rows {
            let id: String = row.try_get("submission_id").map_err(internal)?;
            statuses.push(self.status(&id).await?);
        }
        Ok(statuses)
    }

    pub async fn request(&self, submission_id: &str) -> Result<SubmissionCreate, StoreError> {
        let value: String =
            sqlx::query_scalar("SELECT request_json FROM submissions WHERE submission_id = ?")
                .bind(submission_id)
                .fetch_optional(&self.pool)
                .await
                .map_err(internal)?
                .ok_or(StoreError::NotFound)?;
        from_json(&value)
    }

    pub async fn queued(&self) -> Result<Vec<QueuedSubmission>, StoreError> {
        let rows = sqlx::query(
            r#"SELECT submission_id, request_json, priority, created_at, not_before,
                      start_deadline, workspace_key, workspace_access
               FROM submissions WHERE state = 'queued'
               ORDER BY priority DESC, created_at ASC"#,
        )
        .fetch_all(&self.pool)
        .await
        .map_err(internal)?;
        rows.iter().map(decode_queued).collect()
    }

    pub async fn active_for_source(&self, source_id: &str) -> Result<u32, StoreError> {
        let count: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM attempts WHERE source_id = ? AND state IN ('starting', 'running')",
        )
        .bind(source_id)
        .fetch_one(&self.pool)
        .await
        .map_err(internal)?;
        u32::try_from(count).map_err(|error| internal(anyhow::Error::from(error)))
    }

    pub async fn claim(&self, spec: &ClaimSpec) -> Result<Attempt, StoreError> {
        let mut tx = self.pool.begin().await.map_err(internal)?;
        let row = sqlx::query(
            "SELECT state, workspace_key, workspace_access FROM submissions WHERE submission_id = ?",
        )
        .bind(&spec.submission_id)
        .fetch_optional(&mut *tx)
        .await
        .map_err(internal)?
        .ok_or(StoreError::NotFound)?;
        let state: String = row.try_get("state").map_err(internal)?;
        if state != "queued" {
            return Err(StoreError::NotQueued);
        }
        let workspace_key: Option<String> = row.try_get("workspace_key").map_err(internal)?;
        let workspace_access: Option<String> = row.try_get("workspace_access").map_err(internal)?;
        if workspace_access.as_deref() == Some("writable")
            && let Some(key) = workspace_key
        {
            let busy: i64 = sqlx::query_scalar(
                r#"SELECT COUNT(*) FROM submissions
                   WHERE workspace_key = ? AND workspace_access = 'writable'
                     AND state = 'running' AND submission_id <> ?"#,
            )
            .bind(key)
            .bind(&spec.submission_id)
            .fetch_one(&mut *tx)
            .await
            .map_err(internal)?;
            if busy > 0 {
                return Err(StoreError::WorkspaceBusy);
            }
        }

        let number: i64 = sqlx::query_scalar(
            "SELECT COALESCE(MAX(number), 0) + 1 FROM attempts WHERE submission_id = ?",
        )
        .bind(&spec.submission_id)
        .fetch_one(&mut *tx)
        .await
        .map_err(internal)?;
        let now = Utc::now();
        sqlx::query(
            r#"INSERT INTO attempts (
                attempt_id, submission_id, number, state, route_id, adapter, model, target_id,
                source_id, source_label, sandbox_profile, runtime_json, started_at
            ) VALUES (?, ?, ?, 'starting', ?, ?, ?, ?, ?, ?, ?, ?, ?)"#,
        )
        .bind(&spec.attempt_id)
        .bind(&spec.submission_id)
        .bind(number)
        .bind(&spec.route_id)
        .bind(&spec.adapter)
        .bind(&spec.model)
        .bind(&spec.target_id)
        .bind(&spec.source_id)
        .bind(&spec.source_label)
        .bind(&spec.sandbox_profile)
        .bind(to_json(&spec.runtime)?)
        .bind(now.to_rfc3339())
        .execute(&mut *tx)
        .await
        .map_err(internal)?;
        sqlx::query(
            "UPDATE submissions SET state = 'running', blocker_code = NULL, blocker_detail = NULL, revision = revision + 1, updated_at = ? WHERE submission_id = ?",
        )
        .bind(now.to_rfc3339())
        .bind(&spec.submission_id)
        .execute(&mut *tx)
        .await
        .map_err(internal)?;
        let event = append_event_tx(
            &mut tx,
            &spec.submission_id,
            Some(&spec.attempt_id),
            "attempt.created",
            json!({"attempt_id": spec.attempt_id, "state": "starting"}),
        )
        .await?;
        let state_event = append_event_tx(
            &mut tx,
            &spec.submission_id,
            Some(&spec.attempt_id),
            "submission.state_changed",
            json!({"from": "queued", "to": "running"}),
        )
        .await?;
        tx.commit().await.map_err(internal)?;
        let _ = self.events_tx.send(event);
        let _ = self.events_tx.send(state_event);
        self.attempt(&spec.attempt_id).await
    }

    pub async fn mark_attempt_running(
        &self,
        submission_id: &str,
        attempt_id: &str,
    ) -> Result<(), StoreError> {
        let mut tx = self.pool.begin().await.map_err(internal)?;
        sqlx::query(
            "UPDATE attempts SET state = 'running' WHERE attempt_id = ? AND state = 'starting'",
        )
        .bind(attempt_id)
        .execute(&mut *tx)
        .await
        .map_err(internal)?;
        let event = append_event_tx(
            &mut tx,
            submission_id,
            Some(attempt_id),
            "attempt.state_changed",
            json!({"from": "starting", "to": "running"}),
        )
        .await?;
        tx.commit().await.map_err(internal)?;
        let _ = self.events_tx.send(event);
        Ok(())
    }

    pub async fn append_agent_event(
        &self,
        submission_id: &str,
        attempt_id: &str,
        event_type: &str,
        data: Value,
    ) -> Result<(), StoreError> {
        let mut tx = self.pool.begin().await.map_err(internal)?;
        let event =
            append_event_tx(&mut tx, submission_id, Some(attempt_id), event_type, data).await?;
        tx.commit().await.map_err(internal)?;
        let _ = self.events_tx.send(event);
        Ok(())
    }

    pub async fn finish(&self, spec: FinishSpec) -> Result<(), StoreError> {
        let FinishSpec {
            submission_id,
            attempt_id,
            state,
            output,
            truncated,
            error,
            agent_version,
        } = spec;
        let attempt_state = match state {
            SubmissionState::Completed => "completed",
            SubmissionState::Cancelled => "cancelled",
            SubmissionState::Uncertain => "uncertain",
            _ => "failed",
        };
        let state_name = state_name(state);
        let now = Utc::now();
        let mut tx = self.pool.begin().await.map_err(internal)?;
        sqlx::query(
            "UPDATE attempts SET state = ?, finished_at = ?, error_json = ?, agent_version = ? WHERE attempt_id = ?",
        )
        .bind(attempt_state)
        .bind(now.to_rfc3339())
        .bind(error.as_ref().map(to_json).transpose()?)
        .bind(agent_version.as_deref())
        .bind(&attempt_id)
        .execute(&mut *tx)
        .await
        .map_err(internal)?;
        sqlx::query(
            r#"UPDATE submissions SET state = ?, revision = revision + 1, result_text = ?,
               result_truncated = ?, error_json = ?, updated_at = ?, finished_at = ?
               WHERE submission_id = ? AND state = 'running'"#,
        )
        .bind(state_name)
        .bind(output.as_deref())
        .bind(i64::from(truncated))
        .bind(error.as_ref().map(to_json).transpose()?)
        .bind(now.to_rfc3339())
        .bind(now.to_rfc3339())
        .bind(&submission_id)
        .execute(&mut *tx)
        .await
        .map_err(internal)?;
        let attempt_event = append_event_tx(
            &mut tx,
            &submission_id,
            Some(&attempt_id),
            "attempt.state_changed",
            json!({"to": attempt_state}),
        )
        .await?;
        let state_event = append_event_tx(
            &mut tx,
            &submission_id,
            Some(&attempt_id),
            "submission.state_changed",
            json!({"from": "running", "to": state_name}),
        )
        .await?;
        tx.commit().await.map_err(internal)?;
        let _ = self.events_tx.send(attempt_event);
        let _ = self.events_tx.send(state_event);
        Ok(())
    }

    pub async fn set_blocker(
        &self,
        submission_id: &str,
        code: &str,
        detail: Option<&str>,
    ) -> Result<(), StoreError> {
        let current: Option<(Option<String>, Option<String>)> = sqlx::query_as(
            "SELECT blocker_code, blocker_detail FROM submissions WHERE submission_id = ? AND state = 'queued'",
        )
        .bind(submission_id)
        .fetch_optional(&self.pool)
        .await
        .map_err(internal)?;
        let Some((old_code, old_detail)) = current else {
            return Ok(());
        };
        if old_code.as_deref() == Some(code) && old_detail.as_deref() == detail {
            return Ok(());
        }
        let mut tx = self.pool.begin().await.map_err(internal)?;
        sqlx::query(
            "UPDATE submissions SET blocker_code = ?, blocker_detail = ?, revision = revision + 1, updated_at = ? WHERE submission_id = ? AND state = 'queued'",
        )
        .bind(code)
        .bind(detail)
        .bind(Utc::now().to_rfc3339())
        .bind(submission_id)
        .execute(&mut *tx)
        .await
        .map_err(internal)?;
        let event = append_event_tx(
            &mut tx,
            submission_id,
            None,
            "queue.blocked",
            json!({"code": code, "detail": detail}),
        )
        .await?;
        tx.commit().await.map_err(internal)?;
        let _ = self.events_tx.send(event);
        Ok(())
    }

    pub async fn expire(&self, submission_id: &str) -> Result<(), StoreError> {
        self.finish_queued(
            submission_id,
            SubmissionState::Expired,
            "start_deadline_expired",
        )
        .await
    }

    pub async fn cancel(&self, submission_id: &str) -> Result<(String, u64), StoreError> {
        let status = self.status(submission_id).await?;
        if status.terminal {
            return Ok(("already_terminal".to_owned(), status.revision));
        }
        if status.state == SubmissionState::Queued {
            self.finish_queued(
                submission_id,
                SubmissionState::Cancelled,
                "cancelled_by_client",
            )
            .await?;
            let updated = self.status(submission_id).await?;
            return Ok(("cancelled".to_owned(), updated.revision));
        }
        let mut tx = self.pool.begin().await.map_err(internal)?;
        sqlx::query(
            "UPDATE submissions SET cancel_requested = 1, revision = revision + 1, updated_at = ? WHERE submission_id = ? AND state = 'running'",
        )
        .bind(Utc::now().to_rfc3339())
        .bind(submission_id)
        .execute(&mut *tx)
        .await
        .map_err(internal)?;
        let event = append_event_tx(
            &mut tx,
            submission_id,
            status
                .attempts
                .last()
                .map(|attempt| attempt.attempt_id.as_str()),
            "cancellation.requested",
            json!({}),
        )
        .await?;
        tx.commit().await.map_err(internal)?;
        let _ = self.events_tx.send(event);
        let updated = self.status(submission_id).await?;
        Ok(("cancellation_requested".to_owned(), updated.revision))
    }

    pub async fn patch_scheduling(
        &self,
        submission_id: &str,
        expected_revision: u64,
        patch: &thieving_eyes_protocol::SubmissionPatch,
    ) -> Result<SubmissionStatus, StoreError> {
        if patch.priority.is_none() && patch.not_before.is_none() && patch.start_deadline.is_none()
        {
            return Err(StoreError::NotQueued);
        }
        let current = self.status(submission_id).await?;
        if current.revision != expected_revision {
            return Err(StoreError::RevisionConflict);
        }
        if current.state != SubmissionState::Queued {
            return Err(StoreError::NotQueued);
        }
        let mut tx = self.pool.begin().await.map_err(internal)?;
        if let Some(priority) = patch.priority {
            sqlx::query("UPDATE submissions SET priority = ? WHERE submission_id = ?")
                .bind(i64::from(priority))
                .bind(submission_id)
                .execute(&mut *tx)
                .await
                .map_err(internal)?;
        }
        if let Some(not_before) = patch.not_before {
            sqlx::query("UPDATE submissions SET not_before = ? WHERE submission_id = ?")
                .bind(not_before.map(|value| value.to_rfc3339()))
                .bind(submission_id)
                .execute(&mut *tx)
                .await
                .map_err(internal)?;
        }
        if let Some(start_deadline) = patch.start_deadline {
            sqlx::query("UPDATE submissions SET start_deadline = ? WHERE submission_id = ?")
                .bind(start_deadline.map(|value| value.to_rfc3339()))
                .bind(submission_id)
                .execute(&mut *tx)
                .await
                .map_err(internal)?;
        }
        sqlx::query(
            "UPDATE submissions SET revision = revision + 1, updated_at = ? WHERE submission_id = ?",
        )
        .bind(Utc::now().to_rfc3339())
        .bind(submission_id)
        .execute(&mut *tx)
        .await
        .map_err(internal)?;
        let event = append_event_tx(
            &mut tx,
            submission_id,
            None,
            "submission.scheduling_changed",
            serde_json::to_value(patch).map_err(|error| internal(anyhow::Error::from(error)))?,
        )
        .await?;
        tx.commit().await.map_err(internal)?;
        let _ = self.events_tx.send(event);
        self.status(submission_id).await
    }

    pub async fn cancellation_requested(&self, submission_id: &str) -> Result<bool, StoreError> {
        sqlx::query_scalar::<_, bool>(
            "SELECT cancel_requested FROM submissions WHERE submission_id = ?",
        )
        .bind(submission_id)
        .fetch_optional(&self.pool)
        .await
        .map_err(internal)?
        .ok_or(StoreError::NotFound)
    }

    pub async fn events_after(
        &self,
        submission_id: &str,
        after: u64,
    ) -> Result<Vec<EventEnvelope>, StoreError> {
        let exists: i64 =
            sqlx::query_scalar("SELECT COUNT(*) FROM submissions WHERE submission_id = ?")
                .bind(submission_id)
                .fetch_one(&self.pool)
                .await
                .map_err(internal)?;
        if exists == 0 {
            return Err(StoreError::NotFound);
        }
        let rows = sqlx::query(
            "SELECT * FROM events WHERE submission_id = ? AND sequence > ? ORDER BY sequence ASC",
        )
        .bind(submission_id)
        .bind(i64::try_from(after).map_err(|error| internal(anyhow::Error::from(error)))?)
        .fetch_all(&self.pool)
        .await
        .map_err(internal)?;
        rows.iter().map(decode_event).collect()
    }

    pub async fn result(&self, submission_id: &str) -> Result<SubmissionResult, StoreError> {
        let status = self.status(submission_id).await?;
        if !status.terminal {
            return Err(StoreError::NotQueued);
        }
        let row = sqlx::query("SELECT result_text, result_truncated, finished_at FROM submissions WHERE submission_id = ?")
            .bind(submission_id)
            .fetch_one(&self.pool)
            .await
            .map_err(internal)?;
        let text: Option<String> = row.try_get("result_text").map_err(internal)?;
        let truncated: bool = row.try_get("result_truncated").map_err(internal)?;
        let finished_at: Option<String> = row.try_get("finished_at").map_err(internal)?;
        Ok(SubmissionResult {
            submission_id: submission_id.to_owned(),
            state: status.state,
            terminal: true,
            output: text.map(|text| OutputValue::Text { text, truncated }),
            artifacts: Vec::<ArtifactRef>::new(),
            attempts: status.attempts,
            usage: None,
            error: status.error,
            finished_at: parse_time_required(finished_at.as_deref())?,
        })
    }

    pub async fn record_capacity(
        &self,
        source_id: &str,
        health: &str,
        usage_kind: Option<&str>,
        in_use: Option<u32>,
        observed_at: Option<DateTime<Utc>>,
    ) -> Result<(), StoreError> {
        sqlx::query(
            r#"INSERT INTO capacity_observations (source_id, health, usage_kind, in_use, observed_at, received_at)
               VALUES (?, ?, ?, ?, ?, ?)
               ON CONFLICT(source_id) DO UPDATE SET health=excluded.health, usage_kind=excluded.usage_kind,
                 in_use=excluded.in_use, observed_at=excluded.observed_at, received_at=excluded.received_at"#,
        )
        .bind(source_id)
        .bind(health)
        .bind(usage_kind)
        .bind(in_use.map(i64::from))
        .bind(observed_at.map(|value| value.to_rfc3339()))
        .bind(Utc::now().to_rfc3339())
        .execute(&self.pool)
        .await
        .map_err(internal)?;
        Ok(())
    }

    pub async fn recover_running_as_uncertain(&self) -> Result<u64, StoreError> {
        let rows = sqlx::query(
            "SELECT submission_id, attempt_id FROM attempts WHERE state IN ('starting', 'running')",
        )
        .fetch_all(&self.pool)
        .await
        .map_err(internal)?;
        let mut recovered = 0_u64;
        for row in rows {
            let submission_id: String = row.try_get("submission_id").map_err(internal)?;
            let attempt_id: String = row.try_get("attempt_id").map_err(internal)?;
            let error = ErrorDetail {
                code: "runner_lost".to_owned(),
                message: "daemon restarted before local runner recovery was confirmed".to_owned(),
                retryable: false,
                scope: thieving_eyes_protocol::ErrorScope::Attempt,
                retry_after_seconds: None,
                field: None,
            };
            self.finish(FinishSpec {
                submission_id,
                attempt_id,
                state: SubmissionState::Uncertain,
                output: None,
                truncated: false,
                error: Some(error),
                agent_version: None,
            })
            .await?;
            recovered += 1;
        }
        Ok(recovered)
    }

    async fn finish_queued(
        &self,
        submission_id: &str,
        state: SubmissionState,
        reason: &str,
    ) -> Result<(), StoreError> {
        let state_name = state_name(state);
        let now = Utc::now();
        let mut tx = self.pool.begin().await.map_err(internal)?;
        let result = sqlx::query(
            "UPDATE submissions SET state = ?, revision = revision + 1, updated_at = ?, finished_at = ? WHERE submission_id = ? AND state = 'queued'",
        )
        .bind(state_name)
        .bind(now.to_rfc3339())
        .bind(now.to_rfc3339())
        .bind(submission_id)
        .execute(&mut *tx)
        .await
        .map_err(internal)?;
        if result.rows_affected() == 0 {
            return Err(StoreError::NotQueued);
        }
        let event = append_event_tx(
            &mut tx,
            submission_id,
            None,
            "submission.state_changed",
            json!({"from": "queued", "to": state_name, "reason": reason}),
        )
        .await?;
        tx.commit().await.map_err(internal)?;
        let _ = self.events_tx.send(event);
        Ok(())
    }

    async fn attempts(&self, submission_id: &str) -> Result<Vec<Attempt>, StoreError> {
        let rows =
            sqlx::query("SELECT * FROM attempts WHERE submission_id = ? ORDER BY number ASC")
                .bind(submission_id)
                .fetch_all(&self.pool)
                .await
                .map_err(internal)?;
        rows.iter().map(decode_attempt).collect()
    }

    async fn attempt(&self, attempt_id: &str) -> Result<Attempt, StoreError> {
        let row = sqlx::query("SELECT * FROM attempts WHERE attempt_id = ?")
            .bind(attempt_id)
            .fetch_optional(&self.pool)
            .await
            .map_err(internal)?
            .ok_or(StoreError::NotFound)?;
        decode_attempt(&row)
    }
}

fn accepted_response(
    submission_id: String,
    revision: u64,
    request_digest: String,
    profile: ResourceRef,
    policy: ResourceRef,
    replay: bool,
) -> SubmissionAccepted {
    SubmissionAccepted {
        status_url: format!("/v1/submissions/{submission_id}"),
        events_url: format!("/v1/submissions/{submission_id}/events"),
        result_url: format!("/v1/submissions/{submission_id}/result"),
        submission_id,
        state: SubmissionState::Queued,
        terminal: false,
        revision,
        request_digest,
        resolved_profile: profile,
        resolved_policy: policy,
        idempotent_replay: replay,
    }
}

async fn append_event_tx(
    tx: &mut Transaction<'_, sqlx::Sqlite>,
    submission_id: &str,
    attempt_id: Option<&str>,
    event_type: &str,
    data: Value,
) -> Result<EventEnvelope, StoreError> {
    let sequence: i64 = sqlx::query_scalar(
        "UPDATE submissions SET latest_event_sequence = latest_event_sequence + 1 WHERE submission_id = ? RETURNING latest_event_sequence",
    )
    .bind(submission_id)
    .fetch_one(&mut **tx)
    .await
    .map_err(internal)?;
    let sequence = u64::try_from(sequence).map_err(|error| internal(anyhow::Error::from(error)))?;
    let envelope = EventEnvelope {
        event_id: format!("evt_{}", Ulid::new()),
        submission_id: submission_id.to_owned(),
        session_id: None,
        attempt_id: attempt_id.map(str::to_owned),
        sequence,
        occurred_at: Utc::now(),
        event_type: event_type.to_owned(),
        data,
    };
    sqlx::query(
        "INSERT INTO events (event_id, submission_id, attempt_id, sequence, occurred_at, event_type, data_json) VALUES (?, ?, ?, ?, ?, ?, ?)",
    )
    .bind(&envelope.event_id)
    .bind(&envelope.submission_id)
    .bind(&envelope.attempt_id)
    .bind(i64::try_from(sequence).map_err(|error| internal(anyhow::Error::from(error)))?)
    .bind(envelope.occurred_at.to_rfc3339())
    .bind(&envelope.event_type)
    .bind(to_json(&envelope.data)?)
    .execute(&mut **tx)
    .await
    .map_err(internal)?;
    Ok(envelope)
}

fn decode_status(
    row: &sqlx::sqlite::SqliteRow,
    attempts: Vec<Attempt>,
) -> Result<SubmissionStatus, StoreError> {
    let state = parse_submission_state(&row.try_get::<String, _>("state").map_err(internal)?)?;
    let blocker_code: Option<String> = row.try_get("blocker_code").map_err(internal)?;
    Ok(SubmissionStatus {
        submission_id: row.try_get("submission_id").map_err(internal)?,
        client_reference: from_json::<SubmissionCreate>(
            &row.try_get::<String, _>("request_json").map_err(internal)?,
        )?
        .client_reference,
        request_digest: row.try_get("request_digest").map_err(internal)?,
        resolved_profile: from_json(
            &row.try_get::<String, _>("resolved_profile_json")
                .map_err(internal)?,
        )?,
        resolved_policy: from_json(
            &row.try_get::<String, _>("resolved_policy_json")
                .map_err(internal)?,
        )?,
        state,
        terminal: state.terminal(),
        revision: to_u64(row.try_get("revision").map_err(internal)?)?,
        mode: row.try_get("mode").map_err(internal)?,
        created_at: parse_time_required(Some(
            &row.try_get::<String, _>("created_at").map_err(internal)?,
        ))?,
        updated_at: parse_time_required(Some(
            &row.try_get::<String, _>("updated_at").map_err(internal)?,
        ))?,
        blocker: blocker_code.map(|code| Blocker {
            code,
            retry_after: None,
            detail: row.try_get("blocker_detail").ok().flatten(),
        }),
        session_id: None,
        attempts,
        latest_event_sequence: to_u64(row.try_get("latest_event_sequence").map_err(internal)?)?,
        error: row
            .try_get::<Option<String>, _>("error_json")
            .map_err(internal)?
            .map(|value| from_json(&value))
            .transpose()?,
    })
}

fn decode_queued(row: &sqlx::sqlite::SqliteRow) -> Result<QueuedSubmission, StoreError> {
    Ok(QueuedSubmission {
        submission_id: row.try_get("submission_id").map_err(internal)?,
        request: from_json(&row.try_get::<String, _>("request_json").map_err(internal)?)?,
        priority: u8::try_from(row.try_get::<i64, _>("priority").map_err(internal)?)
            .map_err(|error| internal(anyhow::Error::from(error)))?,
        created_at: parse_time_required(Some(
            &row.try_get::<String, _>("created_at").map_err(internal)?,
        ))?,
        not_before: parse_time(
            row.try_get::<Option<String>, _>("not_before")
                .map_err(internal)?
                .as_deref(),
        )?,
        start_deadline: parse_time(
            row.try_get::<Option<String>, _>("start_deadline")
                .map_err(internal)?
                .as_deref(),
        )?,
        workspace_key: row.try_get("workspace_key").map_err(internal)?,
        workspace_access: row.try_get("workspace_access").map_err(internal)?,
    })
}

fn decode_attempt(row: &sqlx::sqlite::SqliteRow) -> Result<Attempt, StoreError> {
    Ok(Attempt {
        attempt_id: row.try_get("attempt_id").map_err(internal)?,
        number: u32::try_from(row.try_get::<i64, _>("number").map_err(internal)?)
            .map_err(|error| internal(anyhow::Error::from(error)))?,
        state: parse_attempt_state(&row.try_get::<String, _>("state").map_err(internal)?)?,
        route_id: row.try_get("route_id").map_err(internal)?,
        adapter: row.try_get("adapter").map_err(internal)?,
        model: row.try_get("model").map_err(internal)?,
        target_id: row.try_get("target_id").map_err(internal)?,
        source_label: row.try_get("source_label").map_err(internal)?,
        sandbox_profile: row.try_get("sandbox_profile").map_err(internal)?,
        runtime: from_json(&row.try_get::<String, _>("runtime_json").map_err(internal)?)?,
        agent_version: row.try_get("agent_version").map_err(internal)?,
        session_id: None,
        started_at: parse_time(
            row.try_get::<Option<String>, _>("started_at")
                .map_err(internal)?
                .as_deref(),
        )?,
        finished_at: parse_time(
            row.try_get::<Option<String>, _>("finished_at")
                .map_err(internal)?
                .as_deref(),
        )?,
        usage: row
            .try_get::<Option<String>, _>("usage_json")
            .map_err(internal)?
            .map(|value| from_json(&value))
            .transpose()?,
        error: row
            .try_get::<Option<String>, _>("error_json")
            .map_err(internal)?
            .map(|value| from_json(&value))
            .transpose()?,
    })
}

fn decode_event(row: &sqlx::sqlite::SqliteRow) -> Result<EventEnvelope, StoreError> {
    Ok(EventEnvelope {
        event_id: row.try_get("event_id").map_err(internal)?,
        submission_id: row.try_get("submission_id").map_err(internal)?,
        session_id: None,
        attempt_id: row.try_get("attempt_id").map_err(internal)?,
        sequence: to_u64(row.try_get("sequence").map_err(internal)?)?,
        occurred_at: parse_time_required(Some(
            &row.try_get::<String, _>("occurred_at").map_err(internal)?,
        ))?,
        event_type: row.try_get("event_type").map_err(internal)?,
        data: from_json(&row.try_get::<String, _>("data_json").map_err(internal)?)?,
    })
}

fn parse_submission_state(value: &str) -> Result<SubmissionState, StoreError> {
    match value {
        "queued" => Ok(SubmissionState::Queued),
        "running" => Ok(SubmissionState::Running),
        "completed" => Ok(SubmissionState::Completed),
        "failed" => Ok(SubmissionState::Failed),
        "cancelled" => Ok(SubmissionState::Cancelled),
        "expired" => Ok(SubmissionState::Expired),
        "uncertain" => Ok(SubmissionState::Uncertain),
        other => Err(internal(anyhow::anyhow!(
            "invalid persisted submission state {other}"
        ))),
    }
}

fn parse_attempt_state(value: &str) -> Result<AttemptState, StoreError> {
    match value {
        "starting" => Ok(AttemptState::Starting),
        "running" => Ok(AttemptState::Running),
        "completed" => Ok(AttemptState::Completed),
        "failed" => Ok(AttemptState::Failed),
        "cancelled" => Ok(AttemptState::Cancelled),
        "uncertain" => Ok(AttemptState::Uncertain),
        other => Err(internal(anyhow::anyhow!(
            "invalid persisted attempt state {other}"
        ))),
    }
}

const fn state_name(state: SubmissionState) -> &'static str {
    match state {
        SubmissionState::Queued => "queued",
        SubmissionState::Running => "running",
        SubmissionState::Completed => "completed",
        SubmissionState::Failed => "failed",
        SubmissionState::Cancelled => "cancelled",
        SubmissionState::Expired => "expired",
        SubmissionState::Uncertain => "uncertain",
    }
}

fn parse_time(value: Option<&str>) -> Result<Option<DateTime<Utc>>, StoreError> {
    value
        .map(|value| {
            DateTime::parse_from_rfc3339(value)
                .map(|value| value.with_timezone(&Utc))
                .map_err(|error| internal(anyhow::Error::from(error)))
        })
        .transpose()
}

fn parse_time_required(value: Option<&str>) -> Result<DateTime<Utc>, StoreError> {
    parse_time(value)?.ok_or_else(|| internal(anyhow::anyhow!("missing persisted timestamp")))
}

fn to_json<T: serde::Serialize + ?Sized>(value: &T) -> Result<String, StoreError> {
    serde_json::to_string(value).map_err(|error| internal(anyhow::Error::from(error)))
}

fn from_json<T: serde::de::DeserializeOwned>(value: &str) -> Result<T, StoreError> {
    serde_json::from_str(value).map_err(|error| internal(anyhow::Error::from(error)))
}

fn to_u64(value: i64) -> Result<u64, StoreError> {
    u64::try_from(value).map_err(|error| internal(anyhow::Error::from(error)))
}

fn internal(error: impl Into<anyhow::Error>) -> StoreError {
    StoreError::Internal(error.into())
}

#[cfg(test)]
mod tests {
    use super::*;
    use thieving_eyes_protocol::{ContentPart, Input, TaskMode};

    async fn test_store() -> Store {
        Store::open("sqlite::memory:")
            .await
            .unwrap_or_else(|error| panic!("open test store: {error}"))
    }

    fn resource(id: &str) -> ResourceRef {
        ResourceRef {
            id: id.to_owned(),
            version: "1".to_owned(),
            digest: format!("sha256:{}", "0".repeat(64)),
        }
    }

    fn request() -> SubmissionCreate {
        SubmissionCreate {
            client_reference: None,
            labels: Default::default(),
            mode: TaskMode::Task,
            input: Input {
                parts: vec![ContentPart::Text {
                    text: "hello".to_owned(),
                }],
            },
            workspace: None,
            output: None,
            agent: None,
            execution: None,
            session: None,
            scheduling: None,
            limits: None,
            policy: None,
        }
    }

    #[tokio::test]
    async fn idempotency_is_scoped_and_strict() {
        let store = test_store().await;
        let first = store
            .accept(AcceptSpec {
                client_id: "uid:1".to_owned(),
                idempotency_key: "key".to_owned(),
                request: request(),
                request_digest: "sha256:a".to_owned(),
                profile: resource("p"),
                policy: resource("q"),
                workspace_key: None,
                workspace_access: None,
            })
            .await
            .unwrap_or_else(|error| panic!("first accept: {error}"));
        let replay = store
            .accept(AcceptSpec {
                client_id: "uid:1".to_owned(),
                idempotency_key: "key".to_owned(),
                request: request(),
                request_digest: "sha256:a".to_owned(),
                profile: resource("p"),
                policy: resource("q"),
                workspace_key: None,
                workspace_access: None,
            })
            .await
            .unwrap_or_else(|error| panic!("replay accept: {error}"));
        assert_eq!(first.response.submission_id, replay.response.submission_id);
        assert!(replay.replay);
        assert!(matches!(
            store
                .accept(AcceptSpec {
                    client_id: "uid:1".to_owned(),
                    idempotency_key: "key".to_owned(),
                    request: request(),
                    request_digest: "sha256:b".to_owned(),
                    profile: resource("p"),
                    policy: resource("q"),
                    workspace_key: None,
                    workspace_access: None,
                })
                .await,
            Err(StoreError::IdempotencyConflict)
        ));
    }
}
