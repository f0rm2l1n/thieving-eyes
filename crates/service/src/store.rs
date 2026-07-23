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

use crate::config::{PolicyConfig, ProfileConfig, RouteConfig};

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
    #[error("invalid scheduling constraints")]
    InvalidScheduling,
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
    pub profile: ProfileConfig,
    pub policy: PolicyConfig,
    pub routes: Vec<RouteConfig>,
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
    pub profile_config: ProfileConfig,
    pub policy_config: PolicyConfig,
    pub routes: Vec<RouteConfig>,
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

    pub async fn idempotent_replay(
        &self,
        client_id: &str,
        idempotency_key: &str,
        request_digest: &str,
    ) -> Result<Option<SubmissionAccepted>, StoreError> {
        let row = sqlx::query(
            r#"SELECT submission_id, request_digest, resolved_profile_json,
                      resolved_policy_json
               FROM submissions
               WHERE client_id = ? AND idempotency_key = ?"#,
        )
        .bind(client_id)
        .bind(idempotency_key)
        .fetch_optional(&self.pool)
        .await
        .map_err(internal)?;
        let Some(row) = row else {
            return Ok(None);
        };
        if row
            .try_get::<String, _>("request_digest")
            .map_err(internal)?
            != request_digest
        {
            return Err(StoreError::IdempotencyConflict);
        }
        let submission_id = row.try_get("submission_id").map_err(internal)?;
        let profile = from_json(
            &row.try_get::<String, _>("resolved_profile_json")
                .map_err(internal)?,
        )?;
        let policy = from_json(
            &row.try_get::<String, _>("resolved_policy_json")
                .map_err(internal)?,
        )?;
        Ok(Some(accepted_response(
            submission_id,
            1,
            request_digest.to_owned(),
            profile,
            policy,
            true,
        )))
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
            profile_config,
            policy_config,
            routes,
        } = spec;
        let mut tx = self.pool.begin().await.map_err(internal)?;
        if let Some(row) = sqlx::query(
            "SELECT submission_id, request_digest, resolved_profile_json, resolved_policy_json FROM submissions WHERE client_id = ? AND idempotency_key = ?",
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
            let frozen_profile: ResourceRef =
                from_json(&row.try_get::<String, _>("resolved_profile_json").map_err(internal)?)?;
            let frozen_policy: ResourceRef =
                from_json(&row.try_get::<String, _>("resolved_policy_json").map_err(internal)?)?;
            tx.commit().await.map_err(internal)?;
            return Ok(AcceptedRecord {
                response: accepted_response(
                    submission_id,
                    1,
                    request_digest,
                    frozen_profile,
                    frozen_policy,
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
                not_before, start_deadline, workspace_key, workspace_access,
                resolved_profile_config_json, resolved_policy_config_json, resolved_routes_json,
                created_at, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, 'queued', 1, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"#,
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
        .bind(to_json(&profile_config)?)
        .bind(to_json(&policy_config)?)
        .bind(to_json(&routes)?)
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
                      start_deadline, workspace_key, workspace_access,
                      resolved_profile_config_json, resolved_policy_config_json,
                      resolved_routes_json
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
            "SELECT COUNT(*) FROM attempts WHERE source_id = ? AND capacity_held = 1",
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
        if let Some(key) = workspace_key {
            let busy: i64 = sqlx::query_scalar(
                r#"SELECT COUNT(*) FROM submissions
                   WHERE workspace_key = ? AND state IN ('running', 'uncertain')
                     AND submission_id <> ?
                     AND (workspace_access = 'writable' OR ? = 'writable')"#,
            )
            .bind(key)
            .bind(&spec.submission_id)
            .bind(workspace_access.as_deref())
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
        let updated = sqlx::query(
            "UPDATE submissions SET state = 'running', blocker_code = NULL, blocker_detail = NULL, revision = revision + 1, updated_at = ? WHERE submission_id = ? AND state = 'queued'",
        )
        .bind(now.to_rfc3339())
        .bind(&spec.submission_id)
        .execute(&mut *tx)
        .await
        .map_err(internal)?;
        if updated.rows_affected() != 1 {
            return Err(StoreError::NotQueued);
        }
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
        agent_version: Option<&str>,
    ) -> Result<(), StoreError> {
        let mut tx = self.pool.begin().await.map_err(internal)?;
        let updated = sqlx::query(
            "UPDATE attempts SET state = 'running', agent_version = ? WHERE attempt_id = ? AND submission_id = ? AND state = 'starting'",
        )
        .bind(agent_version)
        .bind(attempt_id)
        .bind(submission_id)
        .execute(&mut *tx)
        .await
        .map_err(internal)?;
        if updated.rows_affected() == 0 {
            tx.rollback().await.map_err(internal)?;
            return Ok(());
        }
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
            SubmissionState::Failed => "failed",
            _ => {
                return Err(internal(anyhow::anyhow!(
                    "finish requires a terminal running-state outcome"
                )));
            }
        };
        let capacity_held = i64::from(state == SubmissionState::Uncertain);
        let state_name = state_name(state);
        let now = Utc::now();
        let mut tx = self.pool.begin().await.map_err(internal)?;
        let submission_update = sqlx::query(
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
        if submission_update.rows_affected() != 1 {
            tx.rollback().await.map_err(internal)?;
            return Err(StoreError::NotQueued);
        }
        let attempt_update = sqlx::query(
            r#"UPDATE attempts SET state = ?, finished_at = ?, error_json = ?,
               agent_version = COALESCE(?, agent_version), capacity_held = ?
               WHERE attempt_id = ? AND submission_id = ?
                 AND state IN ('starting', 'running')"#,
        )
        .bind(attempt_state)
        .bind(now.to_rfc3339())
        .bind(error.as_ref().map(to_json).transpose()?)
        .bind(agent_version.as_deref())
        .bind(capacity_held)
        .bind(&attempt_id)
        .bind(&submission_id)
        .execute(&mut *tx)
        .await
        .map_err(internal)?;
        if attempt_update.rows_affected() != 1 {
            return Err(internal(anyhow::anyhow!(
                "active submission does not own the expected active attempt"
            )));
        }
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
        let mut tx = self.pool.begin().await.map_err(internal)?;
        let current: Option<(Option<String>, Option<String>)> = sqlx::query_as(
            "SELECT blocker_code, blocker_detail FROM submissions WHERE submission_id = ? AND state = 'queued'",
        )
        .bind(submission_id)
        .fetch_optional(&mut *tx)
        .await
        .map_err(internal)?;
        let Some((old_code, old_detail)) = current else {
            tx.commit().await.map_err(internal)?;
            return Ok(());
        };
        if old_code.as_deref() == Some(code) && old_detail.as_deref() == detail {
            tx.commit().await.map_err(internal)?;
            return Ok(());
        }
        let update = sqlx::query(
            "UPDATE submissions SET blocker_code = ?, blocker_detail = ?, revision = revision + 1, updated_at = ? WHERE submission_id = ? AND state = 'queued'",
        )
        .bind(code)
        .bind(detail)
        .bind(Utc::now().to_rfc3339())
        .bind(submission_id)
        .execute(&mut *tx)
        .await
        .map_err(internal)?;
        if update.rows_affected() != 1 {
            tx.rollback().await.map_err(internal)?;
            return Ok(());
        }
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
        let mut tx = self.pool.begin().await.map_err(internal)?;
        let row = sqlx::query(
            "SELECT state, revision, cancel_requested FROM submissions WHERE submission_id = ?",
        )
        .bind(submission_id)
        .fetch_optional(&mut *tx)
        .await
        .map_err(internal)?
        .ok_or(StoreError::NotFound)?;
        let state = parse_submission_state(&row.try_get::<String, _>("state").map_err(internal)?)?;
        let revision = to_u64(row.try_get("revision").map_err(internal)?)?;
        let already_requested: bool = row.try_get("cancel_requested").map_err(internal)?;
        if state.terminal() {
            tx.commit().await.map_err(internal)?;
            return Ok(("already_terminal".to_owned(), revision));
        }
        let now = Utc::now();
        if state == SubmissionState::Queued {
            let update = sqlx::query(
                r#"UPDATE submissions SET state = 'cancelled', revision = revision + 1,
                   updated_at = ?, finished_at = ? WHERE submission_id = ? AND state = 'queued'"#,
            )
            .bind(now.to_rfc3339())
            .bind(now.to_rfc3339())
            .bind(submission_id)
            .execute(&mut *tx)
            .await
            .map_err(internal)?;
            if update.rows_affected() != 1 {
                return Err(StoreError::NotQueued);
            }
            let event = append_event_tx(
                &mut tx,
                submission_id,
                None,
                "submission.state_changed",
                json!({"from": "queued", "to": "cancelled", "reason": "cancelled_by_client"}),
            )
            .await?;
            tx.commit().await.map_err(internal)?;
            let _ = self.events_tx.send(event);
            return Ok(("cancelled".to_owned(), revision.saturating_add(1)));
        }
        if already_requested {
            tx.commit().await.map_err(internal)?;
            return Ok(("cancellation_requested".to_owned(), revision));
        }
        let attempt_id: Option<String> = sqlx::query_scalar(
            "SELECT attempt_id FROM attempts WHERE submission_id = ? AND state IN ('starting', 'running') ORDER BY number DESC LIMIT 1",
        )
        .bind(submission_id)
        .fetch_optional(&mut *tx)
        .await
        .map_err(internal)?;
        let update = sqlx::query(
            "UPDATE submissions SET cancel_requested = 1, revision = revision + 1, updated_at = ? WHERE submission_id = ? AND state = 'running' AND cancel_requested = 0",
        )
        .bind(now.to_rfc3339())
        .bind(submission_id)
        .execute(&mut *tx)
        .await
        .map_err(internal)?;
        if update.rows_affected() != 1 {
            return Err(StoreError::NotQueued);
        }
        let event = append_event_tx(
            &mut tx,
            submission_id,
            attempt_id.as_deref(),
            "cancellation.requested",
            json!({}),
        )
        .await?;
        tx.commit().await.map_err(internal)?;
        let _ = self.events_tx.send(event);
        Ok((
            "cancellation_requested".to_owned(),
            revision.saturating_add(1),
        ))
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
        let mut tx = self.pool.begin().await.map_err(internal)?;
        let row = sqlx::query(
            "SELECT state, revision, priority, not_before, start_deadline FROM submissions WHERE submission_id = ?",
        )
        .bind(submission_id)
        .fetch_optional(&mut *tx)
        .await
        .map_err(internal)?
        .ok_or(StoreError::NotFound)?;
        let revision = to_u64(row.try_get("revision").map_err(internal)?)?;
        if revision != expected_revision {
            return Err(StoreError::RevisionConflict);
        }
        if row.try_get::<String, _>("state").map_err(internal)? != "queued" {
            return Err(StoreError::NotQueued);
        }
        let priority = patch.priority.map_or_else(
            || {
                u8::try_from(row.try_get::<i64, _>("priority").map_err(internal)?)
                    .map_err(|error| internal(anyhow::Error::from(error)))
            },
            Ok,
        )?;
        let current_not_before = parse_time(
            row.try_get::<Option<String>, _>("not_before")
                .map_err(internal)?
                .as_deref(),
        )?;
        let current_deadline = parse_time(
            row.try_get::<Option<String>, _>("start_deadline")
                .map_err(internal)?
                .as_deref(),
        )?;
        let not_before = patch.not_before.unwrap_or(current_not_before);
        let start_deadline = patch.start_deadline.unwrap_or(current_deadline);
        if let (Some(not_before), Some(start_deadline)) = (not_before, start_deadline)
            && start_deadline <= not_before
        {
            return Err(StoreError::InvalidScheduling);
        }
        let update = sqlx::query(
            r#"UPDATE submissions SET priority = ?, not_before = ?, start_deadline = ?,
               revision = revision + 1, updated_at = ?
               WHERE submission_id = ? AND state = 'queued' AND revision = ?"#,
        )
        .bind(i64::from(priority))
        .bind(not_before.map(|value| value.to_rfc3339()))
        .bind(start_deadline.map(|value| value.to_rfc3339()))
        .bind(Utc::now().to_rfc3339())
        .bind(submission_id)
        .bind(
            i64::try_from(expected_revision)
                .map_err(|error| internal(anyhow::Error::from(error)))?,
        )
        .execute(&mut *tx)
        .await
        .map_err(internal)?;
        if update.rows_affected() != 1 {
            return Err(StoreError::RevisionConflict);
        }
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

    pub async fn reject_unfrozen_queued(&self) -> Result<u64, StoreError> {
        let rows: Vec<String> = sqlx::query_scalar(
            r#"SELECT submission_id FROM submissions
               WHERE state = 'queued' AND (
                   resolved_profile_config_json IS NULL OR
                   resolved_policy_config_json IS NULL OR
                   resolved_routes_json IS NULL
               )"#,
        )
        .fetch_all(&self.pool)
        .await
        .map_err(internal)?;
        let mut rejected = 0_u64;
        for submission_id in rows {
            let now = Utc::now();
            let error = ErrorDetail {
                code: "runtime_unavailable".to_owned(),
                message: "submission predates durable execution snapshots; resubmit it".to_owned(),
                retryable: false,
                scope: thieving_eyes_protocol::ErrorScope::Submission,
                retry_after_seconds: None,
                field: None,
            };
            let mut tx = self.pool.begin().await.map_err(internal)?;
            let update = sqlx::query(
                r#"UPDATE submissions SET state = 'failed', revision = revision + 1,
                   error_json = ?, updated_at = ?, finished_at = ?
                   WHERE submission_id = ? AND state = 'queued'"#,
            )
            .bind(to_json(&error)?)
            .bind(now.to_rfc3339())
            .bind(now.to_rfc3339())
            .bind(&submission_id)
            .execute(&mut *tx)
            .await
            .map_err(internal)?;
            if update.rows_affected() == 0 {
                tx.rollback().await.map_err(internal)?;
                continue;
            }
            let event = append_event_tx(
                &mut tx,
                &submission_id,
                None,
                "submission.state_changed",
                json!({
                    "from": "queued",
                    "to": "failed",
                    "reason": "configuration_snapshot_missing"
                }),
            )
            .await?;
            tx.commit().await.map_err(internal)?;
            let _ = self.events_tx.send(event);
            rejected = rejected.saturating_add(1);
        }
        Ok(rejected)
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
        profile: from_json(
            &row.try_get::<String, _>("resolved_profile_config_json")
                .map_err(internal)?,
        )?,
        policy: from_json(
            &row.try_get::<String, _>("resolved_policy_config_json")
                .map_err(internal)?,
        )?,
        routes: from_json(
            &row.try_get::<String, _>("resolved_routes_json")
                .map_err(internal)?,
        )?,
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
    use crate::config::NetworkMode;
    use chrono::TimeDelta;
    use thieving_eyes_protocol::{ContentPart, Input, SubmissionPatch, TaskMode};

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

    fn profile_config() -> ProfileConfig {
        ProfileConfig {
            id: "p".to_owned(),
            version: "1".to_owned(),
            description: "test".to_owned(),
            network: NetworkMode::None,
        }
    }

    fn policy_config() -> PolicyConfig {
        PolicyConfig {
            id: "q".to_owned(),
            version: "1".to_owned(),
            description: "test".to_owned(),
            run_timeout_seconds: 30,
            idle_timeout_seconds: 10,
        }
    }

    fn routes() -> Vec<RouteConfig> {
        vec![RouteConfig {
            id: "r".to_owned(),
            adapter: "opencode".to_owned(),
            model: String::new(),
            source_ids: vec!["s".to_owned()],
            target_id: "local".to_owned(),
        }]
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

    fn accept_spec(key: &str, digest: &str) -> AcceptSpec {
        AcceptSpec {
            client_id: "uid:1".to_owned(),
            idempotency_key: key.to_owned(),
            request: request(),
            request_digest: digest.to_owned(),
            profile: resource("p"),
            policy: resource("q"),
            workspace_key: None,
            workspace_access: None,
            profile_config: profile_config(),
            policy_config: policy_config(),
            routes: routes(),
        }
    }

    fn claim_spec(submission_id: &str, attempt_id: &str) -> ClaimSpec {
        ClaimSpec {
            submission_id: submission_id.to_owned(),
            attempt_id: attempt_id.to_owned(),
            route_id: "r".to_owned(),
            adapter: "opencode".to_owned(),
            model: "test".to_owned(),
            target_id: "local".to_owned(),
            source_id: "s".to_owned(),
            source_label: "source".to_owned(),
            sandbox_profile: "p".to_owned(),
            runtime: RuntimeRef {
                name: "sandbox-agent".to_owned(),
                version: "1".to_owned(),
                digest: format!("sha256:{}", "1".repeat(64)),
            },
        }
    }

    #[tokio::test]
    async fn idempotency_is_scoped_and_strict() {
        let store = test_store().await;
        let first = store
            .accept(accept_spec("key", "sha256:a"))
            .await
            .unwrap_or_else(|error| panic!("first accept: {error}"));
        let mut changed_defaults = accept_spec("key", "sha256:a");
        changed_defaults.profile = resource("new-profile");
        changed_defaults.policy = resource("new-policy");
        let replay = store
            .accept(changed_defaults)
            .await
            .unwrap_or_else(|error| panic!("replay accept: {error}"));
        assert_eq!(first.response.submission_id, replay.response.submission_id);
        assert_eq!(
            first.response.resolved_profile,
            replay.response.resolved_profile
        );
        assert_eq!(
            first.response.resolved_policy,
            replay.response.resolved_policy
        );
        assert_eq!(replay.response.revision, 1);
        assert!(replay.replay);
        let early_replay = store
            .idempotent_replay("uid:1", "key", "sha256:a")
            .await
            .unwrap_or_else(|error| panic!("early replay: {error}"))
            .unwrap_or_else(|| panic!("existing idempotency key should replay"));
        assert_eq!(first.response.submission_id, early_replay.submission_id);
        assert_eq!(
            first.response.resolved_profile,
            early_replay.resolved_profile
        );
        assert!(matches!(
            store.accept(accept_spec("key", "sha256:b")).await,
            Err(StoreError::IdempotencyConflict)
        ));
        assert!(matches!(
            store.idempotent_replay("uid:1", "key", "sha256:b").await,
            Err(StoreError::IdempotencyConflict)
        ));
    }

    #[tokio::test]
    async fn terminal_transition_cannot_be_rewritten() {
        let store = test_store().await;
        let accepted = store
            .accept(accept_spec("terminal", "sha256:terminal"))
            .await
            .unwrap_or_else(|error| panic!("accept: {error}"));
        let id = accepted.response.submission_id;
        store
            .claim(&claim_spec(&id, "attempt-1"))
            .await
            .unwrap_or_else(|error| panic!("claim: {error}"));
        store
            .finish(FinishSpec {
                submission_id: id.clone(),
                attempt_id: "attempt-1".to_owned(),
                state: SubmissionState::Completed,
                output: Some("ok".to_owned()),
                truncated: false,
                error: None,
                agent_version: Some("test".to_owned()),
            })
            .await
            .unwrap_or_else(|error| panic!("finish: {error}"));
        assert!(matches!(
            store
                .finish(FinishSpec {
                    submission_id: id.clone(),
                    attempt_id: "attempt-1".to_owned(),
                    state: SubmissionState::Uncertain,
                    output: None,
                    truncated: false,
                    error: None,
                    agent_version: None,
                })
                .await,
            Err(StoreError::NotQueued)
        ));
        assert_eq!(
            store
                .status(&id)
                .await
                .unwrap_or_else(|error| panic!("status: {error}"))
                .state,
            SubmissionState::Completed
        );
    }

    #[tokio::test]
    async fn uncertain_retains_source_and_writable_workspace_capacity() {
        let store = test_store().await;
        let mut first_spec = accept_spec("workspace-1", "sha256:workspace-1");
        first_spec.workspace_key = Some("/workspace".to_owned());
        first_spec.workspace_access = Some("writable".to_owned());
        let first = store
            .accept(first_spec)
            .await
            .unwrap_or_else(|error| panic!("accept first: {error}"));
        store
            .claim(&claim_spec(&first.response.submission_id, "attempt-1"))
            .await
            .unwrap_or_else(|error| panic!("claim first: {error}"));
        store
            .finish(FinishSpec {
                submission_id: first.response.submission_id,
                attempt_id: "attempt-1".to_owned(),
                state: SubmissionState::Uncertain,
                output: None,
                truncated: false,
                error: None,
                agent_version: None,
            })
            .await
            .unwrap_or_else(|error| panic!("finish uncertain: {error}"));
        assert_eq!(
            store
                .active_for_source("s")
                .await
                .unwrap_or_else(|error| panic!("capacity: {error}")),
            1
        );

        let mut second_spec = accept_spec("workspace-2", "sha256:workspace-2");
        second_spec.workspace_key = Some("/workspace".to_owned());
        second_spec.workspace_access = Some("read_only".to_owned());
        let second = store
            .accept(second_spec)
            .await
            .unwrap_or_else(|error| panic!("accept second: {error}"));
        assert!(matches!(
            store
                .claim(&claim_spec(&second.response.submission_id, "attempt-2"))
                .await,
            Err(StoreError::WorkspaceBusy)
        ));
    }

    #[tokio::test]
    async fn cancellation_is_idempotent_while_running() {
        let store = test_store().await;
        let accepted = store
            .accept(accept_spec("cancel", "sha256:cancel"))
            .await
            .unwrap_or_else(|error| panic!("accept: {error}"));
        let id = accepted.response.submission_id;
        store
            .claim(&claim_spec(&id, "attempt-1"))
            .await
            .unwrap_or_else(|error| panic!("claim: {error}"));
        let first = store
            .cancel(&id)
            .await
            .unwrap_or_else(|error| panic!("first cancel: {error}"));
        let second = store
            .cancel(&id)
            .await
            .unwrap_or_else(|error| panic!("second cancel: {error}"));
        assert_eq!(first, second);
        let events = store
            .events_after(&id, 0)
            .await
            .unwrap_or_else(|error| panic!("events: {error}"));
        assert_eq!(
            events
                .iter()
                .filter(|event| event.event_type == "cancellation.requested")
                .count(),
            1
        );
    }

    #[tokio::test]
    async fn scheduling_patch_clears_nullable_time_and_checks_combination() {
        let store = test_store().await;
        let mut spec = accept_spec("patch", "sha256:patch");
        let not_before = Utc::now() + TimeDelta::minutes(10);
        let deadline = not_before + TimeDelta::minutes(10);
        spec.request.scheduling = Some(thieving_eyes_protocol::Scheduling {
            priority: Some(50),
            not_before: Some(not_before),
            start_deadline: Some(deadline),
        });
        let accepted = store
            .accept(spec)
            .await
            .unwrap_or_else(|error| panic!("accept: {error}"));
        let id = accepted.response.submission_id;
        store
            .patch_scheduling(
                &id,
                1,
                &SubmissionPatch {
                    priority: None,
                    not_before: Some(None),
                    start_deadline: None,
                },
            )
            .await
            .unwrap_or_else(|error| panic!("clear not_before: {error}"));
        let queued = store
            .queued()
            .await
            .unwrap_or_else(|error| panic!("queued: {error}"));
        assert_eq!(
            queued
                .first()
                .unwrap_or_else(|| panic!("submission should remain queued"))
                .not_before,
            None
        );

        assert!(matches!(
            store
                .patch_scheduling(
                    &id,
                    2,
                    &SubmissionPatch {
                        priority: None,
                        not_before: Some(Some(deadline + TimeDelta::minutes(1))),
                        start_deadline: None,
                    },
                )
                .await,
            Err(StoreError::InvalidScheduling)
        ));
    }
}
