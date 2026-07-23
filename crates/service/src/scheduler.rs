use std::collections::HashMap;
use std::path::{Component, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result, bail};
use chrono::Utc;
use serde_json::json;
use thieving_eyes_protocol::{
    ContentPart, ErrorDetail, ErrorScope, RuntimeRef, SubmissionCreate, SubmissionState,
    WorkspaceAccess, WorkspaceRef,
};
use thieving_eyes_runner::{CredentialMount, RunnerRequest};
use thieving_eyes_runtime_sandbox_agent::RuntimeEvent;
use tokio::sync::{Notify, RwLock, mpsc, watch};
use tokio::task::JoinSet;
use tokio::time::MissedTickBehavior;
use ulid::Ulid;

use crate::capacity::CapacityManager;
use crate::config::{Config, SANDBOX_AGENT_SHA256, SANDBOX_AGENT_VERSION};
use crate::store::{ClaimSpec, FinishSpec, QueuedSubmission, Store, StoreError};

pub type CancellationRegistry = Arc<RwLock<HashMap<String, watch::Sender<bool>>>>;

#[derive(Clone)]
pub struct SchedulerContext {
    pub config: Arc<Config>,
    pub store: Store,
    pub capacity: CapacityManager,
    pub notify: Arc<Notify>,
    pub cancellations: CancellationRegistry,
}

pub async fn run(context: SchedulerContext, mut shutdown: watch::Receiver<bool>) -> Result<()> {
    context.capacity.initialize().await?;
    let recovered = context.store.recover_running_as_uncertain().await?;
    if recovered > 0 {
        tracing::warn!(recovered, "recovered unfinished attempts as uncertain");
    }
    let mut interval = tokio::time::interval(Duration::from_secs(1));
    interval.set_missed_tick_behavior(MissedTickBehavior::Skip);
    let mut attempts = JoinSet::new();

    loop {
        tokio::select! {
            _ = interval.tick() => {}
            _ = context.notify.notified() => {}
            Some(joined) = attempts.join_next(), if !attempts.is_empty() => {
                if let Err(error) = joined {
                    tracing::error!(%error, "attempt task panicked or was cancelled");
                }
            }
            changed = shutdown.changed() => {
                if changed.is_err() || *shutdown.borrow() {
                    break;
                }
            }
        }

        context.capacity.refresh_due().await;
        let mut queued = context.store.queued().await?;
        queued.sort_by_key(|item| std::cmp::Reverse(effective_priority(item)));
        for submission in queued {
            if let Some(deadline) = submission.start_deadline
                && deadline <= Utc::now()
            {
                context.store.expire(&submission.submission_id).await?;
                continue;
            }
            if let Some(not_before) = submission.not_before
                && not_before > Utc::now()
            {
                context
                    .store
                    .set_blocker(&submission.submission_id, "not_before", None)
                    .await?;
                continue;
            }
            match select_dispatch(&context, &submission).await? {
                DispatchDecision::Blocked(code) => {
                    context
                        .store
                        .set_blocker(&submission.submission_id, code, None)
                        .await?;
                }
                DispatchDecision::Ready(dispatch) => {
                    let attempt_id = format!("att_{}", Ulid::new());
                    let claim = ClaimSpec {
                        submission_id: submission.submission_id.clone(),
                        attempt_id: attempt_id.clone(),
                        route_id: dispatch.route_id.clone(),
                        adapter: "opencode".to_owned(),
                        model: dispatch
                            .model
                            .clone()
                            .unwrap_or_else(|| "agent_default".to_owned()),
                        target_id: "local".to_owned(),
                        source_id: dispatch.source_id.clone(),
                        source_label: dispatch.source_label.clone(),
                        sandbox_profile: context.config.defaults.profile_id.clone(),
                        runtime: RuntimeRef {
                            name: "sandbox-agent".to_owned(),
                            version: SANDBOX_AGENT_VERSION.to_owned(),
                            digest: format!("sha256:{SANDBOX_AGENT_SHA256}"),
                        },
                    };
                    match context.store.claim(&claim).await {
                        Ok(_) => {
                            let (cancel_tx, cancel_rx) = watch::channel(false);
                            context
                                .cancellations
                                .write()
                                .await
                                .insert(submission.submission_id.clone(), cancel_tx);
                            let task_context = context.clone();
                            attempts.spawn(async move {
                                if let Err(error) = run_attempt(
                                    task_context.clone(),
                                    submission,
                                    attempt_id,
                                    dispatch,
                                    cancel_rx,
                                )
                                .await
                                {
                                    tracing::error!(%error, "attempt execution failed internally");
                                }
                            });
                        }
                        Err(StoreError::WorkspaceBusy) => {
                            context
                                .store
                                .set_blocker(&submission.submission_id, "workspace_busy", None)
                                .await?;
                        }
                        Err(StoreError::NotQueued) => {}
                        Err(error) => return Err(error.into()),
                    }
                }
            }
        }
    }

    for sender in context.cancellations.write().await.values() {
        let _ = sender.send(true);
    }
    while let Some(result) = attempts.join_next().await {
        if let Err(error) = result {
            tracing::error!(%error, "attempt task failed during shutdown");
        }
    }
    Ok(())
}

#[derive(Debug)]
enum DispatchDecision {
    Blocked(&'static str),
    Ready(Dispatch),
}

#[derive(Debug)]
struct Dispatch {
    route_id: String,
    source_id: String,
    source_label: String,
    model: Option<String>,
}

async fn select_dispatch(
    context: &SchedulerContext,
    submission: &QueuedSubmission,
) -> Result<DispatchDecision> {
    let requested_routes = submission
        .request
        .execution
        .as_ref()
        .and_then(|execution| execution.route_ids.as_ref());
    let route = requested_routes
        .and_then(|ids| ids.iter().find_map(|id| context.config.route(id)))
        .or_else(|| context.config.route(&context.config.defaults.route_id))
        .context("resolved route disappeared from configuration")?;
    let requested_model = submission
        .request
        .agent
        .as_ref()
        .and_then(|agent| agent.model.clone());
    let model = requested_model.or_else(|| (!route.model.is_empty()).then(|| route.model.clone()));
    let mut saw_unknown = false;
    for source_id in &route.source_ids {
        let source = context
            .config
            .source(source_id)
            .with_context(|| format!("route references missing source {source_id}"))?;
        let active = context.store.active_for_source(source_id).await?;
        match context.capacity.available(source, active).await {
            Some(available) if available > 0 => {
                return Ok(DispatchDecision::Ready(Dispatch {
                    route_id: route.id.clone(),
                    source_id: source.id.clone(),
                    source_label: source.label.clone(),
                    model,
                }));
            }
            None => saw_unknown = true,
            Some(_) => {}
        }
    }
    Ok(DispatchDecision::Blocked(if saw_unknown {
        "capacity_unknown"
    } else {
        "capacity_unavailable"
    }))
}

async fn run_attempt(
    context: SchedulerContext,
    submission: QueuedSubmission,
    attempt_id: String,
    dispatch: Dispatch,
    cancel: watch::Receiver<bool>,
) -> Result<()> {
    let prompt = submission
        .request
        .input
        .parts
        .iter()
        .filter_map(|part| match part {
            ContentPart::Text { text } => Some(text.as_str()),
            _ => None,
        })
        .collect::<Vec<_>>()
        .join("\n\n");
    let (workspace_path, writable) =
        resolve_workspace(&context.config, &submission.request).await?;
    let policy_id = submission
        .request
        .policy
        .as_ref()
        .map_or(context.config.defaults.policy_id.as_str(), |policy| {
            policy.id.as_str()
        });
    let policy = context
        .config
        .policy(policy_id)
        .context("resolved policy is unavailable")?;
    let run_timeout = submission
        .request
        .limits
        .as_ref()
        .and_then(|limits| limits.run_timeout_seconds)
        .unwrap_or(policy.run_timeout_seconds)
        .min(policy.run_timeout_seconds);
    let idle_timeout = submission
        .request
        .limits
        .as_ref()
        .and_then(|limits| limits.idle_timeout_seconds)
        .unwrap_or(policy.idle_timeout_seconds)
        .min(policy.idle_timeout_seconds);
    let credential_mounts = context
        .config
        .local_runner
        .credential_files
        .iter()
        .filter(|mapping| mapping.source_id == dispatch.source_id)
        .map(|mapping| CredentialMount {
            host_path: mapping.host_path.clone(),
            sandbox_path: mapping.sandbox_path.clone(),
        })
        .collect();
    let runner_request = RunnerRequest {
        attempt_id: attempt_id.clone(),
        sandbox_agent_path: context.config.sandbox_agent_path(),
        sandbox_agent_sha256: SANDBOX_AGENT_SHA256.to_owned(),
        opencode_path: context.config.local_runner.opencode_binary.clone(),
        opencode_sha256: context.config.local_runner.opencode_sha256.clone(),
        bubblewrap_path: context.config.local_runner.bubblewrap_binary.clone(),
        workspace_path,
        workspace_writable: writable,
        credential_mounts,
        prompt,
        model: dispatch.model,
        run_timeout_seconds: run_timeout,
        idle_timeout_seconds: idle_timeout,
    };
    let (event_tx, mut event_rx) = mpsc::channel(256);
    let runner_binary = context.config.local_runner.runner_binary.clone();
    let mut runner_task = tokio::spawn(async move {
        thieving_eyes_runner::execute(&runner_binary, runner_request, cancel, event_tx).await
    });
    let mut terminal_seen = false;
    while let Some(event) = tokio::select! {
        event = event_rx.recv() => event,
        result = &mut runner_task => {
            match result {
                Ok(Ok(())) => None,
                Ok(Err(error)) => {
                    tracing::warn!(%error, "runner failed before terminal report");
                    None
                }
                Err(error) => {
                    tracing::warn!(%error, "runner task join failed");
                    None
                }
            }
        }
    } {
        match event {
            RuntimeEvent::Started { agent_version } => {
                context
                    .store
                    .mark_attempt_running(&submission.submission_id, &attempt_id)
                    .await?;
                context
                    .store
                    .append_agent_event(
                        &submission.submission_id,
                        &attempt_id,
                        "diagnostic.warning",
                        json!({"runtime": "ready", "agent_version": agent_version}),
                    )
                    .await?;
            }
            RuntimeEvent::MessageDelta { text } => {
                context
                    .store
                    .append_agent_event(
                        &submission.submission_id,
                        &attempt_id,
                        "agent.message",
                        json!({"delta": text}),
                    )
                    .await?;
            }
            RuntimeEvent::Tool { title, status } => {
                context
                    .store
                    .append_agent_event(
                        &submission.submission_id,
                        &attempt_id,
                        "agent.tool",
                        json!({"title": title, "status": status}),
                    )
                    .await?;
            }
            RuntimeEvent::Plan { data } => {
                context
                    .store
                    .append_agent_event(&submission.submission_id, &attempt_id, "agent.plan", data)
                    .await?;
            }
            RuntimeEvent::Usage { data } => {
                context
                    .store
                    .append_agent_event(&submission.submission_id, &attempt_id, "agent.usage", data)
                    .await?;
            }
            RuntimeEvent::Completed { output } => {
                let (output, truncated) =
                    truncate_output(&output, context.config.daemon.max_inline_output_bytes);
                context
                    .store
                    .finish(FinishSpec {
                        submission_id: submission.submission_id.clone(),
                        attempt_id: attempt_id.clone(),
                        state: SubmissionState::Completed,
                        output: Some(output),
                        truncated,
                        error: None,
                        agent_version: None,
                    })
                    .await?;
                terminal_seen = true;
                break;
            }
            RuntimeEvent::Cancelled => {
                context
                    .store
                    .finish(FinishSpec {
                        submission_id: submission.submission_id.clone(),
                        attempt_id: attempt_id.clone(),
                        state: SubmissionState::Cancelled,
                        output: None,
                        truncated: false,
                        error: None,
                        agent_version: None,
                    })
                    .await?;
                terminal_seen = true;
                break;
            }
            RuntimeEvent::Failed { code, message } => {
                let error = ErrorDetail {
                    retryable: false,
                    scope: ErrorScope::Attempt,
                    retry_after_seconds: None,
                    field: None,
                    code,
                    message,
                };
                context
                    .store
                    .finish(FinishSpec {
                        submission_id: submission.submission_id.clone(),
                        attempt_id: attempt_id.clone(),
                        state: SubmissionState::Failed,
                        output: None,
                        truncated: false,
                        error: Some(error),
                        agent_version: None,
                    })
                    .await?;
                terminal_seen = true;
                break;
            }
        }
    }
    if !terminal_seen {
        let error = ErrorDetail {
            code: "runner_lost".to_owned(),
            message: "local runner exited without a terminal report".to_owned(),
            retryable: false,
            scope: ErrorScope::Attempt,
            retry_after_seconds: None,
            field: None,
        };
        context
            .store
            .finish(FinishSpec {
                submission_id: submission.submission_id.clone(),
                attempt_id: attempt_id.clone(),
                state: SubmissionState::Failed,
                output: None,
                truncated: false,
                error: Some(error),
                agent_version: None,
            })
            .await?;
    }
    runner_task.abort();
    let _ = runner_task.await;
    context
        .cancellations
        .write()
        .await
        .remove(&submission.submission_id);
    context.notify.notify_one();
    Ok(())
}

async fn resolve_workspace(
    config: &Config,
    request: &SubmissionCreate,
) -> Result<(Option<PathBuf>, bool)> {
    let Some(workspace) = request.workspace.as_ref() else {
        return Ok((None, false));
    };
    let WorkspaceRef::Local {
        root_id,
        path,
        access,
        ..
    } = workspace
    else {
        bail!("remote workspace bindings are unavailable in v0.1");
    };
    let root = config
        .workspace_root(root_id)
        .context("workspace root is unavailable")?;
    let relative = path.as_deref().unwrap_or("");
    if PathBuf::from(relative).is_absolute()
        || PathBuf::from(relative)
            .components()
            .any(|component| matches!(component, Component::ParentDir | Component::Prefix(_)))
    {
        bail!("workspace path escapes configured root");
    }
    let canonical_root = tokio::fs::canonicalize(&root.path).await?;
    let canonical = tokio::fs::canonicalize(canonical_root.join(relative)).await?;
    if !canonical.starts_with(&canonical_root) {
        bail!("workspace path escapes configured root through a symlink");
    }
    let writable = *access == WorkspaceAccess::Writable;
    if writable && !root.allow_writable {
        bail!("workspace root does not allow writable execution");
    }
    Ok((Some(canonical), writable))
}

fn effective_priority(item: &QueuedSubmission) -> u16 {
    let age_minutes = Utc::now()
        .signed_duration_since(item.created_at)
        .num_minutes()
        .max(0);
    let age = u16::try_from(age_minutes).unwrap_or(u16::MAX).min(50);
    u16::from(item.priority).saturating_add(age).min(100)
}

fn truncate_output(output: &str, max_bytes: usize) -> (String, bool) {
    if output.len() <= max_bytes {
        return (output.to_owned(), false);
    }
    let mut boundary = max_bytes;
    while boundary > 0 && !output.is_char_boundary(boundary) {
        boundary -= 1;
    }
    (output[..boundary].to_owned(), true)
}

#[cfg(test)]
mod tests {
    use super::truncate_output;

    #[test]
    fn truncation_preserves_utf8() {
        let (value, truncated) = truncate_output("你好", 4);
        assert_eq!(value, "你");
        assert!(truncated);
    }
}
