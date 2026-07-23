use std::collections::{BTreeMap, HashSet};
use std::convert::Infallible;
use std::path::{Component, PathBuf};

use axum::extract::{Path, Query, State};
use axum::http::{HeaderMap, StatusCode, header};
use axum::response::sse::{Event, KeepAlive};
use axum::response::{IntoResponse, Response, Sse};
use axum::routing::{get, post};
use axum::{Json, Router};
use serde::Deserialize;
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use thieving_eyes_protocol::{
    ApiError, CancellationResult, CapabilityCatalog, CapabilityDescriptor, ErrorDetail, ErrorScope,
    FinalOutput, Page, ResourceRef, ResourceSummary, SessionBinding, SideEffects,
    SubmissionAccepted, SubmissionCreate, SubmissionPatch, SubmissionStatus, TaskMode,
    WorkspaceAccess, WorkspaceRef,
};
use ulid::Ulid;

use crate::ServiceState;
use crate::config::{Config, PolicyConfig, ProfileConfig, RouteConfig};
use crate::store::{AcceptSpec, StoreError};

pub fn router(state: ServiceState) -> Router {
    Router::new()
        .route(
            "/v1/submissions",
            post(create_submission).get(list_submissions),
        )
        .route(
            "/v1/submissions/{submission_id}",
            get(get_submission).patch(patch_submission),
        )
        .route(
            "/v1/submissions/{submission_id}/cancel",
            post(cancel_submission),
        )
        .route("/v1/submissions/{submission_id}/events", get(events))
        .route("/v1/submissions/{submission_id}/result", get(result))
        .route("/v1/sessions", get(empty_page))
        .route("/v1/sessions/{session_id}", get(unavailable_session))
        .route(
            "/v1/sessions/{session_id}/submissions",
            get(empty_page_with_id),
        )
        .route("/v1/sessions/{session_id}/events", get(empty_page_with_id))
        .route("/v1/capabilities", get(capabilities))
        .route("/v1/profiles", get(profiles))
        .route(
            "/v1/profiles/{resource_id}/versions/{version}",
            get(profile),
        )
        .route("/v1/policies", get(policies))
        .route("/v1/policies/{resource_id}/versions/{version}", get(policy))
        .route("/v1/extensions", get(empty_page))
        .route(
            "/v1/extensions/{resource_id}/versions/{version}",
            get(unavailable_resource),
        )
        .with_state(state)
}

async fn create_submission(
    State(state): State<ServiceState>,
    headers: HeaderMap,
    Json(request): Json<SubmissionCreate>,
) -> Result<(StatusCode, Json<SubmissionAccepted>), ApiFailure> {
    let key = headers
        .get("Idempotency-Key")
        .and_then(|value| value.to_str().ok())
        .filter(|value| !value.is_empty() && value.len() <= 200)
        .ok_or_else(|| ApiFailure::invalid("Idempotency-Key header is required", None))?;
    let digest = request_digest(&request)?;
    let client_id = local_client_id();
    if let Some(replay) = state
        .store
        .idempotent_replay(&client_id, key, &digest)
        .await
        .map_err(ApiFailure::from_store)?
    {
        return Ok((StatusCode::OK, Json(replay)));
    }
    let resolved = validate_request(&state.config, &request).await?;
    let accepted = state
        .store
        .accept(AcceptSpec {
            client_id,
            idempotency_key: key.to_owned(),
            request,
            request_digest: digest,
            profile: resolved.profile,
            policy: resolved.policy,
            workspace_key: resolved.workspace_key,
            workspace_access: resolved.workspace_access,
            profile_config: resolved.profile_config,
            policy_config: resolved.policy_config,
            routes: resolved.routes,
        })
        .await
        .map_err(ApiFailure::from_store)?;
    state.notify.notify_one();
    Ok((
        if accepted.replay {
            StatusCode::OK
        } else {
            StatusCode::CREATED
        },
        Json(accepted.response),
    ))
}

async fn list_submissions(
    State(state): State<ServiceState>,
) -> Result<Json<Page<SubmissionStatus>>, ApiFailure> {
    let items = state
        .store
        .list_statuses(100)
        .await
        .map_err(ApiFailure::from_store)?;
    Ok(Json(Page {
        items,
        next_cursor: None,
    }))
}

async fn get_submission(
    State(state): State<ServiceState>,
    Path(submission_id): Path<String>,
) -> Result<Response, ApiFailure> {
    let status = state
        .store
        .status(&submission_id)
        .await
        .map_err(ApiFailure::from_store)?;
    let revision = status.revision;
    let mut response = Json(status).into_response();
    response.headers_mut().insert(
        header::ETAG,
        format!("\"{revision}\"")
            .parse()
            .map_err(|_| ApiFailure::internal("failed to encode ETag"))?,
    );
    Ok(response)
}

async fn patch_submission(
    State(state): State<ServiceState>,
    Path(submission_id): Path<String>,
    headers: HeaderMap,
    Json(patch): Json<SubmissionPatch>,
) -> Result<Json<SubmissionStatus>, ApiFailure> {
    let revision = headers
        .get(header::IF_MATCH)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.trim_matches('"').parse::<u64>().ok())
        .ok_or_else(|| ApiFailure::invalid("If-Match revision is required", None))?;
    if patch.priority.is_some_and(|priority| priority > 100) {
        return Err(ApiFailure::invalid(
            "priority must be between 0 and 100",
            Some("/priority"),
        ));
    }
    let status = state
        .store
        .patch_scheduling(&submission_id, revision, &patch)
        .await
        .map_err(ApiFailure::from_store)?;
    state.notify.notify_one();
    Ok(Json(status))
}

async fn cancel_submission(
    State(state): State<ServiceState>,
    Path(submission_id): Path<String>,
) -> Result<Json<CancellationResult>, ApiFailure> {
    let (disposition, revision) = state
        .store
        .cancel(&submission_id)
        .await
        .map_err(ApiFailure::from_store)?;
    if disposition == "cancellation_requested"
        && let Some(sender) = state.cancellations.read().await.get(&submission_id)
    {
        let _ = sender.send(true);
    }
    state.notify.notify_one();
    Ok(Json(CancellationResult {
        submission_id,
        disposition,
        revision,
    }))
}

#[derive(Debug, Deserialize)]
struct EventQuery {
    after_sequence: Option<u64>,
}

async fn events(
    State(state): State<ServiceState>,
    Path(submission_id): Path<String>,
    Query(query): Query<EventQuery>,
    headers: HeaderMap,
) -> Result<Sse<impl futures::Stream<Item = Result<Event, Infallible>>>, ApiFailure> {
    let after = query
        .after_sequence
        .or_else(|| {
            headers
                .get("Last-Event-ID")
                .and_then(|value| value.to_str().ok())
                .and_then(|value| value.parse().ok())
        })
        .unwrap_or(0);
    let mut receiver = state.store.subscribe();
    let initial = state
        .store
        .events_after(&submission_id, after)
        .await
        .map_err(ApiFailure::from_store)?;
    let store = state.store.clone();
    let event_stream = async_stream::stream! {
        let mut last_sequence = after;
        for event in initial {
            if event.sequence > last_sequence {
                last_sequence = event.sequence;
                yield event_to_sse(event);
            }
        }
        loop {
            match receiver.recv().await {
                Ok(event) if event.submission_id == submission_id && event.sequence > last_sequence => {
                    if event.sequence == last_sequence.saturating_add(1) {
                        last_sequence = event.sequence;
                        yield event_to_sse(event);
                    } else {
                        match store.events_after(&submission_id, last_sequence).await {
                            Ok(events) => {
                                for event in events {
                                    if event.sequence > last_sequence {
                                        last_sequence = event.sequence;
                                        yield event_to_sse(event);
                                    }
                                }
                            }
                            Err(error) => {
                                tracing::warn!(%error, %submission_id, "failed to recover SSE sequence gap");
                                break;
                            }
                        }
                    }
                }
                Ok(_) => {}
                Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => {
                    match store.events_after(&submission_id, last_sequence).await {
                        Ok(events) => {
                            for event in events {
                                if event.sequence > last_sequence {
                                    last_sequence = event.sequence;
                                    yield event_to_sse(event);
                                }
                            }
                        }
                        Err(error) => {
                            tracing::warn!(%error, %submission_id, "failed to recover lagged SSE stream");
                            break;
                        }
                    }
                }
                Err(tokio::sync::broadcast::error::RecvError::Closed) => break,
            }
        }
    };
    Ok(Sse::new(event_stream).keep_alive(KeepAlive::default()))
}

async fn result(
    State(state): State<ServiceState>,
    Path(submission_id): Path<String>,
) -> Result<Response, ApiFailure> {
    match state.store.result(&submission_id).await {
        Ok(result) => Ok(Json(result).into_response()),
        Err(StoreError::NotQueued) => Err(ApiFailure::new(
            StatusCode::CONFLICT,
            "result_not_ready",
            "submission has not reached a terminal state",
            ErrorScope::Submission,
        )),
        Err(error) => Err(ApiFailure::from_store(error)),
    }
}

async fn capabilities() -> Json<CapabilityCatalog> {
    Json(CapabilityCatalog {
        capabilities: vec![
            capability("core.task", BTreeMap::new()),
            capability(
                "core.workspace.local",
                BTreeMap::from([("access".to_owned(), json!(["read_only", "writable"]))]),
            ),
            capability("core.events.sse", BTreeMap::new()),
            capability(
                "agent.opencode",
                BTreeMap::from([("mode".to_owned(), json!(["task"]))]),
            ),
            capability("sandbox.bubblewrap", BTreeMap::new()),
        ],
    })
}

async fn profiles(State(state): State<ServiceState>) -> Json<Page<ResourceSummary>> {
    Json(Page {
        items: state
            .config
            .profiles
            .iter()
            .map(|profile| ResourceSummary {
                resource_ref: resource_ref(&profile.id, &profile.version, profile),
                description: Some(profile.description.clone()),
                capabilities: vec![capability("sandbox.bubblewrap", BTreeMap::new())],
                deprecated: false,
            })
            .collect(),
        next_cursor: None,
    })
}

async fn profile(
    State(state): State<ServiceState>,
    Path((resource_id, version)): Path<(String, String)>,
) -> Result<Json<ResourceSummary>, ApiFailure> {
    let profile = state
        .config
        .profiles
        .iter()
        .find(|profile| profile.id == resource_id && profile.version == version)
        .ok_or_else(ApiFailure::not_found)?;
    Ok(Json(ResourceSummary {
        resource_ref: resource_ref(&profile.id, &profile.version, profile),
        description: Some(profile.description.clone()),
        capabilities: vec![capability("sandbox.bubblewrap", BTreeMap::new())],
        deprecated: false,
    }))
}

async fn policies(State(state): State<ServiceState>) -> Json<Page<ResourceSummary>> {
    Json(Page {
        items: state
            .config
            .policies
            .iter()
            .map(|policy| ResourceSummary {
                resource_ref: resource_ref(&policy.id, &policy.version, policy),
                description: Some(policy.description.clone()),
                capabilities: Vec::new(),
                deprecated: false,
            })
            .collect(),
        next_cursor: None,
    })
}

async fn policy(
    State(state): State<ServiceState>,
    Path((resource_id, version)): Path<(String, String)>,
) -> Result<Json<ResourceSummary>, ApiFailure> {
    let policy = state
        .config
        .policies
        .iter()
        .find(|policy| policy.id == resource_id && policy.version == version)
        .ok_or_else(ApiFailure::not_found)?;
    Ok(Json(ResourceSummary {
        resource_ref: resource_ref(&policy.id, &policy.version, policy),
        description: Some(policy.description.clone()),
        capabilities: Vec::new(),
        deprecated: false,
    }))
}

async fn empty_page() -> Json<Page<Value>> {
    Json(Page {
        items: Vec::new(),
        next_cursor: None,
    })
}

async fn empty_page_with_id(Path(_id): Path<String>) -> Json<Page<Value>> {
    empty_page().await
}

async fn unavailable_session(Path(_id): Path<String>) -> ApiFailure {
    ApiFailure::new(
        StatusCode::NOT_FOUND,
        "session_unavailable",
        "persistent sessions are not available in this runtime profile",
        ErrorScope::Session,
    )
}

async fn unavailable_resource(Path((_id, _version)): Path<(String, String)>) -> ApiFailure {
    ApiFailure::not_found()
}

struct ResolvedRequest {
    profile: ResourceRef,
    policy: ResourceRef,
    workspace_key: Option<String>,
    workspace_access: Option<String>,
    profile_config: ProfileConfig,
    policy_config: PolicyConfig,
    routes: Vec<RouteConfig>,
}

async fn validate_request(
    config: &Config,
    request: &SubmissionCreate,
) -> Result<ResolvedRequest, ApiFailure> {
    if !matches!(request.mode, TaskMode::Task) {
        return Err(ApiFailure::capability("goal mode is not published by v0.1"));
    }
    if request.input.parts.is_empty() {
        return Err(ApiFailure::invalid(
            "input.parts must not be empty",
            Some("/input/parts"),
        ));
    }
    let mut text_bytes = 0_usize;
    for (index, part) in request.input.parts.iter().enumerate() {
        let thieving_eyes_protocol::ContentPart::Text { text } = part else {
            return Err(ApiFailure::capability("v0.1 only accepts text input parts"));
        };
        text_bytes = text_bytes.saturating_add(text.len());
        if text.is_empty() {
            return Err(ApiFailure::invalid(
                "text input must not be empty",
                Some(&format!("/input/parts/{index}/text")),
            ));
        }
    }
    if text_bytes > 1_048_576 {
        return Err(ApiFailure::invalid(
            "inline input exceeds 1 MiB",
            Some("/input"),
        ));
    }
    if request.output.as_ref().is_some_and(|output| {
        output.artifacts.is_some() || !matches!(output.final_output, None | Some(FinalOutput::Text))
    }) {
        return Err(ApiFailure::capability(
            "v0.1 only supports inline text output",
        ));
    }
    if request.agent.as_ref().is_some_and(|agent| {
        agent
            .adapter
            .as_deref()
            .is_some_and(|adapter| adapter != "opencode")
            || !agent.extensions.is_empty()
            || !agent.required_capabilities.is_empty()
    }) {
        return Err(ApiFailure::capability(
            "requested Agent capability is unavailable",
        ));
    }
    if !matches!(request.session, None | Some(SessionBinding::Ephemeral)) {
        return Err(ApiFailure::capability(
            "persistent sessions are unavailable in v0.1",
        ));
    }
    if request
        .limits
        .as_ref()
        .is_some_and(|limits| limits.max_tokens.is_some() || limits.max_provider_requests.is_some())
    {
        return Err(ApiFailure::capability(
            "provider token/request hard limits are unavailable",
        ));
    }
    if request
        .scheduling
        .as_ref()
        .and_then(|scheduling| scheduling.priority)
        .is_some_and(|priority| priority > 100)
    {
        return Err(ApiFailure::invalid(
            "priority must be between 0 and 100",
            Some("/scheduling/priority"),
        ));
    }
    if let Some(scheduling) = request.scheduling.as_ref()
        && let (Some(not_before), Some(deadline)) =
            (scheduling.not_before, scheduling.start_deadline)
        && deadline <= not_before
    {
        return Err(ApiFailure::invalid(
            "start_deadline must be after not_before",
            Some("/scheduling/start_deadline"),
        ));
    }

    let profile_selector = request
        .agent
        .as_ref()
        .and_then(|agent| agent.profile.as_ref())
        .map(|profile| (profile.id.as_str(), profile.version.as_deref()));
    let (profile_id, profile_version) =
        profile_selector.unwrap_or((config.defaults.profile_id.as_str(), None));
    let profile = config.profile(profile_id, profile_version).ok_or_else(|| {
        ApiFailure::new(
            StatusCode::UNPROCESSABLE_ENTITY,
            "policy_denied",
            "profile is unavailable",
            ErrorScope::Request,
        )
    })?;
    let policy_selector = request
        .policy
        .as_ref()
        .map(|policy| (policy.id.as_str(), policy.version.as_deref()));
    let (policy_id, policy_version) =
        policy_selector.unwrap_or((config.defaults.policy_id.as_str(), None));
    let policy = config.policy(policy_id, policy_version).ok_or_else(|| {
        ApiFailure::new(
            StatusCode::UNPROCESSABLE_ENTITY,
            "policy_denied",
            "policy is unavailable",
            ErrorScope::Request,
        )
    })?;

    if let Some(execution) = request.execution.as_ref() {
        validate_nonempty_unique(execution.route_ids.as_ref(), "/execution/route_ids")?;
        validate_nonempty_unique(execution.target_ids.as_ref(), "/execution/target_ids")?;
        if execution
            .target_ids
            .as_ref()
            .is_some_and(|targets| targets.iter().any(|target| target != "local"))
        {
            return Err(ApiFailure::new(
                StatusCode::UNPROCESSABLE_ENTITY,
                "route_unsatisfied",
                "v0.1 only has the local target",
                ErrorScope::Route,
            ));
        }
    }

    let requested_model = request
        .agent
        .as_ref()
        .and_then(|agent| agent.model.as_deref());
    let route_ids = request
        .execution
        .as_ref()
        .and_then(|execution| execution.route_ids.as_ref())
        .cloned()
        .unwrap_or_else(|| vec![config.defaults.route_id.clone()]);
    let routes: Vec<RouteConfig> = route_ids
        .iter()
        .filter_map(|id| config.route(id))
        .filter(|route| {
            requested_model.is_none_or(|model| route.model.is_empty() || route.model == model)
        })
        .cloned()
        .collect();
    if routes.is_empty() {
        return Err(ApiFailure::new(
            StatusCode::UNPROCESSABLE_ENTITY,
            "route_unsatisfied",
            "no requested route can satisfy the Agent model",
            ErrorScope::Route,
        ));
    }

    let (workspace_key, workspace_access) = validate_workspace(config, request).await?;
    if request
        .execution
        .as_ref()
        .and_then(|execution| execution.side_effects)
        == Some(SideEffects::ReadOnly)
        && workspace_access.as_deref() == Some("writable")
    {
        return Err(ApiFailure::invalid(
            "writable workspace conflicts with read_only side_effects",
            Some("/execution/side_effects"),
        ));
    }
    Ok(ResolvedRequest {
        profile: resource_ref(&profile.id, &profile.version, profile),
        policy: resource_ref(&policy.id, &policy.version, policy),
        workspace_key,
        workspace_access,
        profile_config: profile.clone(),
        policy_config: policy.clone(),
        routes,
    })
}

async fn validate_workspace(
    config: &Config,
    request: &SubmissionCreate,
) -> Result<(Option<String>, Option<String>), ApiFailure> {
    let Some(workspace) = request.workspace.as_ref() else {
        return Ok((None, None));
    };
    let WorkspaceRef::Local {
        root_id,
        path,
        access,
        ..
    } = workspace
    else {
        return Err(ApiFailure::capability(
            "remote workspace bindings are unavailable in v0.1",
        ));
    };
    if *access == WorkspaceAccess::WritableOverlay {
        return Err(ApiFailure::capability(
            "writable_overlay is unavailable in v0.1",
        ));
    }
    let root = config.workspace_root(root_id).ok_or_else(|| {
        ApiFailure::new(
            StatusCode::UNPROCESSABLE_ENTITY,
            "workspace_unavailable",
            "workspace root is unavailable",
            ErrorScope::Request,
        )
    })?;
    if *access == WorkspaceAccess::Writable && !root.allow_writable {
        return Err(ApiFailure::new(
            StatusCode::FORBIDDEN,
            "policy_denied",
            "workspace root does not allow writable access",
            ErrorScope::Request,
        ));
    }
    let relative = PathBuf::from(path.as_deref().unwrap_or(""));
    if relative.is_absolute()
        || relative.components().any(|component| {
            matches!(
                component,
                Component::ParentDir | Component::Prefix(_) | Component::RootDir
            )
        })
    {
        return Err(ApiFailure::invalid(
            "workspace path must be a safe relative path",
            Some("/workspace/path"),
        ));
    }
    let canonical_root = tokio::fs::canonicalize(&root.path).await.map_err(|_| {
        ApiFailure::new(
            StatusCode::UNPROCESSABLE_ENTITY,
            "workspace_unavailable",
            "workspace root cannot be resolved",
            ErrorScope::Request,
        )
    })?;
    let canonical = tokio::fs::canonicalize(canonical_root.join(relative))
        .await
        .map_err(|_| {
            ApiFailure::new(
                StatusCode::UNPROCESSABLE_ENTITY,
                "workspace_unavailable",
                "workspace path cannot be resolved",
                ErrorScope::Request,
            )
        })?;
    if !canonical.starts_with(&canonical_root) {
        return Err(ApiFailure::invalid(
            "workspace path escapes root through a symlink",
            Some("/workspace/path"),
        ));
    }
    Ok((
        Some(canonical.to_string_lossy().into_owned()),
        Some(
            match access {
                WorkspaceAccess::ReadOnly => "read_only",
                WorkspaceAccess::Writable => "writable",
                WorkspaceAccess::WritableOverlay => "writable_overlay",
            }
            .to_owned(),
        ),
    ))
}

fn validate_nonempty_unique(values: Option<&Vec<String>>, field: &str) -> Result<(), ApiFailure> {
    if let Some(values) = values {
        let unique: HashSet<_> = values.iter().collect();
        if values.is_empty() || unique.len() != values.len() {
            return Err(ApiFailure::invalid(
                "selector must be non-empty and unique",
                Some(field),
            ));
        }
    }
    Ok(())
}

fn request_digest(request: &SubmissionCreate) -> Result<String, ApiFailure> {
    let mut normalized = request.clone();
    normalized.client_reference = None;
    let bytes = serde_jcs::to_vec(&normalized)
        .map_err(|_| ApiFailure::internal("encode request digest"))?;
    Ok(format!("sha256:{:x}", Sha256::digest(bytes)))
}

fn resource_ref<T: serde::Serialize>(id: &str, version: &str, value: &T) -> ResourceRef {
    let bytes = serde_json::to_vec(value).unwrap_or_default();
    ResourceRef {
        id: id.to_owned(),
        version: version.to_owned(),
        digest: format!("sha256:{:x}", Sha256::digest(bytes)),
    }
}

fn capability(name: &str, constraints: BTreeMap<String, Value>) -> CapabilityDescriptor {
    CapabilityDescriptor {
        name: name.to_owned(),
        version: "1".to_owned(),
        constraints,
    }
}

fn event_to_sse(event: thieving_eyes_protocol::EventEnvelope) -> Result<Event, Infallible> {
    let data = serde_json::to_string(&event).unwrap_or_else(|_| "{}".to_owned());
    Ok(Event::default()
        .id(event.sequence.to_string())
        .event(event.event_type)
        .data(data))
}

fn local_client_id() -> String {
    #[cfg(unix)]
    {
        format!("uid:{}", nix::unistd::getuid().as_raw())
    }
    #[cfg(not(unix))]
    {
        "local".to_owned()
    }
}

#[derive(Debug)]
struct ApiFailure {
    status: StatusCode,
    detail: ErrorDetail,
}

impl ApiFailure {
    fn new(status: StatusCode, code: &str, message: &str, scope: ErrorScope) -> Self {
        Self {
            status,
            detail: ErrorDetail {
                code: code.to_owned(),
                message: message.to_owned(),
                retryable: false,
                retry_after_seconds: None,
                scope,
                field: None,
            },
        }
    }

    fn invalid(message: &str, field: Option<&str>) -> Self {
        let mut failure = Self::new(
            StatusCode::BAD_REQUEST,
            "invalid_request",
            message,
            ErrorScope::Request,
        );
        failure.detail.field = field.map(str::to_owned);
        failure
    }

    fn capability(message: &str) -> Self {
        Self::new(
            StatusCode::UNPROCESSABLE_ENTITY,
            "capability_unavailable",
            message,
            ErrorScope::Route,
        )
    }

    fn not_found() -> Self {
        Self::new(
            StatusCode::NOT_FOUND,
            "not_found",
            "resource not found",
            ErrorScope::Request,
        )
    }

    fn internal(message: &str) -> Self {
        Self::new(
            StatusCode::INTERNAL_SERVER_ERROR,
            "internal_error",
            message,
            ErrorScope::Request,
        )
    }

    fn from_store(error: StoreError) -> Self {
        match error {
            StoreError::NotFound => Self::not_found(),
            StoreError::IdempotencyConflict => Self::new(
                StatusCode::CONFLICT,
                "idempotency_conflict",
                "idempotency key was reused with a different request",
                ErrorScope::Request,
            ),
            StoreError::RevisionConflict => Self::new(
                StatusCode::PRECONDITION_FAILED,
                "revision_conflict",
                "resource revision changed",
                ErrorScope::Submission,
            ),
            StoreError::NotQueued => Self::new(
                StatusCode::CONFLICT,
                "invalid_request",
                "operation is only valid for a queued submission",
                ErrorScope::Submission,
            ),
            StoreError::WorkspaceBusy => Self::new(
                StatusCode::CONFLICT,
                "workspace_unavailable",
                "workspace has an active writable attempt",
                ErrorScope::Submission,
            ),
            StoreError::InvalidScheduling => Self::invalid(
                "start_deadline must be after not_before",
                Some("/start_deadline"),
            ),
            StoreError::Internal(error) => {
                tracing::error!(%error, "store request failed");
                Self::internal("persistent state operation failed")
            }
        }
    }
}

impl IntoResponse for ApiFailure {
    fn into_response(self) -> Response {
        (
            self.status,
            Json(ApiError {
                request_id: format!("req_{}", Ulid::new()),
                error: self.detail,
            }),
        )
            .into_response()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use thieving_eyes_protocol::{ContentPart, Input};

    #[test]
    fn digest_ignores_client_reference() {
        let mut request = SubmissionCreate {
            client_reference: Some("a".to_owned()),
            labels: BTreeMap::new(),
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
        };
        let first = request_digest(&request).unwrap_or_else(|error| panic!("digest: {error:?}"));
        request.client_reference = Some("b".to_owned());
        let second = request_digest(&request).unwrap_or_else(|error| panic!("digest: {error:?}"));
        assert_eq!(first, second);
    }
}
