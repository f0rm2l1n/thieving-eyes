//! Stable public HTTP/JSON protocol types.

use std::collections::BTreeMap;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct SubmissionCreate {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub client_reference: Option<String>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub labels: BTreeMap<String, String>,
    #[serde(default)]
    pub mode: TaskMode,
    pub input: Input,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub workspace: Option<WorkspaceRef>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub output: Option<OutputSpec>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub agent: Option<AgentSelector>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub execution: Option<ExecutionSelector>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub session: Option<SessionBinding>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub scheduling: Option<Scheduling>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub limits: Option<Limits>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub policy: Option<ResourceSelector>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum TaskMode {
    #[default]
    Task,
    Goal {
        #[serde(default, skip_serializing_if = "Option::is_none")]
        token_budget: Option<u64>,
    },
}

impl TaskMode {
    #[must_use]
    pub const fn name(&self) -> &'static str {
        match self {
            Self::Task => "task",
            Self::Goal { .. } => "goal",
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct Input {
    pub parts: Vec<ContentPart>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
pub enum ContentPart {
    Text {
        text: String,
    },
    Data {
        data: Value,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        media_type: Option<String>,
    },
    File {
        file: FileRef,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        name: Option<String>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        media_type: Option<String>,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum FileRef {
    Workspace {
        path: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        digest: Option<String>,
    },
    Object {
        object: ObjectRef,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct ObjectRef {
    pub resolver_id: String,
    pub object_key: String,
    pub digest: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub size_bytes: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub media_type: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum WorkspaceRef {
    Local {
        root_id: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        path: Option<String>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        revision: Option<String>,
        #[serde(default)]
        access: WorkspaceAccess,
    },
    Binding {
        binding_id: String,
        revision: String,
        #[serde(default)]
        access: WorkspaceAccess,
    },
}

impl WorkspaceRef {
    #[must_use]
    pub const fn access(&self) -> WorkspaceAccess {
        match self {
            Self::Local { access, .. } | Self::Binding { access, .. } => *access,
        }
    }
}

#[derive(Debug, Clone, Copy, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum WorkspaceAccess {
    #[default]
    ReadOnly,
    Writable,
    WritableOverlay,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct OutputSpec {
    #[serde(rename = "final")]
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub final_output: Option<FinalOutput>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub artifacts: Option<ArtifactCollection>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum FinalOutput {
    Text,
    JsonSchema { schema: JsonSchemaSource },
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum JsonSchemaSource {
    Inline { value: Value },
    Object { object: ObjectRef },
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct ArtifactCollection {
    pub sink_id: String,
    pub include: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct AgentSelector {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub profile: Option<ResourceSelector>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub adapter: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub model: Option<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub required_capabilities: Vec<CapabilityRequirement>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub extensions: Vec<ExtensionRef>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct CapabilityRequirement {
    pub name: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub min_version: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct ExtensionRef {
    pub kind: ExtensionKind,
    pub resource: ResourceSelector,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ExtensionKind {
    Skill,
    Mcp,
    Plugin,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct ExecutionSelector {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub route_ids: Option<Vec<String>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub target_ids: Option<Vec<String>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub locality: Option<Locality>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub side_effects: Option<SideEffects>,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum Locality {
    LocalOnly,
    Any,
}

#[derive(Debug, Clone, Copy, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum SideEffects {
    ReadOnly,
    IdempotentWrite,
    #[default]
    SideEffecting,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "mode", rename_all = "snake_case", deny_unknown_fields)]
pub enum SessionBinding {
    Ephemeral,
    Create {
        #[serde(default, skip_serializing_if = "Option::is_none")]
        retention_seconds: Option<u64>,
    },
    Resume {
        session_id: String,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct Scheduling {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub priority: Option<u8>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub not_before: Option<DateTime<Utc>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub start_deadline: Option<DateTime<Utc>>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct Limits {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub run_timeout_seconds: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub idle_timeout_seconds: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_tokens: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_provider_requests: Option<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct ResourceSelector {
    pub id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub version: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct ResourceRef {
    pub id: String,
    pub version: String,
    pub digest: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct RuntimeRef {
    pub name: String,
    pub version: String,
    pub digest: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct SubmissionAccepted {
    pub submission_id: String,
    pub state: SubmissionState,
    pub terminal: bool,
    pub revision: u64,
    pub request_digest: String,
    pub resolved_profile: ResourceRef,
    pub resolved_policy: ResourceRef,
    pub status_url: String,
    pub events_url: String,
    pub result_url: String,
    pub idempotent_replay: bool,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum SubmissionState {
    Queued,
    Running,
    Completed,
    Failed,
    Cancelled,
    Expired,
    Uncertain,
}

impl SubmissionState {
    #[must_use]
    pub const fn terminal(self) -> bool {
        matches!(
            self,
            Self::Completed | Self::Failed | Self::Cancelled | Self::Expired | Self::Uncertain
        )
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct SubmissionStatus {
    pub submission_id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub client_reference: Option<String>,
    pub request_digest: String,
    pub resolved_profile: ResourceRef,
    pub resolved_policy: ResourceRef,
    pub state: SubmissionState,
    pub terminal: bool,
    pub revision: u64,
    pub mode: String,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub blocker: Option<Blocker>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub session_id: Option<String>,
    pub attempts: Vec<Attempt>,
    pub latest_event_sequence: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<ErrorDetail>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct Blocker {
    pub code: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub retry_after: Option<DateTime<Utc>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub detail: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct Attempt {
    pub attempt_id: String,
    pub number: u32,
    pub state: AttemptState,
    pub route_id: String,
    pub adapter: String,
    pub model: String,
    pub target_id: String,
    pub source_label: String,
    pub sandbox_profile: String,
    pub runtime: RuntimeRef,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub agent_version: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub session_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub started_at: Option<DateTime<Utc>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub finished_at: Option<DateTime<Utc>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub usage: Option<UsageSummary>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<ErrorDetail>,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum AttemptState {
    Starting,
    Running,
    Completed,
    Failed,
    Cancelled,
    Uncertain,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct UsageSummary {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub input_tokens: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub output_tokens: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub reasoning_tokens: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub provider_requests: Option<u64>,
    pub measurement: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct EventEnvelope {
    pub event_id: String,
    pub submission_id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub session_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub attempt_id: Option<String>,
    pub sequence: u64,
    pub occurred_at: DateTime<Utc>,
    #[serde(rename = "type")]
    pub event_type: String,
    pub data: Value,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct SubmissionResult {
    pub submission_id: String,
    pub state: SubmissionState,
    pub terminal: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub output: Option<OutputValue>,
    pub artifacts: Vec<ArtifactRef>,
    pub attempts: Vec<Attempt>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub usage: Option<UsageSummary>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<ErrorDetail>,
    pub finished_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum OutputValue {
    Text { text: String, truncated: bool },
    Data { data: Value },
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ArtifactRef {
    pub sink_id: String,
    pub object_key: String,
    pub digest: String,
    pub size_bytes: u64,
    pub media_type: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct CancellationResult {
    pub submission_id: String,
    pub disposition: String,
    pub revision: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct SubmissionPatch {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub priority: Option<u8>,
    #[serde(
        default,
        deserialize_with = "deserialize_optional_nullable",
        skip_serializing_if = "Option::is_none"
    )]
    pub not_before: Option<Option<DateTime<Utc>>>,
    #[serde(
        default,
        deserialize_with = "deserialize_optional_nullable",
        skip_serializing_if = "Option::is_none"
    )]
    pub start_deadline: Option<Option<DateTime<Utc>>>,
}

fn deserialize_optional_nullable<'de, D, T>(deserializer: D) -> Result<Option<Option<T>>, D::Error>
where
    D: serde::Deserializer<'de>,
    T: Deserialize<'de>,
{
    Option::<T>::deserialize(deserializer).map(Some)
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct CapabilityDescriptor {
    pub name: String,
    pub version: String,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub constraints: BTreeMap<String, Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct CapabilityCatalog {
    pub capabilities: Vec<CapabilityDescriptor>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ResourceSummary {
    #[serde(rename = "ref")]
    pub resource_ref: ResourceRef,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    pub capabilities: Vec<CapabilityDescriptor>,
    pub deprecated: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct Page<T> {
    pub items: Vec<T>,
    pub next_cursor: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ApiError {
    pub request_id: String,
    pub error: ErrorDetail,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ErrorDetail {
    pub code: String,
    pub message: String,
    pub retryable: bool,
    pub scope: ErrorScope,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub retry_after_seconds: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub field: Option<String>,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ErrorScope {
    Request,
    Submission,
    Attempt,
    Route,
    Target,
    Source,
    Session,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn minimal_submission_has_stable_defaults() {
        let submission: SubmissionCreate = serde_json::from_value(serde_json::json!({
            "input": {"parts": [{"type": "text", "text": "hello"}]}
        }))
        .unwrap_or_else(|error| panic!("minimal submission should decode: {error}"));

        assert_eq!(submission.mode, TaskMode::Task);
        assert!(submission.labels.is_empty());
    }

    #[test]
    fn terminal_is_explicit() {
        assert!(!SubmissionState::Queued.terminal());
        assert!(SubmissionState::Uncertain.terminal());
    }

    #[test]
    fn patch_distinguishes_missing_from_null() {
        let patch: SubmissionPatch = serde_json::from_value(serde_json::json!({
            "not_before": null
        }))
        .unwrap_or_else(|error| panic!("patch should decode: {error}"));
        assert_eq!(patch.not_before, Some(None));
        assert_eq!(patch.start_deadline, None);
    }
}
