PRAGMA journal_mode = WAL;
PRAGMA foreign_keys = ON;

CREATE TABLE submissions (
    submission_id TEXT PRIMARY KEY,
    client_id TEXT NOT NULL,
    idempotency_key TEXT NOT NULL,
    request_digest TEXT NOT NULL,
    request_json TEXT NOT NULL,
    resolved_profile_json TEXT NOT NULL,
    resolved_policy_json TEXT NOT NULL,
    state TEXT NOT NULL,
    revision INTEGER NOT NULL,
    mode TEXT NOT NULL,
    priority INTEGER NOT NULL,
    not_before TEXT,
    start_deadline TEXT,
    blocker_code TEXT,
    blocker_detail TEXT,
    cancel_requested INTEGER NOT NULL DEFAULT 0,
    workspace_key TEXT,
    workspace_access TEXT,
    result_text TEXT,
    result_truncated INTEGER NOT NULL DEFAULT 0,
    error_json TEXT,
    latest_event_sequence INTEGER NOT NULL DEFAULT 0,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    finished_at TEXT,
    UNIQUE(client_id, idempotency_key)
);

CREATE INDEX submissions_queue_idx
    ON submissions(state, priority DESC, created_at ASC);

CREATE TABLE attempts (
    attempt_id TEXT PRIMARY KEY,
    submission_id TEXT NOT NULL REFERENCES submissions(submission_id) ON DELETE CASCADE,
    number INTEGER NOT NULL,
    state TEXT NOT NULL,
    route_id TEXT NOT NULL,
    adapter TEXT NOT NULL,
    model TEXT NOT NULL,
    target_id TEXT NOT NULL,
    source_id TEXT NOT NULL,
    source_label TEXT NOT NULL,
    sandbox_profile TEXT NOT NULL,
    runtime_json TEXT NOT NULL,
    agent_version TEXT,
    started_at TEXT,
    finished_at TEXT,
    usage_json TEXT,
    error_json TEXT,
    UNIQUE(submission_id, number)
);

CREATE INDEX attempts_active_source_idx ON attempts(source_id, state);

CREATE TABLE events (
    event_id TEXT PRIMARY KEY,
    submission_id TEXT NOT NULL REFERENCES submissions(submission_id) ON DELETE CASCADE,
    attempt_id TEXT,
    sequence INTEGER NOT NULL,
    occurred_at TEXT NOT NULL,
    event_type TEXT NOT NULL,
    data_json TEXT NOT NULL,
    UNIQUE(submission_id, sequence)
);

CREATE TABLE capacity_observations (
    source_id TEXT PRIMARY KEY,
    health TEXT NOT NULL,
    usage_kind TEXT,
    in_use INTEGER,
    observed_at TEXT,
    received_at TEXT NOT NULL
);

CREATE TABLE sessions (
    session_id TEXT PRIMARY KEY,
    client_id TEXT NOT NULL,
    state TEXT NOT NULL,
    metadata_json TEXT NOT NULL,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);
