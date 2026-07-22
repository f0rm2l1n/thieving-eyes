# thieving-eyes development guide

## Project purpose

thieving-eyes is a capacity-aware background execution daemon for headless coding agents. Clients submit durable `Submission` resources; the daemon queues and schedules them across multiple provider sources/accounts and local or remote targets, then exposes status, events, sessions, results, and cancellation through one HTTP/JSON + SSE API.

The project is an execution control plane, not a business workflow engine. Do not add Git/finding semantics, prompt interpretation, result correctness checks, or Agent-to-Agent orchestration to the core.

Read these before changing the corresponding area:

- `docs/submission-api.md`: public API and state model;
- `docs/architecture.md`: fixed v1 engineering choices;
- `docs/runner-gateway-contract.md`: daemon/runner trust and dispatch contract;
- `docs/sandbox-agent-runtime.md`: mandatory Sandbox Agent HTTP/ACP binding;
- `docs/goal-mode.md`: provider-native long-running goal mode;
- `docs/configuration.md`: resource and configuration model.

## Fixed v1 decisions

- Use stable Rust with Tokio, Axum/rustls, Serde, SQLx, and SQLite WAL.
- Ship `thieving-eyesd`, `thieving-eyes-runner`, and the thin `eyes` CLI from one Rust workspace.
- There is one authoritative daemon/SQLite writer. Do not add PostgreSQL, HA, or distributed consensus abstractions in v1.
- `api/openapi.yaml` will be the public schema source of truth. Public Rust types and generated SDKs must agree with it.
- Sandbox Agent is the only general Agent runtime in v1. Integrate it through its pinned HTTP/ACP process boundary; do not embed its unstable internal crates or add a parallel provider runtime.
- The local OpenCode goal feature is a narrow patch to the pinned Sandbox Agent OpenCode adapter. It must not fork the public Submission protocol.
- Linux is the v1 host platform. The first-party local sandbox backend is bubblewrap; `external` is an explicit administrator assertion for an already isolated runner.

## Domain language

Use the documented names consistently:

- `Submission`: caller-visible durable request;
- `Attempt`: one concrete dispatch to a route/source/target;
- `Session`: resumable Agent context, independent of a Submission;
- `Source`: global provider account/endpoint capacity and credential scope;
- `SourceBinding`: target-local way to use a Source;
- `Route`: logical adapter/model/source pool/target choice;
- `Profile`: immutable Agent approval, sandbox, and extension configuration;
- `Policy`: immutable retry, retention, budget, and scheduling policy;
- `Runtime`: a pinned Sandbox Agent build/digest/patch set;
- `WorkspaceBinding`: administrator-provisioned remote mirror/materialization mapping.

Do not use “credential profile” as a synonym for Source, expose a concrete source/account selector to clients, or leak Sandbox Agent/provider session IDs into the public API.

## Architectural boundaries

- The daemon owns API authorization, idempotency, queueing, scheduling, capacity, leases, state transitions, normalized events, and recovery.
- The runner owns target-local workspace materialization, sandbox setup, secret resolution, Sandbox Agent lifecycle, Agent execution, artifact transfer, and factual reporting.
- The runner binary has a host supervisor role and a sandboxed worker role. The worker shares loopback/network namespace with Sandbox Agent and reports over a private inherited socket; never expose the runtime port to make namespace access easier.
- A runner never reads SQLite directly. Local dispatch uses a private UDS but follows the same state machine and lease semantics as remote dispatch.
- Large files and workspaces never transit the daemon. Use `ObjectRef`, artifact sinks, and administrator-provisioned WorkspaceBinding mirrors.
- Sandbox Agent is a controller inside a system sandbox, not the sandbox provider. Establish the sandbox before enabling Agent auto-approval.
- Long-lived secrets remain in target-local resolvers. Submission, grant, event, trace, artifact, and normal logs must not contain credentials.

## Public protocol discipline

- Keep one Submission API for simple and advanced deployments; complexity belongs in versioned configuration resources.
- Do not expose provider-specific flags, arbitrary environment variables, MCP commands/URLs, runtime IDs, or `yolo`/`bypass` switches in Submission.
- v1 fields, meanings, and defaults are compatibility commitments. Additive optional changes require unknown-field/unknown-event tolerance and an OpenAPI compatibility test.
- Resolve defaults and immutable Profile/Policy/Extension versions at acceptance time. Later configuration reloads must not mutate accepted work.
- State and the core event describing that state change commit in the same SQLite transaction.
- `completed` means the runtime ended normally and declared output export succeeded; it never means the business answer was correct.
- Never infer completion, retry safety, or goal status from free-form Agent text.

## Sandbox Agent binding

- Pin release, commit, binary digest, Agent versions, and local patch set. Never track upstream `main` at runtime.
- Implement the narrow Rust HTTP/ACP client described in `docs/sandbox-agent-runtime.md`; do not depend on the TypeScript SDK for runtime semantics.
- Persist normalized events as they arrive. Sandbox Agent memory/SSE buffers and SDK persist drivers are not authoritative.
- Do not implement session resume by replaying transcript text into a new prompt. Only provider-native load/resume may advertise `core.session_resume`.
- Subscribe to ACP events before prompting, keep one in-flight prompt per session, deduplicate by runtime instance/generation/event ID, and keep public Submission sequence numbers separate.
- Auto-approve permission requests only after the sandbox is established. Business questions become `interaction_required`; credential login/renewal/MFA becomes `source_auth_required`.
- Do not expose or forward Sandbox Agent filesystem, process, desktop, credential extraction, lazy Agent install, or Inspector APIs to clients.
- Skills/MCP/plugins are pre-registered immutable Extensions. Runtime install scripts, arbitrary URLs/commands, and task-provided secrets are forbidden.

## Durability and concurrency

- Use explicit state transition functions and test every allowed and forbidden transition. Terminal states never reopen.
- Every dispatch gets a new Attempt and lease generation. Use database transactions for claims; an in-process mutex is not a durable lock.
- A stale control lease does not prove remote execution stopped. Without termination or fencing evidence, use `uncertain` and conservatively retain/quarantine source capacity.
- Capacity observations carry freshness. Unknown or stale capacity is unavailable, never optimistically free.
- Do not retry side-effecting work unless the previous execution is proven stopped and the frozen Policy/`side_effects` contract permits replay.
- One Session has at most one active Submission. Direct local `writable` workspaces require an exclusive workspace lock; `read_only` and isolated overlays may share only when the sandbox backend can enforce it.
- Use bounded channels, explicit timeouts, cancellation propagation, and structured task ownership. Do not detach background Tokio tasks whose shutdown or errors cannot be observed.
- Keep blocking filesystem/database preparation off async executor threads when it can block materially.

## Rust implementation practices

- Prefer small crates with one direction of dependency: protocol/domain types must not depend on Axum, SQLx, or Sandbox Agent adapters.
- Model IDs, digests, revisions, priorities, and durations with validated domain types rather than unstructured strings/integers throughout the codebase.
- Avoid `unwrap`, `expect`, panics, and unchecked indexing in daemon/runner request paths. Convert failures to typed internal errors and stable public error codes at boundaries.
- Keep provider/Sandbox Agent payloads in the runtime adapter. Do not spread raw `serde_json::Value` through scheduler or state-machine code.
- Use UTC for persisted timestamps and a monotonic clock for live lease/timeout measurement.
- Use structured tracing with IDs and classifications, never raw prompts, source credentials, auth headers, workspace contents, or full provider payloads.
- Avoid `unsafe` unless a platform boundary truly requires it; document the invariant and add focused tests.
- Keep migrations deterministic and forward-only once released. Migration and state recovery tests must use real SQLite transactions.

## Verification and change workflow

Before committing code, run the applicable workspace equivalents of:

```text
cargo fmt --check
cargo clippy --workspace --all-targets --all-features -- -D warnings
cargo test --workspace --all-features
```

Also run OpenAPI compatibility/generation checks, SQL migrations, Sandbox Agent conformance tests, and patched OpenCode goal tests when those areas change. Do not weaken or skip a failing check to land a change.

Keep documentation and protocol examples synchronized with implementation. Any change to public types, defaults, states, events, errors, runtime capabilities, or recovery semantics must update the relevant document and compatibility tests in the same commit.
