# Submission API v1

本文件定义调用方与 thieving-eyes daemon 之间的稳定协议。CLI 与各语言 SDK 都只是这个 HTTP/JSON + SSE API 的薄封装。

thieving-eyes 是后台 Agent 工作的**排队与执行控制面**，不是新的对话协议。它可以在 adapter 内使用 ACP、Codex app-server、OpenCode SDK 或 CLI，但不把任何 provider 的私有参数暴露给调用方。

## 1. 设计边界

v1 只有一个写入入口：`Submission`。一个 Submission 表示“使用指定输入，在某个 workspace 中完成一次后台 Agent 工作”。

以下概念独立存在，不嵌入 Submission：

- `Attempt`：一次实际派发，记录最终选中的 route、source、target 与 session；
- `Session`：可恢复的 Agent 上下文与记录；
- `Profile`：版本化的 Agent approval、sandbox、extension 及默认能力配置；
- `Policy`：版本化的 retry、记录、保留与预算策略；
- `Extension`：预注册的 skill、MCP binding 或 provider plugin；
- `Artifact`：输入或输出对象，API 只传引用，不中转大文件。

调用方可以直接提交一个 prompt；只有需要时才提供 workspace、session、输出 schema、扩展或调度约束。账号、credential、实时容量和具体 runner 始终由 daemon 选择。

## 2. 兼容性规则

- `/v1` 是 major version。v1 不得删除字段、改变字段含义或默认值、把可选字段改为必填。
- v1 可以新增可选字段、事件、错误码与 capability。客户端必须忽略未知可选字段和未知事件。
- 生命周期响应必须包含 `terminal`；客户端不得根据自己认识的 state 枚举判断终态。
- JSON 使用 `snake_case`；时间使用 RFC 3339 UTC；duration 使用非负整数秒；ID 是不透明字符串。
- digest 格式为 `sha256:<lowercase-hex>`；金额不得使用 JSON 浮点数。
- provider 私有参数、环境变量、credential、任意 MCP command/URL 不得进入核心 schema。
- 列表接口使用 opaque cursor；可修改资源携带单调递增的 `revision` 与 `ETag`。

## 3. 传输、身份与幂等

- 本机默认通过 Unix domain socket 提供 HTTP，以 socket peer identity 或 scoped bearer token 鉴权；可配置 loopback TCP。跨机器客户端必须使用 HTTPS，并通过 scoped bearer token 或 mTLS client certificate 鉴权。token 只允许出现在 `Authorization: Bearer` header，不得进入 URL、Submission、日志或事件；daemon 只保存不可逆 verifier，并支持轮换与吊销。
- runner 控制通道不使用普通 client token，固定使用独立的 mTLS runner identity。
- 每个已认证请求映射到一个 `client_id` namespace；`client_id` 不能由请求字段自报。授权策略限制其可见的 Submission、Session、workspace、profile、policy、route、target 与 extension。
- `POST /v1/submissions` 必须携带 `Idempotency-Key`，作用域为 `(client_id, key)`。
- 同 key、同规范化请求返回原 Submission；同 key、不同请求返回 `409 idempotency_conflict`。
- 规范化请求是去掉 `client_reference` 后、按 RFC 8785 JSON Canonicalization Scheme 编码的原始请求体；默认值解析与当前配置不参与幂等比较。
- prompt、源码、原始 trace 与 secret 不得进入普通服务日志。

## 4. 资源与端点

```text
POST   /v1/submissions
GET    /v1/submissions
GET    /v1/submissions/{submission_id}
PATCH  /v1/submissions/{submission_id}
POST   /v1/submissions/{submission_id}/cancel
GET    /v1/submissions/{submission_id}/events
GET    /v1/submissions/{submission_id}/result

GET    /v1/sessions
GET    /v1/sessions/{session_id}
GET    /v1/sessions/{session_id}/submissions
GET    /v1/sessions/{session_id}/events

GET    /v1/capabilities
GET    /v1/profiles
GET    /v1/profiles/{profile_id}/versions/{version}
GET    /v1/policies
GET    /v1/policies/{policy_id}/versions/{version}
GET    /v1/extensions
GET    /v1/extensions/{extension_id}/versions/{version}
```

所有列表接口返回 `Page<T> { items: T[], next_cursor: string | null }`。

`POST /v1/submissions` 永远异步。SDK 的 `run()` 只是 submit、watch、result 的便利组合，不是第二套协议。

## 5. 创建 Submission

```text
SubmissionCreate {
  client_reference?: string
  labels?: map<string, string>
  mode?: TaskMode | GoalMode
  input: Input
  workspace?: WorkspaceRef
  output?: OutputSpec
  agent?: AgentSelector
  execution?: ExecutionSelector
  session?: SessionBinding
  scheduling?: Scheduling
  limits?: Limits
  policy?: ResourceSelector
}

TaskMode { kind: "task" }
GoalMode { kind: "goal", token_budget?: integer }

ResourceSelector {
  id: string
  version?: string
}

ResourceRef {
  id: string
  version: string
  digest: string
}

RuntimeRef {
  name: "sandbox-agent"
  version: string
  digest: string
}
```

默认值是 `mode={kind:"task"}`、纯文本输出、默认 profile、默认 policy、优先级 50。`task` 默认使用临时 session；`goal` 默认创建持久 session。

`client_reference` 只供调用方关联，不参与幂等。`labels` 只能包含少量、非敏感、可索引的字符串，不得保存 prompt、路径或 credential。

daemon 接收请求时解析所有默认值并冻结 `request_digest`、profile、policy、允许的 route/target 和 extension digest。之后的配置变化不得改变已接收 Submission 的执行边界。

最小请求只有输入：

```json
{
  "input": {
    "parts": [{ "type": "text", "text": "检查当前项目并修复测试失败" }]
  }
}
```

`ResourceSelector.version` 省略时解析为提交瞬间获准使用的默认/最新版本；接收响应始终返回带 version 与 digest 的 `ResourceRef`。调用方要求完全复现时应显式给出 version。

同一 Idempotency-Key 的重放始终返回首次接收时冻结的资源版本，即使默认配置已经更新。

### 5.1 Input 与 content part

输入采用类似现有 agent 协议的 content-part 模型，而不是为文本、附件和结构化数据分别增加顶层字段：

```text
Input {
  parts: ContentPart[]
}

ContentPart =
  | { type: "text", text: string }
  | { type: "data", data: json_value, media_type?: string }
  | { type: "file", file: FileRef, name?: string, media_type?: string }

FileRef =
  | { kind: "workspace", path: string, digest?: string }
  | { kind: "object", object: ObjectRef }

ObjectRef {
  resolver_id: string
  object_key: string
  digest: string
  size_bytes?: integer
  media_type?: string
}
```

`parts` 必须至少包含一个元素。所有 inline 内容受 daemon 大小上限约束；大文件必须使用 `ObjectRef`。

`ObjectRef` 是注册 resolver 下的稳定对象标识，不得是任意 URL，也不得携带 bearer token、cookie 或 presigned URL。执行时由 daemon/Gateway 取得绑定 Attempt 的短期授权并校验 digest。

`workspace` file 的 `path` 相对于当前 workspace，必须规范化且不得越界。没有 workspace 时不得使用这种 file ref。

### 5.2 Workspace

```text
WorkspaceRef =
  | {
      kind: "local"
      root_id: string
      path?: string
      revision?: string
      access?: "read_only" | "writable" | "writable_overlay"
    }
  | {
      kind: "binding"
      binding_id: string
      revision: string
      access?: "read_only" | "writable_overlay"
    }
```

`local` 引用管理员为 client 注册的本机根目录，只能派到能够安全访问该根的本机 target。`path` 不能是绝对路径，也不能经 symlink 逃逸。

`binding` 引用开发者或管理员预先布置好的 workspace/mirror。daemon 不上传或同步整个仓库，也不把宿主路径写进协议。跨机器扫描大型仓库时必须使用 binding；其准备、刷新与一致性由部署方负责。详细约束见 [Runner Gateway Contract](runner-gateway-contract.md)。

workspace 省略时表示任务不依赖工作目录，不表示 daemon 可以随意选择一个目录。

`access` 默认 `read_only`。`writable` 只适用于本机 workspace：Agent 直接修改注册目录，必须由 Profile 明确授权，且调用方承担真实副作用与不确定执行的风险。`writable_overlay` 在隔离副本/overlay 中修改，基础 workspace 不变；需要保留的文件必须通过 artifact collection 导出。远程 binding 只允许 `read_only` 或 `writable_overlay`。

三种模式都受 sandbox 和 `side_effects` 约束；workspace 可写不代表网络、secret 或其他宿主路径可写。可恢复 session 必须重新绑定相同的 workspace identity；若本机 `revision` 省略，服务只能保证 `root_id + path` 相同，不承诺目录内容可复现。

同一 canonical local workspace 同时最多有一个 active `writable` Attempt；锁在派发前取得，未确认旧执行停止时继续保留。冲突的 Submission 保持 `queued` 并报告 `workspace_busy`。`read_only` 与真正隔离的 `writable_overlay` 只有在 sandbox backend 能证明不会回写基础目录时才可并行。

### 5.3 Output

```text
OutputSpec {
  final?: TextOutput | StructuredOutput
  artifacts?: ArtifactCollection
}

TextOutput { kind: "text" }
StructuredOutput { kind: "json_schema", schema: JsonSchemaSource }

JsonSchemaSource =
  | { kind: "inline", value: json_value }
  | { kind: "object", object: ObjectRef }

ArtifactCollection {
  sink_id: string
  include: string[]
}

ArtifactRef {
  sink_id: string
  object_key: string
  digest: string
  size_bytes: integer
  media_type: string
}
```

默认只返回最终文本。小型 JSON Schema 可以 inline，超过请求上限时使用 `ObjectRef`。schema 按 JSON Schema Draft 2020-12 解释；route 可以通过 capability 声明其支持的受限子集。`json_schema` 要求 route 支持 `core.structured_output`，并在完成前通过校验；不得静默退化为文本。

`include` 是相对于 workspace 的声明式路径/glob 列表，统一使用 `/` 分隔符，支持字面路径以及 `*`、`?`、`**`、`[]`，不支持否定模式。它只决定收集哪些输出，不授予额外文件访问权；绝对路径、`..` 和经 symlink 逃逸的结果必须拒绝。`sink_id` 必须预先注册；API 不接受上传 URL。artifact 内容绕过 daemon 写入 sink，结果中只返回 `ArtifactRef`。

请求 artifact collection 但没有 workspace，或 include 路径可能逃逸 workspace 时，必须拒绝请求。

未声明 artifact collection 时，daemon 不承诺保存 workspace 变更、patch 或生成文件。

### 5.4 Agent、route 与执行环境

```text
AgentSelector {
  profile?: ResourceSelector
  adapter?: string
  model?: string
  required_capabilities?: CapabilityRequirement[]
  extensions?: ExtensionRef[]
}

CapabilityRequirement {
  name: string
  min_version?: string
}

ExtensionRef {
  kind: "skill" | "mcp" | "plugin"
  resource: ResourceSelector
}

ExecutionSelector {
  route_ids?: string[]
  target_ids?: string[]
  locality?: "local_only" | "any"
  side_effects?: "read_only" | "idempotent_write" | "side_effecting"
}
```

这些字段是约束，不是 provider 参数：daemon 只能选择同时满足全部约束且当前获准使用的 route。数组按偏好排序；权限、容量、公平性和健康度仍优先。

`locality` 默认 `any`；但 `WorkspaceRef.kind=local` 隐式收紧为 `local_only`。显式数组存在时必须非空，重复项在接收时拒绝。

`adapter` 表示 Codex、OpenCode 或其他 adapter；`model` 表示逻辑模型要求。调用方不能指定 credential、账号或具体 source。一个逻辑 route 可以在多个账号、endpoint 与 target 之间调度。

Extension 必须预注册、固定版本与 digest。Submission 不得定义 MCP command/URL、plugin 安装脚本、hook、任意环境变量或 secret。缺少必需 extension 时拒绝请求，不得静默跳过。

策略叠加顺序固定为：daemon 安全策略 → profile/policy → Submission 的更严格约束 → workspace 内 Agent 指令。后层不能放宽 sandbox、网络、secret、workspace 或 extension 白名单。sandbox 由版本化 Profile 选择，不另设裸字符串选择器。

`side_effects` 默认 `side_effecting`，用于约束失败后的安全重试。retry 的次数、原因和退避由选中的 Policy 定义，不在每次 Submission 中重复配置。无法证明旧执行已停止时，`side_effecting` 工作不得重放。

`WorkspaceRef.access=writable` 与 `side_effects=read_only` 组合无效，接收时必须拒绝。workspace access 只描述文件 materialization；`side_effects` 描述整个任务的重放安全性，不能由 overlay 模式自动推断。

fallback 还必须满足失败发生在执行副作用前，或旧执行已确认停止且 `side_effects` 允许安全重放。source/route 切换永远创建新 Attempt 并产生可见事件；不得在同一 Attempt 内静默换账号。goal mode 不做自动 fallback。

### 5.5 Session

```text
SessionBinding =
  | { mode: "ephemeral" }
  | { mode: "create", retention_seconds?: integer }
  | { mode: "resume", session_id: string }
```

`task` 默认使用 `ephemeral`；`goal` 默认使用 `create`。provider 可以为 ephemeral task 内部创建临时 session，但 daemon 不承诺它可恢复。

`create` 返回新的 `session_id`；`resume` 创建新的 Submission 并继续既有 session，旧 Submission 保持不可变。provider 原始 thread/session ID 不对调用方公开。

恢复必须满足 owner、adapter、RuntimeRef、credential scope、execution domain、workspace identity/revision constraint、profile 与 extension 亲和性。一个 session 同时最多有一个 active Submission；后续请求在队列中以 `session_busy` 阻塞，而不是并发修改上下文。

Session 的“可恢复”与“可读取历史”是两个独立 capability。仍保留事件或 transcript 不代表 provider session 仍然存在。

### 5.6 Scheduling、limits 与 policy

```text
Scheduling {
  priority?: integer
  not_before?: timestamp
  start_deadline?: timestamp
}

Limits {
  run_timeout_seconds?: integer
  idle_timeout_seconds?: integer
  max_tokens?: integer
  max_provider_requests?: integer
}
```

`priority` 范围 0–100，100 最高。daemon 仍应用 aging、公平份额、容量窗口与 client 配额。

`start_deadline` 只约束开始时间；到期仍未开始则进入 `expired`。run/idle timeout 从 Attempt 开始后计算。provider 无法可靠测量某项 limit 时必须拒绝硬约束，而不是假装执行成功。

`policy` 是已注册的 Policy 引用，承载 retry、退避、记录级别、retention、通知 binding 与成本上限等低频配置。省略时使用 client 默认 Policy。这样简单调用保持短小，同时高级策略仍能版本化、授权并冻结。

### 5.7 Goal mode

`mode.kind="goal"` 表示使用 adapter/provider 的原生持久 goal 或等价长程机制，而不是由 daemon 重复发送普通 prompt。

它要求 `core.native_goal`、有效的 `session.mode=create|resume`、长程 heartbeat 与持久 session。session 省略时按 `create` 解析。objective 来自 Input；`token_budget` 是 goal 自身的原生预算，并与 Policy/Limits 的更低上限共同生效。

Goal 不得自动跨账号、provider、runner 或新 session 重放。详细规则见 [Goal mode](goal-mode.md)。

## 6. 接收与查询

```text
SubmissionAccepted {
  submission_id: string
  state: "queued"
  terminal: false
  revision: integer
  request_digest: string
  resolved_profile: ResourceRef
  resolved_policy: ResourceRef
  status_url: string
  events_url: string
  result_url: string
  idempotent_replay: boolean
}
```

新请求返回 `201`；幂等重放返回 `200`。接收只表示请求已通过静态校验并持久化，不表示已有容量。

```text
SubmissionStatus {
  submission_id: string
  client_reference?: string
  request_digest: string
  resolved_profile: ResourceRef
  resolved_policy: ResourceRef
  state: SubmissionState
  terminal: boolean
  revision: integer
  mode: "task" | "goal"
  created_at: timestamp
  updated_at: timestamp
  blocker?: Blocker
  session_id?: string
  attempts: Attempt[]
  latest_event_sequence: integer
  error?: ErrorDetail
}

SubmissionState =
  "queued" | "running" |
  "completed" | "failed" | "cancelled" | "expired" | "uncertain"

Blocker {
  code: string
  retry_after?: timestamp
  detail?: string
}
```

容量不足、窗口关闭、`not_before`、source cooldown、无空闲 target 与 `session_busy` 都是 `queued` 的 blocker，不扩张 state 枚举。v1 的标准 blocker code 是 `not_before`、`capacity_unavailable`、`capacity_unknown`、`source_cooldown`、`window_closed`、`target_unavailable`、`workspace_pending`、`workspace_busy`、`session_busy` 和 `budget_pending`。未来可以增加新 code；客户端必须容忍未知值。blocker 是可变诊断信息，不应被客户端当作持久状态机。

静态不存在、未授权或永远不兼容的 route/target/workspace 在接收时拒绝，不进入队列。只有可能随容量、时间、健康度或目标侧准备状态变化而解除的条件才使用 blocker。

终态为 `completed`、`failed`、`cancelled`、`expired`、`uncertain`。`completed` 只表示 adapter 正常结束且声明输出已导出，不表示业务答案正确。运行超时在确认进程停止后是 `failed` + `timeout`；无法确认停止则是 `uncertain`。

允许的迁移为：

```text
queued  -> running | cancelled | expired
running -> queued                    # Policy 允许且可安全重试
running -> completed | failed | cancelled | uncertain
```

`running -> queued` 前，当前 Attempt 必须先进入 `failed` 或 `cancelled` 终态；后续派发创建新 Attempt，不能把同一个 Attempt 退回 starting。

终态不可再迁移。`queued -> queued` 的 blocker、优先级或调度时间变化只增加 revision，不制造新 state。

`PATCH /v1/submissions/{id}` 只允许在 `queued` 时通过 `If-Match` 提交以下结构；至少出现一个字段，显式 `null` 表示清除可选时间：

```text
SubmissionPatch {
  priority?: integer
  not_before?: timestamp | null
  start_deadline?: timestamp | null
}
```

其他语义变化必须创建新 Submission。

## 7. Attempt

```text
Attempt {
  attempt_id: string
  number: integer
  state: "starting" | "running" | "completed" |
         "failed" | "cancelled" | "uncertain"
  route_id: string
  adapter: string
  model: string
  target_id: string
  source_label: string
  sandbox_profile: string
  runtime: RuntimeRef
  agent_version?: string
  session_id?: string
  started_at?: timestamp
  finished_at?: timestamp
  usage?: UsageSummary
  error?: ErrorDetail
}

UsageSummary {
  input_tokens?: integer
  output_tokens?: integer
  reasoning_tokens?: integer
  provider_requests?: integer
  cost?: { currency: string, amount: decimal_string }
  measurement: "provider_reported" | "estimated" | "partial"
}
```

Attempt 在真正派发时创建，绝不复用。每次 route/source/target 的实际重选都产生新 Attempt。容量等待不创建 Attempt，也不消耗 retry 次数。

runner/control channel 丢失不是独立 Attempt 终态：已确认执行停止时归为 `failed` + `runner_lost`，无法确认 provider 或副作用停止时归为 `uncertain`。

`source_label` 必须脱敏；响应不得包含 credential、provider endpoint、宿主绝对路径或原始 auth 状态。

## 8. Events 与记录

```text
EventEnvelope {
  event_id: string
  submission_id: string
  session_id?: string
  attempt_id?: string
  sequence: integer
  occurred_at: timestamp
  type: string
  data: object
}
```

保证持久化的核心事件只有：

- `submission.created`
- `submission.state_changed`
- `submission.scheduling_changed`
- `queue.blocked`
- `attempt.created`
- `attempt.state_changed`
- `session.bound`
- `artifact.published`
- `cancellation.requested`
- `diagnostic.warning`

adapter 可以在 Policy 允许时额外产生 `agent.message`、`agent.plan`、`agent.tool`、`agent.usage`、`agent.goal` 等规范化事件。它们用于观察和记录，不参与 Submission 状态归约；原始 provider event 可以直接导出为 transcript artifact，但不得混入核心事件 schema。

核心事件的 `data` 必须在 OpenAPI 中定义为按 `type` 判别的 schema；同一事件类型在 v1 内只能增加可选字段，不能改变既有字段语义。`queue.blocked` 只在 blocker 发生实质变化时产生，不能按 scheduler tick 重复写入。

Submission 是非交互的。默认 Profile 在系统级 sandbox 成功建立后，以 YOLO/auto-approve 方式自动响应 Agent `permission`；这只消除逐项审批，不放宽 workspace、network、secret、extension 或 side-effect 边界。越界操作直接失败为 `sandbox_violation` 或 `policy_denied`，不能等待人工提权。

真正需要业务选择或补充输入且无法自动处理的 `question` 才失败为 `interaction_required`；source credential 需要登录、续期或 MFA 时使用 `source_auth_required` 并隔离该 source。v1 不提供运行中人工回复端点。Submission 不暴露 provider-specific `yolo`、`bypass` 或 approval flag；这些行为只能由已授权、不可变的 Profile 定义。

`GET /events` 使用 SSE，支持 `Last-Event-ID` 或 `after_sequence`。每条 SSE 的 `id` 是十进制 `sequence`，`event` 是事件 `type`，`data` 是完整 `EventEnvelope` JSON；keepalive 使用 SSE comment，不占 sequence。事件至少一次投递，客户端按 `(submission_id, sequence)` 去重。游标超出保留窗口返回 `410 event_cursor_expired`，客户端重新读取当前状态和结果。

状态以 `GET SubmissionStatus` 为权威；事件流断开不影响执行。未知事件必须忽略并推进 sequence。

`sequence` 在单个 Submission 内单调递增。session events 是关联 Submission events 的只读聚合，使用 opaque cursor；它不另造一份不同语义的 provider history，也不承诺跨 Submission 的全局 sequence。

## 9. Result 与 artifact

```text
SubmissionResult {
  submission_id: string
  state: SubmissionState
  terminal: true
  output?: OutputValue
  artifacts: ArtifactRef[]
  attempts: Attempt[]
  usage?: UsageSummary
  error?: ErrorDetail
  finished_at: timestamp
}

OutputValue =
  | { kind: "text", text: string, truncated: boolean }
  | { kind: "data", data: json_value }
```

非终态读取 result 返回 `409 result_not_ready`；结果已过保留期返回 `410 result_expired`。超过 inline 上限的文本必须截断并通过声明的 artifact sink 保存完整内容；没有 sink 时明确标记 `truncated=true`。

## 10. 取消与 uncertain

`POST /v1/submissions/{id}/cancel` 是幂等操作：

```text
CancellationResult {
  submission_id: string
  disposition: "cancelled" | "cancellation_requested" |
               "already_terminal" | "delivery_unknown"
  revision: integer
}
```

排队任务立即取消；运行任务先调用 provider/session 的 interrupt/abort，再按 policy 终止 sandbox/process。终态不被取消覆盖。

runner 失联时持久化取消意图并返回 `delivery_unknown`。无法证明 provider、工具调用或外部副作用已停止时，Submission 必须进入 `uncertain`，不能伪造 `cancelled` 或自动重试。

`uncertain` 时不能因为控制面 lease 过期就立即释放对应 source 占用；容量必须保守保留或 quarantine，直到 monitor、fencing 或管理员处理证明旧执行不再占用资源。

## 11. Session API

```text
Session {
  session_id: string
  state: "active" | "idle" | "unavailable" | "archived"
  adapter: string
  model: string
  target_id: string
  profile: ResourceRef
  runtime: RuntimeRef
  agent_version?: string
  workspace_revision?: string
  can_resume: boolean
  has_history: boolean
  created_at: timestamp
  updated_at: timestamp
  retention_expires_at?: timestamp
  transcript?: ArtifactRef
}
```

`GET /sessions/{id}/submissions` 返回该 session 的 Submission；`GET /sessions/{id}/events` 返回 Policy 允许保留的规范化事件。完整 transcript 默认不由 daemon 保存，只有 Policy 明确启用并配置 sink 时才返回 artifact 引用。

Session 历史只用于检查、审计和恢复上下文，不提供修改 provider 历史的 API。session 被清理后 `can_resume=false`，即使 metadata 或 transcript 仍在。

## 12. Capability、Profile 与 Extension

`GET /v1/capabilities` 返回当前 client 可使用的 adapter、route、model、target、sandbox、session、goal、content part、structured output、usage measurement 和 extension capability。它只用于发现与预检；提交时必须重新授权。

```text
CapabilityDescriptor {
  name: string
  version: string
  constraints?: map<string, scalar_or_array>
}

CapabilityCatalog {
  capabilities: CapabilityDescriptor[]
}

ResourceSummary {
  ref: ResourceRef
  description?: string
  capabilities: CapabilityDescriptor[]
  deprecated: boolean
}

ExtensionSummary {
  kind: "skill" | "mcp" | "plugin"
  resource: ResourceSummary
}
```

Profile 与 Policy 列表返回 `Page<ResourceSummary>`；Extension 列表返回 `Page<ExtensionSummary>`。详情端点可以增加该资源类型专属的非敏感只读字段。

Profile、Policy 与 Extension 都是管理员发布的不可变版本。引用包含 ID、version 与 digest；更新必须创建新版本。Extension 的公开描述可以列出能力和目标要求，但不得暴露 MCP credential、command 参数中的 secret 或宿主安装路径。

能力名称使用稳定 namespace，例如：

```text
core.session_resume
core.session_history
core.native_goal
core.structured_output
core.image_input
extension.skill:<id>
extension.mcp:<id>
```

## 13. 错误

```text
ApiError {
  request_id: string
  error: ErrorDetail
}

ErrorDetail {
  code: string
  message: string
  retryable: boolean
  retry_after_seconds?: integer
  scope: "request" | "submission" | "attempt" |
         "route" | "target" | "source" | "session"
  field?: string
}
```

v1 至少定义：

| code | 含义 |
| --- | --- |
| `invalid_request` | schema 或字段组合无效 |
| `unauthenticated` | 调用身份无效 |
| `policy_denied` | 请求的资源、能力或 Agent 动作被已冻结的 policy 拒绝 |
| `not_found` | 资源不存在或不可见 |
| `idempotency_conflict` | 同 key 对应不同请求 |
| `revision_conflict` | ETag/revision 已过期 |
| `route_unsatisfied` | 无 route 同时满足约束 |
| `capability_unavailable` | 所需能力不可用 |
| `workspace_unavailable` | workspace/binding/revision 不可用 |
| `sandbox_violation` | 执行试图越过已建立的 sandbox 或 workspace 边界 |
| `object_unavailable` | 输入或 schema 对象不可用 |
| `artifact_export_failed` | 声明的输出 artifact 未能完整导出 |
| `session_unavailable` | session 不存在、已清理或不可恢复 |
| `session_affinity_violation` | session 与执行域或 workspace 不兼容 |
| `output_schema_invalid` | 最终结构化输出校验失败 |
| `interaction_required` | Agent 需要无法自动处理的业务输入 |
| `source_auth_required` | source credential 需要登录、续期或 MFA |
| `rate_limited` | provider 暂时限流 |
| `quota_exhausted` | source 配额已耗尽或进入冷却期 |
| `budget_exhausted` | 已达到冻结 Policy/Limits 的预算上限 |
| `goal_blocked` | provider-native goal 以 blocked 状态停止且没有可自动解决的后续动作 |
| `provider_error` | Agent/provider 返回未能进一步归类的错误 |
| `runtime_unavailable` | 固定 Sandbox Agent runtime 不健康、断开或协议不兼容 |
| `timeout` | 已确认停止的运行超时 |
| `runner_lost` | runner lease 丢失 |
| `result_not_ready` | Submission 尚未到达终态 |
| `result_expired` | 结果已超过保留期 |
| `event_cursor_expired` | SSE 恢复游标已超过事件保留窗口 |
| `internal_error` | daemon 内部错误 |

客户端以 `code` 分支，不得解析 `message`。message 必须可诊断但不能包含 secret。

`retryable` 只表示 daemon 可以在同一 Submission、冻结的 route 集合和 Policy 下安全自动重试；它不表示用户修复配置后永远不能提交新请求。`field` 存在时使用 RFC 6901 JSON Pointer 指向请求字段。

`sandbox_violation` 专指 runtime 已开始后被系统级隔离边界阻止的操作；提交时无权选择 Profile/route，或 Agent 动作被 side-effect/network 等冻结策略拒绝时使用 `policy_denied`。两者都不能通过运行中人工批准继续。

## 14. 与已有协议的关系

本 API 有意复用成熟协议的资源边界，而不声称 wire compatibility：

- [A2A](https://github.com/a2aproject/A2A/blob/main/docs/specification.md) 的 Message/Part/Artifact 启发了 Input、ContentPart 与 ArtifactRef；A2A 的 agent-to-agent 对话状态不直接成为队列状态。
- [Agent Client Protocol](https://agentclientprotocol.com/protocol/v1/overview) 的 capability negotiation、session/prompt/cancel/update 适合 adapter 与 headless agent 之间，不适合承载跨账号容量调度。
- [Codex app-server](https://github.com/openai/codex/blob/main/codex-rs/app-server/README.md) 的 Thread/Turn/Item 和 [Agent Protocol](https://github.com/langchain-ai/agent-protocol) 的 Thread/Run 说明 Session 与一次执行必须分离。
- [Sandbox Agent](https://github.com/rivet-dev/sandbox-agent) 是 v1 固定的 Agent runtime，提供 sandbox 内的进程、session、HTTP/ACP 与事件适配；thieving-eyes 在其上层原生实现队列、容量、账号池、持久状态和跨 target 调度。本机 OpenCode 等非上游能力通过窄 adapter patch 扩展，不改变本 API。内部映射见 [Sandbox Agent Runtime Binding](sandbox-agent-runtime.md)。

因此，外部客户端只依赖 Submission/Session/Event；provider adapter 可以演进或替换，而不迫使调用方改协议。
