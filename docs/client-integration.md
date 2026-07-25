# 客户端接入指南

本文面向把业务任务交给 thieving-eyes 的本机应用。调用方拥有业务状态、输入快照、结果校验和
重试决策；thieving-eyes 只拥有 Submission 的持久队列、容量准入、Agent 执行与执行事实。

公共 wire contract 以 [`api/openapi.yaml`](../api/openapi.yaml) 为准，详细状态语义见
[`submission-api.md`](submission-api.md)。`eyes` CLI 和 SDK 都只是该 API 的客户端，不构成第二套协议。

## 1. 当前可用边界

`0.1` 的本机服务通过 Unix domain socket 提供 HTTP：

```text
$XDG_RUNTIME_DIR/thieving-eyes/daemon.sock
```

当前实现适用于与 daemon 同一 Unix 用户下的受信任进程。它们共享同一个 client namespace，
因此可以看到彼此的 Submission，并共享 Idempotency-Key 命名空间。每个应用必须使用稳定且互不
冲突的 key 前缀，例如：

```text
tunascope:analysis-task:<task-id>:attempt:<number>:v1
brainafk:<task-id>:<phase>:v1
```

跨 Unix 用户隔离、HTTPS bearer token、mTLS、远程 Runner Gateway、WorkspaceBinding、
ObjectRef/artifact transport、持久 Session 和 goal 尚未在 `0.1` 发布。客户端必须先读取
`GET /v1/capabilities`，不能仅因类型中已有未来字段就假设 daemon 已支持。

`0.1` 只接受不超过 1 MiB 的 text input，并将完整 Submission（包括 prompt）持久化到本机
SQLite。它不进入普通日志，但仍属于持久敏感数据面。不要把 provider key、临时下载 URL、私密源码
片段或 Tunascope 长期 credential 放入 prompt。Tunascope 的不可变 manifest、对象存储直传和
结构化 artifact 闭环要等 ObjectRef/Runner Gateway 发布后才能满足生产 contract；当前 SDK 面向
本机 Phase 0 集成和真实执行链路验证。

## 2. 调用方需要持久化什么

业务数据库至少保存：

```text
business_job_id
idempotency_key
submission_id
last_event_sequence
terminal_state
request_digest
```

业务进程重启时使用既有 `submission_id` 读取状态并从 `last_event_sequence` 恢复事件，不创建
新的 Submission。只有在业务语义明确要求新执行时才创建新的 attempt 和新的 idempotency key。

同一 key 的网络重试必须发送完全相同的请求体；不同请求体会得到
`409 idempotency_conflict`。`client_reference` 不参与幂等比较，只用于关联。

## 3. 标准生命周期

```text
冻结业务输入
  -> POST /v1/submissions
  -> 在同一业务事务边界持久化 submission_id
  -> GET status / SSE events
  -> terminal=true
  -> GET result
  -> 调用方校验业务 schema、evidence 和 Git 快照
```

`POST` 只表示请求已验证并持久化。容量不足时 Submission 保持 `queued`，不是提交失败。

事件是至少一次投递。客户端按 `(submission_id, sequence)` 去重，保存最新 sequence，并容忍未知
事件类型。事件流断开不改变执行状态；恢复失败时重新读取 `SubmissionStatus`。状态接口是执行状态
权威，Tunascope 等上层系统仍是业务状态权威。

终态包括：

```text
completed | failed | cancelled | expired | uncertain
```

`completed` 只表示 Agent 正常结束且声明输出成功导出，不表示回答正确。`uncertain` 表示旧执行
可能仍在运行；调用方不得自动重放 side-effecting 工作。

## 4. Workspace 与大仓库

本机应用使用管理员配置的 `WorkspaceRoot`，请求只发送 `root_id` 和相对路径。源码和大型仓库不经
daemon 传输。以当前本机配置为例：

```text
root_id: local
root:    /home/linmalin/aiaiai

Tunascope path: innovation/tunascope
BrainAFK path:  brainafk
```

Tunascope 必须先在自身控制下物化固定 Git object 的只读 worktree，再提交该路径。当前本机 runner
不会解释 Git，也不会自行验证 `revision` 对应的 checkout；Tunascope 仍须在执行前后验证 object
SHA 和 worktree identity。扫描大型远程仓库时应等待 WorkspaceBinding/Runner Gateway 落地，而不是
通过 Submission 上传仓库。

默认使用 `read_only` workspace 与 `side_effects=read_only`。只有业务任务明确允许真实修改时才
同时使用 `access=writable` 和相应 side-effect contract。

## 5. TypeScript SDK

SDK 位于 [`sdk/typescript`](../sdk/typescript)，包名为 `@thieving-eyes/sdk`，要求 Node.js 22。
当前先作为源码仓库内的本地包使用，不发布 npm。

在 Tunascope 的 `apps/worker/package.json` 中，当前目录布局可使用：

```json
{
  "dependencies": {
    "@thieving-eyes/sdk": "file:../../../../thieving-eyes/sdk/typescript"
  }
}
```

在 thieving-eyes 中先生成并构建 SDK：

```text
cd sdk/typescript
pnpm install
pnpm run check
pnpm run test
pnpm run build
```

Tunascope worker 的最小只读提交：

```ts
import {
  ThievingEyesClient,
  type SubmissionCreate,
} from "@thieving-eyes/sdk";

const client = new ThievingEyesClient({
  socketPath:
    process.env.THIEVING_EYES_SOCKET ??
    "/run/user/1000/thieving-eyes/daemon.sock",
  userAgent: "tunascope-worker/0.1.0",
});

const taskId = "task_01...";
const attempt = 1;
const commitSha = "0123456789abcdef0123456789abcdef01234567";

const request: SubmissionCreate = {
  client_reference: `${taskId}:${attempt}`,
  labels: {
    app: "tunascope",
    task_kind: "repository_change_review",
  },
  input: {
    parts: [
      {
        type: "text",
        text:
          `Review the read-only worktree at commit ${commitSha}. ` +
          "Return a concise security assessment with file and line evidence.",
      },
    ],
  },
  workspace: {
    kind: "local",
    root_id: "local",
    path: "innovation/tunascope",
    revision: commitSha,
    access: "read_only",
  },
  execution: {
    route_ids: ["opencode_default"],
    target_ids: ["local"],
    locality: "local_only",
    side_effects: "read_only",
  },
  scheduling: {
    priority: 60,
  },
  limits: {
    run_timeout_seconds: 3600,
    idle_timeout_seconds: 900,
  },
};

const accepted = await client.submit(request, {
  idempotencyKey:
    `tunascope:analysis-task:${taskId}:attempt:${attempt}:v1`,
});

// Persist accepted.submission_id and accepted.request_digest before treating
// the business attempt as dispatched.
```

SDK 有意不依赖 `@tunascope/core`，也不解释 `AnalysisTask`、Git range、manifest 或 Finding。
Tunascope 应在自己的 worker adapter 中把已冻结的 `ExecutionSubmission` 映射为
`SubmissionCreate`，并把 thieving-eyes 事件投影为自身的 `AgentRun`/`ReasoningTrace`。这样 SDK
保持公共执行协议客户端，而 Tunascope 继续拥有业务语义和 schema validator。

恢复事件：

```ts
for await (const event of client.watch(submissionId, {
  afterSequence: persistedLastSequence,
  reconnect: true,
})) {
  await persistNormalizedExecutionEvent(event);
  await saveLastSequence(event.sequence);
}
```

`run()` 是 `submit + status + watch + result` 的便利组合，适合单进程工具和 smoke test：

```ts
const result = await client.run(request, {
  idempotencyKey:
    `tunascope:analysis-task:${taskId}:attempt:${attempt}:v1`,
  onEvent: persistNormalizedExecutionEvent,
});
```

正式 worker 更适合分别调用 `submit()`、`status()`、`watch()` 和 `result()`，在每个持久化边界保存
进度。中止 SDK 的 `AbortSignal` 只停止本地等待；要停止远端执行必须显式调用
`client.cancel(submissionId)`。

## 6. 模型与 route

省略 Agent/route 约束时使用 daemon 默认 route。显式模型必须使用管理员批准的逻辑 route，不得
指定 Source、账号、endpoint 或 credential。当前本机配置为：

| Route | Model |
| --- | --- |
| `opencode_default` | `csi-provider/GLM-5.1` |
| `csi_glm_5_2` | `csi-provider/GLM-5.2` |
| `deepseek_flash` | `deepseek-official/deepseek-v4-flash` |
| `deepseek_pro` | `deepseek-official/deepseek-v4-pro` |

应用应把获准 route 链冻结进自己的 task snapshot。配置变化需要创建新的业务 task revision，不应
静默改变已接受任务的模型语义。

## 7. 错误与恢复

SDK 对非 2xx 响应抛出 `ThievingEyesError`；成功响应或 SSE 不符合公开协议时抛出
`ThievingEyesProtocolError`，后者不会被 `watch()` 当作临时断线自动重连：

```ts
try {
  await client.result(submissionId);
} catch (error) {
  if (
    error instanceof ThievingEyesError &&
    error.detail?.code === "result_not_ready"
  ) {
    // Keep watching the existing Submission.
  }
}
```

客户端只按稳定的 `error.detail.code` 分支，不解析 message。SDK 不自动打印请求体、响应体或 prompt，
非 JSON 中间层错误也不会被拼入异常消息。`maxResponseBytes` 限制单个 JSON response 或单条 SSE
event，避免异常服务端无限积累内存。

`requestTimeoutMs` 只限制等待 response header 或后续 response 数据的传输空闲时间，不是
Submission 的 `run_timeout_seconds`，也不会在超时时替调用方取消服务端任务。

常见恢复规则：

- transport 失败：用相同 Idempotency-Key 重试原始 submit，或按 submission ID 读取状态；
- SSE 断开：从最后持久化的 sequence 重连；
- `result_not_ready`：继续观察现有 Submission；
- `idempotency_conflict`：调用方持久化/请求构造错误，不能生成随机 key 掩盖；
- `interaction_required`、`source_auth_required`：形成可诊断业务/运维状态；
- `uncertain`：人工或管理员确认旧执行停止前不得重放。

## 8. SDK API

```text
submit(submission, { idempotencyKey, signal? })
status(submissionId, { signal? })
watch(submissionId, { afterSequence?, reconnect?, untilTerminal?, signal? })
result(submissionId, { signal? })
cancel(submissionId, { signal? })
patchScheduling(submissionId, revision, patch, { signal? })
capabilities({ signal? })
run(submission, { idempotencyKey, onEvent?, signal? })
```

SDK 只实现 Node.js 本机 UDS transport。未来 HTTPS/mTLS transport 应实现为独立、显式配置，不得让
本机默认值意外退化成无 TLS 的 TCP。
