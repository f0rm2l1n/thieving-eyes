# Runner Gateway Contract

Runner Gateway 是 daemon 可配置的远程 execution target。daemon 监管的本机 runner 是独立进程，通过私有 UDS 接受同语义派发；它复用相同的 adapter、sandbox policy、source lease、取消和事件语义，但不建立 mTLS 连接，也不通过网络领取 DispatchGrant。

## 注册与领取

Gateway 注册稳定 `runner_id`、sandbox 类型、固定 Sandbox Agent build/digest、支持的 agent adapter/version、capability profile、资源类别、workspace capability 和健康状态。注册身份使用 mTLS；runner ID 不能由任务参数声明或覆盖。

Gateway 通过长轮询或流式出站连接领取 grant。服务只将匹配 runner ID、未过期且未领取的 grant 返回给它。Gateway 验证签名、nonce、attempt/generation、route、source binding、expiry 和 target 后，才读取冻结的 `DispatchSpec`。

runner 无权直接访问 daemon 数据库。所有 claim、heartbeat、report 和 cancel acknowledgement 都必须经过这里定义的控制通道；本机 UDS 不能成为绕过 lease/state machine 的快捷路径。

## 执行接口

```text
claim(runner_identity) -> dispatch_grant | no_work
heartbeat(dispatch_grant, progress) -> lease_valid | cancel_requested | lease_revoked
report(dispatch_grant, lifecycle_event) -> accepted sequence
```

```text
DispatchGrant {
  grant_id
  nonce
  signature
  attempt_id
  lease_generation
  runner_id
  route_id
  source_id
  source_binding_id
  dispatch_spec
  dispatch_spec_digest
  issued_at
  expires_at
  heartbeat_interval_seconds
}
```

所有 ID 都是不透明控制面 ID。grant 不携带 credential；Gateway 只能通过 `source_binding_id` 在本 target 的 secret resolver 中解析该 source。过期、generation 不匹配或 runner 不匹配的 grant 必须拒绝。

调用方提交、状态查询和取消属于 daemon 的统一 Submission API，不属于 Gateway API。`DispatchSpec` 是 Submission 在默认值、权限、profile 与 policy 解析后的不可变执行快照，包含小型 Input、WorkspaceRef、OutputSpec、session binding、解析后的 Profile/Policy、选定 Sandbox Agent RuntimeRef、limits 与 digest。它始终内联在 grant 中；大型输入已经是 `ObjectRef`，不再为控制快照引入第二层引用协议。

远程 grant 的 `signature` 覆盖除 `signature` 自身外的全部规范化字段；算法与公钥轮换属于 runner trust configuration，不进入公共 Submission API。本机 UDS 使用同语义 envelope，但依赖 socket peer identity 和文件权限，不要求网络签名。

大文件仍是 `ObjectRef`，输出仍写入注册的 artifact sink。Gateway 在 Attempt 开始时取得短期、只读输入授权和短期写入授权；短期 URL/credential 不进入 Submission 或持久 DispatchSpec。thieving-eyes 不代理 artifact 内容。

Gateway 在运行中必须按 grant 指定间隔发送 heartbeat。heartbeat 可以携带脱敏进度、provider session 活性和已导出 artifact 摘要；它不得携带 prompt、源码、secret 或完整 transcript。daemon 通过 heartbeat 下发已持久化的取消请求；Gateway 必须确认取消、继续报告最终事实，或在 lease 被撤销时停止执行。Gateway 重连后只能以同一 grant/attempt 恢复 heartbeat，不能自行认领新的 route 或重新执行已不确定的 Attempt。

## Agent Runtime 复用边界

Gateway 通过内部 `AgentRuntime` 边界执行 Agent。runtime 负责能力发现、session 创建/恢复、提交输入、事件流、取消、进程终止和清理；队列、容量、source lease、重试、DispatchGrant、Submission 状态和 workspace placement 仍由 thieving-eyes 负责。`AgentRuntime` 是实现边界，不是新的公共 wire protocol。

[Sandbox Agent](https://github.com/rivet-dev/sandbox-agent) 是 v1 固定的通用 runtime：Gateway 把它作为 sandbox 内独立进程，通过其 HTTP/ACP 接口驱动 Codex、OpenCode 及其他受支持 Agent，并把事件归一化到本 contract。实现固定已验证的 Sandbox Agent 版本，通过 wrapper 隔离其 API；不得把它的 session ID、事件类型、credential 方式或 filesystem/process/desktop API 直接暴露给 Submission 客户端。具体 binding 见 [Sandbox Agent Runtime Binding](sandbox-agent-runtime.md)。

仅当 Sandbox Agent 缺少项目必需能力时才修改其 adapter/wrapper。修改范围必须尽可能沿用上游的安装、进程管理、session、取消和事件处理；不得为单个 Agent 能力引入平行 runtime 或长期 fork 整个控制面。能够回馈上游的通用修改应优先上游化，本项目只维护必要的兼容层和窄 patch set。

本机修改版 OpenCode 是明确的窄扩展：普通 task 走 Sandbox Agent/OpenCode 的上游路径；原生 goal 由受控 Sandbox Agent OpenCode adapter patch 执行。只有该 build 实际通过 `session.goal.*` 与 `session.next.goal.updated` 的能力探测和兼容测试后，Gateway 才能发布 `core.native_goal`，不能因 agent 名称为 OpenCode 就默认声明支持。

## Workspace 数据本地性

DispatchSpec 必须使用 Submission API 的远程 workspace 引用，而不是携带目录内容、绝对路径、Git remote URL 或获取凭据：

```text
WorkspaceRef {
  kind = "binding"
  binding_id          // 调用方有权使用的逻辑 binding
  revision            // Git SHA、内容 digest 或其他不可变版本
  access              // read_only | writable_overlay
}
```

每个 Gateway 由开发者或 runner 管理员预先配置 `WorkspaceBinding`：`binding_id` 到该执行域的受控 mirror/cache、允许 revision 范围、materialization 方式和隔离 checkout 根目录的映射。Submission 只引用 binding ID；remote URL、宿主路径和凭据不注册到 thieving-eyes，也不写入 DispatchSpec。

Gateway 只能通过本地已配置的 binding materialize 指定 revision。它可以使用 binding 允许的目标侧 Git fetch 或 artifact-store 读取，但不得接受任务提供的任意 URL、目录或凭据，也不得要求 thieving-eyes 搬运数据。

`read_only` materialization 不允许修改；`writable_overlay` 为 Attempt/session 建立隔离写层，不得回写或污染共享 mirror。需要跨重启恢复的 session，其 overlay 生命周期和 identity 必须由 binding 明确定义；未声明 artifact collection 的 overlay 在清理后可以丢弃。

Gateway 不得通过 thieving-eyes 请求或上传完整 workspace。对于大型仓库，开发者应先将 mirror/cache 部署到目标执行域，并由调用方将任务派往具备对应 binding 的 runner；无法满足时，Gateway 报告 `workspace_unavailable`，服务不发起隐式跨机器复制。workspace 的 materialization、cache 命中或不可用原因可作为脱敏 lifecycle metadata 报告，但内容本身不进入服务。

## 执行不变量

- 一个 grant 只对应一个 runner、route 和 execution attempt；不得转发或重放。
- Gateway 在 provider/profile 选择后不得自行更换 route；capacity fallback 必须回到服务重新授权。
- Gateway 必须先建立 `DispatchSpec` 指定的系统级 sandbox，再映射 runtime 的 YOLO/auto-approve 参数；Agent 审批绕过不能被当作 sandbox 实现。
- sandbox 内的 permission 自动批准，越界操作由 Gateway 直接拒绝并报告 `sandbox_violation` 或 `policy_denied`；不得在运行中请求人工提权。
- 只有业务选择或补充输入等无法自动回答的 question 才报告 `interaction_required`；source credential 需要登录、续期或 MFA 时报告 `source_auth_required` 并停止使用该 source。Gateway 不等待实时人工回复。
- Gateway 报告 provider error、wall/idle timeout、cancel acknowledgement、artifact export 与 cleanup 的真实执行事实；不得把 agent prose 解释为调用方业务成功。
- provider 会话仅在同一 route、同一 credential scope 和明确支持 resume 时恢复。
- 目标 sandbox 与服务控制面、其他任务、长期开凭据和宿主机资源相互隔离。
- `goal` 的 provider session 是长程执行，Gateway 必须以心跳报告其仍在运行；它持有的 lease 上限、最长无进展时间和取消期限由 route 与所选 Policy 明确限制。
- Gateway 不得把一个已开始的原生 goal 静默迁移到另一 provider、credential scope 或 runner；原生 session 是否可 resume 由 adapter/provider 能力决定。
- runtime 崩溃、协议断开和取消结果必须转换为 thieving-eyes 的 Attempt/Event 事实；不得直接把 runtime 自己的内存 session 状态当作 Submission 权威状态。
- Gateway 失联或 grant/lease 过期不能证明远端 provider 已停止。没有终止或 fencing 证据时必须报告/保持 `uncertain`，对应 source 占用由 daemon 保守保留或 quarantine，不能仅因控制通道超时立即释放。
