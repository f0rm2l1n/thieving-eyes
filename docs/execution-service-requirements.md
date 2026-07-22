# 执行服务要求

## 边界

thieving-eyes 是共享执行准入与后台运行服务，不是业务控制面。它必须管理 provider source、并发、预算/额度、cooldown、route fallback、target runner 健康度、dispatch grant 与取消；不得解释 prompt、源码、finding、审核或调用方任务终态。

调用方始终提交同一种 `Submission`：可选调用方引用、mode、content-part 输入、可选 workspace/output/session/route/target 约束与 priority。daemon 从配置解析未指定的 profile、policy、route、source、sandbox 与 target；相同 idempotency key 重复提交必须返回同一 Submission。

服务持久化调度元数据、受保护的请求快照、脱敏 source label、lease、事件、用量和错误分类。prompt 与小型 inline data 只作为运行数据保存到配置的短期保留期，不进入普通日志；大型文件必须使用稳定 `ObjectRef`，artifact 内容不经过 daemon。

execution mode 是 `task` 或 `goal`。`task` 是普通非交互执行；`goal` 要求目标 Gateway 使用选定 provider 的原生持久 goal/长程运行机制。它是对执行方式和 route capability 的要求，不是 thieving-eyes 自己的目标编排；详见 [Goal mode](goal-mode.md)。两者都不改变服务不解释业务 prompt、源码或结果的边界。

## 数据本地性与 workspace

thieving-eyes 是控制面，不是源码、workspace 或 artifact 的数据通道。跨机器调度时，服务不得下载、复制、中转或缓存 workspace，也不得把大型输入文件嵌入 Submission、grant 或事件。

远程 target 的 Submission 必须使用 `WorkspaceRef.kind=binding` 和不可变 revision。workspace mirror、cache、remote URL、宿主路径、拉取凭据及预热策略均由开发者或 runner 管理员在目标执行域预先布置；thieving-eyes 和调用方任务均不得创建、配置或修改它们。Gateway 只能根据其已注册的 workspace binding materialize 对应 revision。不存在、未授权或静态不兼容的 binding/revision 在接收时以 `workspace_unavailable` 拒绝；已注册但因目标侧刷新或预热而暂时不可用时保持 `queued` 并报告 `workspace_pending` blocker。服务不得以搬运仓库作为兜底。

派往本机 target 的 `WorkspaceRef.kind=local` 只能引用配置允许根目录内的工作目录，并由本地 runner 以解析后的 sandbox profile 启动；它不需要 mirror 或 WorkspaceBinding。本机 `writable` 直接修改注册目录，属于不可假定幂等的真实副作用，并要求 canonical workspace 独占锁；冲突任务以 `workspace_busy` 排队。`writable_overlay` 不修改基础目录。远程 binding 只能只读或使用隔离 overlay。

服务可根据 Gateway 注册的脱敏 workspace capability 或 runner label 调度，但不保存 mirror 地址、仓库内容、目录树、credential 或 cache 索引。Submission、grant 和事件只承载小型控制数据与对象引用；大体积输入/输出在对象存储与目标执行域之间直连传输，优先使用内容寻址 revision、预热 mirror/cache、增量 fetch 和目标侧 artifact store。

## 准入与路由

任务可派发的条件是：目标 runner 健康、处于允许窗口、预算可用、兼容 provider/profile 可 lease，且调用方允许该 route。服务以稳定优先级排序并防止单一调用方或 profile 垄断容量。

只有 `task` mode 的 source-scoped `rate_limited`、`quota_exhausted`，以及发生在 Agent 启动前的 `source_auth_required`，才可在冻结的 route 集合与 Policy 允许时考虑切换 source/provider；同时必须证明旧执行已停止或尚未产生副作用，并满足 Submission 的 `side_effects` 重放约束。权限、配置、schema 和业务错误不得触发 fallback。每次切换都创建新的 Attempt 和可见事件，禁止跨 provider 续用 session。`goal` mode 不自动 fallback，其亲和性与恢复规则见 [Goal mode](goal-mode.md)。

## 目标 sandbox 与凭据

远程 Runner Gateway 采用出站 mTLS/pull 模式；服务不要求目标环境暴露入站端口。Grant 必须一次性、短期、绑定 target runner 与 route，领取或过期后即失效。daemon 监管的本机 runner 是独立进程，不经 mTLS/grant 网络交互，但仍取得相同的 source lease 并遵守同一 route/sandbox policy。

长期 credential 不得写入 Submission、grant、日志、trace、artifact 或调用方仓库。source secret 保存在执行 target 的受限 resolver 中，Gateway 凭 grant 中的 source ID 取得本 Attempt 所需 credential。不能安全委托的本地/交互账号只能在持有其授权的 ExecutionDomain 使用，禁止复制 HOME/auth cache 到远端 sandbox。若 Agent 会把环境传给工具子进程，该 exposure 必须由 route 明确声明并通过 sandbox、最小 credential 和网络策略限制。

Gateway 必须以本地 sandbox policy 强制 worktree/scratch/HOME 隔离、最小网络、非特权运行、无 Docker socket/hostPath/平台数据库挂载，并在输出导出后清理。服务不把 Kubernetes Pod/Job 状态当作调用方任务的权威状态。

Submission 是非交互任务。默认 Profile 在确认系统级 sandbox 已建立后，让 provider/runtime 在边界内以 YOLO/auto-approve 方式运行，permission 不逐项等待人工确认。YOLO 只是 Agent 审批策略，不能替代 sandbox；越过 workspace、network、secret 或 side-effect 边界的操作必须直接拒绝，不能在运行中请求人工提权。

真正需要业务选择或补充输入且无法自动处理的 question 以 `interaction_required` 失败。source credential 需要登录、续期或 MFA 时以 `source_auth_required` 失败并隔离该 source。运行时不能可靠支持非交互 permission 处理时，route 不得声明对应 capability。调用方不能在 Submission 中传递 provider-specific bypass flag 或关闭 sandbox；无隔离的宿主机自动批准只能由独立的高风险管理员 Profile 显式启用。

## 事件与恢复

Submission 状态、Attempt、终态、事件 envelope、SSE 恢复、取消、错误码和 retryability 以 [Submission API](submission-api.md) 为唯一规范。每个事件带 submission、attempt、sequence、时间与脱敏详情；它们描述执行事实，不裁定调用方的业务终态。Gateway 失联、grant 过期或 lease stale 时，服务必须先确认执行已停止或已被 fence，才能释放 source 占用并按 Policy 重试；无法确认时进入 `uncertain` 并保守保留/quarantine 容量。

调用方以 event sequence、attempt 和自身状态机校验事件；服务事件不能直接写入 calling product 的 Finding 或业务终态。
