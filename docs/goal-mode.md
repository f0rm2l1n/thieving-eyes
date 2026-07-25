# Goal mode

## 定义

`goal` 是一种执行模式：调用方要求 Gateway 用所选 agent/provider 的**原生持久 goal 或等价长程运行机制**处理 Submission 中的正常任务输入。它表达“这不是一次短 prompt，而是允许 Agent 自行持续推进的长程工作”。

thieving-eyes 不创建、拆分、评估或完成 goal；也不从 agent 文本、artifact 或 checkpoint 推断 goal 是否完成。它只负责为兼容的 route/source/runner 授予容量、记录 provider 报告的生命周期，并执行 timeout、取消和 stale-lease 回收。

任务输入仍使用 Submission API 的 Input、WorkspaceRef 与 OutputSpec；daemon 在接收时冻结其执行快照。`goal` 不引入独立的 GoalManifest、completion contract 或平台级 continuation 协议。

## 准入与生命周期

提交使用 `mode.kind="goal"`。候选 route 必须声明 `core.native_goal` capability；没有该能力的 route 不得被选中，也不得用“多次普通 prompt”伪造为 goal mode。goal 的有效 session binding 必须是 `create` 或 `resume`；调用方省略 session 时默认解析为 `create`。

一个 Goal Submission 最多创建一个长程 Attempt，不做自动 route/source fallback；它可以新建 provider session，也可以显式恢复已有 session。Attempt 持有相应 source lease，直到 provider 报告结束、被取消、达到 route 的硬 deadline，或服务取得已停止/已 fence 的证据。仅 heartbeat stale 时执行状态与容量都按不确定处理，不能假定 provider 已结束。它的调度成本和最长占用时间必须在所选 Policy 中单独限制，避免长程任务无限占用稀缺 source。

服务通过 `agent.goal` 事件转发 provider 的 active、blocked、complete、budget/usage limited 等事实。Submission 本身仍只使用通用的 queued、running 与终态；provider 宣告 goal complete 且 runtime 正常结束时可以归约为 `completed`，但不代表调用方的业务结果已被 thieving-eyes 验证。blocked、预算耗尽或中断按明确错误归约，不能伪装成成功。

## 亲和性、恢复与切换

原生 goal 默认具有严格亲和性：同一 adapter、credential scope、runner、受控 agent HOME/session store 和 workspace revision。服务不得复制 provider session、OAuth cache、HOME 或 thread/session ID 到另一机器、账号或 provider。

发生 rate limit、quota、runner 失联或服务重启时，Gateway 只在该 provider 明确支持、且仍位于相同受控执行域时恢复原 session；否则报告可诊断的中断、失败或 `uncertain` 状态。thieving-eyes 不自动以另一账号、另一 provider 或新 session 重放这个 goal。调用方若要恢复可用的原 session，或改用其他 route，必须提交新的 request。

## Provider adapter 映射

- **Codex app-server**：adapter 使用 materialized thread 的 `thread/goal/set|get|clear` 和 goal 更新事件，随后以该 thread 的原生 turn/自动继续语义运行。Goal 状态与 token/usage 状态作为 provider 事实转发。当前固定的 `codex-acp` 尚未向 ACP 暴露这组方法，因此普通 Codex task route 不发布 `core.native_goal`；只有窄 adapter extension 与 conformance 完成后才能启用。
- **Codex TypeScript SDK / `codex exec`**：SDK 可创建/恢复 thread 和运行 turn，但不应被当作原生 goal 控制面。需要 goal mode 的 Gateway 选择 app-server adapter，而不是依赖 prompt 让模型“碰巧”调用 goal 工具。
- **OpenCode v2 SDK/API**：adapter 创建 session 后使用 `session.goal.set` 设置 objective/可选 token budget，订阅 `session.next.goal.updated`，并以该 session 的原生 loop/continuation 运行。`session.goal.get|update|clear` 提供状态、暂停/恢复、结束与清理；adapter 应把 `active`、`complete`、`blocked`、`budget_limited`、`usage_limited` 等原生状态作为 provider 事实转发。普通 `session.prompt`、session history 或 plan agent 本身不等价于 goal mode，只有目标 Gateway 实际安装并声明支持上述 v2 goal contract 时才可声明 `core.native_goal` capability。

Sandbox Agent 承载上述 provider 的通用 session、取消和事件处理，但其上游 OpenCode adapter 不被协议假定具备本机扩展的 goal 能力。本项目在固定 Sandbox Agent revision 上以局部 OpenCode adapter patch 增加该能力，并保持 Submission/Goal contract 不依赖补丁私有字段。
