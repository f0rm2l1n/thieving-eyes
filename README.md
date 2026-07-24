# thieving-eyes

一个面向多个 AI 系统与目标 sandbox 的容量感知后台执行层。

它接收 Codex、OpenCode 等非交互 Agent 任务，按优先级排队，并在授权的 provider source 与执行 target 同时可用时派发执行。它集中维护多个账号或 source 的并发、额度、冷却和执行生命周期，让任务在容量不足时等待，而不是盲目发起请求或被错误地视为失败。

本项目不负责业务任务、Git 语义、Agent workflow、finding 或结果解释；这些仍属于 Tunascope、BrainAFK 及其他调用方。它只负责为它们提供共享、保守且可审计的执行准入与目标 sandbox 运行能力。

## 核心模型

所有调用方都提交同一种 `Submission`：任务输入、优先级、execution mode、可选 route/target 要求和 idempotency key。daemon 按配置解析默认 source、route、sandbox 与执行 target；target 可以是 daemon 监管的本机 runner，也可以是以出站 mTLS 领取 grant 的远程 Runner Gateway。跨机器客户端通过 HTTPS bearer token 或 mTLS 使用同一 API。

简单配置只需一个本地 source、默认 route、允许工作目录和 sandbox profile，提交体验接近本地 `opencode run` 或 `codex exec`。复杂配置可增加多个 source/profile、容量窗口、远程 runner、workspace binding 和 route fallback，但不改变调用方 API、队列或事件 contract。

后台任务默认在系统级 sandbox 内以 YOLO/auto-approve 运行，不逐项等待权限确认；sandbox 外的文件、网络、secret 与副作用边界仍由 runner 强制拒绝。真正需要业务输入的任务以 `interaction_required` 结束，source 登录或 MFA 则单独报告 `source_auth_required`。

项目使用 Rust 实现控制面和 runner；v1 统一使用 Sandbox Agent 作为 Agent runtime。本机修改版 OpenCode 的 goal 能力只会通过局部 adapter patch 接入，并在通过能力探测和 conformance 前保持关闭。

当前 `0.1` 已实现本机 OpenCode task：UDS HTTP API、SQLite 持久队列、priority/aging、静态或命令容量探测、bubblewrap sandbox、事件/结果/取消，以及固定 Sandbox Agent 的下载校验。goal、持久 session、Extension、远程 runner 和 artifact/object transport 仍按 v1 协议保留，但在能力发布前会明确拒绝。

```text
cargo build --workspace
eyes init --workspace-root /absolute/project/root
eyes doctor
thieving-eyesd
eyes run --workspace /absolute/project "完成后台任务"
eyes run --model provider/model "使用已配置 route 的模型"
```

`eyes init` 是显式安装步骤：它从 Sandbox Agent 官方 release 分发地址取得固定版本并校验 SHA-256；daemon 不会在接收任务时临时安装 runtime 或 Agent。

## 文档

- [执行服务要求](docs/execution-service-requirements.md)
- [架构与工程基线](docs/architecture.md)
- [Submission API](docs/submission-api.md)
- [Runner Gateway contract](docs/runner-gateway-contract.md)
- [Sandbox Agent runtime binding](docs/sandbox-agent-runtime.md)
- [Goal mode](docs/goal-mode.md)
- [配置模型](docs/configuration.md)
- [内网构建与部署](docs/intranet-deployment.md)
