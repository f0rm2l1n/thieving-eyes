# 架构与工程基线

本文件固定 thieving-eyes v1 的工程选择。公共 wire contract 以 [Submission API](submission-api.md) 和 [Runner Gateway Contract](runner-gateway-contract.md) 为准；本文件约束实现形态，避免开发阶段重新讨论基础方向。

## 1. 产品拓扑

项目使用 Rust workspace，交付三个程序：

- `thieving-eyesd`：常驻控制面，提供 Submission API、持久队列、调度、容量与 source lease；
- `thieving-eyes-runner`：执行面，领取 Attempt，准备 workspace/sandbox，并驱动 Agent runtime；
- `eyes`：面向人和脚本的 CLI，是公共 API 的薄客户端。

```text
TypeScript / Python / Go / eyes
                │ HTTP/JSON + SSE
                ▼
         thieving-eyesd
        API · SQLite · scheduler
           │              │ outbound pull/mTLS
           ▼              ▼
 managed local runner   remote runner
           │              │ loopback HTTP/ACP
           └──────┬───────┘
                  ▼
         Sandbox Agent runtime
                  ▼
       Codex / OpenCode / other Agent
```

个人模式只启动 `thieving-eyesd`；daemon 自动监管同一安装包中的本地 runner，调用体验仍是一项本机服务。远程模式在目标机器单独运行 runner，由 runner 主动连接 daemon，不要求目标机器开放入站端口。

本地 runner 通过 daemon 创建的私有 UDS 接收 grant-equivalent 派发；它不能直接读取或修改 SQLite。远程与本机只在身份和传输方式上不同，Attempt、lease、heartbeat、取消和 runtime 语义相同。

`thieving-eyes-runner` 内部有 supervisor 与 sandbox worker 两个进程角色，但仍是同一个二进制：supervisor 连接 daemon、materialize workspace 并建立 sandbox；worker 在 sandbox 内与 Sandbox Agent 共享 network namespace，通过 loopback HTTP/ACP 驱动它，再经继承的私有 socket 向 supervisor 回报。这样启用 network namespace 隔离时不需要把 Sandbox Agent 端口暴露到宿主或网络。

v1 只有一个权威 daemon，不实现多 daemon 高可用、共识或分布式数据库。远程 runner 可以有多个，控制面不能有多个写入者。

## 2. 技术选择

| 领域 | v1 选择 |
| --- | --- |
| 语言 | stable Rust |
| 主机平台 | Linux；其他平台不在 v1 支持范围 |
| 异步运行时 | Tokio |
| HTTP | Axum + rustls |
| 序列化 | Serde |
| 持久化 | SQLx + SQLite WAL + 显式 migration |
| 公共流式传输 | SSE；v1 不增加 WebSocket |
| 本机传输 | Unix domain socket；可选 loopback TCP |
| 远程客户端 | HTTPS；scoped bearer token 或 mTLS |
| 远程 runner | HTTPS/mTLS，runner outbound pull |
| 本地 sandbox | runner-side `bubblewrap` backend；预隔离环境可用显式 `external` backend |
| API 定义 | checked-in OpenAPI 3.1 是 schema source of truth |
| SDK | 从 OpenAPI 生成基础 client，再手写少量 submit/watch/run helper |

SQLite 是有意选择：v1 是单控制面 daemon，SQLite 可以提供本机零运维、事务 claim、WAL 和崩溃恢复。数据库访问不预先抽象成可替换后端；如果未来需要多控制面，再以新架构版本引入 PostgreSQL，而不是在 v1 同时维护两套语义。

Markdown 文档解释约束，不能替代 OpenAPI schema。开始编码时首先落 `api/openapi.yaml`；Rust 类型、handler、示例和各语言 SDK 必须由 CI 对照该文件做兼容性检查。

## 3. 代码所有权与复用

thieving-eyes 原生拥有：

- Submission/Attempt/Session 的持久状态机；
- 幂等、队列、优先级、aging、公平性、deadline 与 retry 判定；
- source/account 容量、lease、cooldown、预算和 route/target 选择；
- DispatchGrant、runner heartbeat、取消与 uncertain recovery；
- workspace binding、artifact 引用和公共鉴权；
- provider/runtime 事件到公共 Event/Error 的最终归一化。

Agent 安装、进程管理、provider session、permission/question、取消和原始事件读取统一由 [Sandbox Agent](https://github.com/rivet-dev/sandbox-agent) 承载。它是 v1 唯一的通用 Agent runtime，与 runner worker 位于同一 sandbox，不成为公共 API，也不成为 daemon 的内部状态存储。

runner 通过私有 `AgentRuntime` 接口包装 Sandbox Agent，并使用其 HTTP/ACP 进程边界，不直接耦合尚未稳定的内部 Rust crate。runtime 只监听随机 loopback 端口并使用独立认证 token；其 filesystem、process、terminal、desktop 和 credential-extraction API 不向 Submission client 或网络暴露。完整映射见 [Sandbox Agent Runtime Binding](sandbox-agent-runtime.md)。

依赖必须固定到已验证的 release 与 commit/digest，禁止跟随 `main`。需要的下游改动保存为小型 patch queue，并记录：上游 revision、patch 目的、对应 capability 和兼容测试。Apache-2.0 LICENSE/NOTICE 与修改声明必须随分发保留。

建议的 workspace 边界固定为：

```text
api/                         OpenAPI source of truth
bins/thieving-eyesd/         控制面入口
bins/thieving-eyes-runner/   本机/远程 runner 入口
bins/eyes/                   CLI
crates/protocol/             公共类型与兼容性测试
crates/store/                SQLite migration 与事务
crates/scheduler/            queue/capacity/lease
crates/gateway/              grant/heartbeat/report
crates/runtime/              AgentRuntime trait 与规范化事件
crates/runtime-sandbox-agent/ Sandbox Agent wrapper
patches/sandbox-agent/       固定上游版本的窄 patch queue
sdks/                        生成 client 与薄 helper
```

目录可以在编码前微调名称，但控制面、runner、runtime wrapper 和上游 patch 不得重新耦合到一个 crate。

## 4. Codex binding

普通 Codex task 仍通过 Sandbox Agent 的 HTTP/ACP 边界执行。固定的 `codex-acp` Agent process 在 sandbox 内启动所选固定 digest 的 `codex app-server`，把 ACP session/prompt/cancel 与事件映射到 app-server thread/turn/item。runner 通过 `CODEX_PATH` 指向本 Attempt 挂载的 Codex binary，不使用 adapter 自带的浮动 binary。

Codex SDK 不是第三种 runtime。Python SDK 是 app-server 的高层 client；TypeScript SDK 是 `codex exec` JSONL 的高层 client。thieving-eyes 不嵌入任一语言 SDK，也不为 `codex exec` 维护平行状态机。需要的 task、stream、permission、usage 与取消语义统一从 app-server 经 `codex-acp` 进入既有 Sandbox Agent binding。

Codex binary、`codex-acp` package/entrypoint 与 Node runtime 都属于 target 安装物，必须在派发前存在并核对 digest/version；Attempt 阶段禁止 npm/npx 下载。`core.session_resume`、`core.native_goal` 或结构化输出只有各自的 app-server/ACP 映射通过 conformance 后才能发布，不能因为 app-server 自身拥有某个方法就自动对外宣称支持。

## 5. 修改版 OpenCode

普通 OpenCode task 使用 Sandbox Agent 的安装、server、session、SSE、取消和清理路径。

本机 OpenCode 的原生 goal 是局部扩展：在 Sandbox Agent 的 OpenCode adapter/runtime 边界增加最小 patch，并继续遵守相同的 health、ACP session、event、permission 与 cancel contract。v1 不为 goal 引入第二套独立 runtime；如果补丁不能稳定承载，则该 build 不发布 `core.native_goal`。Submission、Attempt、Session 和 `agent.goal` 事件保持不变。

只有兼容测试实际验证以下行为后，runtime 才能发布 `core.native_goal`：

- `session.goal.set|get|update|clear`；
- `session.next.goal.updated` 的重连与顺序；
- token/time usage 与终止状态映射；
- session resume、cancel 和 daemon/runner 重启后的真实行为。

通用修改优先贡献上游；只对本机 OpenCode 有意义的能力保留在窄 patch 中。不得维护一份与上游全面分叉的 Sandbox Agent。

## 6. Runtime 与 credential 隔离

`ExecutionDomain` 至少由以下内容确定：

```text
target_id
source_id / credential_scope
agent adapter + runtime build
sandbox profile
Agent HOME/session store
workspace identity + revision constraint（有 session 时）
```

不同 credential scope 不得共享 Agent HOME、Codex app-server、OpenCode server、OAuth cache 或 provider session。runtime 进程只可在同一 ExecutionDomain 内复用；不能证明兼容时必须新建隔离 runtime。

source secret 默认保存在执行 target 的受限 secret resolver 中。daemon 保存全局 source ID、脱敏 label、容量/lease 元数据，以及 target 对该 source 的 SourceBinding；DispatchGrant 携带授权后的 source/binding ID，不携带长期 secret。一个 source 可以有多个 target binding，但所有 binding 共享同一份全局容量，不能重复计算。个人本机部署可以使用权限受限文件或 OS keyring，远程部署可以接 secret broker。

不使用 Sandbox Agent 的 credential-extraction 功能作为生产 secret 来源。Gateway 只向选定 runtime 注入本 Attempt 所需 credential。若某 Agent 天生会把父进程环境传给工具子进程，该 route 必须显式声明这种 exposure，并依靠 sandbox、最小 credential 与网络策略控制风险；实现不得虚假声称 credential 一定不会被继承。

daemon SQLite、配置和 UDS 默认只对服务用户可读写。v1 不自造应用层数据库加密；prompt 采用短 retention，长期 secret 不入库，磁盘静态加密由操作系统/部署环境提供。

## 7. 持久化与恢复

SQLite 是 Submission、Attempt、Session metadata、lease、取消意图和规范化事件的权威存储。Sandbox Agent 的内存 session/event buffer 不是事实来源。

状态变化与对应核心事件必须在同一数据库事务中提交。scheduler claim 使用事务和 lease generation 防止重复派发；进程内 mutex 不能充当分布式锁。

daemon 重启后：

1. 恢复 queued Submission；
2. 将未确认的 Attempt 标记为待核查；
3. 接受同 generation runner heartbeat 的恢复；
4. 超过 stale deadline 后，根据 side effects 与停止证据归约为重新排队、failed 或 uncertain；
5. 永不因为 Sandbox Agent 内存中不存在 session 就直接重放任务。

lease timeout 使用单调时钟判断运行期超时；持久化 deadline 使用 UTC 时间。系统时间回拨不得延长已授予 lease。

runner 失联或 dispatch lease stale 不等于远端 provider 已停止。只有取得进程终止、provider session 结束或有效 fencing 的证据后才能释放对应 source 占用并安全重试；否则 Attempt/Submission 进入 `uncertain`，source 容量保持保留或进入保守 quarantine，直到 capacity monitor 证明占用消失或管理员处理。控制面 lease 过期不能凭空制造算力。

## 8. 调度与容量

daemon 使用单 scheduler authority。若 monitor 能观测 provider 的总在用并发（包含 thieving-eyes 自己的任务），source 的保守可用量按下式理解：

```text
configured_limit
- max(active_leases, observed_total_in_use)
- safety_reserve
```

若 monitor 明确只报告服务外占用，则改为减去 `active_leases + observed_external_usage`，不得重复计算同一执行。观测必须带采样时间与 freshness；过期、无法确认的用量、健康度或账号状态都按“不可用”处理，不按空闲处理。capacity monitor 只影响 blocker 和派发，不自行创建 Attempt。

排序至少考虑 client fair share、priority、aging、not_before 和 start_deadline。高 priority 不能绕过权限、预算、session affinity、target compatibility 或 source 安全余量。

## 9. 非交互执行

Submission 是后台非交互任务。默认执行语义是：Agent 在已建立的 sandbox 内以自动批准模式运行，permission 不逐项等待人工确认；sandbox、workspace、network、secret、extension 与 side-effect policy 仍是不可绕过的系统边界。

YOLO/auto-approve 是 Agent 层的审批策略，不是 sandbox。runner 必须先成功建立 Profile 指定的系统级隔离，再向 runtime 开启自动批准；不得只给 Agent 传递 bypass 参数便宣称任务已隔离。越过边界的操作直接拒绝并报告 `sandbox_violation` 或 `policy_denied`，不能通过运行中人工批准升级权限。

真正需要业务选择或补充输入且无法自动处理的 question 才使 Attempt 失败为 `interaction_required`；source credential 需要登录、续期或 MFA 时使用 `source_auth_required` 并隔离 source。任务不能无限等待，v1 也不暴露实时人工回复 API。若某 runtime 无法在非交互模式下可靠地自动响应 permission，该 runtime 不得声明相应 route capability。

这些行为由管理员发布的不可变 Profile 固化。Submission 可以选择获准的 Profile 或施加更严格约束，但不能直接传递 provider-specific `yolo`、`bypass`、approval flag，不能关闭 sandbox。宿主机无隔离自动批准若未来支持，必须使用独立、显式标记为高风险的管理员 Profile，不属于默认模式。

## 10. Workspace 与 sandbox

Sandbox Agent 是 sandbox 内的 Agent controller，不是 sandbox provider。thieving-eyes v1 不实现通用 VM/container/Kubernetes 编排层；target 管理员负责准备可运行 runner 和 Sandbox Agent 的隔离环境。

v1 的本机 first-party backend 是 Linux `bubblewrap`。它负责 user/mount/process namespace、只暴露授权 workspace/scratch/HOME，并按 Profile 接入目标侧网络与资源限制。已经运行在受控容器或 VM 中的 runner 可以配置 `external` backend，将该部署边界作为 sandbox；这是管理员信任声明，不能由 Submission 选择。`sandbox_mode=required` 时 backend 不可用或能力不足必须拒绝派发，绝不静默退化为宿主机执行。

网络能力同样由 Profile 显式声明并由 target 强制实现；`none`、受控 egress 与显式 `inherited` 必须可区分。需要 provider API 的 Agent 不等于其工具自动获得任意网络。若目标无法强制所声明的网络边界，就不能发布对应 sandbox capability。

本机 runner 也必须应用 sandbox profile。远程大仓库必须使用预置 WorkspaceBinding/mirror；daemon 不上传仓库。详细规则见 [Runner Gateway Contract](runner-gateway-contract.md)。

本机 workspace 可以显式选择 `writable` 以获得接近直接运行 `codex exec`/`opencode run` 的原地修改体验；这是实际宿主副作用，不得安全自动重试。`writable_overlay` 修改隔离副本，基础 workspace 不变，输出通过已声明 artifact sink 导出。远程 binding 不允许直接修改 mirror，只能只读或使用隔离 overlay。

scheduler 对 canonical local workspace identity 持有持久 `writable` 独占锁；锁与 Attempt/lease generation 一起恢复，不能因 runner 失联自动释放。`read_only` 和 overlay 是否可并行由 sandbox capability 决定，不能只相信调用方声明。

## 11. SDK 与集成

TypeScript、Python 和 Go SDK 共享 OpenAPI 生成的模型与基础 client。手写部分只提供：

- UDS/HTTP 鉴权和连接；
- Idempotency-Key 生成；
- SSE 断线重连与 sequence 去重；
- `submit()`、`watch()`、`result()` 和组合 `run()`。

SDK 不实现 route 选择、重试、capacity、credential 或状态归约。Tunascope、BrainAFK 与其他项目无论使用何种语言，都调用同一 daemon API。

## 12. 验证与发布门槛

在发布 v1 前必须具备：

- OpenAPI backward-compatibility 检查和多语言生成 client smoke test；
- SQLite migration、断电/重启、重复提交和 lease recovery 测试；
- 确定性 scheduler 测试，包括 priority、aging、fair share、deadline 和 capacity uncertainty；
- AgentRuntime conformance suite，统一验证 start/resume/events/cancel/cleanup；
- 对固定 Sandbox Agent revision 的兼容测试；
- 对本机 OpenCode goal patch 的独立 capability 测试；
- bubblewrap/external sandbox capability、路径逃逸与禁止静默降级测试；
- runner 失联、重复 heartbeat、late report、取消竞态和 uncertain 状态测试。

runtime capability 必须由启动时探测与测试决定，不能由 agent 名称或配置声明单方面伪造。升级 Sandbox Agent、Codex、OpenCode 或 patch revision 时必须重新跑 conformance suite。

## 13. v1 明确不做

- 多控制面高可用或跨区域共识；
- 通用 sandbox provider 编排；
- 任务运行时安装任意 plugin、MCP command 或 secret；
- Agent-to-Agent workflow、业务 goal 分解或结果正确性判断；
- 交互式 permission/question 控制台；
- 把完整 workspace、artifact 或 transcript 经 daemon 中转；
- 同时支持 SQLite 与 PostgreSQL 两套持久化后端；
- fork 并独立演进整个 Sandbox Agent；
- 除 Sandbox Agent 及其受控 OpenCode patch 外的第二套通用 Agent runtime。

这些内容未来可以通过新 target/runtime、独立资源或 API major version 增加，不能预先污染 v1 核心。
