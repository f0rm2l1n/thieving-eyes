# Sandbox Agent Runtime Binding

本文件固定 thieving-eyes v1 与 [Sandbox Agent](https://github.com/rivet-dev/sandbox-agent) 的内部协议边界。Sandbox Agent 是 v1 唯一的通用 Agent runtime；这不是可由 Submission 直接调用的第二套公共 API。

本文设计核对过 Sandbox Agent `0.4.2` 及上游 revision `bbc195cc3fb5a1dd9cb05d8437442768c511e17e`。实现时必须固定一个通过 conformance suite 的 release、commit 与二进制 digest；这里记录的版本不是自动升级规则。

## 1. 部署与所有权

runner supervisor 先建立系统级 sandbox，再以同一 `thieving-eyes-runner` 二进制的 worker 角色在 sandbox 内启动 `sandbox-agent server`。worker 与 Sandbox Agent 共享 network namespace，使用 loopback HTTP/ACP；worker 通过 supervisor 创建并继承的私有 socket 接收控制、回报事件。一个 server 只属于一个 `ExecutionDomain`，因此不会跨 source credential、Agent HOME、sandbox Profile 或 session store 复用。

```text
thieving-eyes-runner supervisor
  └─ private inherited socket
      └─ system sandbox (bubblewrap / external)
           ├─ thieving-eyes-runner worker
           ├─ sandbox-agent server
           ├─ Agent HOME + source credential
           ├─ materialized workspace
           └─ Codex / OpenCode process or server
```

Sandbox Agent 只监听 sandbox network namespace 内的随机 loopback 端口，并使用每次启动生成的高熵 token。只有 worker 持有 endpoint/token；它们不进入 supervisor 持久状态、daemon、DispatchGrant、事件或日志。runtime 不监听宿主或 target 的公网接口。

Agent binary 与 Sandbox Agent 必须在 target image/安装阶段预置并固定版本。任务执行时不得使用 Sandbox Agent 的 lazy install、任意上传或 credential extraction 作为兜底；缺失二进制时该 target 不发布对应 capability。

thieving-eyes 原生拥有队列、Attempt、Session metadata、持久事件、重试、容量和结果。Sandbox Agent 拥有当前 runtime instance 内的 Agent 进程、ACP connection 与 provider session 操作，但其内存状态不是权威存储。

## 2. 使用的上游协议面

Rust runner 实现窄的 Sandbox Agent HTTP/ACP client，不嵌入其 TypeScript SDK。只依赖以下上游协议面：

- `GET /v1/health`：runtime readiness；
- `GET /v1/agents?config=true`：Agent 安装版本和静态 capability 探测；
- `POST/GET/DELETE /v1/acp/{server_id}`：ACP JSON-RPC request/response、SSE 与 connection lifecycle；
- `/v1/config/skills`、`/v1/config/mcp`：仅由 runner 根据冻结 Extension 配置调用。

filesystem、process、terminal、desktop、credential extraction、Agent install 与 Inspector API 不暴露给 Submission client，也不作为任务任意调用的 escape hatch。runner 自身如需 workspace staging 或最后手段的进程终止，应优先使用已受控的本地 OS handle；使用 Sandbox Agent system API 时也必须由内部固定逻辑调用，而不是转发 prompt 参数。

每个 `(runtime_instance, adapter)` 使用一个随机 `server_id`，允许同一 ExecutionDomain 内的多个 provider session 复用其 Agent server。第一次 POST 绑定 adapter 后不得改变；一个 Session 同时最多一个在途 `session/prompt`。

## 3. 启动与 capability 协商

启动顺序固定为：

1. runner 建立 sandbox、workspace、scratch、Agent HOME、网络和 resource limits；
2. 注入本 SourceBinding 的最小 credential；
3. 启动固定 digest 的 Sandbox Agent，等待 `/v1/health`；
4. 查询 `/v1/agents?config=true`，核对 adapter、Agent 版本和配置能力；
5. 创建 ACP connection，完成 `initialize` 并保存实际协商的 protocol/capabilities；
6. 订阅 ACP SSE 后才创建/load session 和发送 prompt。

health 只表示 runtime 可响应，不表示 provider credential 有效、source 有容量或 workspace 已准备。后面三项仍由 thieving-eyes 分别验证。

Gateway 发布的 capability 必须是 target sandbox capability、固定 Sandbox Agent build、Agent adapter 与 ACP initialize 结果的交集。model、mode、thought level 和其他 config option 只能使用实际发现的 ID；不支持时以 `capability_unavailable` 拒绝，不猜测 provider CLI flag。

## 4. Session 映射

公共 `Session` 与上游 session ID 分离。target-local session registry 至少保存：

```text
RuntimeSessionBinding {
  binding_id
  thieving_session_id
  runtime_build_digest
  runtime_instance_id + generation
  acp_server_id
  agent_session_id
  adapter + agent_version
  execution_domain_digest
  workspace_identity + revision_constraint
  last_runtime_event_id
}
```

daemon 只持久化 opaque `binding_id`、亲和性与可恢复性，不向调用方返回 `acp_server_id` 或 `agent_session_id`。target-local registry 与 Agent HOME 使用服务用户权限保护。

`session.mode=create` 映射到 ACP `session/new`；`resume` 只能在相同 ExecutionDomain 中使用 ACP/provider 实际声明的 `session/load`、resume 或等价原生能力。Sandbox Agent server 重启后内存 session/SSE buffer 可能消失；这不等于 provider session 已消失，也不等于它一定可恢复。

不得采用 Sandbox Agent TypeScript SDK 的 transcript replay auto-restore 来实现 `core.session_resume`。把历史事件重新拼进新 prompt 会产生新的 provider session、可能重复副作用，语义上不是恢复。只有原生 load/resume 通过 conformance test 时 `can_resume=true`；否则 session 变为 `unavailable`，历史仍可读取。

ephemeral session 在 Attempt 完成后从 target-local registry 清理。若 adapter 没有 session delete，runner 在同一 ExecutionDomain 无 active session 后按 Policy 回收整个 runtime/Agent HOME；不得为了清理一个 session 杀死共享 runtime 中的其他 active session。

## 5. 输入、Profile 与 Extension 映射

runner 在发送 prompt 前完成全部输入 materialization：

- text 映射为 ACP text content；
- data 映射为 ACP embedded resource，adapter 不支持时使用带 media type 的确定性 JSON text 表示；
- workspace file 映射到 sandbox 内已校验的相对路径/resource link；
- object file 先由注册 resolver 下载、校验 digest，再以只读文件放入 input staging 目录。

adapter 不支持某种 content part 时必须在派发前以 capability 拒绝，不能静默丢弃。任何传给 ACP 的路径都必须是 sandbox 内路径，不能出现宿主绝对路径。

结构化输出只在 Sandbox Agent/Agent adapter 能可靠设置 provider schema，或 wrapper 能对最终完整文本执行严格 JSON Schema 校验时发布 `core.structured_output`。校验失败是 `output_schema_invalid`，不能从自由文本中猜测或修补 JSON。

Profile 的 model/mode/thought/config 通过 ACP 的标准 session config 方法和实际发现的 option 映射。YOLO 不依赖某个 provider 的 bypass mode：runner 响应 `session/request_permission`，在系统 sandbox 已建立且 Profile 为 `auto_allow` 时优先选择一次性允许选项；每个后续请求继续自动允许。只有上游实际只提供 session-wide allow 且 conformance test 验证边界不变时才可使用 always/bypass。拒绝或无法安全映射时返回 `policy_denied`，不等待人工。

permission 与业务 question 必须分开：工具执行许可按上述规则自动处理；adapter 暴露的澄清问题、自由文本输入或无法由 Profile 决定的选项不自动编造答案，立即取消当前 turn 并归类为 `interaction_required`。登录、续期或 MFA 归类为 `source_auth_required`，不伪装成普通 question。

skill/MCP/plugin 仍由 Submission 中的冻结 `ExtensionRef` 驱动。runner 只把管理员预注册、固定 version/digest 且已授权给 target 的配置写入 Sandbox Agent：

- 不接受 Submission 提供 Git URL、MCP URL/command、env 或 secret；
- 外部 skill 在 build/预热阶段取得并校验，任务运行时不跟随 branch；
- MCP credential 由 target-local resolver 注入，配置与事件不得保存明文；
- config 必须在 `session/new` 前完成，并在 session 生命周期内保持不变；
- 每个 Attempt/session 使用唯一 sandbox cwd/config namespace，不能污染共享源码目录或其他任务。

Sandbox Agent 没有稳定通用 plugin 安装协议。`ExtensionRef.kind=plugin` 在 v1 只能选择已编入或预装到固定 Runtime build、并由 capability 标识的 plugin；运行时不得下载或执行安装脚本。custom tool 应建模为预注册 MCP/skill，或同样进入固定 Runtime build。

## 6. 事件与完成

runner 在 prompt 前建立 SSE，并以 Sandbox Agent SSE ID 在同一 runtime generation 内断点续读。收到的 ACP envelope 先写入 runner-to-daemon report 流，再归一化：

| Sandbox Agent / ACP 事实 | thieving-eyes 处理 |
| --- | --- |
| `agent_message_chunk` | 聚合最终输出；按 Policy 产生 `agent.message` |
| `tool_call` / `tool_call_update` | `agent.tool` |
| `plan` | `agent.plan` |
| `usage_update` | `agent.usage` 与 Attempt usage |
| `agent_thought_chunk` | 默认不公开；仅按明确 Policy 写受保护 transcript |
| permission request/response | runner 内部自动处理；可写脱敏 diagnostic |
| 未知 update | 保留为受保护 raw event 或 diagnostic，不改变核心状态 |

daemon 的 Submission `sequence` 与 Sandbox Agent SSE ID 是两个命名空间。去重键至少包含 `(runtime_instance_id, generation, runtime_event_id)`；归一化事件不能把上游 ID 直接当公共 sequence。

prompt 的 ACP response 与其因果上先发生的 session update 都被接收后，runner 才判定该 turn 已结束。`stopReason=end_turn` 或等价正常结束可进入结果导出；取消、预算、拒绝、adapter exit 和未知 stop reason 必须显式映射，不能都当作成功。最终 text 来自当前 turn 的 agent message 聚合，不能从进程 stdout 随意截取。

Sandbox Agent SSE buffer 和 SDK persist driver 都不是持久事实来源。runner/daemon 必须边收边保存；同一 runtime 重连使用 Last-Event-ID，runtime generation 改变后不得假设旧 offset 仍有效。

## 7. 取消、超时与故障

取消首先发送 ACP `session/cancel`，随后等待 prompt 结束和 provider/Agent 停止证据。超过 cancel deadline 时，只有该 runtime 没有其他 active session，或底层 adapter 提供 session-scoped kill，才能升级为进程终止；否则不能为了取消一个任务破坏无关任务。

关闭 `/v1/acp/{server_id}` 是 connection/adapter lifecycle 操作，不等价于证明远端 provider 副作用已停止。HTTP/SSE 断开、Sandbox Agent 崩溃或 runner 失联都按恢复规则核查；无法证明停止时归为 `uncertain` 并保留/quarantine source 容量。

错误至少按以下顺序分类：

- Sandbox Agent 不健康、HTTP/ACP framing 或协议不兼容：`runtime_unavailable`；
- ACP/provider authentication required：`source_auth_required`；
- 不支持的 method/config/content：`capability_unavailable`；
- sandbox/path/policy 拒绝：`sandbox_violation` 或 `policy_denied`；
- adapter/provider 已归类错误：对应稳定 provider error；
- 无法归类的 Agent/provider 失败：`provider_error`。

## 8. OpenCode goal 扩展

普通 task 必须走上述上游 binding。修改版 OpenCode 的 goal 是唯一预先批准的窄扩展：在固定 Sandbox Agent OpenCode adapter 上增加 `session.goal.*` 与 `session.next.goal.updated` 转发，不引入平行 runtime。

扩展必须保留标准 health、ACP session、event、permission、cancel 和 error 语义；goal method 通过 capability negotiation 发布为 `core.native_goal`。补丁不能把 OpenCode 私有字段泄漏到 Submission API，不能改变普通 task 的上游兼容路径。

runner 与 patched Sandbox Agent 之间使用以下私有 ACP extension；它们不属于公共 Submission API，也不声称是上游 ACP 标准：

```text
_thieving/goal/set
_thieving/goal/get
_thieving/goal/update
_thieving/goal/clear
_thieving/goal/updated       # agent -> runner notification
```

initialize response 的 `_meta.thievingEyes.capabilities` 必须显式包含 `core.native_goal` 和 binding version，runner 还要核对预期 build digest；只配置 method 名称不能发布能力。patch 将这些 extension 映射到本机 OpenCode 的 `session.goal.*` 与 `session.next.goal.updated`，并保留原生状态和 usage，不要求 thieving-eyes 解析 Agent 文本。

## 9. Conformance 必测项

- health/agent discovery 与版本不匹配拒绝；
- initialize、session/new、prompt、事件顺序和正常 stop reason；
- SSE 断线、Last-Event-ID 重连、重复 envelope 去重；
- permission 自动批准、不可用 reply 和 sandbox 拒绝；
- session 原生 resume 与 runtime 重启后不可恢复分支；
- cancel acknowledgement、超时升级、共享 runtime 不误杀；
- skill/MCP 固定配置、secret 脱敏与 cwd 隔离；
- Agent crash、Sandbox Agent crash、runner loss 和 `uncertain` 映射；
- Codex/OpenCode 各固定版本，以及修改版 OpenCode goal patch。
