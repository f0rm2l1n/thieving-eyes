# 配置模型

thieving-eyes 始终暴露同一种 Submission API。配置只决定 daemon 如何把调用方约束解析成可执行快照，不产生简单/复杂两套协议。

v1 使用 TOML 配置文件，不提供第二套数据库配置 UI。个人模式默认读取 `$XDG_CONFIG_HOME/thieving-eyes/config.toml`，系统服务通过 `--config` 指定绝对路径。环境变量只允许覆盖 bootstrap 项（配置路径、日志级别、监听地址），不能承载完整 route/profile/policy 或 secret。

## 核心实体

| 实体 | 含义 | 是否对 Submission 可见 |
| --- | --- | --- |
| `Target` | 一个本机或远程 runner 执行域及其 sandbox backend/capability | 可作为约束 |
| `Source` | 一个 provider endpoint/account/credential scope 与容量单元 | 否 |
| `SourceBinding` | 某 target 解析并使用一个 Source 的受控绑定 | 否 |
| `Route` | 逻辑 Agent、model、允许 source pool、target 与 fallback | 可作为约束 |
| `Profile` | Agent approval、sandbox、extensions 与默认行为 | 可按版本选择 |
| `Policy` | retry、记录、retention、通知、预算与调度上限 | 可按版本选择 |
| `Runtime` | 固定版本/digest/patch set 的 Sandbox Agent build | 否 |
| `WorkspaceBinding` | 远程逻辑 workspace 到目标 mirror/cache 的映射 | 只引用 binding ID |

`Source` 才代表需要监控和 lease 的全局算力来源。多个 source 可以属于同一个 route；调用方选择 route/model，不能选择具体账号或 credential。

同一个 Source 可以在多个 target 上有 SourceBinding，但它们共享 daemon 中同一份并发、额度、cooldown 和 active lease 计数，不能被当成多份容量。每个 binding 只保存 target-local secret resolver ID、允许的 runtime 与 execution-domain 约束。

`Runtime` 不是 provider，也不是 route。v1 的 runtime kind 固定为 Sandbox Agent；不同 Runtime 资源只表示经过验证的上游版本、二进制 digest 与窄 patch set。普通 route 使用标准 build；OpenCode goal route 使用通过 `core.native_goal` conformance 的 patched build。

Profile 分开定义 Agent 层审批与系统层隔离。推荐默认值是 `interaction_mode=non_interactive`、`approval_mode=auto_allow`、`sandbox_mode=required`、`escape_behavior=deny`：sandbox 建立后，Agent 在边界内无需逐项确认；越界操作直接拒绝。`approval_mode=auto_allow` 不得隐式关闭或放宽 sandbox。

这些字段属于管理员配置，不进入 Submission。provider-specific `yolo`、`bypass` 或 approval flag 只能由 runtime adapter 根据冻结的 Profile 映射，调用方不能直接注入。无隔离的宿主机自动批准若被部署者启用，必须位于独立的高风险 Profile，不能成为默认值或由普通 client 选择。

## 简单配置

个人项目只需一个本机 target、一个 source、默认 route/profile/policy、允许 workspace root 和 sandbox profile。下面只表示最小资源关系，不固定未来 TOML 的表名：

```text
database = local SQLite
default_target = local
default_route = opencode_default
default_profile = local_coding
default_policy = standard
sandbox_backend = bubblewrap
allowed_workspace_roots = [/home/user/projects]
local_coding = non_interactive + auto_allow + required_sandbox + deny_escape
```

`thieving-eyesd` 自动监管本机 runner；项目只提交 text part 和可选本机 workspace。调用方无需了解 source、lease、runtime、mirror 或 grant。

## 扩展配置

复杂部署在相同模型上增加资源。下面同样是资源关系示意，不是第二套配置语法：

```text
targets = [local, runner_a, runner_b]
sources = [codex_account_1, codex_account_2, internal_opencode]
source_bindings = [codex_1_local, codex_1_runner_a, opencode_local]
routes = [codex_primary, opencode_fallback]
runtimes = [sandbox_agent_standard, sandbox_agent_opencode_goal]
workspace_bindings = [tunascope_runner_a, brainafk_runner_b]
```

每个 route 必须明确：adapter/model 约束、source pool、允许 target/runtime、fallback 顺序与必需 capability。每个 source 必须明确：配置并发上限、safety reserve、容量观测方式和 cooldown policy。每个 SourceBinding 必须明确：target、credential resolver reference、允许 runtime 和隔离要求。

长期 credential、OAuth cache、HOME 路径、MCP secret 和 mirror remote URL 不写入 daemon-facing 配置；这里只保存 target-local resolver/binding ID。

## 解析与变更

配置加载必须原子化：整份新配置验证成功后才替换当前视图，不能部分生效。SIGHUP 或管理 CLI 只触发重新读取同一文件，不直接修改配置。已接收 Submission 使用冻结的 profile、policy、route/target 集合与 extension digest，不因热加载改变。

Profile、Policy 与 Extension 的已发布版本不可原地修改。新配置创建新版本；旧版本在仍被 Submission/Session 引用时继续可解析。

禁用 source 后不再授予新 lease；已有 Attempt 按管理员配置选择 drain 或取消。删除 target/runtime/workspace binding 前必须先处理关联的 active Session，不能让它们静默迁移。

Submission 可以在权限允许时收紧 route、target、profile 或 policy 选择，不能扩展配置未授权的资源。未指定时使用 client namespace 的默认值。
