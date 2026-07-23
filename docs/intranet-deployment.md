# 内网构建与部署

本文说明如何把当前 `0.1` 本机执行栈迁入可通过代理访问公网的企业内网。当前可发布拓扑是 daemon、runner、Sandbox Agent、OpenCode 与 workspace 位于同一台 Linux 主机；跨机器 HTTPS API、远程 runner、WorkspaceBinding、goal 和持久 session 尚未实现，不能仅靠配置启用。

## 1. 迁移基线

发布时必须固定并记录：

- thieving-eyes Git commit，以及同一次 `cargo build --release --locked` 产生的 `thieving-eyesd`、`thieving-eyes-runner`、`eyes`；
- Rust stable `1.94` 或与 workspace `rust-version` 相容的更高稳定版本；
- Linux x86_64 运行环境、glibc/libgcc 兼容性和 bubblewrap 版本；
- Sandbox Agent `0.4.2`，SHA-256 `bab098abef874ade481aa7b50463662814fbf27294399f545307fedb638f029b`；
- 本地 OpenCode binary、provider 配置、patch set 与 SHA-256；
- `Cargo.lock`、最终 `config.toml` 和所有发布二进制的 SHA-256 manifest。

不要分发 `target/debug`。当前 thieving-eyes release binary 不是完全静态产物，应在与内网目标相同或更老的 Linux 发行版上构建；Sandbox Agent 使用项目固定的 x86_64 musl 发布件。

## 2. 系统依赖

构建机至少需要 Rust/Cargo、C toolchain、Git、CA certificate、pkg-config 和常见构建工具。运行机需要：

- `/usr/bin/bwrap`，以及允许 user、mount、PID、IPC、UTS 和 network namespace 的内核/服务策略；
- 可执行的 OpenCode binary；
- 系统 DNS 和 CA trust 能访问 provider endpoint；
- 服务用户可读的 OpenCode 配置/认证文件；
- 服务用户可读写的 SQLite state、runtime cache 和 workspace；
- 足够的临时目录和 workspace 磁盘。

thieving-eyes 使用 rustls，不依赖系统 OpenSSL；SQLite 由当前依赖构建进 binary。具体包名随发行版变化，先在目标基础镜像中运行完整构建和 `eyes doctor`，不要只依据包清单推断兼容性。

## 3. 代理与 CA

构建代理只应存在于构建 shell、CI secret 或构建机配置中，不应写入仓库：

```bash
export HTTP_PROXY="http://proxy.example.internal:8080"
export HTTPS_PROXY="http://proxy.example.internal:8080"
export NO_PROXY="127.0.0.1,localhost,.example.internal"
export http_proxy="$HTTP_PROXY"
export https_proxy="$HTTPS_PROXY"
export no_proxy="$NO_PROXY"
```

代理凭据不得出现在 Git URL、Cargo config、构建日志、daemon config 或 systemd unit 的公开字段中。需要企业 CA 时，先把 CA 安装到构建机和运行机的系统 trust store，并分别验证：

```bash
curl -fsS https://index.crates.io/config.json >/dev/null
curl -fsSI \
  https://releases.rivet.dev/sandbox-agent/0.4.2/binaries/sandbox-agent-x86_64-unknown-linux-musl \
  >/dev/null
```

OpenCode 的运行时网络与构建网络是两个边界。runner 会清空宿主环境，只显式建立 sandbox HOME、XDG 和 PATH；它不会把 daemon 的 `HTTP_PROXY`、`HTTPS_PROXY` 或任意 provider 环境变量传入 Agent。provider 若要求代理，应优先在内网网关、DNS、OpenCode 固定配置或系统网络层解决。若必须注入代理环境变量，需要先实现管理员控制、按 SourceBinding 冻结的环境白名单，不能让 Submission 自由提供。

## 4. Rustup 镜像

能稳定访问官方源时无需替换。企业内部有 rustup mirror 时，可以在构建环境设置：

```bash
export RUSTUP_DIST_SERVER="https://rustup-mirror.example.internal"
export RUSTUP_UPDATE_ROOT="https://rustup-mirror.example.internal/rustup"
rustup toolchain install 1.94.0 \
  --profile minimal \
  --component rustfmt \
  --component clippy
rustup default 1.94.0
```

国内公共镜像可以作为临时方案。例如 RsProxy 当前公开的配置是：

```bash
export RUSTUP_DIST_SERVER="https://rsproxy.cn"
export RUSTUP_UPDATE_ROOT="https://rsproxy.cn/rustup"
```

公共镜像是额外的供应链与可用性依赖。正式企业构建应优先使用受控内部 mirror，或者预装并固定 toolchain；CI 不应在每次构建时隐式更新 Rust。

## 5. Cargo registry

优先使用企业内部、与 crates.io 内容一致的 sparse mirror。在构建用户的 `$CARGO_HOME/config.toml` 配置，不要提交含企业地址或 token 的项目级配置：

```toml
[source.crates-io]
replace-with = "company-sparse"

[source.company-sparse]
registry = "sparse+https://cargo-mirror.example.internal/index/"

[net]
git-fetch-with-cli = true
retry = 3
```

`source replacement` 只能替换为 crates.io 的精确镜像，不能用来混入企业私有 crate 或修改后的同名 crate。私有 registry 应作为独立 registry 配置。需要认证时使用 Cargo credential provider、构建机 secret store 或 `CARGO_REGISTRIES_<NAME>_TOKEN`，不要把 token 写入 `.cargo/config.toml`。

RsProxy 的当前 sparse 示例为：

```toml
[source.crates-io]
replace-with = "rsproxy-sparse"

[source.rsproxy-sparse]
registry = "sparse+https://rsproxy.cn/index/"

[net]
git-fetch-with-cli = true
retry = 3
```

本项目当前 `Cargo.lock` 没有 Git dependency；若未来增加 Git dependency，registry mirror 不会替代对应 Git 仓库，必须确保代理、企业 Git mirror 或固定 commit 可用。

## 6. 直接在内网构建

从固定 commit、未修改的 `Cargo.lock` 开始：

```bash
rustc --version
cargo --version
git status --short

cargo fetch --locked
cargo metadata --locked --offline >/dev/null

cargo fmt --all --check
cargo clippy --workspace --all-targets --all-features -- -D warnings
cargo test --workspace --all-features
cargo build --release --workspace --locked
```

`cargo metadata --offline` 用于确认依赖已经完整进入本地 cache；最终 build 仍使用 `--locked`，禁止 CI 临时运行 `cargo update`。生成发布清单：

```bash
sha256sum \
  target/release/thieving-eyesd \
  target/release/thieving-eyes-runner \
  target/release/eyes \
  /absolute/path/to/opencode \
  > SHA256SUMS
```

把 commit、Rust 版本、目标系统、`Cargo.lock` digest、OpenCode digest、Sandbox Agent digest 和测试结果一并作为 release provenance 保存。

## 7. Vendor/离线回退

代理或 registry 仍不稳定时，在可信联网构建机生成 vendor 目录：

```bash
cargo vendor --locked vendor > cargo-vendor-config.toml
```

随源码、`Cargo.lock` 和 `vendor/` 一起传入内网，再把 Cargo 输出的 source replacement 写入构建用户的 `$CARGO_HOME/config.toml`。其核心形式是：

```toml
[source.crates-io]
replace-with = "vendored-sources"

[source.vendored-sources]
directory = "/absolute/path/to/thieving-eyes/vendor"
```

随后执行：

```bash
cargo build --release --workspace --locked --offline
```

vendor 目录是 release 输入，应和其他第三方依赖一起进行许可证、漏洞与来源审计；默认不提交到本仓库。

## 8. Sandbox Agent 安装

`eyes init` 是唯一的自动安装入口；daemon 启动和任务执行不会下载 runtime。代理可直接访问固定 release URL 时：

```bash
eyes init \
  --opencode /absolute/path/to/opencode \
  --workspace-root /absolute/path/to/projects
eyes doctor
```

先把三个 release binary 安装到最终目录，再使用最终服务用户和最终 XDG/HOME 环境执行 `eyes init`。生成器会把当前 `eyes` 的同目录 `thieving-eyes-runner`、当前 OpenCode、credential 文件和绝对 workspace 路径写进配置；在 build tree 中生成后直接复制配置通常会留下错误路径。

若下载不稳定，先从受控内部 artifact store 取得固定 binary。使用默认 XDG 路径时，可以在运行 `eyes init` 前预置：

```bash
export XDG_DATA_HOME="/var/lib/thieving-eyes-data"
install -D -m 0500 sandbox-agent \
  "$XDG_DATA_HOME/thieving-eyes/runtimes/sandbox-agent/0.4.2/sandbox-agent"

eyes init \
  --opencode /absolute/path/to/opencode \
  --workspace-root /absolute/path/to/projects
```

`eyes init` 会发现已有文件并校验 digest，不再下载。若使用手工维护的 system config，则把 binary 放到：

```text
<runtime.cache_dir>/0.4.2/sandbox-agent
```

并配置：

```toml
[runtime]
cache_dir = "/var/cache/thieving-eyes/sandbox-agent"
download_if_missing = false
```

文件权限建议为 `0500`，owner 为服务用户。无论通过哪种路径取得，`eyes doctor` 都必须验证固定 digest；不能因为内网镜像可用而跳过校验。

## 9. 服务目录、凭据和 systemd

建议使用独立服务用户，并分离：

```text
/etc/thieving-eyes/config.toml
/opt/thieving-eyes/bin/
/var/lib/thieving-eyes/state.db
/var/cache/thieving-eyes/sandbox-agent/
/var/lib/thieving-eyes/secrets/
/run/thieving-eyes/daemon.sock
```

OpenCode credential/config 由 `[[local_runner.credential_files]]` 引用，以 `0600` 保存并只读挂载进每个 Attempt。不要把 token 写入 Source、Route、capacity probe 参数或 systemd `ExecStart`。

当前 daemon 处理 SIGINT 以执行受控关机，尚未单独处理 SIGTERM。systemd unit 应设置：

```ini
[Service]
User=thieving-eyes
Group=thieving-eyes
ExecStart=/opt/thieving-eyes/bin/thieving-eyesd --config /etc/thieving-eyes/config.toml
KillSignal=SIGINT
Restart=on-failure
UMask=0077
```

不要启用会禁止 bubblewrap namespace 的 `PrivateUsers` 或过严 `RestrictNamespaces`，除非已经用 `eyes doctor` 和真实任务证明兼容。配置变更后当前需要重启 daemon；尚未实现 SIGHUP 热加载。

## 10. 容量探针

一个 Source 对应一个 provider endpoint/account/credential scope。独立容量的账号必须拆成不同 Source，再由 Route 按顺序引用。

建议先以 `static`、并发上限 1 完成迁移，再切换 `command` monitor。command probe 必须是绝对路径、快速、无交互的可执行文件：

```toml
[[sources]]
id = "internal_account_a"
label = "internal-account-a"
concurrency_limit = 8
safety_reserve = 1

[sources.monitor]
kind = "command"
program = "/opt/thieving-eyes/bin/capacity-probe"
args = []
interval_seconds = 15
timeout_seconds = 3
max_age_seconds = 45
```

daemon 向 stdin 写入：

```json
{
  "protocol_version": 1,
  "source_id": "internal_account_a",
  "requested_at": "2026-07-23T08:00:00Z"
}
```

探针 stdout 只能包含一个不超过 64 KiB 的 JSON：

```json
{
  "protocol_version": 1,
  "observed_at": "2026-07-23T08:00:01Z",
  "health": "healthy",
  "usage": {
    "kind": "total_in_use",
    "count": 5
  }
}
```

- `total_in_use` 表示包括 thieving-eyes 在内的全局占用，daemon 使用 `max(active_leases, count)`；
- `external_in_use` 表示其他系统占用，daemon 使用 `active_leases + count`；
- 可用量为 `concurrency_limit - used - safety_reserve`；
- timeout、非零退出、非法 JSON、未来时间戳或过期 observation 一律 fail closed；
- `health=healthy` 必须携带 usage；查询事实不确定时返回 `health=unavailable`。

探针应查询权威网关、Prometheus、Redis/semaphore 或 provider 管理接口，不要发送模型请求进行探测。白天/夜间容量差异可以转化为策略保留量：

```text
effective_usage = observed_usage + hard_limit - allowed_limit
```

当前多个 Source 的 probe 在 scheduler refresh 中顺序执行，适合少量快速探针；大量账号上线前应改为有界并发刷新。

## 11. 验收顺序

1. `eyes doctor` 验证 config、Sandbox Agent、OpenCode digest 和 bubblewrap。
2. 用 `static` source、并发 1 执行无 workspace 的最小 prompt。
3. 执行只读 workspace 扫描，再单独验证 writable workspace。
4. 直接向 probe 写测试 request，验证 healthy、unavailable、timeout 和 stale response。
5. 切换 command monitor，验证队列在无容量时保持 queued、容量恢复后自动派发。
6. 验证取消、daemon 重启后的 uncertain、SQLite/WAL 权限和日志脱敏。
7. 最后再逐步提高 source concurrency limit。

当前 daemon 只监听本机 Unix socket。调用方与 daemon 不在同一机器时，应先实现并验收 HTTPS/mTLS、scoped authorization 和网络 API conformance；不要通过共享 SQLite、共享 UDS 文件或让客户端直接启动 runner 绕过控制面。

## 参考

- [Cargo source replacement](https://doc.rust-lang.org/cargo/reference/source-replacement.html)
- [Cargo registry index 与 sparse protocol](https://doc.rust-lang.org/cargo/reference/registry-index.html)
- [Cargo registry authentication](https://doc.rust-lang.org/stable/cargo/reference/registry-authentication.html)
- [rustup proxy](https://rust-lang.github.io/rustup/network-proxies.html)
- [rustup environment variables](https://rust-lang.github.io/rustup/devel/environment-variables.html)
- [RsProxy 当前配置](https://rsproxy.cn/)
