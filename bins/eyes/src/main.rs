use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};
use std::process::Stdio;

use anyhow::{Context, Result, bail};
use bytes::Bytes;
use clap::{Parser, Subcommand};
use http::{Method, Request, StatusCode};
use http_body_util::{BodyExt, Full};
use hyper::body::Incoming;
use hyper::client::conn::http1;
use hyper_util::rt::TokioIo;
use serde::de::DeserializeOwned;
use sha2::{Digest, Sha256};
use thieving_eyes_protocol::{
    ContentPart, ExecutionSelector, Input, Scheduling, SubmissionAccepted, SubmissionCreate,
    SubmissionResult, SubmissionState, SubmissionStatus, TaskMode, WorkspaceAccess, WorkspaceRef,
};
use thieving_eyes_service::config::{
    AgentBinaryConfig, CapacityMonitorConfig, Config, CredentialFile, DaemonConfig, Defaults,
    LocalRunnerConfig, NetworkMode, PolicyConfig, ProfileConfig, RouteConfig, RuntimeConfig,
    SourceConfig, WorkspaceRootConfig, default_config_path, default_data_dir, default_runtime_dir,
    default_state_dir,
};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::UnixStream;
use ulid::Ulid;

#[derive(Debug, Parser)]
#[command(version, about = "Thin client for the thieving-eyes execution daemon")]
struct Args {
    #[arg(long, global = true)]
    config: Option<PathBuf>,
    #[command(subcommand)]
    command: Command,
}

#[derive(Debug, Subcommand)]
enum Command {
    Init(InitArgs),
    Doctor,
    Submit(TaskArgs),
    Run(TaskArgs),
    Status { submission_id: String },
    Watch { submission_id: String },
    Result { submission_id: String },
    Cancel { submission_id: String },
}

#[derive(Debug, clap::Args)]
struct InitArgs {
    #[arg(long)]
    workspace_root: PathBuf,
    #[arg(long, default_value = "local")]
    root_id: String,
    #[arg(long)]
    opencode: Option<PathBuf>,
    #[arg(long)]
    codex: Option<PathBuf>,
    #[arg(long)]
    codex_acp: Option<PathBuf>,
    #[arg(long, default_value = "opencode")]
    adapter: String,
    #[arg(long, default_value = "")]
    model: String,
    #[arg(long)]
    force: bool,
}

#[derive(Debug, clap::Args)]
struct TaskArgs {
    prompt: String,
    #[arg(long)]
    workspace: Option<PathBuf>,
    #[arg(long)]
    write: bool,
    #[arg(long, default_value_t = 50)]
    priority: u8,
    #[arg(long)]
    model: Option<String>,
    #[arg(long)]
    adapter: Option<String>,
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();
    let config_path = args.config.map_or_else(default_config_path, Ok)?;
    match args.command {
        Command::Init(options) => init(&config_path, options).await,
        Command::Doctor => doctor(&config_path).await,
        Command::Submit(task) => {
            let config = Config::load(&config_path).await?;
            let accepted = submit(&config, task).await?;
            println!("{}", serde_json::to_string_pretty(&accepted)?);
            Ok(())
        }
        Command::Run(task) => {
            let config = Config::load(&config_path).await?;
            let accepted = submit(&config, task).await?;
            eprintln!("submitted {}", accepted.submission_id);
            watch(&config, &accepted.submission_id, true).await?;
            let result: SubmissionResult = get_json(
                &config,
                &format!("/v1/submissions/{}/result", accepted.submission_id),
            )
            .await?;
            println!("{}", serde_json::to_string_pretty(&result)?);
            if result.state != SubmissionState::Completed {
                bail!("submission finished as {:?}", result.state);
            }
            Ok(())
        }
        Command::Status { submission_id } => {
            let config = Config::load(&config_path).await?;
            let status: SubmissionStatus =
                get_json(&config, &format!("/v1/submissions/{submission_id}")).await?;
            println!("{}", serde_json::to_string_pretty(&status)?);
            Ok(())
        }
        Command::Watch { submission_id } => {
            let config = Config::load(&config_path).await?;
            watch(&config, &submission_id, false).await
        }
        Command::Result { submission_id } => {
            let config = Config::load(&config_path).await?;
            let result: SubmissionResult =
                get_json(&config, &format!("/v1/submissions/{submission_id}/result")).await?;
            println!("{}", serde_json::to_string_pretty(&result)?);
            Ok(())
        }
        Command::Cancel { submission_id } => {
            let config = Config::load(&config_path).await?;
            let (status, body) = request(
                &config,
                Method::POST,
                &format!("/v1/submissions/{submission_id}/cancel"),
                None,
                &[],
            )
            .await?;
            ensure_success(status, &body)?;
            println!("{}", String::from_utf8_lossy(&body));
            Ok(())
        }
    }
}

async fn init(config_path: &Path, options: InitArgs) -> Result<()> {
    let InitArgs {
        workspace_root,
        root_id,
        opencode,
        codex,
        codex_acp,
        adapter,
        model,
        force,
    } = options;
    if tokio::fs::try_exists(config_path).await? && !force {
        bail!(
            "configuration already exists at {}; pass --force to replace it",
            config_path.display()
        );
    }
    let config_dir = config_path.parent().context("config path has no parent")?;
    let workspace_root = tokio::fs::canonicalize(&workspace_root)
        .await
        .with_context(|| format!("resolve workspace root {}", workspace_root.display()))?;
    let (agent, agent_process) = match adapter.as_str() {
        "opencode" => {
            if codex.is_some() || codex_acp.is_some() {
                bail!("--codex and --codex-acp require --adapter codex");
            }
            let binary = match opencode {
                Some(path) => tokio::fs::canonicalize(path).await?,
                None => find_in_path("opencode")
                    .context("OpenCode was not found in PATH; pass --opencode")?,
            };
            (binary, None)
        }
        "codex" => {
            if opencode.is_some() {
                bail!("--opencode cannot be combined with --adapter codex");
            }
            let binary = match codex {
                Some(path) => tokio::fs::canonicalize(path).await?,
                None => {
                    find_in_path("codex").context("Codex was not found in PATH; pass --codex")?
                }
            };
            let process = match codex_acp {
                Some(path) => tokio::fs::canonicalize(path).await?,
                None => find_in_path("codex-acp")
                    .context("codex-acp was not found in PATH; pass --codex-acp")?,
            };
            (binary, Some(process))
        }
        _ => bail!("--adapter must be codex or opencode"),
    };
    let agent_sha256 = digest_file(&agent).await?;
    let agent_process_sha256 = match agent_process.as_deref() {
        Some(path) => Some(digest_file(path).await?),
        None => None,
    };
    let current = std::env::current_exe()?;
    let runner_binary = current
        .parent()
        .context("eyes executable has no parent directory")?
        .join("thieving-eyes-runner");
    let state_dir = default_state_dir()?;
    let data_dir = default_data_dir()?;
    let runtime_dir = default_runtime_dir()?;
    let home = home_dir()?;
    tokio::fs::create_dir_all(config_dir).await?;
    set_private_dir(config_dir).await?;
    let source_config_dir = config_dir.join(&adapter).join("default");
    tokio::fs::create_dir_all(&source_config_dir).await?;
    if let Some(source_config_root) = source_config_dir.parent() {
        set_private_dir(source_config_root).await?;
    }
    set_private_dir(&source_config_dir).await?;
    let mut credential_files = Vec::new();
    let imports = match adapter.as_str() {
        "opencode" => vec![
            (
                home.join(".local/share/opencode/auth.json"),
                source_config_dir.join("auth.json"),
                PathBuf::from(".local/share/opencode/auth.json"),
            ),
            (
                home.join(".config/opencode/opencode.jsonc"),
                source_config_dir.join("opencode.jsonc"),
                PathBuf::from(".config/opencode/opencode.jsonc"),
            ),
        ],
        "codex" => vec![
            (
                home.join(".codex/auth.json"),
                source_config_dir.join("auth.json"),
                PathBuf::from(".codex/auth.json"),
            ),
            (
                home.join(".codex/config.toml"),
                source_config_dir.join("config.toml"),
                PathBuf::from(".codex/config.toml"),
            ),
        ],
        _ => Vec::new(),
    };
    for (discovered, independent, sandbox) in imports {
        if !tokio::fs::try_exists(&independent).await? && tokio::fs::try_exists(&discovered).await?
        {
            tokio::fs::copy(&discovered, &independent)
                .await
                .with_context(|| {
                    format!(
                        "import {adapter} file {} into {}",
                        discovered.display(),
                        independent.display()
                    )
                })?;
            set_private(&independent).await?;
        }
        if tokio::fs::try_exists(&independent).await? {
            set_private(&independent).await?;
            credential_files.push(CredentialFile {
                source_id: "default".to_owned(),
                host_path: independent,
                sandbox_path: sandbox,
            });
        }
    }
    let agent_binary = AgentBinaryConfig {
        adapter: adapter.clone(),
        binary: agent.clone(),
        sha256: agent_sha256.clone(),
        agent_process_binary: agent_process,
        agent_process_sha256,
    };
    let route_id = format!("{adapter}_default");
    let config = Config {
        daemon: DaemonConfig {
            socket_path: runtime_dir.join("daemon.sock"),
            database_path: state_dir.join("state.db"),
            max_inline_output_bytes: 262_144,
        },
        runtime: RuntimeConfig {
            cache_dir: data_dir.join("runtimes/sandbox-agent"),
            download_if_missing: true,
        },
        local_runner: LocalRunnerConfig {
            runner_binary,
            bubblewrap_binary: PathBuf::from("/usr/bin/bwrap"),
            opencode_binary: None,
            opencode_sha256: None,
            agent_binaries: vec![agent_binary],
            credential_files,
            source_bindings: Vec::new(),
        },
        defaults: Defaults {
            profile_id: "local_coding".to_owned(),
            policy_id: "standard".to_owned(),
            route_id: route_id.clone(),
        },
        profiles: vec![ProfileConfig {
            id: "local_coding".to_owned(),
            version: "1".to_owned(),
            description: format!("Non-interactive {adapter} in required bubblewrap sandbox"),
            network: NetworkMode::Inherited,
        }],
        policies: vec![PolicyConfig {
            id: "standard".to_owned(),
            version: "1".to_owned(),
            description: "Single-attempt local execution".to_owned(),
            run_timeout_seconds: 3_600,
            idle_timeout_seconds: 900,
        }],
        sources: vec![SourceConfig {
            id: "default".to_owned(),
            label: "local-default".to_owned(),
            concurrency_limit: 1,
            safety_reserve: 0,
            monitor: CapacityMonitorConfig::Static,
        }],
        routes: vec![RouteConfig {
            id: route_id,
            adapter,
            model,
            source_ids: vec!["default".to_owned()],
            target_id: "local".to_owned(),
        }],
        workspace_roots: vec![WorkspaceRootConfig {
            id: root_id,
            path: workspace_root,
            allow_writable: true,
        }],
    };
    config.validate()?;
    let encoded = toml::to_string_pretty(&config)?;
    let temporary = config_dir.join(format!(".config-{}.tmp", Ulid::new()));
    let mut file = tokio::fs::OpenOptions::new()
        .create_new(true)
        .write(true)
        .open(&temporary)
        .await?;
    file.write_all(encoded.as_bytes()).await?;
    file.flush().await?;
    drop(file);
    set_private(&temporary).await?;
    if tokio::fs::try_exists(config_path).await? {
        let backup = config_dir.join(format!("config.toml.backup-{}", Ulid::new()));
        tokio::fs::rename(config_path, backup).await?;
    }
    tokio::fs::rename(temporary, config_path).await?;
    thieving_eyes_service::install_runtime(&config).await?;
    println!("initialized {}", config_path.display());
    println!("run `eyes doctor` before starting thieving-eyesd");
    Ok(())
}

async fn doctor(config_path: &Path) -> Result<()> {
    let config = Config::load(config_path).await?;
    check_private(config_path).await?;
    thieving_eyes_service::prepare(&config).await?;
    for mapping in &config.local_runner.credential_files {
        if !tokio::fs::try_exists(&mapping.host_path).await? {
            bail!(
                "credential mapping is missing: {}",
                mapping.host_path.display()
            );
        }
    }
    let adapters = config
        .routes
        .iter()
        .map(|route| route.adapter.as_str())
        .collect::<BTreeSet<_>>();
    let mut agent_versions = Vec::new();
    let mut agent_process_versions = Vec::new();
    for adapter in adapters {
        let binary = config
            .agent_binary(adapter)
            .with_context(|| format!("missing Agent binary for {adapter}"))?;
        if digest_file(&binary.binary).await? != binary.sha256 {
            bail!("{adapter} digest differs from the initialized configuration");
        }
        if let (Some(process), Some(expected)) = (
            binary.agent_process_binary.as_deref(),
            binary.agent_process_sha256.as_deref(),
        ) {
            if digest_file(process).await? != expected {
                bail!("{adapter} Agent process digest differs from the configuration");
            }
            let version = tokio::process::Command::new(process)
                .arg("--version")
                .stdout(Stdio::piped())
                .output()
                .await
                .with_context(|| format!("run {adapter} Agent process version check"))?;
            if !version.status.success() {
                bail!("{adapter} Agent process version check failed");
            }
            agent_process_versions.push((
                adapter.to_owned(),
                String::from_utf8_lossy(&version.stdout).trim().to_owned(),
            ));
        }
        let version = tokio::process::Command::new(&binary.binary)
            .arg("--version")
            .stdout(Stdio::piped())
            .output()
            .await
            .with_context(|| format!("run {adapter} version check"))?;
        if !version.status.success() {
            bail!("{adapter} version check failed");
        }
        agent_versions.push((
            adapter.to_owned(),
            String::from_utf8_lossy(&version.stdout).trim().to_owned(),
        ));
    }
    let bwrap = tokio::process::Command::new(&config.local_runner.bubblewrap_binary)
        .args([
            "--ro-bind",
            "/usr",
            "/usr",
            "--symlink",
            "usr/bin",
            "/bin",
            "--symlink",
            "usr/lib",
            "/lib",
            "--symlink",
            "usr/lib64",
            "/lib64",
            "--ro-bind",
            "/etc",
            "/etc",
            "--proc",
            "/proc",
            "--dev",
            "/dev",
            "--tmpfs",
            "/tmp",
            "--unshare-user",
            "--unshare-pid",
            "--unshare-ipc",
            "--unshare-uts",
            "--share-net",
            "--die-with-parent",
            "/usr/bin/true",
        ])
        .status()
        .await?;
    if !bwrap.success() {
        bail!("bubblewrap smoke test failed");
    }
    println!("configuration: ok");
    println!("Sandbox Agent 0.4.2: ok");
    for (adapter, version) in agent_versions {
        println!("{adapter} {version}: ok");
    }
    for (adapter, version) in agent_process_versions {
        println!("{adapter} Agent process {version}: ok");
    }
    println!("bubblewrap: ok");
    Ok(())
}

async fn submit(config: &Config, task: TaskArgs) -> Result<SubmissionAccepted> {
    if task.priority > 100 {
        bail!("priority must be between 0 and 100");
    }
    let route_ids = (task.model.is_some() || task.adapter.is_some()).then(|| {
        config
            .routes
            .iter()
            .filter(|route| {
                task.adapter
                    .as_deref()
                    .is_none_or(|adapter| route.adapter == adapter)
                    && task
                        .model
                        .as_deref()
                        .is_none_or(|model| route.model.is_empty() || route.model == model)
            })
            .map(|route| route.id.clone())
            .collect::<Vec<_>>()
    });
    if route_ids.as_ref().is_some_and(Vec::is_empty) {
        bail!(
            "no configured route supports adapter/model {}/{}",
            task.adapter.as_deref().unwrap_or("*"),
            task.model.as_deref().unwrap_or("*")
        );
    }
    let workspace = match task.workspace {
        Some(path) => Some(resolve_workspace(config, &path, task.write).await?),
        None => None,
    };
    let request_body = SubmissionCreate {
        client_reference: None,
        labels: BTreeMap::new(),
        mode: TaskMode::Task,
        input: Input {
            parts: vec![ContentPart::Text { text: task.prompt }],
        },
        workspace,
        output: None,
        agent: (task.adapter.is_some() || task.model.is_some()).then(|| {
            thieving_eyes_protocol::AgentSelector {
                profile: None,
                adapter: task.adapter,
                model: task.model,
                required_capabilities: Vec::new(),
                extensions: Vec::new(),
            }
        }),
        execution: Some(ExecutionSelector {
            route_ids,
            target_ids: Some(vec!["local".to_owned()]),
            locality: Some(thieving_eyes_protocol::Locality::LocalOnly),
            side_effects: Some(if task.write {
                thieving_eyes_protocol::SideEffects::SideEffecting
            } else {
                thieving_eyes_protocol::SideEffects::ReadOnly
            }),
        }),
        session: None,
        scheduling: Some(Scheduling {
            priority: Some(task.priority),
            not_before: None,
            start_deadline: None,
        }),
        limits: None,
        policy: None,
    };
    let body = serde_json::to_vec(&request_body)?;
    let (status, response) = request(
        config,
        Method::POST,
        "/v1/submissions",
        Some(body),
        &[("Idempotency-Key", format!("eyes-{}", Ulid::new()))],
    )
    .await?;
    ensure_success(status, &response)?;
    serde_json::from_slice(&response).context("decode submission acceptance")
}

async fn resolve_workspace(config: &Config, workspace: &Path, write: bool) -> Result<WorkspaceRef> {
    let canonical = tokio::fs::canonicalize(workspace).await?;
    let root = config
        .workspace_roots
        .iter()
        .filter_map(|root| {
            std::fs::canonicalize(&root.path)
                .ok()
                .filter(|path| canonical.starts_with(path))
                .map(|path| (root, path))
        })
        .max_by_key(|(_, path)| path.components().count())
        .context("workspace is outside all configured roots")?;
    if write && !root.0.allow_writable {
        bail!("selected workspace root does not allow writable access");
    }
    let relative = canonical.strip_prefix(&root.1)?;
    Ok(WorkspaceRef::Local {
        root_id: root.0.id.clone(),
        path: (!relative.as_os_str().is_empty()).then(|| relative.to_string_lossy().into_owned()),
        revision: None,
        access: if write {
            WorkspaceAccess::Writable
        } else {
            WorkspaceAccess::ReadOnly
        },
    })
}

async fn get_json<T: DeserializeOwned>(config: &Config, path: &str) -> Result<T> {
    let (status, body) = request(config, Method::GET, path, None, &[]).await?;
    ensure_success(status, &body)?;
    serde_json::from_slice(&body).with_context(|| format!("decode response from {path}"))
}

async fn request(
    config: &Config,
    method: Method,
    path: &str,
    body: Option<Vec<u8>>,
    headers: &[(&str, String)],
) -> Result<(StatusCode, Vec<u8>)> {
    let stream = UnixStream::connect(&config.daemon.socket_path)
        .await
        .with_context(|| format!("connect to {}", config.daemon.socket_path.display()))?;
    let io = TokioIo::new(stream);
    let (mut sender, connection) = http1::handshake(io)
        .await
        .context("HTTP handshake over UDS")?;
    let connection_task = tokio::spawn(connection);
    let mut builder = Request::builder()
        .method(method)
        .uri(path)
        .header("Host", "localhost");
    for (name, value) in headers {
        builder = builder.header(*name, value);
    }
    if body.is_some() {
        builder = builder.header("Content-Type", "application/json");
    }
    let request = builder.body(Full::new(Bytes::from(body.unwrap_or_default())))?;
    let response = sender
        .send_request(request)
        .await
        .context("send daemon request")?;
    let status = response.status();
    let body = response.into_body().collect().await?.to_bytes().to_vec();
    drop(sender);
    connection_task.abort();
    let _ = connection_task.await;
    Ok((status, body))
}

async fn watch(config: &Config, submission_id: &str, until_terminal: bool) -> Result<()> {
    let path = format!("/v1/submissions/{submission_id}/events");
    let stream = UnixStream::connect(&config.daemon.socket_path).await?;
    let io = TokioIo::new(stream);
    let (mut sender, connection) = http1::handshake(io).await?;
    let connection_task = tokio::spawn(connection);
    let request = Request::builder()
        .method(Method::GET)
        .uri(path)
        .header("Host", "localhost")
        .header("Accept", "text/event-stream")
        .body(Full::<Bytes>::new(Bytes::new()))?;
    let response = sender.send_request(request).await?;
    if !response.status().is_success() {
        let status = response.status();
        let body = response.into_body().collect().await?.to_bytes();
        ensure_success(status, &body)?;
        return Ok(());
    }
    let mut body: Incoming = response.into_body();
    let mut buffer = Vec::new();
    while let Some(frame) = body.frame().await {
        let frame = frame?;
        if let Some(data) = frame.data_ref() {
            buffer.extend_from_slice(data);
        }
        while let Some(index) = find_double_newline(&buffer) {
            let block = buffer.drain(..index + 2).collect::<Vec<_>>();
            if let Some(data) = parse_sse_data(&block) {
                let event: thieving_eyes_protocol::EventEnvelope = serde_json::from_slice(data)?;
                println!("{}", serde_json::to_string(&event)?);
                if until_terminal
                    && event.event_type == "submission.state_changed"
                    && event
                        .data
                        .get("to")
                        .and_then(serde_json::Value::as_str)
                        .is_some_and(|state| {
                            matches!(
                                state,
                                "completed" | "failed" | "cancelled" | "expired" | "uncertain"
                            )
                        })
                {
                    connection_task.abort();
                    let _ = connection_task.await;
                    return Ok(());
                }
            }
        }
    }
    connection_task.abort();
    let _ = connection_task.await;
    Ok(())
}

fn ensure_success(status: StatusCode, body: &[u8]) -> Result<()> {
    if status.is_success() {
        return Ok(());
    }
    bail!(
        "daemon returned {status}: {}",
        String::from_utf8_lossy(body)
    );
}

fn find_double_newline(value: &[u8]) -> Option<usize> {
    value.windows(2).position(|window| window == b"\n\n")
}

fn parse_sse_data(block: &[u8]) -> Option<&[u8]> {
    block
        .split(|byte| *byte == b'\n')
        .find_map(|line| line.strip_prefix(b"data: "))
}

fn find_in_path(binary: &str) -> Option<PathBuf> {
    std::env::var_os("PATH").and_then(|path| {
        std::env::split_paths(&path)
            .map(|directory| directory.join(binary))
            .find(|candidate| candidate.is_file())
    })
}

fn home_dir() -> Result<PathBuf> {
    std::env::var_os("HOME")
        .map(PathBuf::from)
        .context("HOME is required for Agent credential discovery")
}

async fn digest_file(path: &Path) -> Result<String> {
    let mut file = tokio::fs::File::open(path).await?;
    let mut digest = Sha256::new();
    let mut buffer = vec![0_u8; 1024 * 1024];
    loop {
        let read = file.read(&mut buffer).await?;
        if read == 0 {
            break;
        }
        digest.update(&buffer[..read]);
    }
    Ok(format!("{:x}", digest.finalize()))
}

#[cfg(unix)]
async fn set_private(path: &Path) -> Result<()> {
    use std::os::unix::fs::PermissionsExt;
    let mut permissions = tokio::fs::metadata(path).await?.permissions();
    permissions.set_mode(0o600);
    tokio::fs::set_permissions(path, permissions).await?;
    Ok(())
}

#[cfg(unix)]
async fn set_private_dir(path: &Path) -> Result<()> {
    use std::os::unix::fs::PermissionsExt;
    let mut permissions = tokio::fs::metadata(path).await?.permissions();
    permissions.set_mode(0o700);
    tokio::fs::set_permissions(path, permissions).await?;
    Ok(())
}

#[cfg(unix)]
async fn check_private(path: &Path) -> Result<()> {
    use std::os::unix::fs::PermissionsExt;
    let mode = tokio::fs::metadata(path).await?.permissions().mode() & 0o777;
    if mode & 0o077 != 0 {
        bail!("configuration must not be accessible by group or other users");
    }
    Ok(())
}
