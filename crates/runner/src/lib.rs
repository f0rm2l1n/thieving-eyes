//! Local runner supervisor and bubblewrap worker.

use std::os::fd::AsFd;
use std::path::{Component, Path, PathBuf};
use std::process::Stdio;
use std::time::Duration;

use anyhow::{Context, Result, bail};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use tempfile::TempDir;
use thieving_eyes_runtime_sandbox_agent::{RuntimeEvent, RuntimeRunRequest};
use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::process::{Child, ChildStdin, Command};
use tokio::sync::{mpsc, watch};
use tokio::time::{Instant, timeout};
use tracing::warn;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RunnerRequest {
    pub attempt_id: String,
    pub sandbox_agent_path: PathBuf,
    pub sandbox_agent_sha256: String,
    pub adapter: String,
    pub agent_path: PathBuf,
    pub agent_sha256: String,
    pub agent_process_path: Option<PathBuf>,
    pub agent_process_sha256: Option<String>,
    pub bubblewrap_path: PathBuf,
    pub workspace_path: Option<PathBuf>,
    pub workspace_writable: bool,
    pub network_enabled: bool,
    pub credential_mounts: Vec<CredentialMount>,
    pub inherit_proxy_environment: Vec<String>,
    pub prompt: String,
    pub model: Option<String>,
    pub run_timeout_seconds: u64,
    pub idle_timeout_seconds: u64,
    pub max_output_bytes: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CredentialMount {
    pub host_path: PathBuf,
    pub sandbox_path: PathBuf,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
enum ControlMessage {
    Start { request: Box<RunnerRequest> },
    Cancel,
}

struct SandboxPaths {
    home: PathBuf,
    adapter: String,
    agent_wrapper: Option<PathBuf>,
    runner: PathBuf,
    workspace: PathBuf,
    runtime: PathBuf,
    agent: PathBuf,
    agent_process: Option<PathBuf>,
}

/// Runs one attempt through the host supervisor and forwards normalized events.
///
/// # Errors
///
/// Returns an error when the runner cannot be started or controlled, emits an
/// invalid event, exits before a terminal event, or cannot be terminated.
pub async fn execute(
    runner_binary: &Path,
    request: RunnerRequest,
    mut cancel: watch::Receiver<bool>,
    events: mpsc::Sender<RuntimeEvent>,
) -> Result<()> {
    let mut child = Command::new(runner_binary)
        .arg("supervisor")
        .kill_on_drop(true)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::inherit())
        .spawn()
        .with_context(|| format!("start local runner {}", runner_binary.display()))?;
    let mut stdin = child.stdin.take().context("runner stdin unavailable")?;
    let stdout = child.stdout.take().context("runner stdout unavailable")?;
    write_message(
        &mut stdin,
        &ControlMessage::Start {
            request: Box::new(request),
        },
    )
    .await?;
    let mut lines = BufReader::new(stdout).lines();
    let cancel_deadline = tokio::time::sleep(Duration::from_secs(86_400));
    tokio::pin!(cancel_deadline);
    let mut cancelling = false;

    loop {
        tokio::select! {
            changed = cancel.changed() => {
                if changed.is_ok() && *cancel.borrow() {
                    write_message(&mut stdin, &ControlMessage::Cancel).await?;
                    cancelling = true;
                    cancel_deadline.as_mut().reset(Instant::now() + Duration::from_secs(10));
                }
            }
            () = &mut cancel_deadline, if cancelling => {
                terminate(&mut child).await;
                events.send(RuntimeEvent::Uncertain {
                    code: "runner_lost".to_owned(),
                    message: "runner cancellation exceeded its confirmation deadline".to_owned(),
                }).await.context("report uncertain forced runner cancellation")?;
                return Ok(());
            }
            line = lines.next_line() => {
                if let Some(line) = line.context("read runner event")? {
                    let event: RuntimeEvent = serde_json::from_str(&line).context("decode runner event")?;
                    let terminal = matches!(event, RuntimeEvent::Completed { .. } | RuntimeEvent::Cancelled | RuntimeEvent::Failed { .. } | RuntimeEvent::Uncertain { .. });
                    if terminal {
                        let status = timeout(Duration::from_secs(10), child.wait()).await.context("runner exit timeout")??;
                        if !status.success() {
                            warn!(%status, "runner exited non-zero after terminal event");
                            events
                                .send(RuntimeEvent::Uncertain {
                                    code: "runner_lost".to_owned(),
                                    message: format!(
                                        "runner exited with {status} after reporting a terminal event"
                                    ),
                                })
                                .await
                                .context("report abnormal runner exit")?;
                            return Ok(());
                        }
                        events.send(event).await.context("forward terminal runner event")?;
                        return Ok(());
                    }
                    events.send(event).await.context("forward runner event")?;
                } else {
                    let status = child.wait().await.context("wait for runner")?;
                    bail!("runner exited before a terminal event: {status}");
                }
            }
        }
    }
}

/// Runs the host-side runner supervisor process role.
///
/// # Errors
///
/// Returns an error for invalid control messages, binary verification or
/// sandbox setup failures, worker protocol errors, or a non-zero worker exit.
pub async fn supervisor() -> Result<()> {
    let mut input = BufReader::new(stdin_pipe()?).lines();
    let first = input
        .next_line()
        .await
        .context("read supervisor start message")?
        .context("supervisor input closed before start")?;
    let ControlMessage::Start { request } =
        serde_json::from_str(&first).context("decode runner request")?
    else {
        bail!("first supervisor message must be start");
    };
    let request = *request;
    if !matches!(request.adapter.as_str(), "codex" | "opencode") {
        bail!("unsupported Agent adapter {}", request.adapter);
    }
    verify_file(&request.sandbox_agent_path, &request.sandbox_agent_sha256).await?;
    verify_file(&request.agent_path, &request.agent_sha256).await?;
    match (
        request.agent_process_path.as_deref(),
        request.agent_process_sha256.as_deref(),
    ) {
        (Some(path), Some(digest)) => verify_file(path, digest).await?,
        (None, None) => {}
        _ => bail!("Agent process path and digest must be configured together"),
    }

    let scratch = tempfile::tempdir().context("create runner scratch directory")?;
    let mut child = spawn_bubblewrap_worker(&request, &scratch).await?;
    let mut worker_stdin = child.stdin.take().context("worker stdin unavailable")?;
    let worker_stdout = child.stdout.take().context("worker stdout unavailable")?;
    write_message(
        &mut worker_stdin,
        &ControlMessage::Start {
            request: Box::new(RunnerRequest {
                attempt_id: request.attempt_id,
                sandbox_agent_path: PathBuf::from("/opt/thieving-eyes/bin/sandbox-agent"),
                sandbox_agent_sha256: request.sandbox_agent_sha256,
                adapter: request.adapter.clone(),
                agent_path: PathBuf::from(format!("/opt/thieving-eyes/bin/{}", request.adapter)),
                agent_sha256: request.agent_sha256,
                agent_process_path: request.agent_process_path.map(|_| {
                    PathBuf::from(format!("/opt/thieving-eyes/bin/{}-acp", request.adapter))
                }),
                agent_process_sha256: request.agent_process_sha256,
                bubblewrap_path: PathBuf::new(),
                workspace_path: Some(PathBuf::from("/workspace")),
                workspace_writable: request.workspace_writable,
                network_enabled: request.network_enabled,
                credential_mounts: Vec::new(),
                inherit_proxy_environment: Vec::new(),
                prompt: request.prompt,
                model: request.model,
                run_timeout_seconds: request.run_timeout_seconds,
                idle_timeout_seconds: request.idle_timeout_seconds,
                max_output_bytes: request.max_output_bytes,
            }),
        },
    )
    .await?;

    let mut worker_lines = BufReader::new(worker_stdout).lines();
    let mut output = stdout_pipe()?;
    loop {
        tokio::select! {
            line = input.next_line() => {
                if let Some(line) = line.context("read supervisor control")? {
                    let control: ControlMessage = serde_json::from_str(&line).context("decode supervisor control")?;
                    if matches!(control, ControlMessage::Cancel) {
                        write_message(&mut worker_stdin, &ControlMessage::Cancel).await?;
                    }
                }
            }
            line = worker_lines.next_line() => {
                match line.context("read worker event")? {
                    Some(line) => {
                        output.write_all(line.as_bytes()).await?;
                        output.write_all(b"\n").await?;
                        output.flush().await?;
                    }
                    None => break,
                }
            }
        }
    }
    let status = child.wait().await.context("wait for sandbox worker")?;
    if !status.success() {
        bail!("sandbox worker exited with {status}");
    }
    Ok(())
}

/// Runs the sandbox-side runner worker process role.
///
/// # Errors
///
/// Returns an error for malformed control input, runtime failure without a
/// terminal event, or failure to encode and forward runtime events.
pub async fn worker() -> Result<()> {
    let mut input = BufReader::new(stdin_pipe()?).lines();
    let first = input
        .next_line()
        .await
        .context("read worker request")?
        .context("worker input closed before start")?;
    let ControlMessage::Start { request } =
        serde_json::from_str(&first).context("decode worker request")?
    else {
        bail!("first worker message must be start");
    };
    let request = *request;
    let (cancel_tx, cancel_rx) = watch::channel(false);
    let (event_tx, mut event_rx) = mpsc::channel(256);
    let runtime_request = RuntimeRunRequest {
        sandbox_agent_path: request.sandbox_agent_path,
        adapter: request.adapter,
        cwd: PathBuf::from("/workspace"),
        prompt: request.prompt,
        model: request.model,
        run_timeout_seconds: request.run_timeout_seconds,
        idle_timeout_seconds: request.idle_timeout_seconds,
        max_output_bytes: request.max_output_bytes,
    };
    let mut runtime_task = tokio::spawn(async move {
        thieving_eyes_runtime_sandbox_agent::run(runtime_request, cancel_rx, event_tx).await
    });
    let mut output = stdout_pipe()?;
    loop {
        tokio::select! {
            line = input.next_line() => {
                if let Some(line) = line.context("read worker control")? {
                    let control: ControlMessage = serde_json::from_str(&line).context("decode worker control")?;
                    if matches!(control, ControlMessage::Cancel) {
                        cancel_tx.send(true).context("signal runtime cancellation")?;
                    }
                }
            }
            Some(event) = event_rx.recv() => {
                let terminal = matches!(event, RuntimeEvent::Completed { .. } | RuntimeEvent::Cancelled | RuntimeEvent::Failed { .. } | RuntimeEvent::Uncertain { .. });
                let encoded = serde_json::to_vec(&event).context("encode runtime event")?;
                output.write_all(&encoded).await?;
                output.write_all(b"\n").await?;
                output.flush().await?;
                if terminal {
                    let _ = runtime_task.await;
                    return Ok(());
                }
            }
            result = &mut runtime_task => {
                result.context("join runtime task")??;
                while let Some(event) = event_rx.recv().await {
                    let terminal = matches!(event, RuntimeEvent::Completed { .. } | RuntimeEvent::Cancelled | RuntimeEvent::Failed { .. } | RuntimeEvent::Uncertain { .. });
                    let encoded = serde_json::to_vec(&event).context("encode final runtime event")?;
                    output.write_all(&encoded).await?;
                    output.write_all(b"\n").await?;
                    output.flush().await?;
                    if terminal {
                        return Ok(());
                    }
                }
                bail!("runtime ended without a terminal event");
            }
        }
    }
}

fn stdin_pipe() -> Result<tokio::net::unix::pipe::Receiver> {
    let fd = std::io::stdin()
        .as_fd()
        .try_clone_to_owned()
        .context("duplicate runner stdin")?;
    tokio::net::unix::pipe::Receiver::from_owned_fd(fd).context("open async runner stdin")
}

fn stdout_pipe() -> Result<tokio::net::unix::pipe::Sender> {
    let fd = std::io::stdout()
        .as_fd()
        .try_clone_to_owned()
        .context("duplicate runner stdout")?;
    tokio::net::unix::pipe::Sender::from_owned_fd(fd).context("open async runner stdout")
}

async fn spawn_bubblewrap_worker(request: &RunnerRequest, scratch: &TempDir) -> Result<Child> {
    let paths = prepare_sandbox_paths(request, scratch).await?;
    let mut command = Command::new(&request.bubblewrap_path);
    command.env_clear();
    if request.network_enabled {
        for name in &request.inherit_proxy_environment {
            if !is_allowed_proxy_environment(name) {
                bail!("runner rejected inherited environment variable {name}");
            }
            if let Some(value) = std::env::var_os(name) {
                command.env(name, value);
            }
        }
    }
    configure_base_bubblewrap(&mut command, &paths, request.network_enabled);

    if request.workspace_writable {
        command.arg("--bind");
    } else {
        command.arg("--ro-bind");
    }
    command.arg(&paths.workspace).arg("/workspace");

    for mount in &request.credential_mounts {
        let host = tokio::fs::canonicalize(&mount.host_path)
            .await
            .with_context(|| {
                format!(
                    "canonicalize credential mapping {}",
                    mount.host_path.display()
                )
            })?;
        command
            .arg("--ro-bind")
            .arg(host)
            .arg(Path::new("/home/agent").join(&mount.sandbox_path));
    }

    if request.adapter == "codex" {
        command
            .arg("--setenv")
            .arg("CODEX_PATH")
            .arg("/opt/thieving-eyes/bin/codex")
            .arg("--setenv")
            .arg("CODEX_HOME")
            .arg("/home/agent/.codex");
    }
    command
        .arg("--setenv")
        .arg("HOME")
        .arg("/home/agent")
        .arg("--setenv")
        .arg("XDG_CONFIG_HOME")
        .arg("/home/agent/.config")
        .arg("--setenv")
        .arg("XDG_DATA_HOME")
        .arg("/home/agent/.local/share")
        .arg("--setenv")
        .arg("XDG_STATE_HOME")
        .arg("/home/agent/.local/state")
        .arg("--setenv")
        .arg("PATH")
        .arg("/opt/thieving-eyes/bin:/usr/bin:/bin")
        .arg("--chdir")
        .arg("/workspace")
        .arg("/opt/thieving-eyes/bin/thieving-eyes-runner")
        .arg("worker")
        .kill_on_drop(true)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::inherit());
    command.spawn().context("start bubblewrap worker")
}

fn is_allowed_proxy_environment(name: &str) -> bool {
    matches!(
        name,
        "HTTP_PROXY"
            | "HTTPS_PROXY"
            | "ALL_PROXY"
            | "NO_PROXY"
            | "http_proxy"
            | "https_proxy"
            | "all_proxy"
            | "no_proxy"
    )
}

async fn prepare_sandbox_paths(request: &RunnerRequest, scratch: &TempDir) -> Result<SandboxPaths> {
    let home = scratch.path().join("home");
    tokio::fs::create_dir_all(&home)
        .await
        .context("create sandbox HOME")?;
    let agent_wrapper = if request.adapter == "opencode" {
        let wrapper = scratch.path().join("opencode");
        tokio::fs::write(
            &wrapper,
            b"#!/bin/sh\nexec /opt/thieving-eyes/bin/opencode-real --pure \"$@\"\n",
        )
        .await
        .context("write OpenCode shim")?;
        set_executable(&wrapper).await?;
        Some(wrapper)
    } else {
        None
    };

    for mount in &request.credential_mounts {
        validate_sandbox_relative(&mount.sandbox_path)?;
        let destination = home.join(&mount.sandbox_path);
        if let Some(parent) = destination.parent() {
            tokio::fs::create_dir_all(parent).await?;
        }
        if mount.host_path.is_dir() {
            tokio::fs::create_dir_all(&destination).await?;
        } else {
            tokio::fs::write(&destination, []).await?;
        }
    }

    let workspace = if let Some(path) = &request.workspace_path {
        let canonical = tokio::fs::canonicalize(path)
            .await
            .with_context(|| format!("canonicalize workspace {}", path.display()))?;
        if canonical != *path {
            bail!(
                "workspace identity changed after submission acceptance: {}",
                path.display()
            );
        }
        canonical
    } else {
        let path = scratch.path().join("workspace");
        tokio::fs::create_dir_all(&path).await?;
        path
    };
    Ok(SandboxPaths {
        home,
        adapter: request.adapter.clone(),
        agent_wrapper,
        runner: std::env::current_exe().context("resolve runner binary")?,
        workspace,
        runtime: tokio::fs::canonicalize(&request.sandbox_agent_path).await?,
        agent: tokio::fs::canonicalize(&request.agent_path).await?,
        agent_process: match request.agent_process_path.as_deref() {
            Some(path) => Some(tokio::fs::canonicalize(path).await?),
            None => None,
        },
    })
}

fn configure_base_bubblewrap(command: &mut Command, paths: &SandboxPaths, network_enabled: bool) {
    command
        .arg("--die-with-parent")
        .arg("--new-session")
        .arg("--unshare-all");
    if network_enabled {
        command.arg("--share-net");
    }
    command
        .arg("--ro-bind")
        .arg("/usr")
        .arg("/usr")
        .arg("--symlink")
        .arg("usr/bin")
        .arg("/bin")
        .arg("--symlink")
        .arg("usr/sbin")
        .arg("/sbin")
        .arg("--symlink")
        .arg("usr/lib")
        .arg("/lib")
        .arg("--symlink")
        .arg("usr/lib64")
        .arg("/lib64")
        .arg("--ro-bind")
        .arg("/etc")
        .arg("/etc")
        .arg("--proc")
        .arg("/proc")
        .arg("--dev")
        .arg("/dev")
        .arg("--tmpfs")
        .arg("/tmp")
        .arg("--dir")
        .arg("/opt")
        .arg("--dir")
        .arg("/opt/thieving-eyes")
        .arg("--dir")
        .arg("/opt/thieving-eyes/bin")
        .arg("--ro-bind")
        .arg(&paths.runner)
        .arg("/opt/thieving-eyes/bin/thieving-eyes-runner")
        .arg("--ro-bind")
        .arg(&paths.runtime)
        .arg("/opt/thieving-eyes/bin/sandbox-agent");
    if paths.adapter == "opencode" {
        if let Some(wrapper) = &paths.agent_wrapper {
            command
                .arg("--ro-bind")
                .arg(&paths.agent)
                .arg("/opt/thieving-eyes/bin/opencode-real")
                .arg("--ro-bind")
                .arg(wrapper)
                .arg("/opt/thieving-eyes/bin/opencode");
        }
    } else {
        command
            .arg("--ro-bind")
            .arg(&paths.agent)
            .arg(Path::new("/opt/thieving-eyes/bin").join(&paths.adapter));
    }
    if let Some(agent_process) = &paths.agent_process {
        command
            .arg("--ro-bind")
            .arg(agent_process)
            .arg(Path::new("/opt/thieving-eyes/bin").join(format!("{}-acp", paths.adapter)));
    }
    command.arg("--bind").arg(&paths.home).arg("/home/agent");
}

async fn write_message(stdin: &mut ChildStdin, message: &ControlMessage) -> Result<()> {
    let encoded = serde_json::to_vec(message).context("encode runner control message")?;
    stdin.write_all(&encoded).await?;
    stdin.write_all(b"\n").await?;
    stdin.flush().await?;
    Ok(())
}

async fn verify_file(path: &Path, expected: &str) -> Result<()> {
    let mut file = tokio::fs::File::open(path)
        .await
        .with_context(|| format!("open {}", path.display()))?;
    let mut digest = Sha256::new();
    let mut buffer = vec![0_u8; 1024 * 1024];
    loop {
        let read = file
            .read(&mut buffer)
            .await
            .with_context(|| format!("read {}", path.display()))?;
        if read == 0 {
            break;
        }
        digest.update(&buffer[..read]);
    }
    let actual = format!("{:x}", digest.finalize());
    if actual != expected {
        bail!("digest mismatch for {}", path.display());
    }
    Ok(())
}

fn validate_sandbox_relative(path: &Path) -> Result<()> {
    if path.is_absolute()
        || path.components().any(|component| {
            matches!(
                component,
                Component::ParentDir | Component::RootDir | Component::Prefix(_)
            )
        })
    {
        bail!("credential destination escapes sandbox HOME");
    }
    Ok(())
}

#[cfg(unix)]
async fn set_executable(path: &Path) -> Result<()> {
    use std::os::unix::fs::PermissionsExt;
    let mut permissions = tokio::fs::metadata(path).await?.permissions();
    permissions.set_mode(0o500);
    tokio::fs::set_permissions(path, permissions).await?;
    Ok(())
}

pub async fn terminate(child: &mut Child) {
    if let Err(error) = child.start_kill() {
        warn!(%error, "failed to terminate runner");
    }
    let _ = timeout(Duration::from_secs(5), child.wait()).await;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sandbox_mount_destination_is_relative() {
        assert!(validate_sandbox_relative(Path::new(".local/share/opencode/auth.json")).is_ok());
        assert!(validate_sandbox_relative(Path::new("../../etc/passwd")).is_err());
    }

    #[test]
    fn sandbox_only_inherits_proxy_environment() {
        assert!(is_allowed_proxy_environment("HTTP_PROXY"));
        assert!(is_allowed_proxy_environment("no_proxy"));
        assert!(!is_allowed_proxy_environment("PATH"));
        assert!(!is_allowed_proxy_environment("DEEPSEEK_API_KEY"));
    }
}
