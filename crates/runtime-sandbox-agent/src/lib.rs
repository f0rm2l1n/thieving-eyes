//! Narrow Sandbox Agent HTTP/ACP runtime binding.

use std::path::{Path, PathBuf};
use std::time::Duration;

use anyhow::{Context, Result, anyhow, bail};
use eventsource_stream::Eventsource;
use futures::StreamExt;
use reqwest::{Client, StatusCode};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use tokio::io::AsyncWriteExt;
use tokio::process::{Child, Command};
use tokio::sync::{mpsc, watch};
use tokio::time::{Instant, sleep, timeout};
use tracing::{debug, warn};
use ulid::Ulid;

const MAX_RUNTIME_DOWNLOAD_BYTES: u64 = 64 * 1024 * 1024;

struct ActiveSession {
    acp_url: String,
    session_id: String,
    sse_rx: mpsc::Receiver<Value>,
    sse_task: tokio::task::JoinHandle<Result<()>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RuntimeRunRequest {
    pub sandbox_agent_path: PathBuf,
    pub cwd: PathBuf,
    pub prompt: String,
    pub model: Option<String>,
    pub run_timeout_seconds: u64,
    pub idle_timeout_seconds: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum RuntimeEvent {
    Started { agent_version: Option<String> },
    MessageDelta { text: String },
    Tool { title: String, status: String },
    Plan { data: Value },
    Usage { data: Value },
    Completed { output: String },
    Cancelled,
    Failed { code: String, message: String },
}

/// Installs the pinned Sandbox Agent binary after verifying its digest.
///
/// # Errors
///
/// Returns an error when the existing binary has the wrong digest, downloading
/// is disabled or fails, the response exceeds the size bound, or installation
/// cannot be completed atomically.
pub async fn ensure_binary(
    destination: &Path,
    url: &str,
    expected_sha256: &str,
    download_if_missing: bool,
) -> Result<()> {
    if destination.exists() {
        verify_sha256(destination, expected_sha256).await?;
        return Ok(());
    }
    if !download_if_missing {
        bail!("runtime binary is missing and automatic download is disabled");
    }
    let parent = destination
        .parent()
        .context("runtime destination has no parent")?;
    tokio::fs::create_dir_all(parent)
        .await
        .with_context(|| format!("create runtime cache {}", parent.display()))?;
    let partial = parent.join(format!(".sandbox-agent-{}.partial", Ulid::new()));
    let response = Client::new()
        .get(url)
        .send()
        .await
        .context("download Sandbox Agent")?
        .error_for_status()
        .context("Sandbox Agent download returned an error")?;
    if response
        .content_length()
        .is_some_and(|size| size > MAX_RUNTIME_DOWNLOAD_BYTES)
    {
        bail!("Sandbox Agent download exceeds size limit");
    }
    let mut file = tokio::fs::OpenOptions::new()
        .create_new(true)
        .write(true)
        .open(&partial)
        .await
        .context("create partial runtime download")?;
    let mut stream = response.bytes_stream();
    let mut total = 0_u64;
    while let Some(chunk) = stream.next().await {
        let chunk = chunk.context("read Sandbox Agent download")?;
        total = total
            .checked_add(u64::try_from(chunk.len()).context("download length overflow")?)
            .context("download length overflow")?;
        if total > MAX_RUNTIME_DOWNLOAD_BYTES {
            let _ = tokio::fs::remove_file(&partial).await;
            bail!("Sandbox Agent download exceeds size limit");
        }
        file.write_all(&chunk)
            .await
            .context("write runtime download")?;
    }
    file.flush().await.context("flush runtime download")?;
    drop(file);
    if let Err(error) = verify_sha256(&partial, expected_sha256).await {
        let _ = tokio::fs::remove_file(&partial).await;
        return Err(error);
    }
    set_executable(&partial).await?;
    tokio::fs::rename(&partial, destination)
        .await
        .context("atomically install Sandbox Agent")?;
    Ok(())
}

/// Verifies a file against a lowercase hexadecimal SHA-256 digest.
///
/// # Errors
///
/// Returns an error when the file cannot be read or its digest differs.
pub async fn verify_sha256(path: &Path, expected: &str) -> Result<()> {
    let bytes = tokio::fs::read(path)
        .await
        .with_context(|| format!("read {} for digest verification", path.display()))?;
    let actual = format!("{:x}", Sha256::digest(bytes));
    if actual != expected {
        bail!(
            "digest mismatch for {}: expected {expected}, got {actual}",
            path.display()
        );
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

/// Executes one prompt through a dedicated Sandbox Agent process.
///
/// # Errors
///
/// Returns an error when the runtime cannot start, its HTTP/ACP binding fails,
/// it exits unexpectedly, or the event sink closes during execution.
pub async fn run(
    request: RuntimeRunRequest,
    mut cancel: watch::Receiver<bool>,
    events: mpsc::Sender<RuntimeEvent>,
) -> Result<()> {
    let port = reserve_loopback_port().await?;
    let token = format!("runtime_{}{}", Ulid::new(), Ulid::new());
    let base_url = format!("http://127.0.0.1:{port}");
    let mut child = spawn_server(&request.sandbox_agent_path, port, &token)?;
    let client = Client::builder()
        .timeout(Duration::from_secs(30))
        .build()
        .context("build runtime HTTP client")?;

    let inner = run_inner(&request, &client, &base_url, &token, &mut cancel, &events);
    tokio::pin!(inner);
    let mut child_poll = tokio::time::interval(Duration::from_millis(100));
    child_poll.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    let mut child_exited = false;
    let result = loop {
        tokio::select! {
            result = &mut inner => break result,
            _ = child_poll.tick() => {
                match child.try_wait() {
                    Ok(Some(status)) => {
                        child_exited = true;
                        break Err(anyhow!("Sandbox Agent exited unexpectedly with {status}"));
                    }
                    Ok(None) => {}
                    Err(error) => {
                        child_exited = true;
                        break Err(anyhow!("failed to inspect Sandbox Agent: {error}"));
                    }
                }
            }
        }
    };
    if !child_exited {
        stop_child(&mut child).await;
    }
    if let Err(error) = result {
        let _ = events
            .send(RuntimeEvent::Failed {
                code: classify_error(&error).to_owned(),
                message: error.to_string(),
            })
            .await;
        return Err(error);
    }
    Ok(())
}

async fn run_inner(
    request: &RuntimeRunRequest,
    client: &Client,
    base_url: &str,
    token: &str,
    cancel: &mut watch::Receiver<bool>,
    events: &mpsc::Sender<RuntimeEvent>,
) -> Result<()> {
    let session = prepare_session(request, client, base_url, token, events).await?;
    drive_prompt(request, client, token, cancel, events, session).await
}

async fn prepare_session(
    request: &RuntimeRunRequest,
    client: &Client,
    base_url: &str,
    token: &str,
    events: &mpsc::Sender<RuntimeEvent>,
) -> Result<ActiveSession> {
    wait_for_health(client, base_url, token).await?;
    let agents: Value = authorized(
        client.get(format!("{base_url}/v1/agents?config=true")),
        token,
    )
    .send()
    .await
    .context("query Sandbox Agent agents")?
    .error_for_status()
    .context("Sandbox Agent agent discovery failed")?
    .json()
    .await
    .context("decode Sandbox Agent agent discovery")?;
    let agent_version = discover_opencode_version(&agents)?;
    events
        .send(RuntimeEvent::Started {
            agent_version: agent_version.clone(),
        })
        .await
        .context("report runtime start")?;

    let server_id = format!("eyes-{}", Ulid::new());
    let acp_url = format!("{base_url}/v1/acp/{server_id}");
    rpc(
        client,
        &format!("{acp_url}?agent=opencode"),
        token,
        json!({
            "jsonrpc": "2.0",
            "id": 1,
            "method": "initialize",
            "params": {"protocolVersion": 1, "clientCapabilities": {}}
        }),
    )
    .await?;

    let (sse_tx, sse_rx) = mpsc::channel::<Value>(256);
    let sse_client = client.clone();
    let sse_url = acp_url.clone();
    let sse_token = token.to_owned();
    let sse_task =
        tokio::spawn(async move { stream_sse(sse_client, sse_url, sse_token, sse_tx).await });

    let session_response = rpc(
        client,
        &acp_url,
        token,
        json!({
            "jsonrpc": "2.0",
            "id": 2,
            "method": "session/new",
            "params": {"cwd": request.cwd, "mcpServers": []}
        }),
    )
    .await?;
    let session_id = session_response
        .pointer("/result/sessionId")
        .and_then(Value::as_str)
        .context("ACP session/new omitted sessionId")?
        .to_owned();

    if let Some(model) = request.model.as_deref().filter(|model| !model.is_empty()) {
        let _ = rpc(
            client,
            &acp_url,
            token,
            json!({
                "jsonrpc": "2.0", "id": 3, "method": "session/set_config_option",
                "params": {"sessionId": session_id, "configId": "model", "value": model}
            }),
        )
        .await;
    }

    Ok(ActiveSession {
        acp_url,
        session_id,
        sse_rx,
        sse_task,
    })
}

async fn drive_prompt(
    request: &RuntimeRunRequest,
    client: &Client,
    token: &str,
    cancel: &mut watch::Receiver<bool>,
    events: &mpsc::Sender<RuntimeEvent>,
    mut session: ActiveSession,
) -> Result<()> {
    let prompt_client = client.clone();
    let prompt_url = session.acp_url.clone();
    let prompt_token = token.to_owned();
    let prompt_session = session.session_id.clone();
    let prompt_text = request.prompt.clone();
    let mut prompt_task = tokio::spawn(async move {
        rpc(
            &prompt_client,
            &prompt_url,
            &prompt_token,
            json!({
                "jsonrpc": "2.0", "id": 4, "method": "session/prompt",
                "params": {"sessionId": prompt_session, "prompt": [{"type": "text", "text": prompt_text}]}
            }),
        )
        .await
    });
    let mut prompt_finished = false;

    let run_deadline = sleep(Duration::from_secs(request.run_timeout_seconds.max(1)));
    tokio::pin!(run_deadline);
    let idle = sleep(Duration::from_secs(request.idle_timeout_seconds.max(1)));
    tokio::pin!(idle);
    let mut output = String::new();
    let completion = loop {
        tokio::select! {
            biased;
            changed = cancel.changed() => {
                if changed.is_ok() && *cancel.borrow() {
                    let _ = cancel_session(client, &session.acp_url, token, &session.session_id, 5).await;
                    break RuntimeEvent::Cancelled;
                }
            }
            () = &mut run_deadline => {
                let _ = cancel_session(client, &session.acp_url, token, &session.session_id, 6).await;
                break RuntimeEvent::Failed { code: "timeout".to_owned(), message: "run timeout elapsed".to_owned() };
            }
            () = &mut idle => {
                let _ = cancel_session(client, &session.acp_url, token, &session.session_id, 7).await;
                break RuntimeEvent::Failed { code: "timeout".to_owned(), message: "idle timeout elapsed".to_owned() };
            }
            Some(value) = session.sse_rx.recv() => {
                idle.as_mut().reset(Instant::now() + Duration::from_secs(request.idle_timeout_seconds.max(1)));
                handle_inbound(client, token, events, &session.acp_url, &value, &mut output).await?;
                if is_business_question(&value) {
                    break RuntimeEvent::Failed {
                        code: "interaction_required".to_owned(),
                        message: "agent requested interactive business input".to_owned(),
                    };
                }
            }
            response = &mut prompt_task => {
                prompt_finished = true;
                let response = response.context("join ACP prompt task")??;
                break completion_from_response(&response, output);
            }
        }
    };

    if !prompt_finished {
        prompt_task.abort();
        let _ = prompt_task.await;
    }
    session.sse_task.abort();
    let _ = session.sse_task.await;
    let _ = authorized(client.delete(&session.acp_url), token)
        .send()
        .await;
    events
        .send(completion)
        .await
        .context("report runtime completion")?;
    Ok(())
}

async fn cancel_session(
    client: &Client,
    url: &str,
    token: &str,
    session_id: &str,
    request_id: u64,
) -> Result<Value> {
    rpc(
        client,
        url,
        token,
        json!({
            "jsonrpc": "2.0",
            "id": request_id,
            "method": "session/cancel",
            "params": {"sessionId": session_id}
        }),
    )
    .await
}

async fn handle_inbound(
    client: &Client,
    token: &str,
    events: &mpsc::Sender<RuntimeEvent>,
    acp_url: &str,
    value: &Value,
    output: &mut String,
) -> Result<()> {
    if let Some(event) = normalize_event(value) {
        if let RuntimeEvent::MessageDelta { text } = &event {
            output.push_str(text);
        }
        events.send(event).await.context("report runtime event")?;
    }
    if is_permission_request(value) {
        respond_permission(client, acp_url, token, value).await?;
    }
    Ok(())
}

fn completion_from_response(response: &Value, output: String) -> RuntimeEvent {
    let stop_reason = response
        .pointer("/result/stopReason")
        .and_then(Value::as_str)
        .unwrap_or("unknown");
    if matches!(stop_reason, "end_turn" | "completed" | "stop") {
        RuntimeEvent::Completed { output }
    } else {
        RuntimeEvent::Failed {
            code: "provider_error".to_owned(),
            message: format!("agent stopped with reason {stop_reason}"),
        }
    }
}

async fn wait_for_health(client: &Client, base_url: &str, token: &str) -> Result<()> {
    timeout(Duration::from_secs(20), async {
        loop {
            match authorized(client.get(format!("{base_url}/v1/health")), token)
                .send()
                .await
            {
                Ok(response) if response.status().is_success() => return Ok(()),
                Ok(response) => debug!(status = %response.status(), "runtime not ready"),
                Err(error) => debug!(%error, "runtime health check failed"),
            }
            sleep(Duration::from_millis(150)).await;
        }
    })
    .await
    .context("Sandbox Agent health timeout")?
}

async fn rpc(client: &Client, url: &str, token: &str, payload: Value) -> Result<Value> {
    let response = authorized(client.post(url).json(&payload), token)
        .send()
        .await
        .with_context(|| format!("send ACP request to {url}"))?;
    if !response.status().is_success() {
        let status = response.status();
        let body = response.text().await.unwrap_or_default();
        bail!("ACP request failed with {status}: {}", truncate(&body, 512));
    }
    let value: Value = response.json().await.context("decode ACP response")?;
    if let Some(error) = value.get("error") {
        bail!("ACP error: {}", truncate(&error.to_string(), 512));
    }
    Ok(value)
}

async fn stream_sse(
    client: Client,
    url: String,
    token: String,
    tx: mpsc::Sender<Value>,
) -> Result<()> {
    let response = authorized(client.get(&url), &token)
        .send()
        .await
        .context("subscribe to ACP SSE")?;
    if response.status() != StatusCode::OK {
        bail!("ACP SSE returned {}", response.status());
    }
    let mut stream = response.bytes_stream().eventsource();
    while let Some(event) = stream.next().await {
        let event = event.context("read ACP SSE event")?;
        if event.data.is_empty() {
            continue;
        }
        let value: Value = serde_json::from_str(&event.data).context("decode ACP SSE payload")?;
        if tx.send(value).await.is_err() {
            break;
        }
    }
    Ok(())
}

fn normalize_event(value: &Value) -> Option<RuntimeEvent> {
    let params = value.get("params")?;
    let update = params.get("update").unwrap_or(params);
    let kind = update.get("sessionUpdate")?.as_str()?;
    match kind {
        "agent_message_chunk" => {
            update
                .pointer("/content/text")
                .and_then(Value::as_str)
                .map(|text| RuntimeEvent::MessageDelta {
                    text: text.to_owned(),
                })
        }
        "tool_call" | "tool_call_update" => Some(RuntimeEvent::Tool {
            title: update
                .get("title")
                .and_then(Value::as_str)
                .unwrap_or("tool")
                .to_owned(),
            status: update
                .get("status")
                .and_then(Value::as_str)
                .unwrap_or("unknown")
                .to_owned(),
        }),
        "plan" => Some(RuntimeEvent::Plan {
            data: update.clone(),
        }),
        "usage_update" => Some(RuntimeEvent::Usage {
            data: update.clone(),
        }),
        _ => None,
    }
}

fn is_permission_request(value: &Value) -> bool {
    value.get("id").is_some()
        && value
            .get("method")
            .and_then(Value::as_str)
            .is_some_and(|method| method.contains("request_permission"))
}

fn is_business_question(value: &Value) -> bool {
    value.get("id").is_some()
        && value
            .get("method")
            .and_then(Value::as_str)
            .is_some_and(|method| method.contains("question") || method.contains("request_input"))
}

async fn respond_permission(
    client: &Client,
    url: &str,
    token: &str,
    request: &Value,
) -> Result<()> {
    let id = request
        .get("id")
        .cloned()
        .context("permission request omitted id")?;
    let options = request
        .pointer("/params/options")
        .and_then(Value::as_array)
        .or_else(|| {
            request
                .pointer("/params/permission/options")
                .and_then(Value::as_array)
        })
        .context("permission request omitted options")?;
    let selected = options
        .iter()
        .find(|option| {
            option
                .get("kind")
                .and_then(Value::as_str)
                .is_some_and(|kind| matches!(kind, "allow_once" | "allow"))
        })
        .or_else(|| options.first())
        .and_then(|option| option.get("optionId").or_else(|| option.get("option_id")))
        .and_then(Value::as_str)
        .context("permission request has no selectable option")?;
    let response = json!({
        "jsonrpc": "2.0",
        "id": id,
        "result": {"outcome": {"outcome": "selected", "optionId": selected}}
    });
    let http_response = authorized(client.post(url).json(&response), token)
        .send()
        .await
        .context("send permission response")?;
    if !http_response.status().is_success() {
        bail!("permission response failed with {}", http_response.status());
    }
    Ok(())
}

fn discover_opencode_version(agents: &Value) -> Result<Option<String>> {
    let text = agents.to_string();
    if !text.contains("opencode") {
        bail!("Sandbox Agent did not discover OpenCode");
    }
    Ok(find_opencode_version(agents))
}

fn find_opencode_version(value: &Value) -> Option<String> {
    match value {
        Value::Object(map) => {
            if map.values().any(|item| item.as_str() == Some("opencode")) {
                return map
                    .get("version")
                    .and_then(Value::as_str)
                    .map(str::to_owned);
            }
            map.values().find_map(find_opencode_version)
        }
        Value::Array(values) => values.iter().find_map(find_opencode_version),
        _ => None,
    }
}

fn spawn_server(path: &Path, port: u16, token: &str) -> Result<Child> {
    Command::new(path)
        .arg("server")
        .arg("--host")
        .arg("127.0.0.1")
        .arg("--port")
        .arg(port.to_string())
        .arg("--token")
        .arg(token)
        .arg("--no-telemetry")
        .kill_on_drop(true)
        .stdin(std::process::Stdio::null())
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .spawn()
        .context("start Sandbox Agent")
}

async fn stop_child(child: &mut Child) {
    if let Err(error) = child.start_kill() {
        warn!(%error, "failed to signal Sandbox Agent");
    }
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        match child.try_wait() {
            Ok(Some(_)) => return,
            Ok(None) if Instant::now() < deadline => sleep(Duration::from_millis(50)).await,
            Ok(None) => {
                warn!("timed out reaping Sandbox Agent");
                return;
            }
            Err(error) => {
                warn!(%error, "failed to reap Sandbox Agent");
                return;
            }
        }
    }
}

async fn reserve_loopback_port() -> Result<u16> {
    let listener = tokio::net::TcpListener::bind((std::net::Ipv4Addr::LOCALHOST, 0))
        .await
        .context("reserve Sandbox Agent loopback port")?;
    let port = listener
        .local_addr()
        .context("read loopback address")?
        .port();
    drop(listener);
    Ok(port)
}

fn authorized(builder: reqwest::RequestBuilder, token: &str) -> reqwest::RequestBuilder {
    builder.bearer_auth(token)
}

fn classify_error(error: &anyhow::Error) -> &'static str {
    let message = error.to_string();
    if message.contains("timeout") {
        "timeout"
    } else if message.contains("authentication") || message.contains("login") {
        "source_auth_required"
    } else if message.contains("discover OpenCode") {
        "capability_unavailable"
    } else {
        "runtime_unavailable"
    }
}

fn truncate(value: &str, max: usize) -> &str {
    value.get(..max).unwrap_or(value)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn message_chunks_are_normalized() {
        let value = json!({
            "method": "session/update",
            "params": {"update": {"sessionUpdate": "agent_message_chunk", "content": {"type": "text", "text": "ok"}}}
        });
        assert!(matches!(
            normalize_event(&value),
            Some(RuntimeEvent::MessageDelta { text }) if text == "ok"
        ));
    }

    #[test]
    fn prompt_response_is_the_only_normal_completion_signal() {
        let response = json!({"result": {"stopReason": "end_turn"}});
        assert!(matches!(
            completion_from_response(&response, "ok".to_owned()),
            RuntimeEvent::Completed { output } if output == "ok"
        ));

        let response = json!({"result": {"stopReason": "cancelled"}});
        assert!(matches!(
            completion_from_response(&response, String::new()),
            RuntimeEvent::Failed { code, .. } if code == "provider_error"
        ));
    }
}
