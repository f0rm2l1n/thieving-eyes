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
    pub max_output_bytes: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum RuntimeEvent {
    Started {
        agent_version: Option<String>,
    },
    MessageDelta {
        text: String,
    },
    Tool {
        title: String,
        status: String,
    },
    Plan {
        entries: Vec<PlanEntry>,
    },
    Usage {
        used: u64,
        size: u64,
        cost: Option<UsageCost>,
    },
    Completed {
        output: String,
    },
    Cancelled,
    Failed {
        code: String,
        message: String,
    },
    Uncertain {
        code: String,
        message: String,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PlanEntry {
    pub content: String,
    pub priority: Option<String>,
    pub status: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UsageCost {
    pub amount: f64,
    pub currency: String,
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
        .connect_timeout(Duration::from_secs(5))
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
            .send(RuntimeEvent::Uncertain {
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
    let session = match prepare_session(request, client, base_url, token, events).await {
        Ok(session) => session,
        Err(error) => {
            events
                .send(RuntimeEvent::Failed {
                    code: classify_error(&error).to_owned(),
                    message: error.to_string(),
                })
                .await
                .context("report runtime preparation failure")?;
            return Ok(());
        }
    };
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
    control_rpc(
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

    let (sse_rx, sse_task) =
        subscribe_sse(client.clone(), acp_url.clone(), token.to_owned()).await?;

    let session_response = control_rpc(
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
        control_rpc(
            client,
            &acp_url,
            token,
            json!({
                "jsonrpc": "2.0", "id": 3, "method": "session/set_config_option",
                "params": {"sessionId": session_id, "configId": "model", "value": model}
            }),
        )
        .await
        .context("set requested OpenCode model")?;
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
        post_prompt(
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
    let mut prompt_post_finished = false;

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
                    break match cancel_session(client, &session.acp_url, token, &session.session_id).await {
                        Ok(()) => match wait_for_prompt_response(
                            client,
                            token,
                            events,
                            &mut session,
                            &mut output,
                            request.max_output_bytes,
                            Duration::from_secs(10),
                        ).await {
                            Ok(_) => RuntimeEvent::Cancelled,
                            Err(error) => RuntimeEvent::Uncertain {
                                code: "cancellation_unconfirmed".to_owned(),
                                message: format!("Agent did not confirm cancellation: {error}"),
                            },
                        }
                        Err(error) => RuntimeEvent::Uncertain {
                            code: "cancellation_unconfirmed".to_owned(),
                            message: format!("could not deliver Agent cancellation: {error}"),
                        },
                    };
                }
            }
            () = &mut run_deadline => {
                break match cancel_session(client, &session.acp_url, token, &session.session_id).await {
                    Ok(()) => match wait_for_prompt_response(
                        client,
                        token,
                        events,
                        &mut session,
                        &mut output,
                        request.max_output_bytes,
                        Duration::from_secs(10),
                    ).await {
                        Ok(_) => RuntimeEvent::Failed { code: "timeout".to_owned(), message: "run timeout elapsed".to_owned() },
                        Err(_) => RuntimeEvent::Uncertain {
                            code: "timeout_unconfirmed".to_owned(),
                            message: "run timeout elapsed and Agent stop was not confirmed".to_owned(),
                        },
                    },
                    Err(error) => RuntimeEvent::Uncertain {
                        code: "timeout_unconfirmed".to_owned(),
                        message: format!("run timeout elapsed and cancellation was not confirmed: {error}"),
                    },
                };
            }
            () = &mut idle => {
                break match cancel_session(client, &session.acp_url, token, &session.session_id).await {
                    Ok(()) => match wait_for_prompt_response(
                        client,
                        token,
                        events,
                        &mut session,
                        &mut output,
                        request.max_output_bytes,
                        Duration::from_secs(10),
                    ).await {
                        Ok(_) => RuntimeEvent::Failed { code: "timeout".to_owned(), message: "idle timeout elapsed".to_owned() },
                        Err(_) => RuntimeEvent::Uncertain {
                            code: "timeout_unconfirmed".to_owned(),
                            message: "idle timeout elapsed and Agent stop was not confirmed".to_owned(),
                        },
                    },
                    Err(error) => RuntimeEvent::Uncertain {
                        code: "timeout_unconfirmed".to_owned(),
                        message: format!("idle timeout elapsed and cancellation was not confirmed: {error}"),
                    },
                };
            }
            value = session.sse_rx.recv() => {
                match value {
                    Some(value) => {
                        idle.as_mut().reset(Instant::now() + Duration::from_secs(request.idle_timeout_seconds.max(1)));
                        if is_response_for(&value, 4) {
                            break completion_from_response(&value, output);
                        }
                        handle_inbound(
                            client,
                            token,
                            events,
                            &session.acp_url,
                            &value,
                            &mut output,
                            request.max_output_bytes,
                        )
                        .await?;
                        if is_business_question(&value) {
                            break RuntimeEvent::Failed {
                                code: "interaction_required".to_owned(),
                                message: "agent requested interactive business input".to_owned(),
                            };
                        }
                    }
                    None => {
                        break RuntimeEvent::Uncertain {
                            code: "runtime_stream_lost".to_owned(),
                            message: "Sandbox Agent event stream closed before prompt completion".to_owned(),
                        };
                    }
                }
            }
            result = &mut session.sse_task => {
                let message = match result {
                    Ok(Ok(())) => "Sandbox Agent event stream ended before prompt completion".to_owned(),
                    Ok(Err(error)) => format!("Sandbox Agent event stream failed: {error}"),
                    Err(error) => format!("Sandbox Agent event task failed: {error}"),
                };
                break RuntimeEvent::Uncertain {
                    code: "runtime_stream_lost".to_owned(),
                    message,
                };
            }
            response = &mut prompt_task, if !prompt_post_finished => {
                prompt_post_finished = true;
                let response = response.context("join ACP prompt task")??;
                if response
                    .as_ref()
                    .is_some_and(|response| !is_response_for(response, 4))
                {
                    bail!("ACP prompt POST returned an unrelated response envelope");
                }
            }
        }
    };

    if !prompt_post_finished {
        prompt_task.abort();
        let _ = prompt_task.await;
    }
    session.sse_task.abort();
    let _ = session.sse_task.await;
    events
        .send(completion)
        .await
        .context("report runtime completion")?;
    // DELETE asks Sandbox Agent to shut down the underlying Agent process. It
    // is only best-effort here: the outer runtime owner kills and reaps the
    // dedicated Sandbox Agent process, while the runner withholds the terminal
    // event until the sandbox worker has exited.
    let _ = timeout(
        Duration::from_secs(2),
        authorized(client.delete(&session.acp_url), token).send(),
    )
    .await;
    Ok(())
}

async fn cancel_session(client: &Client, url: &str, token: &str, session_id: &str) -> Result<()> {
    let response = timeout(
        Duration::from_secs(10),
        authorized(
            client.post(url).json(&json!({
                "jsonrpc": "2.0",
                "method": "session/cancel",
                "params": {"sessionId": session_id}
            })),
            token,
        )
        .send(),
    )
    .await
    .context("ACP cancellation delivery timeout")?
    .context("deliver ACP cancellation")?;
    if !response.status().is_success() {
        bail!("ACP cancellation failed with {}", response.status());
    }
    Ok(())
}

async fn handle_inbound(
    client: &Client,
    token: &str,
    events: &mpsc::Sender<RuntimeEvent>,
    acp_url: &str,
    value: &Value,
    output: &mut String,
    max_output_bytes: usize,
) -> Result<()> {
    if let Some(mut event) = normalize_event(value) {
        if let RuntimeEvent::MessageDelta { text } = &mut event {
            truncate_to_bytes(text, max_output_bytes.saturating_sub(output.len()));
            output.push_str(text);
            if text.is_empty() {
                return Ok(());
            }
        }
        events.send(event).await.context("report runtime event")?;
    }
    if is_permission_request(value) {
        respond_permission(client, acp_url, token, value).await?;
    }
    Ok(())
}

fn truncate_to_bytes(value: &mut String, max_bytes: usize) {
    if value.len() <= max_bytes {
        return;
    }
    let mut boundary = max_bytes;
    while boundary > 0 && !value.is_char_boundary(boundary) {
        boundary -= 1;
    }
    value.truncate(boundary);
}

fn completion_from_response(response: &Value, output: String) -> RuntimeEvent {
    if let Some(error) = response.get("error") {
        return RuntimeEvent::Failed {
            code: "provider_error".to_owned(),
            message: format!("Agent prompt failed: {}", truncate(&error.to_string(), 512)),
        };
    }
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

async fn post_prompt(
    client: &Client,
    url: &str,
    token: &str,
    payload: Value,
) -> Result<Option<Value>> {
    let response = authorized(client.post(url).json(&payload), token)
        .send()
        .await
        .with_context(|| format!("send ACP prompt to {url}"))?;
    if !response.status().is_success() {
        let status = response.status();
        let body = response.text().await.unwrap_or_default();
        bail!("ACP prompt failed with {status}: {}", truncate(&body, 512));
    }
    let body = response.text().await.context("read ACP prompt response")?;
    if body.trim().is_empty() {
        return Ok(None);
    }
    let value: Value = serde_json::from_str(&body).context("decode ACP prompt response")?;
    Ok(Some(value))
}

async fn wait_for_prompt_response(
    client: &Client,
    token: &str,
    events: &mpsc::Sender<RuntimeEvent>,
    session: &mut ActiveSession,
    output: &mut String,
    max_output_bytes: usize,
    deadline: Duration,
) -> Result<Value> {
    timeout(deadline, async {
        loop {
            tokio::select! {
                value = session.sse_rx.recv() => {
                    let value = value.context("Sandbox Agent event stream closed")?;
                    if is_response_for(&value, 4) {
                        return Ok(value);
                    }
                    handle_inbound(
                        client,
                        token,
                        events,
                        &session.acp_url,
                        &value,
                        output,
                        max_output_bytes,
                    )
                    .await?;
                }
                result = &mut session.sse_task => {
                    result.context("join Sandbox Agent event stream")??;
                    bail!("Sandbox Agent event stream ended");
                }
            }
        }
    })
    .await
    .context("Agent stop confirmation timeout")?
}

fn is_response_for(value: &Value, request_id: u64) -> bool {
    value.get("method").is_none()
        && value.get("id").and_then(Value::as_u64) == Some(request_id)
        && (value.get("result").is_some() || value.get("error").is_some())
}

async fn control_rpc(client: &Client, url: &str, token: &str, payload: Value) -> Result<Value> {
    timeout(Duration::from_secs(30), rpc(client, url, token, payload))
        .await
        .context("ACP control request timeout")?
}

async fn subscribe_sse(
    client: Client,
    url: String,
    token: String,
) -> Result<(mpsc::Receiver<Value>, tokio::task::JoinHandle<Result<()>>)> {
    let first_response = open_sse(&client, &url, &token, None).await?;
    let (tx, rx) = mpsc::channel(256);
    let task = tokio::spawn(async move {
        let mut response = Some(first_response);
        let mut last_event_id = None;
        loop {
            let current = match response.take() {
                Some(response) => response,
                None => match open_sse(&client, &url, &token, last_event_id).await {
                    Ok(response) => response,
                    Err(error) => {
                        debug!(%error, "reconnect ACP SSE failed");
                        sleep(Duration::from_millis(150)).await;
                        continue;
                    }
                },
            };
            let mut stream = current.bytes_stream().eventsource();
            while let Some(event) = stream.next().await {
                let event = match event {
                    Ok(event) => event,
                    Err(error) => {
                        debug!(%error, "ACP SSE stream interrupted");
                        break;
                    }
                };
                if event.data.is_empty() {
                    continue;
                }
                let event_id = event.id.parse::<u64>().ok();
                if event_id.is_some_and(|id| last_event_id.is_some_and(|last| id <= last)) {
                    continue;
                }
                let value: Value =
                    serde_json::from_str(&event.data).context("decode ACP SSE payload")?;
                if tx.send(value).await.is_err() {
                    return Ok(());
                }
                if let Some(event_id) = event_id {
                    last_event_id = Some(event_id);
                }
            }
            sleep(Duration::from_millis(150)).await;
        }
    });
    Ok((rx, task))
}

async fn open_sse(
    client: &Client,
    url: &str,
    token: &str,
    last_event_id: Option<u64>,
) -> Result<reqwest::Response> {
    let mut request = authorized(client.get(url), token);
    if let Some(last_event_id) = last_event_id {
        request = request.header("Last-Event-ID", last_event_id.to_string());
    }
    let response = timeout(Duration::from_secs(10), request.send())
        .await
        .context("ACP SSE subscription timeout")?
        .context("subscribe to ACP SSE")?;
    if response.status() != StatusCode::OK {
        bail!("ACP SSE returned {}", response.status());
    }
    Ok(response)
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
            entries: update
                .get("entries")
                .and_then(Value::as_array)
                .into_iter()
                .flatten()
                .filter_map(|entry| {
                    Some(PlanEntry {
                        content: entry.get("content")?.as_str()?.to_owned(),
                        priority: entry
                            .get("priority")
                            .and_then(Value::as_str)
                            .map(str::to_owned),
                        status: entry
                            .get("status")
                            .and_then(Value::as_str)
                            .map(str::to_owned),
                    })
                })
                .collect(),
        }),
        "usage_update" => Some(RuntimeEvent::Usage {
            used: update.get("used")?.as_u64()?,
            size: update.get("size")?.as_u64()?,
            cost: update.get("cost").and_then(|cost| {
                Some(UsageCost {
                    amount: cost.get("amount")?.as_f64()?,
                    currency: cost.get("currency")?.as_str()?.to_owned(),
                })
            }),
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

        let response = json!({"id": 4, "error": {"code": -32000, "message": "failed"}});
        assert!(matches!(
            completion_from_response(&response, String::new()),
            RuntimeEvent::Failed { code, message }
                if code == "provider_error" && message.contains("failed")
        ));
    }

    #[test]
    fn prompt_response_is_recognized_on_sse() {
        assert!(is_response_for(
            &json!({"jsonrpc": "2.0", "id": 4, "result": {"stopReason": "end_turn"}}),
            4
        ));
        assert!(is_response_for(
            &json!({"jsonrpc": "2.0", "id": 4, "error": {"code": -1}}),
            4
        ));
        assert!(!is_response_for(
            &json!({"jsonrpc": "2.0", "id": 3, "result": {}}),
            4
        ));
        assert!(!is_response_for(
            &json!({"jsonrpc": "2.0", "id": 4, "method": "session/update"}),
            4
        ));
    }

    #[test]
    fn captured_output_is_bounded_on_utf8_boundary() {
        let mut output = String::new();
        let mut first = "你好世界".to_owned();
        truncate_to_bytes(&mut first, 7);
        output.push_str(&first);
        assert_eq!(output, "你好");
        let mut second = "xyz".to_owned();
        truncate_to_bytes(&mut second, 7_usize.saturating_sub(output.len()));
        output.push_str(&second);
        assert_eq!(output, "你好x");
    }
}
