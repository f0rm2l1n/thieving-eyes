use std::collections::HashMap;
use std::process::Stdio;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result, bail};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::process::Command;
use tokio::sync::RwLock;
use tokio::time::{Instant, timeout};

use crate::config::{CapacityMonitorConfig, SourceConfig};
use crate::store::Store;

const MAX_PROBE_OUTPUT_BYTES: usize = 64 * 1024;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CapacityProbeRequest {
    pub protocol_version: u8,
    pub source_id: String,
    pub requested_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CapacityProbeResponse {
    pub protocol_version: u8,
    pub observed_at: DateTime<Utc>,
    pub health: ProbeHealth,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub usage: Option<CapacityUsage>,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ProbeHealth {
    Healthy,
    Unavailable,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CapacityUsage {
    pub kind: UsageKind,
    pub count: u32,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum UsageKind {
    TotalInUse,
    ExternalInUse,
}

#[derive(Debug, Clone)]
struct Observation {
    response: CapacityProbeResponse,
    received_at: DateTime<Utc>,
    next_poll: Instant,
}

#[derive(Debug, Clone)]
pub struct CapacityManager {
    sources: Arc<Vec<SourceConfig>>,
    observations: Arc<RwLock<HashMap<String, Observation>>>,
    store: Store,
}

impl CapacityManager {
    #[must_use]
    pub fn new(sources: Vec<SourceConfig>, store: Store) -> Self {
        Self {
            sources: Arc::new(sources),
            observations: Arc::new(RwLock::new(HashMap::new())),
            store,
        }
    }

    pub async fn initialize(&self) -> Result<()> {
        let now = Utc::now();
        let mut observations = self.observations.write().await;
        for source in self.sources.iter() {
            if matches!(source.monitor, CapacityMonitorConfig::Static) {
                let response = CapacityProbeResponse {
                    protocol_version: 1,
                    observed_at: now,
                    health: ProbeHealth::Healthy,
                    usage: None,
                };
                observations.insert(
                    source.id.clone(),
                    Observation {
                        response,
                        received_at: now,
                        next_poll: Instant::now() + Duration::from_secs(86_400),
                    },
                );
            }
        }
        Ok(())
    }

    pub async fn refresh_due(&self) {
        for source in self.sources.iter() {
            let CapacityMonitorConfig::Command {
                interval_seconds, ..
            } = &source.monitor
            else {
                continue;
            };
            let due = self
                .observations
                .read()
                .await
                .get(&source.id)
                .is_none_or(|observation| observation.next_poll <= Instant::now());
            if !due {
                continue;
            }
            match run_probe(source).await {
                Ok(response) => {
                    let received_at = Utc::now();
                    let usage_kind = response.usage.as_ref().map(|usage| match usage.kind {
                        UsageKind::TotalInUse => "total_in_use",
                        UsageKind::ExternalInUse => "external_in_use",
                    });
                    let in_use = response.usage.as_ref().map(|usage| usage.count);
                    let _ = self
                        .store
                        .record_capacity(
                            &source.id,
                            match response.health {
                                ProbeHealth::Healthy => "healthy",
                                ProbeHealth::Unavailable => "unavailable",
                            },
                            usage_kind,
                            in_use,
                            Some(response.observed_at),
                        )
                        .await;
                    self.observations.write().await.insert(
                        source.id.clone(),
                        Observation {
                            response,
                            received_at,
                            next_poll: Instant::now() + Duration::from_secs(*interval_seconds),
                        },
                    );
                }
                Err(error) => {
                    tracing::warn!(source_id = %source.id, %error, "capacity probe failed closed");
                    let now = Utc::now();
                    self.observations.write().await.insert(
                        source.id.clone(),
                        Observation {
                            response: CapacityProbeResponse {
                                protocol_version: 1,
                                observed_at: now,
                                health: ProbeHealth::Unavailable,
                                usage: None,
                            },
                            received_at: now,
                            next_poll: Instant::now() + Duration::from_secs(*interval_seconds),
                        },
                    );
                    let _ = self
                        .store
                        .record_capacity(&source.id, "unknown", None, None, None)
                        .await;
                }
            }
        }
    }

    pub async fn available(&self, source: &SourceConfig, active_leases: u32) -> Option<u32> {
        let observations = self.observations.read().await;
        let observation = observations.get(&source.id)?;
        if observation.response.health != ProbeHealth::Healthy {
            return Some(0);
        }
        if let CapacityMonitorConfig::Command {
            max_age_seconds, ..
        } = source.monitor
            && Utc::now()
                .signed_duration_since(observation.received_at)
                .num_seconds()
                > i64::try_from(max_age_seconds).ok()?
        {
            return None;
        }
        let used = match observation.response.usage.as_ref() {
            None => active_leases,
            Some(usage) if usage.kind == UsageKind::TotalInUse => active_leases.max(usage.count),
            Some(usage) => active_leases.saturating_add(usage.count),
        };
        Some(
            source
                .concurrency_limit
                .saturating_sub(used)
                .saturating_sub(source.safety_reserve),
        )
    }
}

async fn run_probe(source: &SourceConfig) -> Result<CapacityProbeResponse> {
    let CapacityMonitorConfig::Command {
        program,
        args,
        timeout_seconds,
        max_age_seconds,
        ..
    } = &source.monitor
    else {
        bail!("static source does not have a command probe");
    };
    if !program.is_absolute() {
        bail!("capacity probe program must be an absolute path");
    }
    let mut child = Command::new(program)
        .args(args)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .kill_on_drop(true)
        .spawn()
        .with_context(|| format!("start capacity probe {}", program.display()))?;
    let request = CapacityProbeRequest {
        protocol_version: 1,
        source_id: source.id.clone(),
        requested_at: Utc::now(),
    };
    if let Some(mut stdin) = child.stdin.take() {
        stdin
            .write_all(&serde_json::to_vec(&request)?)
            .await
            .context("write capacity probe request")?;
    }
    let stdout = child
        .stdout
        .take()
        .context("capacity probe stdout missing")?;
    let (status, output) = timeout(Duration::from_secs((*timeout_seconds).max(1)), async {
        tokio::try_join!(child.wait(), async {
            let mut output = Vec::new();
            stdout
                .take(u64::try_from(MAX_PROBE_OUTPUT_BYTES + 1).unwrap_or(u64::MAX))
                .read_to_end(&mut output)
                .await?;
            Ok::<_, std::io::Error>(output)
        })
    })
    .await
    .context("capacity probe timeout")??;
    if !status.success() {
        bail!("capacity probe exited with {status}");
    }
    if output.len() > MAX_PROBE_OUTPUT_BYTES {
        bail!("capacity probe output exceeds limit");
    }
    let response: CapacityProbeResponse =
        serde_json::from_slice(&output).context("decode capacity probe response")?;
    if response.protocol_version != 1 {
        bail!("unsupported capacity probe protocol version");
    }
    if response.health == ProbeHealth::Healthy && response.usage.is_none() {
        bail!("healthy command probe must report usage");
    }
    let age = Utc::now()
        .signed_duration_since(response.observed_at)
        .num_seconds();
    if age < -30 || age > i64::try_from(*max_age_seconds).context("probe max age is too large")? {
        bail!("capacity observation timestamp is stale or in the future");
    }
    Ok(response)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::CapacityMonitorConfig;

    #[tokio::test]
    async fn total_usage_does_not_double_count_active_leases() {
        let store = Store::open("sqlite::memory:")
            .await
            .unwrap_or_else(|error| panic!("open store: {error}"));
        let source = SourceConfig {
            id: "source".to_owned(),
            label: "source".to_owned(),
            concurrency_limit: 10,
            safety_reserve: 1,
            monitor: CapacityMonitorConfig::Static,
        };
        let manager = CapacityManager::new(vec![source.clone()], store);
        manager
            .initialize()
            .await
            .unwrap_or_else(|error| panic!("initialize: {error}"));
        assert_eq!(manager.available(&source, 3).await, Some(6));
    }
}
