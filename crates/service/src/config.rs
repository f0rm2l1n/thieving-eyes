use std::collections::HashSet;
use std::path::{Component, Path, PathBuf};

use anyhow::{Context, Result, bail};
use serde::{Deserialize, Serialize};

pub const SANDBOX_AGENT_VERSION: &str = "0.4.2";
pub const SANDBOX_AGENT_SHA256: &str =
    "bab098abef874ade481aa7b50463662814fbf27294399f545307fedb638f029b";
pub const SANDBOX_AGENT_URL: &str = "https://releases.rivet.dev/sandbox-agent/0.4.2/binaries/sandbox-agent-x86_64-unknown-linux-musl";

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Config {
    pub daemon: DaemonConfig,
    pub runtime: RuntimeConfig,
    pub local_runner: LocalRunnerConfig,
    pub defaults: Defaults,
    #[serde(default)]
    pub profiles: Vec<ProfileConfig>,
    #[serde(default)]
    pub policies: Vec<PolicyConfig>,
    #[serde(default)]
    pub sources: Vec<SourceConfig>,
    #[serde(default)]
    pub routes: Vec<RouteConfig>,
    #[serde(default)]
    pub workspace_roots: Vec<WorkspaceRootConfig>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DaemonConfig {
    pub socket_path: PathBuf,
    pub database_path: PathBuf,
    #[serde(default = "default_inline_output_bytes")]
    pub max_inline_output_bytes: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RuntimeConfig {
    pub cache_dir: PathBuf,
    #[serde(default = "default_download_runtime")]
    pub download_if_missing: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LocalRunnerConfig {
    pub runner_binary: PathBuf,
    pub bubblewrap_binary: PathBuf,
    pub opencode_binary: PathBuf,
    pub opencode_sha256: String,
    #[serde(default)]
    pub credential_files: Vec<CredentialFile>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CredentialFile {
    pub source_id: String,
    pub host_path: PathBuf,
    pub sandbox_path: PathBuf,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Defaults {
    pub profile_id: String,
    pub policy_id: String,
    pub route_id: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProfileConfig {
    pub id: String,
    pub version: String,
    #[serde(default = "default_profile_description")]
    pub description: String,
    #[serde(default = "default_network")]
    pub network: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PolicyConfig {
    pub id: String,
    pub version: String,
    #[serde(default = "default_policy_description")]
    pub description: String,
    #[serde(default = "default_run_timeout")]
    pub run_timeout_seconds: u64,
    #[serde(default = "default_idle_timeout")]
    pub idle_timeout_seconds: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SourceConfig {
    pub id: String,
    pub label: String,
    pub concurrency_limit: u32,
    #[serde(default)]
    pub safety_reserve: u32,
    pub monitor: CapacityMonitorConfig,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum CapacityMonitorConfig {
    Static,
    Command {
        program: PathBuf,
        #[serde(default)]
        args: Vec<String>,
        #[serde(default = "default_probe_interval")]
        interval_seconds: u64,
        #[serde(default = "default_probe_timeout")]
        timeout_seconds: u64,
        #[serde(default = "default_probe_max_age")]
        max_age_seconds: u64,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RouteConfig {
    pub id: String,
    #[serde(default = "default_adapter")]
    pub adapter: String,
    #[serde(default)]
    pub model: String,
    pub source_ids: Vec<String>,
    #[serde(default = "default_target_id")]
    pub target_id: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WorkspaceRootConfig {
    pub id: String,
    pub path: PathBuf,
    #[serde(default = "default_allow_writable")]
    pub allow_writable: bool,
}

impl Config {
    pub async fn load(path: &Path) -> Result<Self> {
        let contents = tokio::fs::read_to_string(path)
            .await
            .with_context(|| format!("read config {}", path.display()))?;
        let config: Self = toml::from_str(&contents).context("parse TOML configuration")?;
        config.validate()?;
        Ok(config)
    }

    pub fn validate(&self) -> Result<()> {
        if self.sources.is_empty() || self.routes.is_empty() || self.workspace_roots.is_empty() {
            bail!("configuration needs at least one source, route, and workspace root");
        }
        if !self
            .profiles
            .iter()
            .any(|item| item.id == self.defaults.profile_id)
        {
            bail!("default profile does not exist");
        }
        if !self
            .policies
            .iter()
            .any(|item| item.id == self.defaults.policy_id)
        {
            bail!("default policy does not exist");
        }
        if !self
            .routes
            .iter()
            .any(|item| item.id == self.defaults.route_id)
        {
            bail!("default route does not exist");
        }

        ensure_unique(self.sources.iter().map(|item| item.id.as_str()), "source")?;
        ensure_unique(self.routes.iter().map(|item| item.id.as_str()), "route")?;
        ensure_unique(
            self.workspace_roots.iter().map(|item| item.id.as_str()),
            "workspace root",
        )?;

        let source_ids: HashSet<_> = self.sources.iter().map(|item| item.id.as_str()).collect();
        for route in &self.routes {
            if route.adapter != "opencode" {
                bail!("v0.1 only supports the opencode adapter");
            }
            if route.source_ids.is_empty()
                || route
                    .source_ids
                    .iter()
                    .any(|id| !source_ids.contains(id.as_str()))
            {
                bail!(
                    "route {} contains an unknown or empty source pool",
                    route.id
                );
            }
        }

        for mapping in &self.local_runner.credential_files {
            validate_relative_sandbox_path(&mapping.sandbox_path)?;
            if !source_ids.contains(mapping.source_id.as_str()) {
                bail!(
                    "credential mapping references unknown source {}",
                    mapping.source_id
                );
            }
        }
        Ok(())
    }

    #[must_use]
    pub fn profile(&self, id: &str) -> Option<&ProfileConfig> {
        self.profiles.iter().find(|profile| profile.id == id)
    }

    #[must_use]
    pub fn policy(&self, id: &str) -> Option<&PolicyConfig> {
        self.policies.iter().find(|policy| policy.id == id)
    }

    #[must_use]
    pub fn route(&self, id: &str) -> Option<&RouteConfig> {
        self.routes.iter().find(|route| route.id == id)
    }

    #[must_use]
    pub fn source(&self, id: &str) -> Option<&SourceConfig> {
        self.sources.iter().find(|source| source.id == id)
    }

    #[must_use]
    pub fn workspace_root(&self, id: &str) -> Option<&WorkspaceRootConfig> {
        self.workspace_roots.iter().find(|root| root.id == id)
    }

    #[must_use]
    pub fn sandbox_agent_path(&self) -> PathBuf {
        self.runtime
            .cache_dir
            .join(SANDBOX_AGENT_VERSION)
            .join("sandbox-agent")
    }
}

pub fn default_config_path() -> Result<PathBuf> {
    if let Some(root) = std::env::var_os("XDG_CONFIG_HOME") {
        return Ok(PathBuf::from(root).join("thieving-eyes/config.toml"));
    }
    Ok(home_dir()?.join(".config/thieving-eyes/config.toml"))
}

pub fn default_state_dir() -> Result<PathBuf> {
    if let Some(root) = std::env::var_os("XDG_STATE_HOME") {
        return Ok(PathBuf::from(root).join("thieving-eyes"));
    }
    Ok(home_dir()?.join(".local/state/thieving-eyes"))
}

pub fn default_data_dir() -> Result<PathBuf> {
    if let Some(root) = std::env::var_os("XDG_DATA_HOME") {
        return Ok(PathBuf::from(root).join("thieving-eyes"));
    }
    Ok(home_dir()?.join(".local/share/thieving-eyes"))
}

pub fn default_runtime_dir() -> Result<PathBuf> {
    if let Some(root) = std::env::var_os("XDG_RUNTIME_DIR") {
        return Ok(PathBuf::from(root).join("thieving-eyes"));
    }
    Ok(default_state_dir()?.join("run"))
}

fn home_dir() -> Result<PathBuf> {
    std::env::var_os("HOME")
        .map(PathBuf::from)
        .context("HOME is required when the corresponding XDG directory is unset")
}

fn ensure_unique<'a>(items: impl Iterator<Item = &'a str>, label: &str) -> Result<()> {
    let mut values = HashSet::new();
    for item in items {
        if item.is_empty() || !values.insert(item) {
            bail!("{label} IDs must be non-empty and unique");
        }
    }
    Ok(())
}

fn validate_relative_sandbox_path(path: &Path) -> Result<()> {
    if path.is_absolute()
        || path.components().any(|component| {
            matches!(
                component,
                Component::ParentDir | Component::RootDir | Component::Prefix(_)
            )
        })
    {
        bail!("sandbox credential destination must be a safe relative path");
    }
    Ok(())
}

const fn default_inline_output_bytes() -> usize {
    262_144
}

const fn default_download_runtime() -> bool {
    true
}

fn default_profile_description() -> String {
    "Required bubblewrap sandbox with non-interactive approval".to_owned()
}

fn default_policy_description() -> String {
    "Single-attempt local execution policy".to_owned()
}

fn default_network() -> String {
    "inherited".to_owned()
}

const fn default_run_timeout() -> u64 {
    3_600
}

const fn default_idle_timeout() -> u64 {
    900
}

const fn default_probe_interval() -> u64 {
    30
}

const fn default_probe_timeout() -> u64 {
    10
}

const fn default_probe_max_age() -> u64 {
    90
}

fn default_adapter() -> String {
    "opencode".to_owned()
}

fn default_target_id() -> String {
    "local".to_owned()
}

const fn default_allow_writable() -> bool {
    true
}

#[cfg(test)]
mod tests {
    use super::validate_relative_sandbox_path;
    use std::path::Path;

    #[test]
    fn credential_destination_cannot_escape_home() {
        assert!(
            validate_relative_sandbox_path(Path::new(".local/share/opencode/auth.json")).is_ok()
        );
        assert!(validate_relative_sandbox_path(Path::new("../auth.json")).is_err());
        assert!(validate_relative_sandbox_path(Path::new("/etc/passwd")).is_err());
    }
}
