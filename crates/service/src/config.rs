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
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub opencode_binary: Option<PathBuf>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub opencode_sha256: Option<String>,
    #[serde(default)]
    pub agent_binaries: Vec<AgentBinaryConfig>,
    #[serde(default)]
    pub credential_files: Vec<CredentialFile>,
    #[serde(default)]
    pub source_bindings: Vec<LocalSourceBindingConfig>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AgentBinaryConfig {
    pub adapter: String,
    pub binary: PathBuf,
    pub sha256: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub agent_process_binary: Option<PathBuf>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub agent_process_sha256: Option<String>,
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
pub struct LocalSourceBindingConfig {
    pub source_id: String,
    #[serde(default)]
    pub inherit_proxy_environment: Vec<String>,
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
    #[serde(default)]
    pub network: NetworkMode,
}

#[derive(Debug, Clone, Copy, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum NetworkMode {
    #[default]
    Inherited,
    None,
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

        ensure_unique_resource_versions(
            self.profiles
                .iter()
                .map(|item| (item.id.as_str(), item.version.as_str())),
            "profile",
        )?;
        ensure_unique_resource_versions(
            self.policies
                .iter()
                .map(|item| (item.id.as_str(), item.version.as_str())),
            "policy",
        )?;
        ensure_unique(self.sources.iter().map(|item| item.id.as_str()), "source")?;
        ensure_unique(self.routes.iter().map(|item| item.id.as_str()), "route")?;
        ensure_unique(
            self.workspace_roots.iter().map(|item| item.id.as_str()),
            "workspace root",
        )?;

        let source_ids: HashSet<_> = self.sources.iter().map(|item| item.id.as_str()).collect();
        for source in &self.sources {
            if source.concurrency_limit == 0 {
                bail!("source {} has a zero concurrency limit", source.id);
            }
            if source.safety_reserve > source.concurrency_limit {
                bail!("source {} safety reserve exceeds its limit", source.id);
            }
            if let CapacityMonitorConfig::Command {
                interval_seconds,
                timeout_seconds,
                max_age_seconds,
                ..
            } = source.monitor
                && (interval_seconds == 0 || timeout_seconds == 0 || max_age_seconds == 0)
            {
                bail!(
                    "source {} capacity monitor durations must be positive",
                    source.id
                );
            }
        }
        ensure_unique(
            self.local_runner
                .agent_binaries
                .iter()
                .map(|binary| binary.adapter.as_str()),
            "local Agent binary",
        )?;
        if self.local_runner.opencode_binary.is_some()
            != self.local_runner.opencode_sha256.is_some()
        {
            bail!("legacy OpenCode path and digest must be configured together");
        }
        for binary in &self.local_runner.agent_binaries {
            if !matches!(binary.adapter.as_str(), "codex" | "opencode") {
                bail!(
                    "local Agent binary has unsupported adapter {}",
                    binary.adapter
                );
            }
            if binary.agent_process_binary.is_some() != binary.agent_process_sha256.is_some() {
                bail!(
                    "local Agent binary {} must configure both agent process path and digest",
                    binary.adapter
                );
            }
            if !binary.binary.is_absolute()
                || binary
                    .agent_process_binary
                    .as_ref()
                    .is_some_and(|path| !path.is_absolute())
            {
                bail!(
                    "local Agent binary {} paths must be absolute",
                    binary.adapter
                );
            }
            if !is_sha256_hex(&binary.sha256)
                || binary
                    .agent_process_sha256
                    .as_deref()
                    .is_some_and(|digest| !is_sha256_hex(digest))
            {
                bail!(
                    "local Agent binary {} has an invalid SHA-256 digest",
                    binary.adapter
                );
            }
            if binary.adapter == "codex" && binary.agent_process_binary.is_none() {
                bail!("Codex requires a pinned codex-acp Agent process");
            }
        }
        for route in &self.routes {
            if !matches!(route.adapter.as_str(), "codex" | "opencode") {
                bail!("v0.1 route {} has an unsupported adapter", route.id);
            }
            if self.agent_binary(&route.adapter).is_none() {
                bail!(
                    "route {} has no configured local Agent binary for {}",
                    route.id,
                    route.adapter
                );
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
            if route.target_id != "local" {
                bail!("v0.1 route {} must target local", route.id);
            }
        }

        if self.daemon.max_inline_output_bytes == 0 {
            bail!("max_inline_output_bytes must be positive");
        }
        if self
            .policies
            .iter()
            .any(|policy| policy.run_timeout_seconds == 0 || policy.idle_timeout_seconds == 0)
        {
            bail!("policy timeout durations must be positive");
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
        ensure_unique(
            self.local_runner
                .source_bindings
                .iter()
                .map(|binding| binding.source_id.as_str()),
            "local source binding",
        )?;
        for binding in &self.local_runner.source_bindings {
            if !source_ids.contains(binding.source_id.as_str()) {
                bail!(
                    "local source binding references unknown source {}",
                    binding.source_id
                );
            }
            ensure_unique(
                binding.inherit_proxy_environment.iter().map(String::as_str),
                "inherited proxy environment variable",
            )?;
            for name in &binding.inherit_proxy_environment {
                if !is_allowed_proxy_environment(name) {
                    bail!(
                        "local source binding {} cannot inherit environment variable {}",
                        binding.source_id,
                        name
                    );
                }
            }
        }
        Ok(())
    }

    #[must_use]
    pub fn profile(&self, id: &str, version: Option<&str>) -> Option<&ProfileConfig> {
        self.profiles
            .iter()
            .filter(|profile| profile.id == id)
            .filter(|profile| version.is_none_or(|version| profile.version == version))
            .max_by(|left, right| left.version.cmp(&right.version))
    }

    #[must_use]
    pub fn policy(&self, id: &str, version: Option<&str>) -> Option<&PolicyConfig> {
        self.policies
            .iter()
            .filter(|policy| policy.id == id)
            .filter(|policy| version.is_none_or(|version| policy.version == version))
            .max_by(|left, right| left.version.cmp(&right.version))
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
    pub fn local_source_binding(&self, source_id: &str) -> Option<&LocalSourceBindingConfig> {
        self.local_runner
            .source_bindings
            .iter()
            .find(|binding| binding.source_id == source_id)
    }

    #[must_use]
    pub fn agent_binary(&self, adapter: &str) -> Option<AgentBinaryConfig> {
        self.local_runner
            .agent_binaries
            .iter()
            .find(|binary| binary.adapter == adapter)
            .cloned()
            .or_else(|| {
                let binary = self.local_runner.opencode_binary.as_ref()?;
                let sha256 = self.local_runner.opencode_sha256.as_ref()?;
                (adapter == "opencode").then(|| AgentBinaryConfig {
                    adapter: "opencode".to_owned(),
                    binary: binary.clone(),
                    sha256: sha256.clone(),
                    agent_process_binary: None,
                    agent_process_sha256: None,
                })
            })
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

fn ensure_unique_resource_versions<'a>(
    items: impl Iterator<Item = (&'a str, &'a str)>,
    label: &str,
) -> Result<()> {
    let mut values = HashSet::new();
    for (id, version) in items {
        if id.is_empty() || version.is_empty() || !values.insert((id, version)) {
            bail!("{label} ID/version pairs must be non-empty and unique");
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

fn is_sha256_hex(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
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
    use super::{is_allowed_proxy_environment, is_sha256_hex, validate_relative_sandbox_path};
    use std::path::Path;

    #[test]
    fn credential_destination_cannot_escape_home() {
        assert!(
            validate_relative_sandbox_path(Path::new(".local/share/opencode/auth.json")).is_ok()
        );
        assert!(validate_relative_sandbox_path(Path::new("../auth.json")).is_err());
        assert!(validate_relative_sandbox_path(Path::new("/etc/passwd")).is_err());
    }

    #[test]
    fn inherited_environment_is_limited_to_proxy_variables() {
        assert!(is_allowed_proxy_environment("HTTPS_PROXY"));
        assert!(is_allowed_proxy_environment("no_proxy"));
        assert!(!is_allowed_proxy_environment("PATH"));
        assert!(!is_allowed_proxy_environment("OPENAI_API_KEY"));
    }

    #[test]
    fn binary_digest_is_lowercase_sha256() {
        assert!(is_sha256_hex(&"a".repeat(64)));
        assert!(!is_sha256_hex(&"A".repeat(64)));
        assert!(!is_sha256_hex(&"a".repeat(63)));
    }
}
