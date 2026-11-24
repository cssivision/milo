use std::{
    collections::HashSet,
    fs,
    net::SocketAddr,
    path::{Path, PathBuf},
};

use anyhow::{Context, Result, bail};
use serde::Deserialize;

#[derive(Debug, Clone, Deserialize)]
pub struct Config {
    #[serde(default)]
    pub gateway: Vec<GatewayConfig>,
    pub model: ModelConfig,
    #[serde(default)]
    pub agent: AgentConfig,
    #[serde(default)]
    pub log: LogConfig,
    #[serde(default = "defaults::storage_dir")]
    pub storage_dir: PathBuf,
    #[serde(default = "defaults::profile_dir")]
    pub profile_dir: PathBuf,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum GatewayConfig {
    Wecom(WecomConfig),
    Http(HttpConfig),
}

#[derive(Debug, Clone, Deserialize)]
pub struct WecomConfig {
    pub bot_id: String,
    pub secret: String,
    #[serde(default = "defaults::websocket_url")]
    pub websocket_url: String,
    #[serde(default = "defaults::policy")]
    pub dm_policy: AccessPolicy,
    #[serde(default = "defaults::policy")]
    pub group_policy: AccessPolicy,
    #[serde(default)]
    pub dm_allow_from: Vec<String>,
    #[serde(default)]
    pub group_allow_from: Vec<String>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct ModelConfig {
    pub api_key: String,
    pub base_url: String,
    pub model: String,
    #[serde(default = "defaults::reasoning_effort")]
    pub reasoning_effort: ReasoningEffort,
    #[serde(default)]
    pub context_length: Option<usize>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct HttpConfig {
    #[serde(default = "defaults::http_listen_addr")]
    pub listen_addr: String,
    pub bearer_token: String,
}

#[derive(Debug, Clone, Deserialize)]
pub struct AgentConfig {
    #[serde(default = "defaults::system_prompt")]
    pub system_prompt: String,
    #[serde(default = "defaults::max_history_messages")]
    pub max_history_messages: usize,
    #[serde(default)]
    pub compression_threshold_percent: Option<f64>,
    #[serde(default = "defaults::max_compression_attempts")]
    pub max_compression_attempts: usize,
}

#[derive(Debug, Clone, Copy, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ReasoningEffort {
    Off,
    Minimal,
    Low,
    Medium,
    High,
    XHigh,
    Max,
}

#[derive(Debug, Clone, Deserialize)]
pub struct LogConfig {
    #[serde(default = "defaults::log_level")]
    pub level: String,
}

#[derive(Debug, Clone, Copy, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum AccessPolicy {
    Open,
    Allowlist,
    Disabled,
}

impl Config {
    pub fn from_file(path: impl AsRef<Path>) -> Result<Self> {
        let path = path.as_ref();
        let content = fs::read_to_string(path)
            .with_context(|| format!("read configuration {}", path.display()))?;
        let mut config: Self = toml::from_str(&content)
            .with_context(|| format!("parse configuration {}", path.display()))?;
        if config.storage_dir.is_relative() {
            config.storage_dir = path
                .parent()
                .unwrap_or_else(|| Path::new("."))
                .join(&config.storage_dir);
        }
        if config.profile_dir.is_relative() {
            config.profile_dir = path
                .parent()
                .unwrap_or_else(|| Path::new("."))
                .join(&config.profile_dir);
        }
        config.validate()?;
        Ok(config)
    }

    fn validate(&self) -> Result<()> {
        required("model.api_key", &self.model.api_key)?;
        required("model.base_url", &self.model.base_url)?;
        required("model.model", &self.model.model)?;
        if self.model.context_length == Some(0) {
            bail!("model.context_length must be greater than zero");
        }
        if self
            .agent
            .compression_threshold_percent
            .is_some_and(|threshold| !(0.0..=1.0).contains(&threshold) || threshold == 0.0)
        {
            bail!("agent.compression_threshold_percent must be greater than zero and at most one");
        }
        if self.agent.max_compression_attempts == 0 {
            bail!("agent.max_compression_attempts must be greater than zero");
        }
        if self.storage_dir.as_os_str().is_empty() {
            bail!("storage_dir cannot be empty");
        }
        if self.profile_dir.as_os_str().is_empty() {
            bail!("profile_dir cannot be empty");
        }

        if self.gateway.is_empty() {
            bail!("at least one [[gateway]] entry is required");
        }

        let mut http_listen_addrs = HashSet::new();
        for gateway in &self.gateway {
            match gateway {
                GatewayConfig::Wecom(config) => validate_wecom(config)?,
                GatewayConfig::Http(config) => {
                    required("gateway.bearer_token", &config.bearer_token)?;
                    let addr: SocketAddr = config
                        .listen_addr
                        .parse()
                        .with_context(|| "gateway.listen_addr must be an IP address and port")?;
                    if !http_listen_addrs.insert(addr) {
                        bail!("duplicate HTTP gateway listen_addr: {addr}");
                    }
                }
            }
        }
        Ok(())
    }
}

impl Default for AgentConfig {
    fn default() -> Self {
        Self {
            system_prompt: defaults::system_prompt(),
            max_history_messages: defaults::max_history_messages(),
            compression_threshold_percent: None,
            max_compression_attempts: defaults::max_compression_attempts(),
        }
    }
}

impl AgentConfig {
    pub fn compression_threshold_percent(&self) -> f64 {
        self.compression_threshold_percent
            .unwrap_or_else(defaults::compression_threshold_percent)
    }
}

impl Default for LogConfig {
    fn default() -> Self {
        Self {
            level: defaults::log_level(),
        }
    }
}

fn required(name: &str, value: &str) -> Result<()> {
    if value.trim().is_empty() {
        bail!("{name} cannot be empty");
    }
    Ok(())
}

fn validate_allowlist(name: &str, policy: AccessPolicy, entries: &[String]) -> Result<()> {
    if policy == AccessPolicy::Allowlist && entries.is_empty() {
        bail!("{name} must not be empty when its policy is allowlist");
    }
    Ok(())
}

fn validate_wecom(config: &WecomConfig) -> Result<()> {
    required("gateway.bot_id", &config.bot_id)?;
    required("gateway.secret", &config.secret)?;
    if !(config.websocket_url.starts_with("wss://") || config.websocket_url.starts_with("ws://")) {
        bail!("gateway.websocket_url must start with ws:// or wss://");
    }
    validate_allowlist(
        "gateway.dm_allow_from",
        config.dm_policy,
        &config.dm_allow_from,
    )?;
    validate_allowlist(
        "gateway.group_allow_from",
        config.group_policy,
        &config.group_allow_from,
    )?;
    Ok(())
}

mod defaults {
    use super::{AccessPolicy, ReasoningEffort};
    use std::path::PathBuf;

    const WECOM_WS_URL: &str = "wss://openws.work.weixin.qq.com";
    const HTTP_LISTEN_ADDR: &str = "127.0.0.1:3000";
    const SYSTEM_PROMPT: &str =
        "You are a helpful assistant. Reply in the user's language and keep answers concise.";
    const REASONING_EFFORT: ReasoningEffort = ReasoningEffort::Medium;
    const MAX_HISTORY_MESSAGES: usize = 12;
    const COMPRESSION_THRESHOLD_PERCENT: f64 = 0.5;
    const MAX_COMPRESSION_ATTEMPTS: usize = 3;
    const LOG_LEVEL: &str = "info";
    const STORAGE_DIR: &str = ".milo";
    const PROFILE_DIR: &str = ".milo/profile";

    pub(super) fn websocket_url() -> String {
        WECOM_WS_URL.to_owned()
    }

    pub(super) fn http_listen_addr() -> String {
        HTTP_LISTEN_ADDR.to_owned()
    }

    pub(super) fn policy() -> AccessPolicy {
        AccessPolicy::Open
    }

    pub(super) fn system_prompt() -> String {
        SYSTEM_PROMPT.to_owned()
    }

    pub(super) const fn reasoning_effort() -> ReasoningEffort {
        REASONING_EFFORT
    }

    pub(super) const fn max_history_messages() -> usize {
        MAX_HISTORY_MESSAGES
    }

    pub(super) const fn compression_threshold_percent() -> f64 {
        COMPRESSION_THRESHOLD_PERCENT
    }

    pub(super) const fn max_compression_attempts() -> usize {
        MAX_COMPRESSION_ATTEMPTS
    }

    pub(super) fn log_level() -> String {
        LOG_LEVEL.to_owned()
    }

    pub(super) fn storage_dir() -> PathBuf {
        PathBuf::from(STORAGE_DIR)
    }

    pub(super) fn profile_dir() -> PathBuf {
        PathBuf::from(PROFILE_DIR)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn allowlist_policy_requires_users() {
        let config = Config {
            gateway: vec![GatewayConfig::Wecom(WecomConfig {
                bot_id: "bot".into(),
                secret: "secret".into(),
                websocket_url: defaults::websocket_url(),
                dm_policy: AccessPolicy::Allowlist,
                group_policy: AccessPolicy::Open,
                dm_allow_from: vec![],
                group_allow_from: vec![],
            })],
            model: ModelConfig {
                api_key: "key".into(),
                base_url: "https://example.com/v1".into(),
                model: "model".into(),
                reasoning_effort: defaults::reasoning_effort(),
                context_length: None,
            },
            agent: AgentConfig::default(),
            log: LogConfig::default(),
            storage_dir: defaults::storage_dir(),
            profile_dir: defaults::profile_dir(),
        };

        assert!(config.validate().is_err());
    }

    #[test]
    fn parses_multiple_gateway_types() {
        let config: Config = toml::from_str(
            r#"
                [[gateway]]
                type = "wecom"
                bot_id = "bot"
                secret = "secret"

                [[gateway]]
                type = "http"
                listen_addr = "127.0.0.1:3000"
                bearer_token = "token"

                [model]
                api_key = "key"
                base_url = "https://example.com/v1"
                model = "test"
            "#,
        )
        .unwrap();

        config.validate().unwrap();
        assert!(matches!(&config.gateway[0], GatewayConfig::Wecom(_)));
        assert!(matches!(&config.gateway[1], GatewayConfig::Http(_)));
    }

    #[test]
    fn context_compression_is_optional_with_sensible_defaults() {
        let config: Config = toml::from_str(
            r#"
                [[gateway]]
                type = "http"
                bearer_token = "token"

                [model]
                api_key = "key"
                base_url = "https://example.com/v1"
                model = "test"
            "#,
        )
        .unwrap();

        config.validate().unwrap();
        assert_eq!(config.model.context_length, None);
        assert_eq!(config.model.reasoning_effort, ReasoningEffort::Medium);
        assert_eq!(config.agent.compression_threshold_percent(), 0.5);
        assert_eq!(config.agent.max_compression_attempts, 3);
    }

    #[test]
    fn rejects_invalid_context_compression_settings() {
        let mut config: Config = toml::from_str(
            r#"
                [[gateway]]
                type = "http"
                bearer_token = "token"

                [model]
                api_key = "key"
                base_url = "https://example.com/v1"
                model = "test"
            "#,
        )
        .unwrap();

        config.model.context_length = Some(0);
        assert!(config.validate().is_err());

        config.model.context_length = Some(1);
        config.agent.compression_threshold_percent = Some(1.1);
        assert!(config.validate().is_err());

        config.agent.compression_threshold_percent = Some(0.5);
        config.agent.max_compression_attempts = 0;
        assert!(config.validate().is_err());
    }
}
