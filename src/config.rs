use std::{collections::HashSet, fs, net::SocketAddr, path::Path};

use anyhow::{Context, Result, bail};
use serde::Deserialize;

const DEFAULT_WECOM_WS_URL: &str = "wss://openws.work.weixin.qq.com";

#[derive(Debug, Clone, Deserialize)]
pub struct Config {
    #[serde(default)]
    pub gateway: Vec<GatewayConfig>,
    pub model: ModelConfig,
    #[serde(default)]
    pub agent: AgentConfig,
    #[serde(default)]
    pub log: LogConfig,
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
    #[serde(default = "default_websocket_url")]
    pub websocket_url: String,
    #[serde(default = "default_policy")]
    pub dm_policy: AccessPolicy,
    #[serde(default = "default_policy")]
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
}

#[derive(Debug, Clone, Deserialize)]
pub struct HttpConfig {
    #[serde(default = "default_http_listen_addr")]
    pub listen_addr: String,
    pub bearer_token: String,
}

#[derive(Debug, Clone, Deserialize)]
pub struct AgentConfig {
    #[serde(default = "default_system_prompt")]
    pub system_prompt: String,
    #[serde(default = "default_max_history_messages")]
    pub max_history_messages: usize,
}

#[derive(Debug, Clone, Deserialize)]
pub struct LogConfig {
    #[serde(default = "default_log_level")]
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
        let config: Self = toml::from_str(&content)
            .with_context(|| format!("parse configuration {}", path.display()))?;
        config.validate()?;
        Ok(config)
    }

    fn validate(&self) -> Result<()> {
        required("model.api_key", &self.model.api_key)?;
        required("model.base_url", &self.model.base_url)?;
        required("model.model", &self.model.model)?;

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
            system_prompt: default_system_prompt(),
            max_history_messages: default_max_history_messages(),
        }
    }
}

impl Default for LogConfig {
    fn default() -> Self {
        Self {
            level: default_log_level(),
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

fn default_websocket_url() -> String {
    DEFAULT_WECOM_WS_URL.to_owned()
}

fn default_http_listen_addr() -> String {
    "127.0.0.1:3000".to_owned()
}

fn default_policy() -> AccessPolicy {
    AccessPolicy::Open
}

fn default_system_prompt() -> String {
    "You are a helpful assistant. Reply in the user's language and keep answers concise.".to_owned()
}

fn default_max_history_messages() -> usize {
    12
}

fn default_log_level() -> String {
    "info".to_owned()
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
                websocket_url: default_websocket_url(),
                dm_policy: AccessPolicy::Allowlist,
                group_policy: AccessPolicy::Open,
                dm_allow_from: vec![],
                group_allow_from: vec![],
            })],
            model: ModelConfig {
                api_key: "key".into(),
                base_url: "https://example.com/v1".into(),
                model: "model".into(),
            },
            agent: AgentConfig::default(),
            log: LogConfig::default(),
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
}
