use std::collections::HashMap;
use std::path::Path;

use anyhow::{Context, Result};
use serde::Deserialize;

#[derive(Debug, Clone, Deserialize, Default)]
pub struct Config {
    #[serde(default)]
    pub server: ServerConfig,
    #[serde(default)]
    pub channel: ChannelConfig,
    #[serde(default)]
    pub sources: HashMap<String, SourceConfig>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct ServerConfig {
    #[serde(default = "default_listen")]
    pub listen: String,
    #[serde(default = "default_database_path")]
    pub database_path: String,
    #[serde(default = "default_public_url")]
    pub public_url: String,
}

impl Default for ServerConfig {
    fn default() -> Self {
        Self {
            listen: default_listen(),
            database_path: default_database_path(),
            public_url: default_public_url(),
        }
    }
}

#[derive(Debug, Clone, Deserialize, Default)]
pub struct ChannelConfig {
    #[serde(default)]
    pub webhook_url: Option<String>,
}

fn default_listen() -> String {
    "127.0.0.1:8080".to_string()
}

fn default_database_path() -> String {
    "obserr.db".to_string()
}

fn default_public_url() -> String {
    "http://127.0.0.1:8080".to_string()
}

#[derive(Debug, Clone, Deserialize)]
pub struct SourceConfig {
    pub token: String,
}

impl Config {
    /// Parse et valide une configuration serveur à partir d'une chaîne TOML.
    pub fn from_toml_str(content: &str) -> Result<Self> {
        toml::from_str(content).context("failed to parse TOML configuration")
    }

    /// Charge et parse la configuration TOML depuis un fichier sur disque.
    pub fn from_file(path: impl AsRef<Path>) -> Result<Self> {
        let content = std::fs::read_to_string(path.as_ref())
            .with_context(|| format!("failed to read config file {:?}", path.as_ref()))?;
        Self::from_toml_str(&content)
    }

    /// Résout le nom de la `Source` associée à un jeton Bearer secret.
    pub fn resolve_source_by_token(&self, token: &str) -> Option<String> {
        self.sources
            .iter()
            .find(|(_, src)| src.token == token)
            .map(|(name, _)| name.clone())
    }

    /// Retourne l'URL du webhook Slack configurée sur le `Channel`.
    pub fn slack_webhook_url(&self) -> Option<&str> {
        self.channel.webhook_url.as_deref()
    }
}
