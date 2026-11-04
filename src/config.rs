use std::collections::HashMap;
use std::path::Path;

use anyhow::{Context, Result, anyhow};
use serde::Deserialize;

use crate::domain::{Schedule, parse_timezone};

#[derive(Debug, Clone, Default)]
pub struct Config {
    pub server: ServerConfig,
    pub channel: ChannelConfig,
    pub sources: HashMap<String, SourceConfig>,
    pub jobs: Vec<JobConfig>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct ServerConfig {
    #[serde(default)]
    pub web_password: Option<String>,
    #[serde(default = "default_retention_days")]
    pub retention_days: u32,
    #[serde(default = "default_listen")]
    pub listen: String,
    #[serde(default = "default_database_path")]
    pub database_path: String,
    #[serde(default = "default_public_url")]
    pub public_url: String,
    #[serde(default = "default_timezone")]
    pub timezone: String,
}

impl Default for ServerConfig {
    fn default() -> Self {
        Self {
            web_password: None,
            retention_days: default_retention_days(),
            listen: default_listen(),
            database_path: default_database_path(),
            public_url: default_public_url(),
            timezone: default_timezone(),
        }
    }
}

#[derive(Debug, Clone, Deserialize, Default)]
pub struct ChannelConfig {
    #[serde(default)]
    pub webhook_url: Option<String>,
}

fn default_retention_days() -> u32 {
    90
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

fn default_timezone() -> String {
    "UTC".to_string()
}

#[derive(Debug, Clone, Deserialize)]
pub struct SourceConfig {
    pub token: String,
}

/// Configuration déclarative d'un `Job` dans le fichier TOML (`Schedule`, fuseau horaire, propriétaire et `tags`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct JobConfig {
    pub source: String,
    pub name: String,
    pub schedule_cron: Option<String>,
    pub grace_seconds: Option<i64>,
    pub timezone: Option<String>,
    pub owner: Option<String>,
    pub tags: Vec<String>,
}

#[derive(Debug, Deserialize)]
struct RawConfig {
    #[serde(default)]
    server: ServerConfig,
    #[serde(default)]
    channel: ChannelConfig,
    #[serde(default)]
    sources: HashMap<String, SourceConfig>,
    #[serde(default)]
    jobs: HashMap<String, HashMap<String, RawJobBody>>,
}

#[derive(Debug, Default, Deserialize)]
struct RawJobBody {
    #[serde(default)]
    schedule: Option<String>,
    #[serde(default)]
    grace: Option<String>,
    #[serde(default)]
    timezone: Option<String>,
    #[serde(default)]
    owner: Option<String>,
    #[serde(default)]
    tags: Vec<String>,
}

impl RawJobBody {
    fn into_job_config(self, source: String, name: String) -> Result<JobConfig> {
        let parsed_grace = match self.grace.as_deref() {
            Some(g) => Some(parse_duration_seconds(g)?),
            None => None,
        };

        let grace_seconds = match (&self.schedule, parsed_grace) {
            (Some(_), Some(g)) => Some(g),
            (Some(_), None) => Some(0),
            (None, _) => None,
        };

        if let Some(ref cron_expr) = self.schedule {
            Schedule::parse_cron(cron_expr)
                .with_context(|| format!("invalid cron expression for job {source}/{name}"))?;
        }
        if let Some(ref tz) = self.timezone {
            parse_timezone(tz)
                .with_context(|| format!("invalid timezone '{tz}' for job {source}/{name}"))?;
        }

        Ok(JobConfig {
            source,
            name,
            schedule_cron: self.schedule,
            grace_seconds,
            timezone: self.timezone,
            owner: self.owner,
            tags: self.tags,
        })
    }
}

/// Parse une durée textuelle (ex. `"30m"`, `"15m"`, `"1h"`, `"45s"`, `"1h30m"`) en nombre de secondes.
pub fn parse_duration_seconds(input: &str) -> Result<i64> {
    let trimmed = input.trim();
    if trimmed.is_empty() {
        return Err(anyhow!("empty duration string"));
    }
    if let Ok(plain_secs) = trimmed.parse::<i64>() {
        return Ok(plain_secs.max(0));
    }

    let mut total_seconds: i64 = 0;
    let mut num_buf = String::new();

    for ch in trimmed.chars() {
        if ch.is_ascii_digit() {
            num_buf.push(ch);
        } else if ch.is_ascii_whitespace() {
            continue;
        } else {
            if num_buf.is_empty() {
                return Err(anyhow!("invalid duration '{input}'"));
            }
            let val: i64 = num_buf
                .parse()
                .with_context(|| format!("invalid number in duration '{input}'"))?;
            num_buf.clear();
            let multiplier = match ch {
                's' | 'S' => 1,
                'm' | 'M' => 60,
                'h' | 'H' => 3600,
                'd' | 'D' => 86400,
                _ => return Err(anyhow!("unsupported duration unit '{ch}' in '{input}'")),
            };
            total_seconds += val * multiplier;
        }
    }

    if !num_buf.is_empty() {
        let val: i64 = num_buf
            .parse()
            .with_context(|| format!("invalid trailing number in duration '{input}'"))?;
        total_seconds += val;
    }

    Ok(total_seconds.max(0))
}

/// Formate un délai de tolérance en secondes sous forme compacte (`30m`, `1h`, `45s`).
pub fn format_grace_duration(seconds: i64) -> String {
    let s = seconds.max(0);
    if s > 0 && s % 3600 == 0 {
        format!("{}h", s / 3600)
    } else if s > 0 && s % 60 == 0 {
        format!("{}m", s / 60)
    } else {
        format!("{s}s")
    }
}

impl Config {
    /// Parse et valide une configuration serveur à partir d'une chaîne TOML.
    pub fn from_toml_str(content: &str) -> Result<Self> {
        let raw: RawConfig =
            toml::from_str(content).context("failed to parse TOML configuration")?;

        parse_timezone(&raw.server.timezone)
            .with_context(|| format!("invalid global server timezone '{}'", raw.server.timezone))?;

        let mut jobs = Vec::new();
        let mut source_keys: Vec<_> = raw.jobs.into_iter().collect();
        source_keys.sort_by(|a, b| a.0.cmp(&b.0));
        for (source, by_name) in source_keys {
            let mut name_keys: Vec<_> = by_name.into_iter().collect();
            name_keys.sort_by(|a, b| a.0.cmp(&b.0));
            for (name, body) in name_keys {
                jobs.push(body.into_job_config(source.clone(), name)?);
            }
        }

        Ok(Self {
            server: raw.server,
            channel: raw.channel,
            sources: raw.sources,
            jobs,
        })
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
