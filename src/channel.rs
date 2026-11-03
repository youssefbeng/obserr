use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Mutex};

use anyhow::{Context, Result, anyhow};
use chrono::SecondsFormat;
use serde_json::json;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

use crate::domain::IncidentNotification;

pub type BoxFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

/// Interface de notification consommée par le moteur d'`Incidents` (`ADR-0005`).
pub trait Channel: Send + Sync {
    /// Envoie de manière asynchrone une notification d'`Incident` sur ce `Channel`.
    fn send<'a>(
        &'a self,
        notification: &'a IncidentNotification,
        public_url: &'a str,
    ) -> BoxFuture<'a, Result<()>>;
}

/// Adapter de `Channel` envoyant les alertes vers un *Incoming Webhook* Slack (compatible offre gratuite).
#[derive(Debug, Clone)]
pub struct SlackWebhook {
    webhook_url: Option<String>,
}

impl SlackWebhook {
    /// Construit un adapter `SlackWebhook` à partir de l'URL de webhook optionnelle issue de la configuration.
    pub fn new(webhook_url: Option<String>) -> Self {
        Self { webhook_url }
    }

    /// Formate le texte du message Slack d'ouverture ou de résolution d'`Incident`.
    pub fn format_message(notification: &IncidentNotification, public_url: &str) -> String {
        let base = public_url.trim_end_matches('/');
        match notification {
            IncidentNotification::Opened {
                source,
                job_name,
                cause,
                opened_at,
                failed_step,
                error_excerpt,
                external_url,
            } => {
                let job_url = format!("{base}/jobs/{source}/{job_name}");
                let ts = opened_at.to_rfc3339_opts(SecondsFormat::Secs, true);
                let mut lines = vec![
                    format!("🚨 *Incident ouvert* sur le Job `{job_name}`"),
                    format!("• *Source* : `{source}`"),
                    format!("• *Cause* : {}", cause.label()),
                    format!("• *Horodatage* : `{ts}`"),
                ];
                if let Some(step) = failed_step.as_deref().filter(|s| !s.is_empty()) {
                    lines.push(format!("• *Step en échec* : `{step}`"));
                }
                if let Some(src_url) = external_url.as_deref().filter(|s| !s.is_empty()) {
                    lines.push(format!("• *Lien Source* : {src_url}"));
                }
                lines.push(format!("• *Page obserr* : {job_url}"));
                if let Some(excerpt) = error_excerpt.as_deref().filter(|s| !s.is_empty()) {
                    lines.push(format!("```\n{excerpt}\n```"));
                }
                lines.join("\n")
            }
            IncidentNotification::Resolved {
                source,
                job_name,
                duration_seconds,
                ..
            } => {
                let job_url = format!("{base}/jobs/{source}/{job_name}");
                format!(
                    "✅ Résolu — Incident fermé sur le Job `{job_name}` (Source `{source}`) après {}s ({})\n• *Page obserr* : {job_url}",
                    duration_seconds,
                    format_human_duration(*duration_seconds)
                )
            }
        }
    }
}

/// Formate une durée en secondes sous forme lisible (ex. `15m 0s` ou `45s`).
fn format_human_duration(seconds: i64) -> String {
    let secs = seconds.max(0);
    if secs >= 3600 {
        format!("{}h {}m {}s", secs / 3600, (secs % 3600) / 60, secs % 60)
    } else if secs >= 60 {
        format!("{}m {}s", secs / 60, secs % 60)
    } else {
        format!("{secs}s")
    }
}

impl Channel for SlackWebhook {
    fn send<'a>(
        &'a self,
        notification: &'a IncidentNotification,
        public_url: &'a str,
    ) -> BoxFuture<'a, Result<()>> {
        Box::pin(async move {
            let Some(url) = self.webhook_url.as_deref().filter(|u| !u.is_empty()) else {
                return Ok(());
            };
            let text = Self::format_message(notification, public_url);
            let body = json!({ "text": text }).to_string();
            post_json_webhook(url, &body).await
        })
    }
}

/// Envoie une requête HTTP `POST` JSON vers l'URL de webhook fournie.
async fn post_json_webhook(url: &str, body: &str) -> Result<()> {
    let (host, port, path) = parse_http_url(url)?;

    let mut stream = TcpStream::connect((host, port))
        .await
        .with_context(|| format!("failed to connect to webhook at {host}:{port}"))?;

    let req = format!(
        "POST {path} HTTP/1.1\r\n\
         Host: {host}:{port}\r\n\
         Content-Type: application/json\r\n\
         Content-Length: {}\r\n\
         Connection: close\r\n\r\n\
         {body}",
        body.len()
    );
    stream.write_all(req.as_bytes()).await?;

    let mut resp = Vec::new();
    stream.read_to_end(&mut resp).await?;
    let status_line = String::from_utf8_lossy(&resp);
    if !status_line.starts_with("HTTP/1.1 2") && !status_line.starts_with("HTTP/1.0 2") {
        return Err(anyhow!("webhook returned non-2xx response: {status_line}"));
    }
    Ok(())
}

/// Extrait `(host, port, path)` d'une URL HTTP sans dépendre du crate `axum`.
fn parse_http_url(url: &str) -> Result<(&str, u16, &str)> {
    let rest = url
        .strip_prefix("http://")
        .or_else(|| url.strip_prefix("https://"))
        .ok_or_else(|| anyhow!("webhook URL must start with http:// or https://"))?;

    let (authority, path) = match rest.find('/') {
        Some(idx) => (&rest[..idx], &rest[idx..]),
        None => (rest, "/"),
    };

    let (host, port) = match authority.rsplit_once(':') {
        Some((h, p)) => (h, p.parse::<u16>().context("invalid webhook port")?),
        None => (authority, 80),
    };

    if host.is_empty() {
        return Err(anyhow!("webhook URL missing host"));
    }
    Ok((host, port, path))
}

/// Adapter de `Channel` en mémoire enregistrant les notifications émises pour les tests.
#[derive(Debug, Clone, Default)]
pub struct MemoryChannel {
    notifications: Arc<Mutex<Vec<IncidentNotification>>>,
}

impl MemoryChannel {
    /// Crée un nouveau `MemoryChannel` vide.
    pub fn new() -> Self {
        Self::default()
    }

    /// Retourne la liste des notifications enregistrées jusqu'ici.
    pub fn recorded(&self) -> Vec<IncidentNotification> {
        self.notifications.lock().expect("mutex poisoned").clone()
    }
}

impl Channel for MemoryChannel {
    fn send<'a>(
        &'a self,
        notification: &'a IncidentNotification,
        _public_url: &'a str,
    ) -> BoxFuture<'a, Result<()>> {
        Box::pin(async move {
            self.notifications
                .lock()
                .expect("mutex poisoned")
                .push(notification.clone());
            Ok(())
        })
    }
}
