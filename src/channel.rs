use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Mutex};

use anyhow::Result;
use chrono::SecondsFormat;
use serde_json::json;

use crate::domain::IncidentNotification;
use crate::http::{HTTP_TIMEOUT, post_json};

pub type BoxFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

/// Interface de notification consommée par le moteur d'`Incidents` (`ADR-0005`).
pub trait Channel: Send + Sync {
    /// Envoie de manière asynchrone une notification d'`Incident` sur ce `Channel`.
    fn send<'a>(&'a self, notification: &'a IncidentNotification) -> BoxFuture<'a, Result<()>>;
}

#[derive(Debug, Clone)]
struct SlackEndpoint {
    webhook_url: Option<String>,
    public_url: String,
}

/// Adapter de `Channel` envoyant les alertes vers un *Incoming Webhook* Slack (compatible offre gratuite).
#[derive(Debug, Clone)]
pub struct SlackWebhook {
    endpoint: Arc<Mutex<SlackEndpoint>>,
}

impl SlackWebhook {
    /// Construit un adapter `SlackWebhook` à partir de l'URL de webhook optionnelle et de l'URL publique `obserr`.
    pub fn new(webhook_url: Option<String>, public_url: impl Into<String>) -> Self {
        Self {
            endpoint: Arc::new(Mutex::new(SlackEndpoint {
                webhook_url,
                public_url: public_url.into(),
            })),
        }
    }

    /// Met à jour à chaud la configuration de l'adapter (`webhook_url` et `public_url`) lors d'un rechargement TOML.
    pub fn reconfigure(&self, webhook_url: Option<String>, public_url: impl Into<String>) {
        *self.endpoint.lock().expect("slack webhook mutex poisoned") = SlackEndpoint {
            webhook_url,
            public_url: public_url.into(),
        };
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
    fn send<'a>(&'a self, notification: &'a IncidentNotification) -> BoxFuture<'a, Result<()>> {
        let (url_opt, public_url) = {
            let ep = self.endpoint.lock().expect("slack webhook mutex poisoned");
            (
                ep.webhook_url.clone().filter(|u| !u.is_empty()),
                ep.public_url.clone(),
            )
        };
        Box::pin(async move {
            let Some(url) = url_opt else {
                return Ok(());
            };
            let text = Self::format_message(notification, &public_url);
            let body = json!({ "text": text }).to_string();
            post_json(&url, None, &body, HTTP_TIMEOUT).await
        })
    }
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
    fn send<'a>(&'a self, notification: &'a IncidentNotification) -> BoxFuture<'a, Result<()>> {
        Box::pin(async move {
            self.notifications
                .lock()
                .expect("mutex poisoned")
                .push(notification.clone());
            Ok(())
        })
    }
}
