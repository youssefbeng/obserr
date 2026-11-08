pub mod channel;
pub mod cli;
pub mod config;
pub mod domain;
mod http;
pub mod store;
pub mod web;

use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, RwLock};

use anyhow::Result;
use chrono::{DateTime, Utc};

use channel::{Channel, SlackWebhook};
use config::Config;
use domain::{
    IncidentNotification, IngestEvent, IngestPayloadKind, JobDetails, JobWithLatestRun,
    OpenIncidentSummary,
};
use store::Store;

/// Erreur métier d'ingestion d'un événement de `Run`.
#[derive(Debug)]
pub enum IngestError {
    /// Jeton `Bearer` absent, vide ou ne correspondant à aucune `Source` déclarée.
    Unauthorized,
    /// Erreur interne de persistance SQLite.
    Internal(anyhow::Error),
}

/// Moteur applicatif partagé du serveur regroupant la configuration (rechargeable à chaud), la base SQLite et le `Channel` d'alerte.
#[derive(Clone)]
pub struct AppState {
    config: Arc<RwLock<Config>>,
    store: Arc<Mutex<Store>>,
    channel: Arc<dyn Channel>,
    slack_webhook: Option<Arc<SlackWebhook>>,
    config_path: Option<PathBuf>,
    last_toml_content: Arc<Mutex<Option<String>>>,
}

impl AppState {
    /// Construit l'état partagé de l'application avec l'adapter `SlackWebhook` configuré dans le TOML et synchronise les `Jobs` pré-déclarés.
    pub fn new(config: Config, mut store: Store) -> Self {
        let _ = store.sync_config(&config, None);
        let slack = Arc::new(SlackWebhook::new(
            config.slack_webhook_url().map(str::to_string),
            config.server.public_url.clone(),
        ));
        let channel: Arc<dyn Channel> = slack.clone();
        Self {
            config: Arc::new(RwLock::new(config)),
            store: Arc::new(Mutex::new(store)),
            channel,
            slack_webhook: Some(slack),
            config_path: None,
            last_toml_content: Arc::new(Mutex::new(None)),
        }
    }

    /// Construit l'état partagé lié à un fichier TOML sur disque pour permettre le rechargement à chaud automatique.
    pub fn from_config_file(path: impl AsRef<Path>, store: Store) -> Result<Self> {
        let path_buf = path.as_ref().to_path_buf();
        let content = std::fs::read_to_string(&path_buf)?;
        let config = Config::from_toml_str(&content)?;
        let mut state = Self::new(config, store);
        state.config_path = Some(path_buf);
        *state.last_toml_content.lock().expect("toml mutex poisoned") = Some(content);
        Ok(state)
    }

    /// Construit l'état partagé avec un adapter `Channel` explicite (utile pour les tests).
    pub fn with_channel(config: Config, mut store: Store, channel: Arc<dyn Channel>) -> Self {
        let _ = store.sync_config(&config, None);
        Self {
            config: Arc::new(RwLock::new(config)),
            store: Arc::new(Mutex::new(store)),
            channel,
            slack_webhook: None,
            config_path: None,
            last_toml_content: Arc::new(Mutex::new(None)),
        }
    }

    /// Retourne un instantané de la configuration courante du serveur.
    fn current_config(&self) -> Config {
        self.config.read().expect("config lock poisoned").clone()
    }

    /// Applique à chaud une nouvelle configuration `Config` (met à jour les `Sources`, le `Channel` et synchronise les `Jobs` en base).
    pub fn reload_config(
        &self,
        new_config: Config,
        registered_at: Option<DateTime<Utc>>,
    ) -> Result<()> {
        if let Some(ref slack) = self.slack_webhook {
            slack.reconfigure(
                new_config.slack_webhook_url().map(str::to_string),
                new_config.server.public_url.clone(),
            );
        }
        {
            let mut store = self.store.lock().expect("store mutex poisoned");
            store.sync_config(&new_config, registered_at)?;
        }
        *self.config.write().expect("config lock poisoned") = new_config;
        Ok(())
    }

    /// Vérifie si le fichier TOML sur disque a été modifié et le recharge à chaud sans interrompre le serveur.
    fn reload_if_changed(&self, registered_at: Option<DateTime<Utc>>) {
        let Some(ref path) = self.config_path else {
            return;
        };
        let Ok(content) = std::fs::read_to_string(path) else {
            return;
        };
        let changed = {
            let last = self.last_toml_content.lock().expect("toml mutex poisoned");
            last.as_deref() != Some(content.as_str())
        };
        if !changed {
            return;
        }
        if let Ok(new_config) = Config::from_toml_str(&content)
            && self.reload_config(new_config, registered_at).is_ok()
        {
            *self.last_toml_content.lock().expect("toml mutex poisoned") = Some(content);
        }
    }

    /// Authentifie la `Source` par jeton `Bearer`, persiste l'événement de `Run` et expédie toute notification d'`Incident` sur le `Channel`.
    pub async fn ingest_event(
        &self,
        bearer_token: Option<&str>,
        job_name: String,
        run_id: Option<String>,
        timestamp: DateTime<Utc>,
        external_url: Option<String>,
        kind: IngestPayloadKind,
    ) -> Result<(), IngestError> {
        self.reload_if_changed(None);
        let cfg = self.current_config();
        let token = bearer_token
            .map(str::trim)
            .filter(|t| !t.is_empty())
            .ok_or(IngestError::Unauthorized)?;
        let source = cfg
            .resolve_source_by_token(token)
            .ok_or(IngestError::Unauthorized)?;

        let event = IngestEvent {
            source,
            job_name,
            run_id,
            timestamp,
            external_url,
            kind,
        };

        let notification = {
            let mut store = self.store.lock().expect("store mutex poisoned");
            store.record_event(&event).map_err(IngestError::Internal)?
        };

        if let Some(alert) = notification {
            let _ = self.channel.send(&alert).await;
        }

        Ok(())
    }

    /// Évalue à l'instant `now` tous les `Schedules` des `Jobs`, enregistre les `Missed Runs`, purge les historiques expirés et envoie les alertes d'`Incidents`.
    pub async fn tick(&self, now: DateTime<Utc>) -> Result<Vec<IncidentNotification>> {
        self.reload_if_changed(Some(now));
        let cfg = self.current_config();
        let notifications = {
            let mut store = self.store.lock().expect("store mutex poisoned");
            store.tick(now, &cfg.server.timezone, cfg.server.retention_days)?
        };

        for alert in &notifications {
            let _ = self.channel.send(alert).await;
        }
        Ok(notifications)
    }

    /// Vérifie la disponibilité de la base SQLite embarquée (utilisé par `/health`).
    pub fn check_health(&self) -> Result<()> {
        let store = self.store.lock().expect("store mutex poisoned");
        store.ping()
    }

    /// Vérifie le mot de passe des pages web (en rechargeant à chaud le TOML si modifié).
    pub fn verify_web_password(&self, supplied_password: Option<&str>) -> bool {
        self.reload_if_changed(None);
        let cfg = self.current_config();
        match cfg.server.web_password.as_deref() {
            None => true,
            Some(expected) => supplied_password == Some(expected),
        }
    }

    /// Liste tous les `Jobs` avec leur dernier `Run`, leur `Incident` ouvert éventuel et leur prochain créneau de `Schedule`.
    pub fn list_dashboard_jobs(&self) -> Result<Vec<JobWithLatestRun>> {
        let cfg = self.current_config();
        let store = self.store.lock().expect("store mutex poisoned");
        let ref_now = store.last_tick_now().unwrap_or_else(Utc::now);
        store.list_jobs_with_latest_run(&cfg.server.timezone, ref_now)
    }

    /// Liste tous les `Incidents` ouverts avec leur dernier contexte d'erreur (`Failed Run` ou `Missed Run`).
    pub fn list_open_incidents(&self) -> Result<Vec<OpenIncidentSummary>> {
        let store = self.store.lock().expect("store mutex poisoned");
        store.list_open_incidents()
    }

    /// Récupère la vue détaillée d'un `Job` identifié par `(source, name)` et son historique complet.
    pub fn get_job_details(&self, source: &str, name: &str) -> Result<Option<JobDetails>> {
        let cfg = self.current_config();
        let store = self.store.lock().expect("store mutex poisoned");
        let ref_now = store.last_tick_now().unwrap_or_else(Utc::now);
        store.get_job_details(source, name, &cfg.server.timezone, ref_now)
    }
}
