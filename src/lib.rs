pub mod channel;
pub mod cli;
pub mod config;
pub mod domain;
pub mod store;
pub mod web;

use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, RwLock};

use anyhow::Result;
use chrono::{DateTime, Utc};

use channel::{Channel, SlackWebhook};
use config::Config;
use domain::IncidentNotification;
use store::Store;

/// État partagé du serveur HTTP regroupant la configuration (rechargeable à chaud), la base SQLite et le `Channel` d'alerte.
#[derive(Clone)]
pub struct AppState {
    pub config: Arc<RwLock<Config>>,
    pub store: Arc<Mutex<Store>>,
    pub channel: Arc<dyn Channel>,
    config_path: Option<PathBuf>,
    last_toml_content: Arc<Mutex<Option<String>>>,
}

impl AppState {
    /// Construit l'état partagé de l'application avec l'adapter `SlackWebhook` configuré dans le TOML et synchronise les `Jobs` pré-déclarés.
    pub fn new(config: Config, store: Store) -> Self {
        let webhook_url = config.slack_webhook_url().map(str::to_string);
        let channel: Arc<dyn Channel> = Arc::new(SlackWebhook::new(webhook_url));
        Self::with_channel(config, store, channel)
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
            config_path: None,
            last_toml_content: Arc::new(Mutex::new(None)),
        }
    }

    /// Retourne un instantané de la configuration courante du serveur.
    pub fn current_config(&self) -> Config {
        self.config.read().expect("config lock poisoned").clone()
    }

    /// Applique à chaud une nouvelle configuration `Config` (met à jour les `Sources`, le `Channel` et synchronise les `Jobs` en base).
    pub fn reload_config(
        &self,
        new_config: Config,
        registered_at: Option<DateTime<Utc>>,
    ) -> Result<()> {
        self.channel
            .update_webhook_url(new_config.slack_webhook_url().map(str::to_string));
        {
            let mut store = self.store.lock().expect("store mutex poisoned");
            store.sync_config(&new_config, registered_at)?;
        }
        *self.config.write().expect("config lock poisoned") = new_config;
        Ok(())
    }

    /// Vérifie si le fichier TOML sur disque a été modifié et le recharge à chaud sans interrompre le serveur.
    pub fn reload_if_changed(&self, registered_at: Option<DateTime<Utc>>) {
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

    /// Évalue à l'instant `now` tous les `Schedules` des `Jobs`, enregistre les `Missed Runs`, purge les historiques expirés et envoie les alertes d'`Incidents`.
    pub async fn tick(&self, now: DateTime<Utc>) -> Result<Vec<IncidentNotification>> {
        self.reload_if_changed(Some(now));
        let cfg = self.current_config();
        let notifications = {
            let mut store = self.store.lock().expect("store mutex poisoned");
            store.tick(now, &cfg.server.timezone, cfg.server.retention_days)?
        };

        for alert in &notifications {
            let _ = self.channel.send(alert, &cfg.server.public_url).await;
        }
        Ok(notifications)
    }
}
