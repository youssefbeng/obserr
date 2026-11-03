pub mod channel;
pub mod config;
pub mod domain;
pub mod store;
pub mod web;

use std::sync::{Arc, Mutex};

use channel::{Channel, SlackWebhook};
use config::Config;
use store::Store;

/// État partagé du serveur HTTP regroupant la configuration, la base SQLite embarquée et le `Channel` d'alerte.
#[derive(Clone)]
pub struct AppState {
    pub config: Arc<Config>,
    pub store: Arc<Mutex<Store>>,
    pub channel: Arc<dyn Channel>,
}

impl AppState {
    /// Construit l'état partagé de l'application avec l'adapter `SlackWebhook` configuré dans le TOML.
    pub fn new(config: Config, store: Store) -> Self {
        let webhook_url = config.slack_webhook_url().map(str::to_string);
        let channel = Arc::new(SlackWebhook::new(webhook_url));
        Self::with_channel(config, store, channel)
    }

    /// Construit l'état partagé avec un adapter `Channel` explicite (utile pour les tests).
    pub fn with_channel(config: Config, store: Store, channel: Arc<dyn Channel>) -> Self {
        Self {
            config: Arc::new(config),
            store: Arc::new(Mutex::new(store)),
            channel,
        }
    }
}
