pub mod config;
pub mod domain;
pub mod store;
pub mod web;

use std::sync::{Arc, Mutex};

use config::Config;
use store::Store;

/// État partagé du serveur HTTP regroupant la configuration et la base SQLite embarquée.
#[derive(Clone)]
pub struct AppState {
    pub config: Arc<Config>,
    pub store: Arc<Mutex<Store>>,
}

impl AppState {
    /// Construit l'état partagé de l'application à partir d'une `Config` et d'un `Store` initialisé.
    pub fn new(config: Config, store: Store) -> Self {
        Self {
            config: Arc::new(config),
            store: Arc::new(Mutex::new(store)),
        }
    }
}
