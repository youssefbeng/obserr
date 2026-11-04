pub mod auth;
pub mod views;

use axum::{
    Json, Router,
    extract::State,
    http::{HeaderMap, StatusCode},
    routing::{get, post},
};
use chrono::{DateTime, Utc};
use serde::Deserialize;

use crate::{
    AppState,
    domain::{FinishedOutcome, IngestEvent, IngestPayloadKind},
};

/// Construit le routeur HTTP `axum` exposant `/health`, l'API d'ingestion et les pages du Dashboard.
pub fn router(state: AppState) -> Router {
    let pages = Router::new()
        .route("/", get(views::dashboard))
        .route("/incidents", get(views::incidents))
        .route("/jobs/{source}/{name}", get(views::job_detail))
        .route_layer(axum::middleware::from_fn_with_state(
            state.clone(),
            auth::require_web_password,
        ));
    Router::new()
        .merge(pages)
        .route("/health", get(health))
        .route("/api/v1/events", post(ingest_event))
        .route("/static/style.css", get(views::style_css))
        .route("/static/htmx.min.js", get(views::htmx_js))
        .with_state(state)
}

/// Vérifie la disponibilité du serveur et de la base SQLite embarquée sans authentification.
async fn health(State(state): State<AppState>) -> StatusCode {
    let store = state.store.lock().expect("store mutex poisoned");
    match store.ping() {
        Ok(()) => StatusCode::OK,
        Err(_) => StatusCode::SERVICE_UNAVAILABLE,
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "lowercase")]
enum WireEventType {
    Started,
    Finished,
}

#[derive(Debug, Deserialize)]
struct EventPayload {
    job: String,
    event: WireEventType,
    run_id: Option<String>,
    timestamp: Option<DateTime<Utc>>,
    status: Option<FinishedOutcome>,
    step: Option<String>,
    failed_step: Option<String>,
    error_excerpt: Option<String>,
    external_url: Option<String>,
}

/// Authentifie la `Source` par jeton Bearer, valide le payload JSON et persiste l'événement de `Run`.
async fn ingest_event(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(payload): Json<EventPayload>,
) -> Result<StatusCode, StatusCode> {
    let event_ts = payload.timestamp.unwrap_or_else(Utc::now);
    state.reload_if_changed(None);
    let cfg = state.current_config();
    let source = auth::authenticate_source(&headers, &cfg)?;

    let kind = match payload.event {
        WireEventType::Started => IngestPayloadKind::Started,
        WireEventType::Finished => {
            let outcome = payload.status.ok_or(StatusCode::BAD_REQUEST)?;
            IngestPayloadKind::Finished {
                outcome,
                failed_step: payload.failed_step.or(payload.step),
                error_excerpt: payload.error_excerpt,
            }
        }
    };

    let event = IngestEvent {
        source,
        job_name: payload.job,
        run_id: payload.run_id,
        timestamp: event_ts,
        external_url: payload.external_url,
        kind,
    };

    let notification = {
        let mut store = state.store.lock().expect("store mutex poisoned");
        store
            .record_event(&event)
            .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?
    };

    if let Some(alert) = notification {
        let _ = state.channel.send(&alert, &cfg.server.public_url).await;
    }

    Ok(StatusCode::OK)
}
