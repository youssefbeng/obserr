pub mod auth;
pub mod views;

use axum::{
    Json, Router,
    extract::State,
    http::{HeaderMap, StatusCode, header},
    routing::{get, post},
};
use chrono::{DateTime, Utc};
use serde::Deserialize;

use crate::{
    AppState, IngestError,
    domain::{FinishedOutcome, IngestPayloadKind},
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
    match state.check_health() {
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

/// Décode l'en-tête Bearer et le payload JSON, puis délègue l'ingestion du `Run` à `AppState`.
async fn ingest_event(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(payload): Json<EventPayload>,
) -> Result<StatusCode, StatusCode> {
    let bearer_token = headers
        .get(header::AUTHORIZATION)
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.strip_prefix("Bearer "));

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

    state
        .ingest_event(
            bearer_token,
            payload.job,
            payload.run_id,
            payload.timestamp.unwrap_or_else(Utc::now),
            payload.external_url,
            kind,
        )
        .await
        .map_err(|err| match err {
            IngestError::Unauthorized => StatusCode::UNAUTHORIZED,
            IngestError::Internal(_) => StatusCode::INTERNAL_SERVER_ERROR,
        })?;

    Ok(StatusCode::OK)
}
