use axum::http::{HeaderMap, StatusCode, header};

use crate::config::Config;

/// Extrait le jeton `Authorization: Bearer <token>` et résout la `Source` correspondante (`401` si invalide).
pub fn authenticate_source(headers: &HeaderMap, config: &Config) -> Result<String, StatusCode> {
    let auth_header = headers
        .get(header::AUTHORIZATION)
        .and_then(|v| v.to_str().ok())
        .ok_or(StatusCode::UNAUTHORIZED)?;

    let token = auth_header
        .strip_prefix("Bearer ")
        .map(str::trim)
        .filter(|t| !t.is_empty())
        .ok_or(StatusCode::UNAUTHORIZED)?;

    config
        .resolve_source_by_token(token)
        .ok_or(StatusCode::UNAUTHORIZED)
}
