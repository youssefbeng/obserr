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

/// Protège toutes les vues web avec le mot de passe courant, sans modifier l'authentification de l'API.
pub async fn require_web_password(
    axum::extract::State(state): axum::extract::State<crate::AppState>,
    request: axum::extract::Request,
    next: axum::middleware::Next,
) -> axum::response::Response {
    use axum::response::IntoResponse;
    use base64::Engine;

    state.reload_if_changed(None);
    let config = state.current_config();
    if let Some(password) = config.server.web_password {
        let credentials = request
            .headers()
            .get(header::AUTHORIZATION)
            .and_then(|value| value.to_str().ok())
            .and_then(|value| value.split_once(' '))
            .filter(|(scheme, _)| scheme.eq_ignore_ascii_case("Basic"))
            .and_then(|(_, encoded)| {
                base64::engine::general_purpose::STANDARD
                    .decode(encoded)
                    .ok()
            })
            .and_then(|bytes| String::from_utf8(bytes).ok());
        let valid = credentials
            .as_deref()
            .and_then(|value| value.split_once(':'))
            .is_some_and(|(_, supplied)| supplied == password);
        if !valid {
            return (
                StatusCode::UNAUTHORIZED,
                [
                    (
                        header::WWW_AUTHENTICATE,
                        "Basic realm=\"obserr\", charset=\"UTF-8\"",
                    ),
                    (header::CACHE_CONTROL, "no-store"),
                ],
                "Authentification requise",
            )
                .into_response();
        }
    }
    let mut response = next.run(request).await;
    response
        .headers_mut()
        .insert(header::CACHE_CONTROL, "no-store".parse().unwrap());
    response
        .headers_mut()
        .insert(header::VARY, "HX-Request".parse().unwrap());
    response
}
