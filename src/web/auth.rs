use axum::http::{HeaderMap, StatusCode, header};

/// Extrait le mot de passe d'un en-tête `Authorization: Basic <base64(user:password)>`.
fn extract_basic_password(headers: &HeaderMap) -> Option<String> {
    use base64::Engine;

    let credentials = headers
        .get(header::AUTHORIZATION)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.split_once(' '))
        .filter(|(scheme, _)| scheme.eq_ignore_ascii_case("Basic"))
        .and_then(|(_, encoded)| {
            base64::engine::general_purpose::STANDARD
                .decode(encoded)
                .ok()
        })
        .and_then(|bytes| String::from_utf8(bytes).ok())?;

    let (_, password) = credentials.split_once(':')?;
    Some(password.to_string())
}

/// Protège toutes les vues web avec le mot de passe courant, sans modifier l'authentification de l'API.
pub async fn require_web_password(
    axum::extract::State(state): axum::extract::State<crate::AppState>,
    request: axum::extract::Request,
    next: axum::middleware::Next,
) -> axum::response::Response {
    use axum::response::IntoResponse;

    let supplied = extract_basic_password(request.headers());
    if !state.verify_web_password(supplied.as_deref()) {
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

    let mut response = next.run(request).await;
    response
        .headers_mut()
        .insert(header::CACHE_CONTROL, "no-store".parse().unwrap());
    response
        .headers_mut()
        .insert(header::VARY, "HX-Request".parse().unwrap());
    response
}
