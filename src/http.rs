//! Client HTTP/1.1 minimal partagé par l'adapter `SlackWebhook` (`channel`) et le CLI `obserr run` (`cli`).

use std::time::Duration;

use anyhow::{Context, Result, anyhow};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

/// Délai maximal d'une tentative d'envoi HTTP/1.1 (connexion, écriture et lecture de la réponse).
pub const HTTP_TIMEOUT: Duration = Duration::from_secs(2);

/// Envoie une requête HTTP/1.1 `POST` JSON (avec jeton Bearer optionnel) bornée par `timeout` et vérifie un statut `2xx`.
pub async fn post_json(
    url: &str,
    bearer_token: Option<&str>,
    body: &str,
    timeout: Duration,
) -> Result<()> {
    tokio::time::timeout(timeout, post_json_inner(url, bearer_token, body))
        .await
        .map_err(|_| anyhow!("timed out after {timeout:?}"))?
}

async fn post_json_inner(url: &str, bearer_token: Option<&str>, body: &str) -> Result<()> {
    let (host, port, path) = parse_http_url(url)?;
    let mut stream = TcpStream::connect((host, port))
        .await
        .with_context(|| format!("failed to connect to {host}:{port}"))?;

    let auth_header = match bearer_token {
        Some(token) => format!("Authorization: Bearer {token}\r\n"),
        None => String::new(),
    };
    let req = format!(
        "POST {path} HTTP/1.1\r\n\
         Host: {host}:{port}\r\n\
         {auth_header}\
         Content-Type: application/json\r\n\
         Content-Length: {}\r\n\
         Connection: close\r\n\r\n\
         {body}",
        body.len()
    );
    stream.write_all(req.as_bytes()).await?;

    let mut resp = Vec::new();
    stream.read_to_end(&mut resp).await?;
    let head = String::from_utf8_lossy(&resp);
    let status_line = head.lines().next().unwrap_or_default();
    if !status_line.starts_with("HTTP/1.1 2") && !status_line.starts_with("HTTP/1.0 2") {
        return Err(anyhow!("server answered `{status_line}`"));
    }
    Ok(())
}

/// Extrait `(host, port, path)` d'une URL `http://` et rejette explicitement `https://`.
pub fn parse_http_url(url: &str) -> Result<(&str, u16, &str)> {
    if url.starts_with("https://") {
        return Err(anyhow!(
            "HTTPS URLs are not supported directly; please use http:// or an HTTP proxy"
        ));
    }
    let rest = url
        .strip_prefix("http://")
        .ok_or_else(|| anyhow!("URL must start with http://"))?;

    let (authority, path) = match rest.find('/') {
        Some(idx) => (&rest[..idx], &rest[idx..]),
        None => (rest, "/"),
    };

    let (host, port) = match authority.rsplit_once(':') {
        Some((h, p)) => (h, p.parse::<u16>().context("invalid port")?),
        None => (authority, 80),
    };

    if host.is_empty() {
        return Err(anyhow!("URL missing host"));
    }
    Ok((host, port, path))
}
