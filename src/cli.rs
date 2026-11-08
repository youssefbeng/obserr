//! CLI enveloppe `obserr run` : exécute une commande enfant (typiquement dbt Core), pousse les événements
//! `started` / `finished` vers `obserr serve` et renvoie toujours le code de sortie exact de l'enfant (ADR-0004).

use std::collections::VecDeque;
use std::process::{ExitStatus, Stdio};
use std::time::Duration;

use anyhow::{Context, Result, anyhow};
use chrono::{SecondsFormat, Utc};
use serde_json::{Value, json};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::process::Command;

use crate::http::{HTTP_TIMEOUT, post_json};

/// Paramètres de la sous-commande `obserr run`, déjà résolus depuis les options CLI ou l'environnement.
#[derive(Debug, Clone)]
pub struct RunOptions {
    pub job: String,
    pub server: Option<String>,
    pub token: Option<String>,
    pub external_url: Option<String>,
    pub command: Vec<String>,
}

/// Enveloppe la commande enfant, rapporte son `Run` au serveur `obserr` en *fail-open* et renvoie son code de sortie exact.
pub async fn run(opts: RunOptions) -> i32 {
    let reporter = Reporter::new(&opts);
    let run_id = uuid::Uuid::new_v4().to_string();

    reporter.send(&reporter.started_payload(&run_id)).await;

    let (exit_code, excerpt) = match spawn_and_wait(&opts.command).await {
        Ok(result) => result,
        Err(err) => {
            let message = format!("obserr: {err:#}");
            eprintln!("{message}");
            (127, message)
        }
    };

    reporter
        .send(&reporter.finished_payload(&run_id, exit_code, Some(excerpt)))
        .await;
    exit_code
}

/// Lance la commande enfant (`stdout` hérité, `stderr` recopié en direct) et retourne son code de sortie
/// ainsi que l'extrait des dernières lignes de `stderr`.
async fn spawn_and_wait(command: &[String]) -> Result<(i32, String)> {
    let (program, args) = command
        .split_first()
        .ok_or_else(|| anyhow!("no command given after `--`"))?;
    let mut child = Command::new(program)
        .args(args)
        .stdin(Stdio::inherit())
        .stdout(Stdio::inherit())
        .stderr(Stdio::piped())
        .spawn()
        .with_context(|| format!("failed to start command `{program}`"))?;

    // Laisse au processus enfant la gestion du premier Ctrl+C et survit pour rapporter `finished`.
    // Un second Ctrl+C force l'arrêt immédiat du wrapper pour éviter tout blocage.
    tokio::spawn(async {
        if tokio::signal::ctrl_c().await.is_ok() && tokio::signal::ctrl_c().await.is_ok() {
            std::process::exit(130);
        }
    });

    let mut child_stderr = child.stderr.take().context("child stderr not captured")?;
    let mut own_stderr = tokio::io::stderr();
    let mut tail = StderrTail::new(EXCERPT_LINES);
    let mut buf = [0u8; 8192];
    loop {
        let n = match child_stderr.read(&mut buf).await {
            Ok(0) | Err(_) => break,
            Ok(n) => n,
        };
        let _ = own_stderr.write_all(&buf[..n]).await;
        let _ = own_stderr.flush().await;
        tail.push(&buf[..n]);
    }

    let status = child.wait().await?;
    Ok((exit_code_of(status), tail.excerpt()))
}

/// Nombre de lignes de `stderr` conservées comme extrait d'erreur d'un `Failed Run`.
const EXCERPT_LINES: usize = 20;

/// Taille maximale conservée par ligne pour borner la mémoire face à une sortie sans saut de ligne.
const MAX_LINE_BYTES: usize = 4096;

/// Tampon circulaire conservant les `capacity` dernières lignes d'un flux d'octets découpé à la volée.
struct StderrTail {
    capacity: usize,
    lines: VecDeque<Vec<u8>>,
    partial: Vec<u8>,
}

impl StderrTail {
    fn new(capacity: usize) -> Self {
        Self {
            capacity,
            lines: VecDeque::with_capacity(capacity + 1),
            partial: Vec::new(),
        }
    }

    /// Ajoute un morceau brut de `stderr` en ne gardant en mémoire que les dernières lignes complètes.
    fn push(&mut self, chunk: &[u8]) {
        for &byte in chunk {
            if byte == b'\n' {
                let line = std::mem::take(&mut self.partial);
                self.push_line(line);
            } else if self.partial.len() < MAX_LINE_BYTES {
                self.partial.push(byte);
            }
        }
    }

    fn push_line(&mut self, mut line: Vec<u8>) {
        if line.last() == Some(&b'\r') {
            line.pop();
        }
        self.lines.push_back(line);
        if self.lines.len() > self.capacity {
            self.lines.pop_front();
        }
    }

    /// Retourne les dernières lignes (y compris une ligne finale sans saut de ligne) jointes par `\n`.
    fn excerpt(mut self) -> String {
        if !self.partial.is_empty() {
            let line = std::mem::take(&mut self.partial);
            self.push_line(line);
        }
        let lines: Vec<String> = self
            .lines
            .iter()
            .map(|l| String::from_utf8_lossy(l).into_owned())
            .collect();
        lines.join("\n")
    }
}

/// Traduit le statut du processus enfant en code de sortie shell (`128 + signal` si tué par un signal).
fn exit_code_of(status: ExitStatus) -> i32 {
    if let Some(code) = status.code() {
        return code;
    }
    #[cfg(unix)]
    {
        use std::os::unix::process::ExitStatusExt;
        if let Some(sig) = status.signal() {
            return 128 + sig;
        }
    }
    1
}

/// Attente avant chaque tentative : une tentative immédiate puis deux réessais rapides (ADR-0004).
const RETRY_BACKOFFS: [Duration; 3] = [
    Duration::ZERO,
    Duration::from_millis(200),
    Duration::from_millis(500),
];

/// Client HTTP *fail-open* qui pousse les événements de `Run` vers l'API d'ingestion d'`obserr serve`.
struct Reporter {
    endpoint: Option<(String, String)>,
    job: String,
    external_url: Option<String>,
}

impl Reporter {
    /// Prépare le client ; sans URL de serveur ou jeton, les événements sont ignorés avec un avertissement.
    fn new(opts: &RunOptions) -> Self {
        let server = opts.server.clone().filter(|s| !s.is_empty());
        let token = opts.token.clone().filter(|s| !s.is_empty());
        let endpoint = match (server, token) {
            (Some(server), Some(token)) => Some((
                format!("{}/api/v1/events", server.trim_end_matches('/')),
                token,
            )),
            _ => {
                eprintln!(
                    "obserr: warning: --server/OBSERR_URL or --token/OBSERR_TOKEN missing, Run not reported"
                );
                None
            }
        };
        Self {
            endpoint,
            job: opts.job.clone(),
            external_url: opts.external_url.clone(),
        }
    }

    /// Construit le payload `started` du `Run` identifié par `run_id`.
    fn started_payload(&self, run_id: &str) -> Value {
        let mut payload = json!({
            "job": self.job,
            "event": "started",
            "run_id": run_id,
            "timestamp": now_rfc3339(),
        });
        if let Some(url) = &self.external_url {
            payload["external_url"] = json!(url);
        }
        payload
    }

    /// Construit le payload `finished` (`succeeded` si code `0`, `failed` sinon avec l'extrait de `stderr`).
    fn finished_payload(
        &self,
        run_id: &str,
        exit_code: i32,
        error_excerpt: Option<String>,
    ) -> Value {
        let mut payload = self.started_payload(run_id);
        payload["event"] = json!("finished");
        if exit_code == 0 {
            payload["status"] = json!("succeeded");
        } else {
            payload["status"] = json!("failed");
            if let Some(excerpt) = error_excerpt.filter(|e| !e.is_empty()) {
                payload["error_excerpt"] = json!(excerpt);
            }
        }
        payload
    }

    /// Envoie un événement ; en cas d'échec, écrit un avertissement sur `stderr` sans jamais propager l'erreur.
    async fn send(&self, payload: &Value) {
        let Some((url, token)) = &self.endpoint else {
            return;
        };
        let body = payload.to_string();
        let mut last_err = None;
        for (attempt, backoff) in RETRY_BACKOFFS.iter().enumerate() {
            if attempt > 0 {
                tokio::time::sleep(*backoff).await;
            }
            match post_json(url, Some(token), &body, HTTP_TIMEOUT).await {
                Ok(()) => return,
                Err(err) => last_err = Some(err),
            }
        }
        if let Some(err) = last_err {
            eprintln!("obserr: warning: could not report event to obserr server: {err:#}");
        }
    }
}

fn now_rfc3339() -> String {
    Utc::now().to_rfc3339_opts(SecondsFormat::Millis, true)
}
