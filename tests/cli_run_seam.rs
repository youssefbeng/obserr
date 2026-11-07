//! Tests d'intégration du seam CLI enveloppe : exécute le vrai binaire `obserr run` face à un serveur de test.

use std::process::{Command, Output};

use chrono::Utc;
use obserr::{AppState, config::Config, domain::RunStatus, store::Store, web};

const TOKEN: &str = "secret-dbt-token";

/// Démarre le vrai routeur `obserr` sur un port local éphémère et retourne son URL et son état partagé.
async fn start_server() -> (String, AppState) {
    let config = Config::from_toml_str(&format!(
        r#"
[sources.dbt-prod]
token = "{TOKEN}"
"#
    ))
    .expect("valid TOML config");
    let store = Store::open_in_memory().expect("in-memory SQLite store");
    let state = AppState::new(config, store);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let app = web::router(state.clone());
    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    (format!("http://{addr}"), state)
}

/// Lance le binaire `obserr run` compilé avec les arguments donnés, hors du runtime async.
async fn obserr_run(args: Vec<String>) -> Output {
    tokio::task::spawn_blocking(move || {
        Command::new(env!("CARGO_BIN_EXE_obserr"))
            .arg("run")
            .args(args)
            .env_remove("OBSERR_URL")
            .env_remove("OBSERR_TOKEN")
            .output()
            .expect("spawn obserr binary")
    })
    .await
    .unwrap()
}

fn s(v: &str) -> String {
    v.to_string()
}

fn runs_of(state: &AppState, job: &str) -> Vec<obserr::domain::Run> {
    let store = state.store.lock().unwrap();
    store
        .get_job_details("dbt-prod", job, "UTC", Utc::now())
        .unwrap()
        .map(|d| d.runs)
        .unwrap_or_default()
}

#[tokio::test(flavor = "multi_thread")]
async fn successful_command_sends_started_and_succeeded_with_same_run_and_exits_zero() {
    let (url, state) = start_server().await;

    let out = obserr_run(vec![
        s("--job"),
        s("compta_nightly"),
        s("--server"),
        url,
        s("--token"),
        s(TOKEN),
        s("--external-url"),
        s("http://scheduler.local/log/42"),
        s("--"),
        s("sh"),
        s("-c"),
        s("echo hello-from-child"),
    ])
    .await;

    assert_eq!(out.status.code(), Some(0));
    assert!(String::from_utf8_lossy(&out.stdout).contains("hello-from-child"));
    let runs = runs_of(&state, "compta_nightly");
    assert_eq!(
        runs.len(),
        1,
        "started and finished must correlate on one Run"
    );
    let run = &runs[0];
    assert_eq!(run.status, RunStatus::Succeeded);
    assert!(run.started_at.is_some());
    assert!(run.finished_at.is_some());
    assert!(run.external_run_id.is_some());
    assert_eq!(
        run.external_url.as_deref(),
        Some("http://scheduler.local/log/42")
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn failing_command_reports_last_20_stderr_lines_streams_output_and_exits_with_child_code() {
    let (url, state) = start_server().await;

    let out = obserr_run(vec![
        s("--job"),
        s("compta_nightly"),
        s("--server"),
        url,
        s("--token"),
        s(TOKEN),
        s("--"),
        s("sh"),
        s("-c"),
        s("echo out-line; for i in $(seq 1 25); do echo \"err line $i\" >&2; done; exit 2"),
    ])
    .await;

    assert_eq!(out.status.code(), Some(2));
    assert!(String::from_utf8_lossy(&out.stdout).contains("out-line"));
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(
        stderr.contains("err line 1\n"),
        "stderr must still be shown: {stderr}"
    );
    assert!(stderr.contains("err line 25\n"));

    let runs = runs_of(&state, "compta_nightly");
    assert_eq!(runs.len(), 1);
    assert_eq!(runs[0].status, RunStatus::Failed);
    let expected: Vec<String> = (6..=25).map(|i| format!("err line {i}")).collect();
    assert_eq!(
        runs[0].error_excerpt.as_deref(),
        Some(expected.join("\n").as_str())
    );
}

/// Comportement d'un faux serveur `obserr` défaillant.
#[derive(Clone, Copy)]
enum Faulty {
    Http500,
    NeverAnswers,
}

/// Démarre un faux serveur défaillant et retourne son URL et le compteur de connexions reçues.
fn start_faulty_server(mode: Faulty) -> (String, std::sync::Arc<std::sync::atomic::AtomicUsize>) {
    use std::io::{Read, Write};
    use std::sync::atomic::Ordering;

    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();
    let hits = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let counter = hits.clone();
    std::thread::spawn(move || {
        let mut held = Vec::new();
        for stream in listener.incoming() {
            let Ok(mut stream) = stream else { continue };
            counter.fetch_add(1, Ordering::SeqCst);
            match mode {
                Faulty::Http500 => {
                    let mut buf = [0u8; 4096];
                    let _ = stream.read(&mut buf);
                    let _ = stream.write_all(
                        b"HTTP/1.1 500 Internal Server Error\r\nContent-Length: 0\r\nConnection: close\r\n\r\n",
                    );
                }
                Faulty::NeverAnswers => held.push(stream),
            }
        }
    });
    (format!("http://{addr}"), hits)
}

fn closed_port_url() -> String {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();
    drop(listener);
    format!("http://{addr}")
}

fn wrapped(url: String, script: &str) -> Vec<String> {
    vec![
        s("--job"),
        s("compta_nightly"),
        s("--server"),
        url,
        s("--token"),
        s(TOKEN),
        s("--"),
        s("sh"),
        s("-c"),
        s(script),
    ]
}

#[tokio::test(flavor = "multi_thread")]
async fn unreachable_server_never_changes_the_wrapped_exit_code() {
    let ok = obserr_run(wrapped(closed_port_url(), "echo still-ran")).await;
    assert_eq!(ok.status.code(), Some(0));
    assert!(String::from_utf8_lossy(&ok.stdout).contains("still-ran"));
    assert!(String::from_utf8_lossy(&ok.stderr).contains("warning"));

    let ko = obserr_run(wrapped(closed_port_url(), "exit 3")).await;
    assert_eq!(ko.status.code(), Some(3));
}

#[tokio::test(flavor = "multi_thread")]
async fn http_500_is_retried_briefly_then_abandoned_keeping_exit_code() {
    let (url, hits) = start_faulty_server(Faulty::Http500);

    let out = obserr_run(wrapped(url, "echo still-ran; exit 4")).await;

    assert_eq!(out.status.code(), Some(4));
    assert!(String::from_utf8_lossy(&out.stdout).contains("still-ran"));
    let total = hits.load(std::sync::atomic::Ordering::SeqCst);
    assert!(
        (3..=6).contains(&total),
        "started and finished must each be retried briefly, got {total} attempts"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn server_timeout_is_abandoned_quickly_keeping_exit_code() {
    let (url, _hits) = start_faulty_server(Faulty::NeverAnswers);
    let started = std::time::Instant::now();

    let out = obserr_run(wrapped(url, "echo still-ran")).await;

    assert_eq!(out.status.code(), Some(0));
    assert!(String::from_utf8_lossy(&out.stdout).contains("still-ran"));
    assert!(
        started.elapsed() < std::time::Duration::from_secs(20),
        "a hanging server must not block the wrapper for long"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn https_server_url_is_rejected_with_warning_preserving_child_exit_code() {
    let out = obserr_run(wrapped(s("https://127.0.0.1:8443"), "echo child-ran")).await;

    assert_eq!(out.status.code(), Some(0));
    assert!(String::from_utf8_lossy(&out.stdout).contains("child-ran"));
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(
        stderr.contains("warning") && stderr.contains("HTTPS"),
        "must display an HTTPS warning on stderr: {stderr}"
    );
}
