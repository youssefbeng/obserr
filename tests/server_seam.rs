use axum::body::Body;
use axum::http::{Request, StatusCode};
use http_body_util::BodyExt;
use obserr::{AppState, config::Config, store::Store, web};
use tower::ServiceExt;

async fn body_string(res: axum::response::Response) -> String {
    let bytes = res.into_body().collect().await.unwrap().to_bytes();
    String::from_utf8(bytes.to_vec()).unwrap()
}

fn test_app() -> axum::Router {
    let config = Config::from_toml_str(
        r#"
[sources.dbt-prod]
token = "secret-dbt-token"

[sources.airflow-prod]
token = "secret-airflow-token"
"#,
    )
    .expect("valid TOML config");
    let store = Store::open_in_memory().expect("in-memory SQLite store");
    web::router(AppState::new(config, store))
}

#[tokio::test]
async fn health_endpoint_returns_200_ok_without_auth() {
    let app = test_app();

    let response = app
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/health")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
}

#[tokio::test]
async fn ingest_rejects_missing_or_invalid_bearer_token_with_401() {
    let app = test_app();

    let missing_auth = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/v1/events")
                .header("content-type", "application/json")
                .body(Body::from(r#"{"job":"daily_orders","event":"started"}"#))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(missing_auth.status(), StatusCode::UNAUTHORIZED);

    let invalid_token = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/v1/events")
                .header("content-type", "application/json")
                .header("authorization", "Bearer wrong-token")
                .body(Body::from(r#"{"job":"daily_orders","event":"started"}"#))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(invalid_token.status(), StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn started_then_finished_creates_job_and_correlates_single_run_on_dashboard_and_detail() {
    let app = test_app();

    // 1. Send `started` event authenticated as `dbt-prod`
    let started_res = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/v1/events")
                .header("content-type", "application/json")
                .header("authorization", "Bearer secret-dbt-token")
                .body(Body::from(
                    r#"{
                        "job": "compta_nightly",
                        "event": "started",
                        "run_id": "run-001",
                        "timestamp": "2026-11-03T02:00:00Z"
                    }"#,
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(started_res.status(), StatusCode::OK);

    // 2. Send `finished` event with the same `run_id`
    let finished_res = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/v1/events")
                .header("content-type", "application/json")
                .header("authorization", "Bearer secret-dbt-token")
                .body(Body::from(
                    r#"{
                        "job": "compta_nightly",
                        "event": "finished",
                        "run_id": "run-001",
                        "status": "succeeded",
                        "timestamp": "2026-11-03T02:02:05Z",
                        "external_url": "https://ci.internal/runs/001"
                    }"#,
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(finished_res.status(), StatusCode::OK);

    // 3. Dashboard (`GET /`) lists the Job `(dbt-prod, compta_nightly)` and its latest Run summary
    let dashboard_res = app
        .clone()
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(dashboard_res.status(), StatusCode::OK);
    let dashboard_html = body_string(dashboard_res).await;
    assert!(dashboard_html.contains("dbt-prod"));
    assert!(dashboard_html.contains("compta_nightly"));
    assert!(dashboard_html.contains("succeeded"));
    assert!(dashboard_html.contains("/jobs/dbt-prod/compta_nightly"));

    // 4. Job detail page (`GET /jobs/dbt-prod/compta_nightly`) shows a single correlated Run with start, finish, duration (125s), status and external link
    let detail_res = app
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/jobs/dbt-prod/compta_nightly")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(detail_res.status(), StatusCode::OK);
    let detail_html = body_string(detail_res).await;
    assert_eq!(detail_html.matches("run-001").count(), 1);
    assert!(detail_html.contains("2026-11-03T02:00:00Z"));
    assert!(detail_html.contains("2026-11-03T02:02:05Z"));
    assert!(detail_html.contains("125s"));
    assert!(detail_html.contains("succeeded"));
    assert!(detail_html.contains("https://ci.internal/runs/001"));
}

#[tokio::test]
async fn failed_run_records_step_and_truncates_error_excerpt_to_20_lines() {
    let app = test_app();

    let twenty_five_lines = (1..=25)
        .map(|i| format!("err-line-{i:02}"))
        .collect::<Vec<_>>()
        .join("\n");

    let payload = serde_json::json!({
        "job": "finance_dag",
        "event": "finished",
        "run_id": "dagrun-42",
        "status": "failed",
        "step": "load_ledger_task",
        "error_excerpt": twenty_five_lines,
        "external_url": "https://airflow.internal/dags/finance_dag/grid?dag_run_id=dagrun-42",
        "timestamp": "2026-11-03T04:15:00Z"
    });

    let res = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/v1/events")
                .header("content-type", "application/json")
                .header("authorization", "Bearer secret-airflow-token")
                .body(Body::from(payload.to_string()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::OK);

    let detail_res = app
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/jobs/airflow-prod/finance_dag")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(detail_res.status(), StatusCode::OK);
    let html = body_string(detail_res).await;

    assert!(html.contains("failed"));
    assert!(html.contains("load_ledger_task"));
    assert!(html.contains("https://airflow.internal/dags/finance_dag/grid?dag_run_id=dagrun-42"));
    assert!(!html.contains("err-line-05"), "first 5 lines should be truncated");
    assert!(html.contains("err-line-06"), "line 6 (first of last 20) should be kept");
    assert!(html.contains("err-line-25"), "line 25 should be kept");
    assert!(html.contains("Failed Runs : 1"));
}

#[tokio::test]
async fn finished_event_alone_creates_job_and_records_run_without_start_timestamp() {
    let app = test_app();

    let res = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/v1/events")
                .header("content-type", "application/json")
                .header("authorization", "Bearer secret-dbt-token")
                .body(Body::from(
                    r#"{
                        "job": "callback_only_job",
                        "event": "finished",
                        "status": "succeeded",
                        "timestamp": "2026-11-03T06:30:00Z"
                    }"#,
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::OK);

    let detail_res = app
        .clone()
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/jobs/dbt-prod/callback_only_job")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(detail_res.status(), StatusCode::OK);
    let html = body_string(detail_res).await;

    assert!(html.contains("callback_only_job"));
    assert!(html.contains("succeeded"));
    assert!(html.contains("<td>-</td>\n        <td>2026-11-03T06:30:00Z</td>"));
    assert!(html.contains("Failed Runs : 0"));

    // A `finished` event missing the required `status` field is rejected with 400 Bad Request
    let missing_status_res = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/v1/events")
                .header("content-type", "application/json")
                .header("authorization", "Bearer secret-dbt-token")
                .body(Body::from(
                    r#"{"job":"callback_only_job","event":"finished"}"#,
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(missing_status_res.status(), StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn new_started_event_marks_previous_running_run_with_different_run_id_as_abandoned() {
    let app = test_app();

    // First Run starts and never finishes (e.g. kill -9)
    let first_start = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/v1/events")
                .header("content-type", "application/json")
                .header("authorization", "Bearer secret-dbt-token")
                .body(Body::from(
                    r#"{
                        "job": "hourly_sync",
                        "event": "started",
                        "run_id": "run-stuck",
                        "timestamp": "2026-11-03T01:00:00Z"
                    }"#,
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(first_start.status(), StatusCode::OK);

    // Second Run starts on the same Job with a different `run_id`
    let second_start = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/v1/events")
                .header("content-type", "application/json")
                .header("authorization", "Bearer secret-dbt-token")
                .body(Body::from(
                    r#"{
                        "job": "hourly_sync",
                        "event": "started",
                        "run_id": "run-next",
                        "timestamp": "2026-11-03T02:00:00Z"
                    }"#,
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(second_start.status(), StatusCode::OK);

    let detail_res = app
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/jobs/dbt-prod/hourly_sync")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(detail_res.status(), StatusCode::OK);
    let html = body_string(detail_res).await;

    assert!(html.contains("run-stuck"));
    assert!(html.contains("abandoned"));
    assert!(html.contains("3600s"), "abandoned run should have finished_at set to new run start");
    assert!(html.contains("run-next"));
    assert!(html.contains("running"));
    assert!(html.contains("Failed Runs : 1"));
}

async fn spawn_slack_webhook_receiver() -> (String, std::sync::Arc<std::sync::Mutex<Vec<serde_json::Value>>>) {
    let received = std::sync::Arc::new(std::sync::Mutex::new(Vec::<serde_json::Value>::new()));
    let received_clone = std::sync::Arc::clone(&received);

    let app = axum::Router::new().route(
        "/slack-webhook",
        axum::routing::post(move |axum::Json(payload): axum::Json<serde_json::Value>| {
            let store = std::sync::Arc::clone(&received_clone);
            async move {
                store.lock().unwrap().push(payload);
                StatusCode::OK
            }
        }),
    );

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });

    (format!("http://{addr}/slack-webhook"), received)
}

fn test_app_with_slack(webhook_url: &str) -> axum::Router {
    let toml = format!(
        r#"
[server]
public_url = "https://obserr.internal"

[channel]
webhook_url = "{webhook_url}"

[sources.dbt-prod]
token = "secret-dbt-token"

[sources.airflow-prod]
token = "secret-airflow-token"
"#
    );
    let config = Config::from_toml_str(&toml).expect("valid TOML config");
    let store = Store::open_in_memory().expect("in-memory SQLite store");
    web::router(AppState::new(config, store))
}

#[tokio::test]
async fn first_failed_run_opens_incident_and_sends_single_slack_webhook_with_context() {
    let (webhook_url, slack_messages) = spawn_slack_webhook_receiver().await;
    let app = test_app_with_slack(&webhook_url);

    let payload = serde_json::json!({
        "job": "orders_mart",
        "event": "finished",
        "run_id": "run-fail-1",
        "status": "failed",
        "step": "stg_orders",
        "error_excerpt": "Database Error in model stg_orders\nrelation \"raw.orders\" does not exist",
        "external_url": "https://airflow.internal/dags/orders_mart/runs/run-fail-1",
        "timestamp": "2026-11-03T08:15:00Z"
    });

    let res = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/v1/events")
                .header("content-type", "application/json")
                .header("authorization", "Bearer secret-dbt-token")
                .body(Body::from(payload.to_string()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::OK);

    let messages = slack_messages.lock().unwrap().clone();
    assert_eq!(messages.len(), 1, "expected exactly 1 Slack webhook call");
    let text = messages[0]["text"].as_str().expect("Slack payload must have text field");
    assert!(text.contains("orders_mart"), "Slack alert must contain Job name: {text}");
    assert!(text.contains("dbt-prod"), "Slack alert must contain Source: {text}");
    assert!(text.contains("Failed Run"), "Slack alert must contain failure nature: {text}");
    assert!(text.contains("2026-11-03T08:15:00Z"), "Slack alert must contain timestamp: {text}");
    assert!(text.contains("stg_orders"), "Slack alert must contain failed Step: {text}");
    assert!(
        text.contains("relation \"raw.orders\" does not exist"),
        "Slack alert must contain error excerpt: {text}"
    );
    assert!(
        text.contains("https://airflow.internal/dags/orders_mart/runs/run-fail-1"),
        "Slack alert must contain link to Source: {text}"
    );
    assert!(
        text.contains("https://obserr.internal/jobs/dbt-prod/orders_mart"),
        "Slack alert must contain link to Job page in obserr: {text}"
    );
}

#[tokio::test]
async fn abandoned_previous_run_opens_incident_and_sends_single_slack_webhook() {
    let (webhook_url, slack_messages) = spawn_slack_webhook_receiver().await;
    let app = test_app_with_slack(&webhook_url);

    // 1. First Run starts and never finishes
    let first_start = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/v1/events")
                .header("content-type", "application/json")
                .header("authorization", "Bearer secret-airflow-token")
                .body(Body::from(
                    r#"{
                        "job": "inventory_sync",
                        "event": "started",
                        "run_id": "run-hung",
                        "external_url": "https://airflow.internal/dags/inventory_sync/runs/run-hung",
                        "timestamp": "2026-11-03T09:00:00Z"
                    }"#,
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(first_start.status(), StatusCode::OK);
    assert_eq!(slack_messages.lock().unwrap().len(), 0);

    // 2. Second Run starts with a different run_id, marking run-hung as abandoned (Failed Run)
    let second_start = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/v1/events")
                .header("content-type", "application/json")
                .header("authorization", "Bearer secret-airflow-token")
                .body(Body::from(
                    r#"{
                        "job": "inventory_sync",
                        "event": "started",
                        "run_id": "run-fresh",
                        "timestamp": "2026-11-03T10:00:00Z"
                    }"#,
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(second_start.status(), StatusCode::OK);

    let messages = slack_messages.lock().unwrap().clone();
    assert_eq!(messages.len(), 1, "abandoned run should trigger 1 Slack alert");
    let text = messages[0]["text"].as_str().unwrap();
    assert!(text.contains("inventory_sync"));
    assert!(text.contains("airflow-prod"));
    assert!(text.contains("abandoned"));
    assert!(text.contains("2026-11-03T10:00:00Z"));
    assert!(text.contains("https://airflow.internal/dags/inventory_sync/runs/run-hung"));
    assert!(text.contains("https://obserr.internal/jobs/airflow-prod/inventory_sync"));
}

#[tokio::test]
async fn consecutive_failed_runs_are_silent_and_succeeded_run_closes_incident_with_resolution_alert() {
    let (webhook_url, slack_messages) = spawn_slack_webhook_receiver().await;
    let app = test_app_with_slack(&webhook_url);

    // 1. First Failed Run at 10:00:00Z opens the Incident and sends 1 Slack alert
    for (run_id, ts, excerpt) in [
        ("run-1", "2026-11-03T10:00:00Z", "first failure"),
        ("run-2", "2026-11-03T10:15:00Z", "second failure while open"),
        ("run-3", "2026-11-03T10:30:00Z", "third failure while open"),
    ] {
        let res = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/v1/events")
                    .header("content-type", "application/json")
                    .header("authorization", "Bearer secret-dbt-token")
                    .body(Body::from(
                        serde_json::json!({
                            "job": "billing_rollup",
                            "event": "finished",
                            "run_id": run_id,
                            "status": "failed",
                            "error_excerpt": excerpt,
                            "timestamp": ts
                        })
                        .to_string(),
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(res.status(), StatusCode::OK);
    }

    assert_eq!(
        slack_messages.lock().unwrap().len(),
        1,
        "subsequent Failed Runs on an already-open Incident must not send extra Slack alerts"
    );

    // 2. A succeeded Run at 10:45:00Z (2700s = 45m after 10:00:00Z) closes the Incident and sends 1 resolution alert
    let recovery_res = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/v1/events")
                .header("content-type", "application/json")
                .header("authorization", "Bearer secret-dbt-token")
                .body(Body::from(
                    r#"{
                        "job": "billing_rollup",
                        "event": "finished",
                        "run_id": "run-4",
                        "status": "succeeded",
                        "timestamp": "2026-11-03T10:45:00Z"
                    }"#,
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(recovery_res.status(), StatusCode::OK);

    let messages = slack_messages.lock().unwrap().clone();
    assert_eq!(messages.len(), 2, "expected 1 opening alert + 1 resolution alert");
    let resolved_text = messages[1]["text"].as_str().unwrap();
    assert!(
        resolved_text.contains("✅ Résolu"),
        "resolution alert must contain '✅ Résolu': {resolved_text}"
    );
    assert!(resolved_text.contains("billing_rollup"));
    assert!(resolved_text.contains("dbt-prod"));
    assert!(
        resolved_text.contains("2700s") || resolved_text.contains("45m"),
        "resolution alert must contain total Incident duration: {resolved_text}"
    );

    // 3. Another succeeded Run when no Incident is open sends no alert
    let next_ok_res = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/v1/events")
                .header("content-type", "application/json")
                .header("authorization", "Bearer secret-dbt-token")
                .body(Body::from(
                    r#"{
                        "job": "billing_rollup",
                        "event": "finished",
                        "run_id": "run-5",
                        "status": "succeeded",
                        "timestamp": "2026-11-03T11:00:00Z"
                    }"#,
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(next_ok_res.status(), StatusCode::OK);
    assert_eq!(slack_messages.lock().unwrap().len(), 2);
}

#[tokio::test]
async fn open_incidents_appear_at_top_of_dashboard_on_incidents_page_and_in_job_detail_history() {
    let (webhook_url, _slack_messages) = spawn_slack_webhook_receiver().await;
    let app = test_app_with_slack(&webhook_url);

    // 1. Healthy job `airflow-prod / alpha_healthy` (alphabetically first by source & name)
    let ok_res = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/v1/events")
                .header("content-type", "application/json")
                .header("authorization", "Bearer secret-airflow-token")
                .body(Body::from(
                    r#"{
                        "job": "alpha_healthy",
                        "event": "finished",
                        "run_id": "ok-1",
                        "status": "succeeded",
                        "timestamp": "2026-11-03T09:50:00Z"
                    }"#,
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(ok_res.status(), StatusCode::OK);

    // 2. Failing job `dbt-prod / orders_broken` (alphabetically after airflow-prod) fails at 10:00:00Z
    let fail_1 = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/v1/events")
                .header("content-type", "application/json")
                .header("authorization", "Bearer secret-dbt-token")
                .body(Body::from(
                    r#"{
                        "job": "orders_broken",
                        "event": "finished",
                        "run_id": "fail-1",
                        "status": "failed",
                        "step": "dim_customers",
                        "error_excerpt": "initial failure excerpt",
                        "external_url": "https://ci.internal/orders/fail-1",
                        "timestamp": "2026-11-03T10:00:00Z"
                    }"#,
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(fail_1.status(), StatusCode::OK);

    // 3. Second failure on `orders_broken` at 10:05:00Z updates the latest error excerpt and source link
    let fail_2 = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/v1/events")
                .header("content-type", "application/json")
                .header("authorization", "Bearer secret-dbt-token")
                .body(Body::from(
                    r#"{
                        "job": "orders_broken",
                        "event": "finished",
                        "run_id": "fail-2",
                        "status": "failed",
                        "step": "dim_customers",
                        "error_excerpt": "latest failure excerpt on orders_broken",
                        "external_url": "https://ci.internal/orders/fail-2",
                        "timestamp": "2026-11-03T10:05:00Z"
                    }"#,
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(fail_2.status(), StatusCode::OK);

    // 4. Dashboard (`GET /`) displays `orders_broken` (with open Incident) BEFORE `alpha_healthy`
    let dash_res = app
        .clone()
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(dash_res.status(), StatusCode::OK);
    let dash_html = body_string(dash_res).await;
    assert!(dash_html.contains("Incident ouvert"));
    let pos_broken = dash_html.find("orders_broken").expect("orders_broken on dashboard");
    let pos_healthy = dash_html.find("alpha_healthy").expect("alpha_healthy on dashboard");
    assert!(
        pos_broken < pos_healthy,
        "Job with open Incident must appear at the top of the Dashboard before healthy Jobs"
    );

    // 5. Open Incidents page (`GET /incidents`) shows `orders_broken` with opening time, initial cause, latest error excerpt and source link
    let incidents_res = app
        .clone()
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/incidents")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(incidents_res.status(), StatusCode::OK);
    let incidents_html = body_string(incidents_res).await;
    assert!(incidents_html.contains("orders_broken"));
    assert!(incidents_html.contains("dbt-prod"));
    assert!(incidents_html.contains("2026-11-03T10:00:00Z"), "must show Incident opening time");
    assert!(incidents_html.contains("Failed Run"), "must show initial cause");
    assert!(
        incidents_html.contains("latest failure excerpt on orders_broken"),
        "must show latest error excerpt"
    );
    assert!(
        incidents_html.contains("https://ci.internal/orders/fail-2"),
        "must show link to Source"
    );
    assert!(
        !incidents_html.contains("alpha_healthy"),
        "healthy job must not appear on /incidents"
    );

    // 6. Job detail page while open shows the open Incident in history
    let detail_open_res = app
        .clone()
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/jobs/dbt-prod/orders_broken")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(detail_open_res.status(), StatusCode::OK);
    let detail_open_html = body_string(detail_open_res).await;
    assert!(detail_open_html.contains("Historique des Incidents"));
    assert!(detail_open_html.contains("open"));
    assert!(detail_open_html.contains("2026-11-03T10:00:00Z"));

    // 7. Resolve the Incident at 10:20:00Z (duration 1200s)
    let resolve_res = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/v1/events")
                .header("content-type", "application/json")
                .header("authorization", "Bearer secret-dbt-token")
                .body(Body::from(
                    r#"{
                        "job": "orders_broken",
                        "event": "finished",
                        "run_id": "ok-recover",
                        "status": "succeeded",
                        "timestamp": "2026-11-03T10:20:00Z"
                    }"#,
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resolve_res.status(), StatusCode::OK);

    // 8. `/incidents` no longer lists `orders_broken` now that its Incident is closed
    let incidents_after_res = app
        .clone()
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/incidents")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(incidents_after_res.status(), StatusCode::OK);
    let incidents_after_html = body_string(incidents_after_res).await;
    assert!(
        !incidents_after_html.contains("orders_broken"),
        "closed Incident must no longer appear on /incidents"
    );

    // 9. Job detail page (`/jobs/dbt-prod/orders_broken`) shows the closed Incident in its history
    let detail_closed_res = app
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/jobs/dbt-prod/orders_broken")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(detail_closed_res.status(), StatusCode::OK);
    let detail_closed_html = body_string(detail_closed_res).await;
    assert!(detail_closed_html.contains("Historique des Incidents"));
    assert!(detail_closed_html.contains("closed"));
    assert!(detail_closed_html.contains("2026-11-03T10:00:00Z"));
    assert!(detail_closed_html.contains("2026-11-03T10:20:00Z"));
    assert!(detail_closed_html.contains("1200s"));
}

#[tokio::test]
async fn memory_channel_adapter_records_domain_notifications_at_channel_seam() {
    let config = Config::from_toml_str(
        r#"
[sources.dbt-prod]
token = "secret-dbt-token"
"#,
    )
    .expect("valid TOML config");
    let store = Store::open_in_memory().expect("in-memory SQLite store");
    let memory_channel = std::sync::Arc::new(obserr::channel::MemoryChannel::new());
    let app = web::router(AppState::with_channel(
        config,
        store,
        memory_channel.clone(),
    ));

    let res = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/v1/events")
                .header("content-type", "application/json")
                .header("authorization", "Bearer secret-dbt-token")
                .body(Body::from(
                    r#"{
                        "job": "mart_revenue",
                        "event": "finished",
                        "run_id": "r-1",
                        "status": "failed",
                        "timestamp": "2026-11-03T12:00:00Z"
                    }"#,
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::OK);
    assert_eq!(memory_channel.recorded().len(), 1);
}








