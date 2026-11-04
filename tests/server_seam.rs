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

#[tokio::test]
async fn predeclared_jobs_in_toml_appear_immediately_with_status_metadata_and_next_slot() {
    let config = Config::from_toml_str(
        r#"
[server]
timezone = "UTC"

[sources.dbt-prod]
token = "secret-dbt-token"

[sources.airflow-prod]
token = "secret-airflow-token"

[jobs.dbt-prod.compta_nightly]
schedule = "0 2 * * *"
grace = "30m"
timezone = "Europe/Paris"
owner = "equipe-finance"
tags = ["finance", "nightly"]

[jobs.airflow-prod.adhoc_import]
owner = "equipe-data"
tags = ["adhoc"]
"#,
    )
    .expect("valid TOML config with predeclared jobs");

    let store = Store::open_in_memory().expect("in-memory SQLite store");
    let state = AppState::new(config, store);

    // Fix reference clock to 2026-11-03T00:00:00Z (winter time in Europe/Paris: CET = UTC+1, so 02:00 Europe/Paris = 01:00:00Z)
    state
        .tick("2026-11-03T00:00:00Z".parse().unwrap())
        .await
        .expect("initial tick succeeds");

    let app = web::router(state);

    // 1. Dashboard (`GET /`) immediately lists both predeclared Jobs before any HTTP event
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

    assert!(dash_html.contains("compta_nightly"));
    assert!(dash_html.contains("OK"));
    assert!(dash_html.contains("0 2 * * *"));
    assert!(dash_html.contains("equipe-finance"));
    assert!(dash_html.contains("finance"));
    assert!(dash_html.contains("nightly"));
    assert!(
        dash_html.contains("2026-11-03T01:00:00Z"),
        "next expected slot for 02:00 Europe/Paris on 2026-11-03 (CET, UTC+1) must be 2026-11-03T01:00:00Z: {dash_html}"
    );

    assert!(dash_html.contains("adhoc_import"));
    assert!(dash_html.contains("Sans Schedule"));
    assert!(dash_html.contains("equipe-data"));
    assert!(dash_html.contains("adhoc"));

    // Scheduled `OK` job appears before `Sans Schedule` job on the Dashboard
    let pos_ok = dash_html.find("compta_nightly").unwrap();
    let pos_unscheduled = dash_html.find("adhoc_import").unwrap();
    assert!(
        pos_ok < pos_unscheduled,
        "Job with status OK must appear before Job with status Sans Schedule"
    );

    // 2. Job detail page (`GET /jobs/dbt-prod/compta_nightly`) shows status, Schedule, owner, tags, and next expected slot
    let detail_res = app
        .clone()
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
    assert!(detail_html.contains("OK"));
    assert!(detail_html.contains("0 2 * * *"));
    assert!(detail_html.contains("30m") || detail_html.contains("1800s"));
    assert!(detail_html.contains("Europe/Paris"));
    assert!(detail_html.contains("equipe-finance"));
    assert!(detail_html.contains("finance"));
    assert!(detail_html.contains("nightly"));
    assert!(detail_html.contains("2026-11-03T01:00:00Z"));

    // 3. Job detail page for unscheduled job (`GET /jobs/airflow-prod/adhoc_import`) shows `Sans Schedule`
    let unsched_res = app
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/jobs/airflow-prod/adhoc_import")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(unsched_res.status(), StatusCode::OK);
    let unsched_html = body_string(unsched_res).await;
    assert!(unsched_html.contains("Sans Schedule"));
    assert!(unsched_html.contains("equipe-data"));
    assert!(unsched_html.contains("adhoc"));
}

#[tokio::test]
async fn tick_respects_grace_period_and_declares_single_missed_run_unless_covered_by_started_or_finished_run() {
    let config = Config::from_toml_str(
        r#"
[sources.dbt-prod]
token = "secret-dbt-token"

[jobs.dbt-prod.hourly_kpi]
schedule = "0 * * * *"
grace = "15m"
"#,
    )
    .expect("valid TOML config");

    let store = Store::open_in_memory().expect("in-memory SQLite store");
    let state = AppState::new(config, store);
    let app = web::router(state.clone());

    // Anchor schedule at 09:50:00Z (before the 10:00:00Z slot)
    let alerts_init = state
        .tick("2026-11-03T09:50:00Z".parse().unwrap())
        .await
        .unwrap();
    assert!(alerts_init.is_empty());

    // 1. Slot 10:00:00Z: while now < 10:00 + 15m (at 10:14:59Z), NO Missed Run is declared
    let alerts_before_grace = state
        .tick("2026-11-03T10:14:59Z".parse().unwrap())
        .await
        .unwrap();
    assert!(
        alerts_before_grace.is_empty(),
        "no Missed Run should be declared while now < slot + grace"
    );

    let detail_before = body_string(
        app.clone()
            .oneshot(
                Request::builder()
                    .method("GET")
                    .uri("/jobs/dbt-prod/hourly_kpi")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap(),
    )
    .await;
    assert!(detail_before.contains("Missed Runs : 0"));

    // 2. Slot 10:00:00Z: as soon as now >= 10:00 + 15m (at 10:15:00Z) with no Run in [10:00, 10:15],
    //    exactly 1 Missed Run is recorded for 2026-11-03T10:00:00Z.
    let alerts_at_grace = state
        .tick("2026-11-03T10:15:00Z".parse().unwrap())
        .await
        .unwrap();
    assert_eq!(alerts_at_grace.len(), 1);

    // Calling tick again at 10:20:00Z is idempotent (still 1 Missed Run for the 10:00:00Z slot)
    let alerts_repeat = state
        .tick("2026-11-03T10:20:00Z".parse().unwrap())
        .await
        .unwrap();
    assert!(alerts_repeat.is_empty());

    let detail_after_miss = body_string(
        app.clone()
            .oneshot(
                Request::builder()
                    .method("GET")
                    .uri("/jobs/dbt-prod/hourly_kpi")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap(),
    )
    .await;
    assert!(detail_after_miss.contains("Missed Runs : 1"));
    assert!(detail_after_miss.contains("2026-11-03T10:00:00Z"));

    // 3. Slot 11:00:00Z: a Run starts at 11:10:00Z (within [11:00, 11:15]) -> NO Missed Run at 11:20:00Z
    let start_res = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/v1/events")
                .header("content-type", "application/json")
                .header("authorization", "Bearer secret-dbt-token")
                .body(Body::from(
                    r#"{
                        "job": "hourly_kpi",
                        "event": "started",
                        "run_id": "run-11h",
                        "timestamp": "2026-11-03T11:10:00Z"
                    }"#,
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(start_res.status(), StatusCode::OK);

    let finish_11h = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/v1/events")
                .header("content-type", "application/json")
                .header("authorization", "Bearer secret-dbt-token")
                .body(Body::from(
                    r#"{
                        "job": "hourly_kpi",
                        "event": "finished",
                        "run_id": "run-11h",
                        "status": "succeeded",
                        "timestamp": "2026-11-03T11:18:00Z"
                    }"#,
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(finish_11h.status(), StatusCode::OK);

    let alerts_11h = state
        .tick("2026-11-03T11:20:00Z".parse().unwrap())
        .await
        .unwrap();
    assert!(
        alerts_11h.is_empty(),
        "Run started in [11:00, 11:15] must cover the 11:00 slot"
    );

    // 4. Slot 12:00:00Z: a callback-only Run (`finished` without `started_at`) at 12:12:00Z (within [12:00, 12:15]) -> NO Missed Run at 12:20:00Z
    let finish_only_12h = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/v1/events")
                .header("content-type", "application/json")
                .header("authorization", "Bearer secret-dbt-token")
                .body(Body::from(
                    r#"{
                        "job": "hourly_kpi",
                        "event": "finished",
                        "status": "succeeded",
                        "timestamp": "2026-11-03T12:12:00Z"
                    }"#,
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(finish_only_12h.status(), StatusCode::OK);

    let alerts_12h = state
        .tick("2026-11-03T12:20:00Z".parse().unwrap())
        .await
        .unwrap();
    assert!(
        alerts_12h.is_empty(),
        "finished-only Run in [12:00, 12:15] must cover the 12:00 slot"
    );

    let detail_final = body_string(
        app.oneshot(
            Request::builder()
                .method("GET")
                .uri("/jobs/dbt-prod/hourly_kpi")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap(),
    )
    .await;
    assert!(
        detail_final.contains("Missed Runs : 1"),
        "still only 1 Missed Run after covered 11:00 and 12:00 slots"
    );
}

#[tokio::test]
async fn tick_evaluates_schedule_in_job_or_global_timezone_including_summer_winter_dst_transitions() {
    // Global timezone is Europe/Paris; one Job uses global Europe/Paris (08:00 local),
    // another Job overrides timezone with UTC (08:00 UTC).
    let config = Config::from_toml_str(
        r#"
[server]
timezone = "Europe/Paris"

[sources.dbt-prod]
token = "secret-dbt-token"

[jobs.dbt-prod.paris_morning]
schedule = "0 8 * * *"
grace = "10m"

[jobs.dbt-prod.utc_morning]
schedule = "0 8 * * *"
grace = "10m"
timezone = "UTC"
"#,
    )
    .expect("valid TOML config");

    let store = Store::open_in_memory().expect("in-memory SQLite store");
    let state = AppState::new(config, store);
    let app = web::router(state.clone());

    // 1. Summer time (CEST = UTC+2) on 2026-10-24 (day before autumn DST switch):
    //    `paris_morning` (08:00 Europe/Paris) is expected at 2026-10-24T06:00:00Z (grace ends at 06:10:00Z).
    //    `utc_morning` (08:00 UTC) is expected at 2026-10-24T08:00:00Z.
    state
        .tick("2026-10-24T05:50:00Z".parse().unwrap())
        .await
        .unwrap();

    // At 06:09:00Z, neither job has missed its slot yet
    assert!(
        state
            .tick("2026-10-24T06:09:00Z".parse().unwrap())
            .await
            .unwrap()
            .is_empty()
    );

    // At 06:10:00Z, `paris_morning` misses its 08:00 CEST (06:00 UTC) slot, while `utc_morning` is not due until 08:00 UTC
    let summer_alerts = state
        .tick("2026-10-24T06:10:00Z".parse().unwrap())
        .await
        .unwrap();
    assert_eq!(summer_alerts.len(), 1);

    // Cover `utc_morning` at 08:05:00Z on 2026-10-24 so it does not miss
    let cover_utc = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/v1/events")
                .header("content-type", "application/json")
                .header("authorization", "Bearer secret-dbt-token")
                .body(Body::from(
                    r#"{
                        "job": "utc_morning",
                        "event": "finished",
                        "status": "succeeded",
                        "timestamp": "2026-10-24T08:05:00Z"
                    }"#,
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(cover_utc.status(), StatusCode::OK);
    assert!(
        state
            .tick("2026-10-24T08:15:00Z".parse().unwrap())
            .await
            .unwrap()
            .is_empty()
    );

    // 2. Winter time (CET = UTC+1) on 2026-10-25 (autumn DST transition happens at 01:00 UTC on 2026-10-25):
    //    `paris_morning` (08:00 Europe/Paris) is now expected at 2026-10-25T07:00:00Z (NOT 06:00:00Z!).
    //    At 2026-10-25T06:30:00Z, `paris_morning` must NOT have a Missed Run for 2026-10-25 yet.
    state
        .tick("2026-10-25T06:30:00Z".parse().unwrap())
        .await
        .unwrap();
    let paris_detail_before_winter_slot = body_string(
        app.clone()
            .oneshot(
                Request::builder()
                    .method("GET")
                    .uri("/jobs/dbt-prod/paris_morning")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap(),
    )
    .await;
    assert!(
        paris_detail_before_winter_slot.contains("Missed Runs : 1"),
        "at 06:30 UTC on winter-time day, 08:00 CET (07:00 UTC) has not occurred yet"
    );

    // At 2026-10-25T07:10:00Z (08:00 CET + 10m grace), the second Missed Run is recorded for 2026-10-25T07:00:00Z!
    state
        .tick("2026-10-25T07:10:00Z".parse().unwrap())
        .await
        .unwrap();
    let paris_detail_after_winter_slot = body_string(
        app.oneshot(
            Request::builder()
                .method("GET")
                .uri("/jobs/dbt-prod/paris_morning")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap(),
    )
    .await;
    assert!(paris_detail_after_winter_slot.contains("Missed Runs : 2"));
    assert!(paris_detail_after_winter_slot.contains("2026-10-24T06:00:00Z"));
    assert!(paris_detail_after_winter_slot.contains("2026-10-25T07:00:00Z"));
}

#[tokio::test]
async fn missed_run_opens_incident_sends_single_slack_alert_and_resolves_on_next_succeeded_run() {
    let (webhook_url, slack_messages) = spawn_slack_webhook_receiver().await;
    let toml = format!(
        r#"
[server]
public_url = "https://obserr.internal"

[channel]
webhook_url = "{webhook_url}"

[sources.airflow-prod]
token = "secret-airflow-token"

[jobs.airflow-prod.revenue_daily]
schedule = "0 * * * *"
grace = "15m"
"#
    );
    let config = Config::from_toml_str(&toml).expect("valid TOML");
    let store = Store::open_in_memory().expect("in-memory SQLite store");
    let state = AppState::new(config, store);
    let app = web::router(state.clone());

    // Anchor at 09:50:00Z
    state
        .tick("2026-11-03T09:50:00Z".parse().unwrap())
        .await
        .unwrap();
    assert_eq!(slack_messages.lock().unwrap().len(), 0);

    // 0. Earlier Failed Run at 09:52:00Z followed by recovery at 09:55:00Z (opens & closes a prior Incident)
    let _ = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/v1/events")
                .header("content-type", "application/json")
                .header("authorization", "Bearer secret-airflow-token")
                .body(Body::from(
                    r#"{
                        "job": "revenue_daily",
                        "event": "finished",
                        "run_id": "old-fail",
                        "status": "failed",
                        "error_excerpt": "stale error from closed incident",
                        "timestamp": "2026-11-03T09:52:00Z"
                    }"#,
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    let _ = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/v1/events")
                .header("content-type", "application/json")
                .header("authorization", "Bearer secret-airflow-token")
                .body(Body::from(
                    r#"{
                        "job": "revenue_daily",
                        "event": "finished",
                        "run_id": "old-recover",
                        "status": "succeeded",
                        "timestamp": "2026-11-03T09:55:00Z"
                    }"#,
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    slack_messages.lock().unwrap().clear();

    // 1. First Missed Run at 10:15:00Z (for 10:00:00Z slot) opens an Incident and sends 1 Slack alert
    state
        .tick("2026-11-03T10:15:00Z".parse().unwrap())
        .await
        .unwrap();

    let messages = slack_messages.lock().unwrap().clone();
    assert_eq!(messages.len(), 1, "Missed Run must send 1 Slack opening alert");
    let open_text = messages[0]["text"].as_str().unwrap();
    assert!(open_text.contains("revenue_daily"));
    assert!(open_text.contains("airflow-prod"));
    assert!(open_text.contains("Missed Run"));
    assert!(open_text.contains("2026-11-03T10:15:00Z"));
    assert!(open_text.contains("https://obserr.internal/jobs/airflow-prod/revenue_daily"));

    // 2. Dashboard and /incidents show the open Incident caused by Missed Run
    let dash_html = body_string(
        app.clone()
            .oneshot(
                Request::builder()
                    .method("GET")
                    .uri("/")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap(),
    )
    .await;
    assert!(dash_html.contains("Incident ouvert"));

    let inc_html = body_string(
        app.clone()
            .oneshot(
                Request::builder()
                    .method("GET")
                    .uri("/incidents")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap(),
    )
    .await;
    assert!(inc_html.contains("revenue_daily"));
    assert!(inc_html.contains("Missed Run"));
    assert!(
        !inc_html.contains("stale error from closed incident"),
        "open Missed Run incident must not display stale error excerpts from prior closed incidents"
    );

    // 3. Second consecutive Missed Run at 11:15:00Z (for 11:00:00Z slot) while Incident is already open:
    //    recorded in history, but sends 0 additional Slack alerts!
    state
        .tick("2026-11-03T11:15:00Z".parse().unwrap())
        .await
        .unwrap();
    assert_eq!(
        slack_messages.lock().unwrap().len(),
        1,
        "subsequent Missed Run on an already-open Incident must not send extra Slack alerts"
    );

    // 4. Subsequent succeeded Run at 11:30:00Z (75m = 4500s after Incident opened at 10:15:00Z)
    //    closes the Incident and sends 1 Slack resolution alert
    let recovery_res = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/v1/events")
                .header("content-type", "application/json")
                .header("authorization", "Bearer secret-airflow-token")
                .body(Body::from(
                    r#"{
                        "job": "revenue_daily",
                        "event": "finished",
                        "run_id": "dagrun-ok",
                        "status": "succeeded",
                        "timestamp": "2026-11-03T11:30:00Z"
                    }"#,
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(recovery_res.status(), StatusCode::OK);

    let messages_after = slack_messages.lock().unwrap().clone();
    assert_eq!(messages_after.len(), 2, "expected 1 opening alert + 1 resolution alert");
    let resolved_text = messages_after[1]["text"].as_str().unwrap();
    assert!(resolved_text.contains("✅ Résolu"));
    assert!(resolved_text.contains("revenue_daily"));
    assert!(resolved_text.contains("4500s") || resolved_text.contains("1h 15m"));

    // Dashboard now shows status `OK` again
    let dash_after = body_string(
        app.oneshot(
            Request::builder()
                .method("GET")
                .uri("/")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap(),
    )
    .await;
    assert!(!dash_after.contains("<td>Incident ouvert</td>"));
    assert!(dash_after.contains("OK"));
}

#[tokio::test]
async fn no_retroactive_missed_runs_before_schedule_registration_and_hot_reload_updates_live_server() {
    let (webhook_url_1, slack_1) = spawn_slack_webhook_receiver().await;
    let (webhook_url_2, slack_2) = spawn_slack_webhook_receiver().await;

    let tmp_dir = std::env::temp_dir().join(format!("obserr-test-hotreload-{}", std::process::id()));
    std::fs::create_dir_all(&tmp_dir).unwrap();
    let config_path = tmp_dir.join("obserr.toml");

    // Initial TOML: `unscheduled_job` has NO schedule; `scheduled_job` is hourly
    let initial_toml = format!(
        r#"
[server]
public_url = "https://obserr.internal"

[channel]
webhook_url = "{webhook_url_1}"

[sources.dbt-prod]
token = "initial-dbt-token"

[jobs.dbt-prod.unscheduled_job]
owner = "alice"
tags = ["v1"]
"#
    );
    std::fs::write(&config_path, &initial_toml).unwrap();

    let store = Store::open_in_memory().expect("in-memory SQLite store");
    let state = AppState::from_config_file(&config_path, store).expect("load state from file");
    let app = web::router(state.clone());

    // 1. Initial tick at 08:00:00Z: `unscheduled_job` has no schedule and never generates a Missed Run
    state
        .tick("2026-11-03T08:00:00Z".parse().unwrap())
        .await
        .unwrap();
    assert_eq!(slack_1.lock().unwrap().len(), 0);

    // 2. Modify TOML on disk at 10:15:00Z (hot-reload!):
    //    - Add a Schedule (`0 * * * *`, grace `10m`) to `unscheduled_job`
    //    - Update `owner` ("bob") and `tags` (["v2", "hot-reloaded"])
    //    - Rotate `dbt-prod` token to `"rotated-dbt-token"`
    //    - Update Slack `webhook_url` to `webhook_url_2`
    let updated_toml = format!(
        r#"
[server]
public_url = "https://obserr.internal"

[channel]
webhook_url = "{webhook_url_2}"

[sources.dbt-prod]
token = "rotated-dbt-token"

[jobs.dbt-prod.unscheduled_job]
schedule = "0 * * * *"
grace = "10m"
owner = "bob"
tags = ["v2", "hot-reloaded"]
"#
    );
    std::fs::write(&config_path, &updated_toml).unwrap();

    // 3. Tick at 10:15:00Z (when the Schedule is registered):
    //    Slots prior to 10:15:00Z (such as 08:00Z, 09:00Z, and 10:00Z!) must NOT trigger a retroactive Missed Run!
    let retroactive_alerts = state
        .tick("2026-11-03T10:15:00Z".parse().unwrap())
        .await
        .unwrap();
    assert!(
        retroactive_alerts.is_empty(),
        "no retroactive Missed Run may fire for slots prior to Schedule registration at 10:15:00Z"
    );

    // 4. Dashboard immediately reflects the hot-reloaded Schedule, owner, and tags
    let dash_html = body_string(
        app.clone()
            .oneshot(
                Request::builder()
                    .method("GET")
                    .uri("/")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap(),
    )
    .await;
    assert!(dash_html.contains("OK"));
    assert!(dash_html.contains("0 * * * *"));
    assert!(dash_html.contains("bob"));
    assert!(dash_html.contains("v2, hot-reloaded"));
    assert!(dash_html.contains("2026-11-03T11:00:00Z"));

    // 5. Old Source token is now rejected (`401`) and rotated token is accepted (`200`)
    let old_token_res = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/v1/events")
                .header("content-type", "application/json")
                .header("authorization", "Bearer initial-dbt-token")
                .body(Body::from(r#"{"job":"other_job","event":"started"}"#))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(old_token_res.status(), StatusCode::UNAUTHORIZED);

    let new_token_res = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/v1/events")
                .header("content-type", "application/json")
                .header("authorization", "Bearer rotated-dbt-token")
                .body(Body::from(
                    r#"{"job":"other_job","event":"started","run_id":"r1","timestamp":"2026-11-03T10:20:00Z"}"#,
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(new_token_res.status(), StatusCode::OK);

    // 6. At 11:10:00Z (first slot `11:00:00Z` after registration + 10m grace),
    //    `unscheduled_job` misses its 11:00:00Z slot and sends the alert to the NEW Slack webhook URL (`slack_2`)!
    let alerts_11h = state
        .tick("2026-11-03T11:10:00Z".parse().unwrap())
        .await
        .unwrap();
    assert_eq!(alerts_11h.len(), 1);
    assert_eq!(
        slack_1.lock().unwrap().len(),
        0,
        "old Slack webhook URL must not receive alerts after hot-reload"
    );
    assert_eq!(
        slack_2.lock().unwrap().len(),
        1,
        "new Slack webhook URL must receive the alert after hot-reload"
    );

    let _ = std::fs::remove_dir_all(&tmp_dir);
}












/// Exercises optional web authentication and credential rotation without changing ingestion auth.
#[tokio::test]
async fn web_password_protects_all_pages_and_reloads_without_affecting_api_or_health() {
    let config_text = r#"
[server]
web_password = "web:secret"
[sources.dbt-prod]
token = "secret-dbt-token"
[jobs.dbt-prod.daily]
"#;
    let state = AppState::new(
        Config::from_toml_str(config_text).unwrap(),
        Store::open_in_memory().unwrap(),
    );
    let app = web::router(state.clone());
    for path in ["/", "/incidents", "/jobs/dbt-prod/daily"] {
        for auth in [
            None,
            Some("Basic invalid!"),
            Some("Basic dXNlcjp3cm9uZw=="),
            Some("Bearer secret-dbt-token"),
        ] {
            let mut req = Request::builder().uri(path);
            if let Some(auth) = auth {
                req = req.header("authorization", auth);
            }
            let res = app
                .clone()
                .oneshot(req.body(Body::empty()).unwrap())
                .await
                .unwrap();
            assert_eq!(res.status(), StatusCode::UNAUTHORIZED, "{path}");
            assert!(
                res.headers()["www-authenticate"]
                    .to_str()
                    .unwrap()
                    .starts_with("Basic ")
            );
        }
        let res = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri(path)
                    .header("authorization", "Basic dXNlcjp3ZWI6c2VjcmV0")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(res.status(), StatusCode::OK, "{path}");
    }
    let res = app
        .clone()
        .oneshot(
            Request::builder()
                .uri("/health")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::OK);
    for (auth, status) in [
        ("Bearer secret-dbt-token", StatusCode::OK),
        ("Basic dXNlcjp3ZWI6c2VjcmV0", StatusCode::UNAUTHORIZED),
    ] {
        let res = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/v1/events")
                    .header("authorization", auth)
                    .header("content-type", "application/json")
                    .body(Body::from(
                        r#"{"job":"daily","event":"finished","status":"succeeded"}"#,
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(res.status(), status);
    }
    state
        .reload_config(
            Config::from_toml_str(&config_text.replace("web:secret", "rotated")).unwrap(),
            None,
        )
        .unwrap();
    let res = app
        .clone()
        .oneshot(
            Request::builder()
                .uri("/")
                .header("authorization", "Basic dXNlcjp3ZWI6c2VjcmV0")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::UNAUTHORIZED);
    state
        .reload_config(
            Config::from_toml_str(&config_text.replace("web_password = \"web:secret\"", ""))
                .unwrap(),
            None,
        )
        .unwrap();
    for path in ["/", "/incidents", "/jobs/dbt-prod/daily"] {
        let res = app
            .clone()
            .oneshot(Request::builder().uri(path).body(Body::empty()).unwrap())
            .await
            .unwrap();
        assert_eq!(res.status(), StatusCode::OK);
    }
}

/// Checks intersecting dashboard filters and the same results when htmx refreshes the view.
#[tokio::test]
async fn dashboard_filters_status_source_and_exact_tag_and_refreshes_all_views() {
    let config = Config::from_toml_str(
        r#"
[sources.dbt-prod]
token = "secret-dbt-token"
[jobs.dbt-prod.scheduled]
schedule = "0 2 * * *"
tags = ["finance"]
[jobs.dbt-prod.broken]
tags = ["finance", "urgent"]
[jobs.airflow-prod.adhoc]
tags = ["finance-archive"]
"#,
    )
    .unwrap();
    let app = web::router(AppState::new(config, Store::open_in_memory().unwrap()));
    let res = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/v1/events")
                .header("authorization", "Bearer secret-dbt-token")
                .header("content-type", "application/json")
                .body(Body::from(
                    r#"{"job":"broken","event":"finished","status":"failed"}"#,
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::OK);
    for (query, names) in [
        ("", vec!["scheduled", "broken", "adhoc"]),
        ("?status=incident", vec!["broken"]),
        ("?status=ok", vec!["scheduled"]),
        ("?status=unscheduled", vec!["adhoc"]),
        ("?source=airflow-prod", vec!["adhoc"]),
        ("?tag=finance", vec!["scheduled", "broken"]),
        (
            "?status=incident&source=dbt-prod&tag=urgent",
            vec!["broken"],
        ),
        ("?status=ok&source=airflow-prod", vec![]),
        ("?tag=unknown", vec![]),
    ] {
        for fragment in [false, true] {
            let req = Request::builder()
                .uri(format!("/{query}"))
                .header("hx-request", fragment.to_string())
                .body(Body::empty())
                .unwrap();
            let res = app.clone().oneshot(req).await.unwrap();
            assert_eq!(res.status(), StatusCode::OK);
            let html = body_string(res).await;
            for (source, name) in [
                ("dbt-prod", "scheduled"),
                ("dbt-prod", "broken"),
                ("airflow-prod", "adhoc"),
            ] {
                assert_eq!(
                    html.contains(&format!("href=\"/jobs/{source}/{name}\"")),
                    names.contains(&name),
                    "{query}: {name}"
                );
            }
            assert_eq!(html.contains("<!DOCTYPE html>"), !fragment);
            assert!(html.contains("name=\"status\""));
            assert!(html.contains("name=\"source\""));
            assert!(html.contains("name=\"tag\""));
        }
    }
    for path in ["/", "/incidents", "/jobs/dbt-prod/broken"] {
        let html = body_string(
            app.clone()
                .oneshot(Request::builder().uri(path).body(Body::empty()).unwrap())
                .await
                .unwrap(),
        )
        .await;
        assert!(html.contains("src=\"/static/htmx.min.js\""));
        assert!(html.contains("hx-trigger=\"every 30s\""));
        assert!(
            html.contains(&format!("hx-get=\"{path}\"")),
            "polling must use the page path"
        );
        assert!(!html.contains("hx-get=\"\""));
        let fragment = body_string(
            app.clone()
                .oneshot(
                    Request::builder()
                        .uri(path)
                        .header("hx-request", "true")
                        .body(Body::empty())
                        .unwrap(),
                )
                .await
                .unwrap(),
        )
        .await;
        assert!(!fragment.contains("<!DOCTYPE html>"));
    }
    let html = body_string(
        app.clone()
            .oneshot(
                Request::builder()
                    .uri("/?status=incident&source=dbt-prod&tag=urgent")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap(),
    )
    .await;
    assert!(
        html.contains("hx-get=\"/\" hx-trigger=\"every 30s\""),
        "polling a filtered URL must not append filters to an existing query"
    );
    assert!(html.contains("value=\"incident\" selected"));
    assert!(html.contains("value=\"dbt-prod\" selected"));
    assert!(html.contains("value=\"urgent\" selected"));
    let js = app
        .oneshot(
            Request::builder()
                .uri("/static/htmx.min.js")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(js.status(), StatusCode::OK);
    assert!(
        js.headers()["content-type"]
            .to_str()
            .unwrap()
            .contains("javascript")
    );
    assert!(body_string(js).await.contains("htmx"));
}

/// Sends a real ingestion request through the server's public interface.
async fn ingest_json(app: &axum::Router, payload: serde_json::Value) {
    let res = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/v1/events")
                .header("authorization", "Bearer secret-dbt-token")
                .header("content-type", "application/json")
                .body(Body::from(payload.to_string()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::OK);
}

/// Reads the observable HTML rather than inspecting database rows.
async fn page_html(app: &axum::Router, path: &str) -> String {
    let res = app
        .clone()
        .oneshot(Request::builder().uri(path).body(Body::empty()).unwrap())
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::OK);
    body_string(res).await
}

/// Purges expired history at the default cutoff while keeping the boundary, latest Runs and open Incidents.
#[tokio::test]
async fn tick_purges_expired_history_and_preserves_latest_runs_open_incidents_and_boundary() {
    let config_text = "[sources.dbt-prod]\ntoken = \"secret-dbt-token\"";
    let state = AppState::new(
        Config::from_toml_str(config_text).unwrap(),
        Store::open_in_memory().unwrap(),
    );
    let app = web::router(state.clone());
    for (job, run_id, status, timestamp) in [
        (
            "history",
            "expired-failure",
            "failed",
            "2025-12-30T00:00:00Z",
        ),
        (
            "history",
            "expired-recovery",
            "succeeded",
            "2025-12-31T00:00:00Z",
        ),
        (
            "history",
            "boundary-failure",
            "failed",
            "2025-12-31T23:59:59Z",
        ),
        (
            "history",
            "boundary-recovery",
            "succeeded",
            "2026-01-01T00:00:00Z",
        ),
        (
            "history",
            "recent-failure",
            "failed",
            "2026-03-31T00:00:00Z",
        ),
        (
            "history",
            "recent-recovery",
            "succeeded",
            "2026-04-01T00:00:00Z",
        ),
        (
            "old-latest",
            "old-first",
            "succeeded",
            "2025-12-29T00:00:00Z",
        ),
        (
            "old-latest",
            "old-last",
            "succeeded",
            "2025-12-30T00:00:00Z",
        ),
        ("open", "old-open-failure", "failed", "2025-12-30T00:00:00Z"),
    ] {
        ingest_json(&app, serde_json::json!({"job":job,"event":"finished","run_id":run_id,"status":status,"timestamp":timestamp})).await;
    }
    ingest_json(&app, serde_json::json!({"job":"open","event":"started","run_id":"still-running","timestamp":"2025-12-31T00:00:00Z"})).await;
    // A later finished-only event must not make an ongoing Run eligible for deletion.
    ingest_json(&app, serde_json::json!({"job":"open","event":"finished","status":"failed","run_id":"newer-failure","timestamp":"2026-01-01T00:00:00Z"})).await;
    assert!(
        page_html(&app, "/jobs/dbt-prod/history")
            .await
            .contains("expired-failure")
    );
    assert!(
        state
            .tick("2026-04-01T00:00:00Z".parse().unwrap())
            .await
            .unwrap()
            .is_empty()
    );
    let html = page_html(&app, "/jobs/dbt-prod/history").await;
    for gone in [
        "expired-failure",
        "expired-recovery",
        "boundary-failure",
        "2025-12-30T00:00:00Z",
    ] {
        assert!(!html.contains(gone), "{gone}");
    }
    for kept in ["boundary-recovery", "recent-failure", "recent-recovery"] {
        assert!(html.contains(kept), "{kept}");
    }
    assert_eq!(html.matches("<td>closed</td>").count(), 2);
    let old = page_html(&app, "/jobs/dbt-prod/old-latest").await;
    assert!(old.contains("old-last"));
    assert!(!old.contains("old-first"));
    let open = page_html(&app, "/jobs/dbt-prod/open").await;
    assert!(open.contains("still-running"));
    assert!(open.contains("<td>open</td>"));
    assert!(
        page_html(&app, "/incidents")
            .await
            .contains("2025-12-30T00:00:00Z")
    );
    // The next tick observes a shorter retention without restarting the application.
    state
        .reload_config(
            Config::from_toml_str(&format!("[server]\nretention_days = 1\n{config_text}")).unwrap(),
            None,
        )
        .unwrap();
    state
        .tick("2026-04-01T00:00:00Z".parse().unwrap())
        .await
        .unwrap();
    let html = page_html(&app, "/jobs/dbt-prod/history").await;
    assert!(!html.contains("boundary-recovery"));
    assert!(html.contains("recent-failure"));
    assert_eq!(html.matches("<td>closed</td>").count(), 1);
}

/// Purging Missed Runs never replays evaluated slots or closes a still-open Incident.
#[tokio::test]
async fn retention_purges_missed_runs_without_reopening_incidents_or_replaying_slots() {
    let config = Config::from_toml_str(
        r#"
[server]
retention_days = 1
[sources.dbt-prod]
token = "secret-dbt-token"
[jobs.dbt-prod.daily]
schedule = "0 0 * * *"
"#,
    )
    .unwrap();
    let state = AppState::new(config, Store::open_in_memory().unwrap());
    let app = web::router(state.clone());
    state
        .tick("2026-01-01T00:00:01Z".parse().unwrap())
        .await
        .unwrap();
    assert_eq!(
        state
            .tick("2026-01-02T00:00:00Z".parse().unwrap())
            .await
            .unwrap()
            .len(),
        1
    );
    ingest_json(&app, serde_json::json!({"job":"daily","event":"finished","run_id":"recovery","status":"succeeded","timestamp":"2026-01-02T00:01:00Z"})).await;
    assert_eq!(
        state
            .tick("2026-01-03T00:00:00Z".parse().unwrap())
            .await
            .unwrap()
            .len(),
        1
    );
    assert!(
        state
            .tick("2026-01-04T00:00:00Z".parse().unwrap())
            .await
            .unwrap()
            .is_empty()
    );
    let html = page_html(&app, "/jobs/dbt-prod/daily").await;
    assert!(html.contains("Missed Runs : 2"));
    assert!(!html.contains("<td>2026-01-02T00:00:00Z</td>"));
    assert!(html.contains("<td>2026-01-03T00:00:00Z</td>"));
    assert!(!html.contains("<td>closed</td>"));
    for _ in 0..2 {
        assert!(
            state
                .tick("2026-01-05T00:00:01Z".parse().unwrap())
                .await
                .unwrap()
                .is_empty()
        );
        let html = page_html(&app, "/jobs/dbt-prod/daily").await;
        assert!(html.contains("Missed Runs : 1"));
        assert!(html.contains("<td>open</td>"));
        assert!(html.contains("recovery"), "last Run is kept even when old");
        assert!(
            page_html(&app, "/incidents")
                .await
                .contains("2026-01-03T00:00:00Z")
        );
    }
    ingest_json(&app, serde_json::json!({"job":"daily","event":"finished","run_id":"latest-recovery","status":"succeeded","timestamp":"2026-01-05T00:01:00Z"})).await;
    assert!(!page_html(&app, "/incidents").await.contains("daily"));
    assert!(
        page_html(&app, "/jobs/dbt-prod/daily")
            .await
            .contains("<td>closed</td>")
    );
}
