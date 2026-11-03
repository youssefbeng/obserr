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



