use askama::Template;
use axum::{
    extract::{Path, State},
    http::{StatusCode, header},
    response::{Html, IntoResponse, Response},
};
use chrono::{DateTime, SecondsFormat, Utc};

use crate::{
    AppState,
    domain::{JobWithLatestRun, Run},
};

const STYLE_CSS: &str = include_str!("static/style.css");

/// Sert la feuille de style CSS embarquée dans le binaire.
pub async fn style_css() -> impl IntoResponse {
    ([(header::CONTENT_TYPE, "text/css; charset=utf-8")], STYLE_CSS)
}

fn format_opt_ts(ts: Option<DateTime<Utc>>) -> String {
    ts.map(|t| t.to_rfc3339_opts(SecondsFormat::Secs, true))
        .unwrap_or_else(|| "-".to_string())
}

fn format_opt_duration(seconds: Option<i64>) -> String {
    seconds
        .map(|s| format!("{s}s"))
        .unwrap_or_else(|| "-".to_string())
}

pub struct DashboardJobRow {
    pub source: String,
    pub name: String,
    pub latest_status: String,
    pub latest_started_at: String,
    pub latest_finished_at: String,
    pub latest_duration: String,
}

impl From<JobWithLatestRun> for DashboardJobRow {
    fn from(item: JobWithLatestRun) -> Self {
        match item.latest_run {
            Some(run) => {
                let latest_duration = format_opt_duration(run.duration_seconds());
                Self {
                    source: item.job.source,
                    name: item.job.name,
                    latest_status: run.status.as_str().to_string(),
                    latest_started_at: format_opt_ts(run.started_at),
                    latest_finished_at: format_opt_ts(run.finished_at),
                    latest_duration,
                }
            }
            None => Self {
                source: item.job.source,
                name: item.job.name,
                latest_status: "-".to_string(),
                latest_started_at: "-".to_string(),
                latest_finished_at: "-".to_string(),
                latest_duration: "-".to_string(),
            },
        }
    }
}

#[derive(Template)]
#[template(path = "dashboard.html")]
struct DashboardTemplate {
    jobs: Vec<DashboardJobRow>,
}

pub struct JobRunRow {
    pub run_id: String,
    pub status: String,
    pub started_at: String,
    pub finished_at: String,
    pub duration: String,
    pub failed_step: String,
    pub error_excerpt: String,
    pub external_url: String,
}

impl From<Run> for JobRunRow {
    fn from(run: Run) -> Self {
        let duration = format_opt_duration(run.duration_seconds());
        Self {
            run_id: run
                .external_run_id
                .unwrap_or_else(|| format!("#{}", run.id)),
            status: run.status.as_str().to_string(),
            started_at: format_opt_ts(run.started_at),
            finished_at: format_opt_ts(run.finished_at),
            duration,
            failed_step: run.failed_step.unwrap_or_else(|| "-".to_string()),
            error_excerpt: run.error_excerpt.unwrap_or_default(),
            external_url: run.external_url.unwrap_or_default(),
        }
    }
}

#[derive(Template)]
#[template(path = "job_detail.html")]
struct JobDetailTemplate {
    source: String,
    name: String,
    failed_runs_count: usize,
    runs: Vec<JobRunRow>,
}

/// Rend la page HTML du Tableau de bord (`/`) listant tous les `Jobs` et leur dernier `Run`.
pub async fn dashboard(State(state): State<AppState>) -> Result<Html<String>, StatusCode> {
    let items = {
        let store = state.store.lock().expect("store mutex poisoned");
        store
            .list_jobs_with_latest_run()
            .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?
    };
    let jobs = items.into_iter().map(DashboardJobRow::from).collect();
    let tpl = DashboardTemplate { jobs };
    tpl.render()
        .map(Html)
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)
}

/// Rend la page détail d'un `Job` (`/jobs/{source}/{name}`) avec l'historique de ses `Runs`.
pub async fn job_detail(
    State(state): State<AppState>,
    Path((source, name)): Path<(String, String)>,
) -> Result<Response, StatusCode> {
    let job_with_runs = {
        let store = state.store.lock().expect("store mutex poisoned");
        store
            .get_job_with_runs(&source, &name)
            .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?
    };

    let Some((job, runs)) = job_with_runs else {
        return Ok(StatusCode::NOT_FOUND.into_response());
    };

    let failed_runs_count = runs.iter().filter(|r| r.is_failed_run()).count();
    let tpl = JobDetailTemplate {
        source: job.source,
        name: job.name,
        failed_runs_count,
        runs: runs.into_iter().map(JobRunRow::from).collect(),
    };
    let html = tpl.render().map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    Ok(Html(html).into_response())
}
