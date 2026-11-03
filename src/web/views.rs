use askama::Template;
use axum::{
    extract::{Path, State},
    http::{StatusCode, header},
    response::{Html, IntoResponse, Response},
};
use chrono::{DateTime, SecondsFormat, Utc};

use crate::{
    AppState,
    domain::{Incident, JobWithLatestRun, OpenIncidentSummary, Run},
};

const STYLE_CSS: &str = include_str!("static/style.css");

/// Sert la feuille de style CSS embarquée dans le binaire.
pub async fn style_css() -> impl IntoResponse {
    ([(header::CONTENT_TYPE, "text/css; charset=utf-8")], STYLE_CSS)
}

fn format_ts(ts: DateTime<Utc>) -> String {
    ts.to_rfc3339_opts(SecondsFormat::Secs, true)
}

fn format_opt_ts(ts: Option<DateTime<Utc>>) -> String {
    ts.map(format_ts).unwrap_or_else(|| "-".to_string())
}

fn format_opt_duration(seconds: Option<i64>) -> String {
    seconds
        .map(|s| format!("{s}s"))
        .unwrap_or_else(|| "-".to_string())
}

pub struct DashboardJobRow {
    pub source: String,
    pub name: String,
    pub incident_state: String,
    pub latest_status: String,
    pub latest_started_at: String,
    pub latest_finished_at: String,
    pub latest_duration: String,
}

impl From<JobWithLatestRun> for DashboardJobRow {
    fn from(item: JobWithLatestRun) -> Self {
        let incident_state = if item.open_incident.is_some() {
            "Incident ouvert".to_string()
        } else {
            "OK".to_string()
        };
        match item.latest_run {
            Some(run) => {
                let latest_duration = format_opt_duration(run.duration_seconds());
                Self {
                    source: item.job.source,
                    name: item.job.name,
                    incident_state,
                    latest_status: run.status.as_str().to_string(),
                    latest_started_at: format_opt_ts(run.started_at),
                    latest_finished_at: format_opt_ts(run.finished_at),
                    latest_duration,
                }
            }
            None => Self {
                source: item.job.source,
                name: item.job.name,
                incident_state,
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

pub struct JobIncidentRow {
    pub id: i64,
    pub status: String,
    pub cause: String,
    pub opened_at: String,
    pub closed_at: String,
    pub duration: String,
}

impl From<Incident> for JobIncidentRow {
    fn from(inc: Incident) -> Self {
        let duration = format_opt_duration(inc.duration_seconds());
        Self {
            id: inc.id,
            status: inc.status.as_str().to_string(),
            cause: inc.cause.label().to_string(),
            opened_at: format_ts(inc.opened_at),
            closed_at: format_opt_ts(inc.closed_at),
            duration,
        }
    }
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
    incidents: Vec<JobIncidentRow>,
    runs: Vec<JobRunRow>,
}

pub struct OpenIncidentRow {
    pub job_name: String,
    pub source: String,
    pub status: String,
    pub opened_at: String,
    pub cause: String,
    pub latest_error_excerpt: String,
    pub external_url: String,
}

impl From<OpenIncidentSummary> for OpenIncidentRow {
    fn from(summary: OpenIncidentSummary) -> Self {
        Self {
            job_name: summary.job.name,
            source: summary.job.source,
            status: summary.incident.status.as_str().to_string(),
            opened_at: format_ts(summary.incident.opened_at),
            cause: summary.incident.cause.label().to_string(),
            latest_error_excerpt: summary.latest_error_excerpt.unwrap_or_default(),
            external_url: summary.external_url.unwrap_or_default(),
        }
    }
}

#[derive(Template)]
#[template(path = "incidents.html")]
struct IncidentsTemplate {
    incidents: Vec<OpenIncidentRow>,
}

/// Rend la page HTML du Tableau de bord (`/`) listant tous les `Jobs` avec les `Incidents` ouverts en tête.
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

/// Rend la Page des `Incidents` ouverts (`/incidents`) avec l'heure d'ouverture, la cause initiale, le dernier extrait d'erreur et le lien vers la `Source`.
pub async fn incidents(State(state): State<AppState>) -> Result<Html<String>, StatusCode> {
    let summaries = {
        let store = state.store.lock().expect("store mutex poisoned");
        store
            .list_open_incidents()
            .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?
    };
    let rows = summaries.into_iter().map(OpenIncidentRow::from).collect();
    let tpl = IncidentsTemplate { incidents: rows };
    tpl.render()
        .map(Html)
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)
}

/// Rend la page détail d'un `Job` (`/jobs/{source}/{name}`) avec l'historique de ses `Incidents` et de ses `Runs`.
pub async fn job_detail(
    State(state): State<AppState>,
    Path((source, name)): Path<(String, String)>,
) -> Result<Response, StatusCode> {
    let details_opt = {
        let store = state.store.lock().expect("store mutex poisoned");
        store
            .get_job_details(&source, &name)
            .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?
    };

    let Some(details) = details_opt else {
        return Ok(StatusCode::NOT_FOUND.into_response());
    };

    let failed_runs_count = details.runs.iter().filter(|r| r.is_failed_run()).count();
    let tpl = JobDetailTemplate {
        source: details.job.source,
        name: details.job.name,
        failed_runs_count,
        incidents: details
            .incidents
            .into_iter()
            .map(JobIncidentRow::from)
            .collect(),
        runs: details.runs.into_iter().map(JobRunRow::from).collect(),
    };
    let html = tpl.render().map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    Ok(Html(html).into_response())
}
