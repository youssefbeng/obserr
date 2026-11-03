use askama::Template;
use axum::{
    extract::{Path, State},
    http::{StatusCode, header},
    response::{Html, IntoResponse, Response},
};
use chrono::{DateTime, SecondsFormat, Utc};

use crate::{
    AppState,
    config::format_grace_duration,
    domain::{Incident, Job, JobWithLatestRun, MissedRun, OpenIncidentSummary, Run},
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

struct JobViewMeta {
    status_label: String,
    schedule_summary: String,
    owner: String,
    tags: String,
    next_slot: String,
}

impl JobViewMeta {
    fn from_job(
        job: &Job,
        has_open_incident: bool,
        effective_timezone: &str,
        next_expected_slot: Option<DateTime<Utc>>,
    ) -> Self {
        let schedule_summary = match job.schedule_cron.as_deref() {
            Some(cron) => {
                let grace_str = format_grace_duration(job.grace_seconds.unwrap_or(0));
                format!("{cron} (grace {grace_str}, {effective_timezone})")
            }
            None => "-".to_string(),
        };
        let owner = job.owner.clone().unwrap_or_else(|| "-".to_string());
        let tags = if job.tags.is_empty() {
            "-".to_string()
        } else {
            job.tags.join(", ")
        };

        Self {
            status_label: job.status_label(has_open_incident).to_string(),
            schedule_summary,
            owner,
            tags,
            next_slot: format_opt_ts(next_expected_slot),
        }
    }
}

pub struct DashboardJobRow {
    pub source: String,
    pub name: String,
    pub incident_state: String,
    pub schedule_summary: String,
    pub owner: String,
    pub tags: String,
    pub next_slot: String,
    pub latest_status: String,
    pub latest_started_at: String,
    pub latest_finished_at: String,
    pub latest_duration: String,
}

impl From<JobWithLatestRun> for DashboardJobRow {
    fn from(item: JobWithLatestRun) -> Self {
        let meta = JobViewMeta::from_job(
            &item.job,
            item.open_incident.is_some(),
            &item.effective_timezone,
            item.next_expected_slot,
        );

        let (latest_status, latest_started_at, latest_finished_at, latest_duration) =
            match item.latest_run {
                Some(run) => {
                    let duration = format_opt_duration(run.duration_seconds());
                    (
                        run.status.as_str().to_string(),
                        format_opt_ts(run.started_at),
                        format_opt_ts(run.finished_at),
                        duration,
                    )
                }
                None => (
                    "-".to_string(),
                    "-".to_string(),
                    "-".to_string(),
                    "-".to_string(),
                ),
            };

        Self {
            source: item.job.source,
            name: item.job.name,
            incident_state: meta.status_label,
            schedule_summary: meta.schedule_summary,
            owner: meta.owner,
            tags: meta.tags,
            next_slot: meta.next_slot,
            latest_status,
            latest_started_at,
            latest_finished_at,
            latest_duration,
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

pub struct JobMissedRunRow {
    pub scheduled_for: String,
    pub detected_at: String,
}

impl From<MissedRun> for JobMissedRunRow {
    fn from(m: MissedRun) -> Self {
        Self {
            scheduled_for: format_ts(m.scheduled_for),
            detected_at: format_ts(m.detected_at),
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
    status_label: String,
    schedule_summary: String,
    owner: String,
    tags: String,
    next_slot: String,
    failed_runs_count: usize,
    missed_runs_count: usize,
    missed_runs: Vec<JobMissedRunRow>,
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
    state.reload_if_changed(None);
    let cfg = state.current_config();
    let items = {
        let store = state.store.lock().expect("store mutex poisoned");
        let ref_now = store.last_tick_now().unwrap_or_else(Utc::now);
        store
            .list_jobs_with_latest_run(&cfg.server.timezone, ref_now)
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
    state.reload_if_changed(None);
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

/// Rend la page détail d'un `Job` (`/jobs/{source}/{name}`) avec ses métadonnées, son `Schedule` et son historique.
pub async fn job_detail(
    State(state): State<AppState>,
    Path((source, name)): Path<(String, String)>,
) -> Result<Response, StatusCode> {
    state.reload_if_changed(None);
    let cfg = state.current_config();
    let details_opt = {
        let store = state.store.lock().expect("store mutex poisoned");
        let ref_now = store.last_tick_now().unwrap_or_else(Utc::now);
        store
            .get_job_details(&source, &name, &cfg.server.timezone, ref_now)
            .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?
    };

    let Some(details) = details_opt else {
        return Ok(StatusCode::NOT_FOUND.into_response());
    };

    let meta = JobViewMeta::from_job(
        &details.job,
        details.open_incident.is_some(),
        &details.effective_timezone,
        details.next_expected_slot,
    );
    let failed_runs_count = details.runs.iter().filter(|r| r.is_failed_run()).count();
    let missed_runs_count = details.missed_runs.len();

    let tpl = JobDetailTemplate {
        source: details.job.source,
        name: details.job.name,
        status_label: meta.status_label,
        schedule_summary: meta.schedule_summary,
        owner: meta.owner,
        tags: meta.tags,
        next_slot: meta.next_slot,
        failed_runs_count,
        missed_runs_count,
        missed_runs: details
            .missed_runs
            .into_iter()
            .map(JobMissedRunRow::from)
            .collect(),
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
