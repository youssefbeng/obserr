use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

pub type Source = String;
pub type Step = String;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum RunStatus {
    Running,
    Succeeded,
    Failed,
    Abandoned,
}

impl RunStatus {
    /// Retourne la représentation textuelle canonique du statut d'un `Run`.
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Running => "running",
            Self::Succeeded => "succeeded",
            Self::Failed => "failed",
            Self::Abandoned => "abandoned",
        }
    }

    /// Reconstruit un `RunStatus` depuis la valeur stockée en base SQLite.
    pub fn from_db(value: &str) -> Self {
        match value {
            "running" => Self::Running,
            "succeeded" => Self::Succeeded,
            "abandoned" => Self::Abandoned,
            _ => Self::Failed,
        }
    }

    /// Indique si ce statut constitue un `Failed Run` (`failed` ou `abandoned` selon `GLOSSARY.md`).
    pub fn is_failed_run(&self) -> bool {
        matches!(self, Self::Failed | Self::Abandoned)
    }
}

#[derive(Debug, Clone)]
pub struct Job {
    pub id: i64,
    pub source: Source,
    pub name: String,
    pub created_at: DateTime<Utc>,
}

#[derive(Debug, Clone)]
pub struct Run {
    pub id: i64,
    pub job_id: i64,
    pub external_run_id: Option<String>,
    pub status: RunStatus,
    pub started_at: Option<DateTime<Utc>>,
    pub finished_at: Option<DateTime<Utc>>,
    pub failed_step: Option<Step>,
    pub error_excerpt: Option<String>,
    pub external_url: Option<String>,
}

impl Run {
    /// Calcule la durée d'exécution en secondes lorsque le début et la fin du `Run` sont connus.
    pub fn duration_seconds(&self) -> Option<i64> {
        let start = self.started_at?;
        let end = self.finished_at?;
        Some((end - start).num_seconds().max(0))
    }

    /// Indique si ce `Run` est comptabilisé comme un `Failed Run` (`failed` ou `abandoned`).
    pub fn is_failed_run(&self) -> bool {
        self.status.is_failed_run()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum IncidentStatus {
    Open,
    Closed,
}

impl IncidentStatus {
    /// Retourne la représentation textuelle canonique du statut d'un `Incident`.
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Open => "open",
            Self::Closed => "closed",
        }
    }

    /// Reconstruit un `IncidentStatus` depuis la valeur stockée en base SQLite.
    pub fn from_db(value: &str) -> Self {
        match value {
            "closed" => Self::Closed,
            _ => Self::Open,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum IncidentCause {
    FailedRun,
    AbandonedRun,
}

impl IncidentCause {
    /// Retourne le code stocké en base SQLite pour la cause initiale d'un `Incident`.
    pub fn as_db_str(&self) -> &'static str {
        match self {
            Self::FailedRun => "failed_run",
            Self::AbandonedRun => "abandoned_run",
        }
    }

    /// Reconstruit un `IncidentCause` depuis la valeur stockée en base SQLite.
    pub fn from_db(value: &str) -> Self {
        match value {
            "abandoned_run" => Self::AbandonedRun,
            _ => Self::FailedRun,
        }
    }

    /// Retourne le libellé métier affiché dans l'UI et les alertes de `Channel`.
    pub fn label(&self) -> &'static str {
        match self {
            Self::FailedRun => "Failed Run (failed)",
            Self::AbandonedRun => "Failed Run (abandoned — Run abandonné)",
        }
    }
}

#[derive(Debug, Clone)]
pub struct Incident {
    pub id: i64,
    pub job_id: i64,
    pub status: IncidentStatus,
    pub cause: IncidentCause,
    pub opened_at: DateTime<Utc>,
    pub closed_at: Option<DateTime<Utc>>,
    pub opening_run_id: Option<i64>,
    pub closing_run_id: Option<i64>,
}

impl Incident {
    /// Calcule la durée totale de l'`Incident` en secondes lorsqu'il est fermé.
    pub fn duration_seconds(&self) -> Option<i64> {
        let closed = self.closed_at?;
        Some((closed - self.opened_at).num_seconds().max(0))
    }
}

/// Notification métier émise par le moteur d'`Incidents` à destination d'un `Channel`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum IncidentNotification {
    Opened {
        source: Source,
        job_name: String,
        cause: IncidentCause,
        opened_at: DateTime<Utc>,
        failed_step: Option<Step>,
        error_excerpt: Option<String>,
        external_url: Option<String>,
    },
    Resolved {
        source: Source,
        job_name: String,
        opened_at: DateTime<Utc>,
        closed_at: DateTime<Utc>,
        duration_seconds: i64,
    },
}

/// Transition d'état d'un `Incident` décidée par le cœur métier (`ADR-0005`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum IncidentTransition {
    None,
    Open {
        cause: IncidentCause,
        opening_run_id: i64,
        notification: IncidentNotification,
    },
    Close {
        incident_id: i64,
        closing_run_id: i64,
        notification: IncidentNotification,
    },
}

/// Contexte d'un `Run` terminé transmis au moteur d'évaluation des `Incidents`.
#[derive(Debug, Clone)]
pub struct CompletedRunOutcome {
    pub run_id: i64,
    pub status: RunStatus,
    pub failed_step: Option<Step>,
    pub error_excerpt: Option<String>,
    pub external_url: Option<String>,
}

/// Évalue la transition d'`Incident` provoquée par un `Run` terminé (`Failed Run` ou `succeeded`).
pub fn evaluate_incident_transition(
    open_incident: Option<&Incident>,
    event: &IngestEvent,
    completed: CompletedRunOutcome,
) -> IncidentTransition {
    if completed.status.is_failed_run() {
        if open_incident.is_none() {
            let cause = match completed.status {
                RunStatus::Abandoned => IncidentCause::AbandonedRun,
                _ => IncidentCause::FailedRun,
            };
            return IncidentTransition::Open {
                cause,
                opening_run_id: completed.run_id,
                notification: IncidentNotification::Opened {
                    source: event.source.clone(),
                    job_name: event.job_name.clone(),
                    cause,
                    opened_at: event.timestamp,
                    failed_step: completed.failed_step,
                    error_excerpt: completed.error_excerpt,
                    external_url: completed.external_url,
                },
            };
        }
        return IncidentTransition::None;
    }

    if completed.status == RunStatus::Succeeded {
        if let Some(inc) = open_incident {
            let duration_seconds = (event.timestamp - inc.opened_at).num_seconds().max(0);
            return IncidentTransition::Close {
                incident_id: inc.id,
                closing_run_id: completed.run_id,
                notification: IncidentNotification::Resolved {
                    source: event.source.clone(),
                    job_name: event.job_name.clone(),
                    opened_at: inc.opened_at,
                    closed_at: event.timestamp,
                    duration_seconds,
                },
            };
        }
    }

    IncidentTransition::None
}

#[derive(Debug, Clone)]
pub struct JobWithLatestRun {
    pub job: Job,
    pub latest_run: Option<Run>,
    pub open_incident: Option<Incident>,
}

#[derive(Debug, Clone)]
pub struct OpenIncidentSummary {
    pub incident: Incident,
    pub job: Job,
    pub latest_error_excerpt: Option<String>,
    pub external_url: Option<String>,
}

#[derive(Debug, Clone)]
pub struct JobDetails {
    pub job: Job,
    pub runs: Vec<Run>,
    pub incidents: Vec<Incident>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum FinishedOutcome {
    Succeeded,
    Failed,
}

impl FinishedOutcome {
    /// Convertit le résultat d'un événement `finished` en statut de `Run`.
    pub fn into_run_status(self) -> RunStatus {
        match self {
            Self::Succeeded => RunStatus::Succeeded,
            Self::Failed => RunStatus::Failed,
        }
    }
}

#[derive(Debug, Clone)]
pub enum IngestPayloadKind {
    Started,
    Finished {
        outcome: FinishedOutcome,
        failed_step: Option<Step>,
        error_excerpt: Option<String>,
    },
}

#[derive(Debug, Clone)]
pub struct IngestEvent {
    pub source: Source,
    pub job_name: String,
    pub run_id: Option<String>,
    pub timestamp: DateTime<Utc>,
    pub external_url: Option<String>,
    pub kind: IngestPayloadKind,
}

/// Décision métier pure produite par le domaine lors de la corrélation d'un événement (`ADR-0005`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RunCorrelation {
    /// Abandonne les `Runs` en cours listés (identifiants internes) et insère un nouveau `Run` `running`.
    StartNewRun { abandon_run_ids: Vec<i64> },
    /// Événement `started` idempotent sur un `Run` déjà `running` avec le même `run_id`.
    KeepRunningRun { existing_run_id: i64 },
    /// Met à jour un `Run` en cours existant avec son état de fin.
    FinishExistingRun {
        run_id: i64,
        status: RunStatus,
        failed_step: Option<Step>,
        error_excerpt: Option<String>,
    },
    /// Crée directement un `Run` terminé sans horodatage de début (`finished` reçu seul).
    InsertFinishedRunWithoutStart {
        status: RunStatus,
        failed_step: Option<Step>,
        error_excerpt: Option<String>,
    },
}

/// Détermine l'action de corrélation (`RunCorrelation`) à appliquer face aux `Runs` en cours d'un `Job`.
pub fn correlate_event(running_runs: &[Run], event: &IngestEvent) -> RunCorrelation {
    match &event.kind {
        IngestPayloadKind::Started => {
            let mut abandon_run_ids = Vec::new();
            let mut same_run_id = None;

            for prev in running_runs {
                let is_same = match (prev.external_run_id.as_deref(), event.run_id.as_deref()) {
                    (Some(existing), Some(incoming)) => existing == incoming,
                    _ => false,
                };
                if is_same {
                    same_run_id = Some(prev.id);
                } else {
                    abandon_run_ids.push(prev.id);
                }
            }

            if let Some(existing_run_id) = same_run_id {
                RunCorrelation::KeepRunningRun { existing_run_id }
            } else {
                RunCorrelation::StartNewRun { abandon_run_ids }
            }
        }
        IngestPayloadKind::Finished {
            outcome,
            failed_step,
            error_excerpt,
        } => {
            let status = outcome.into_run_status();
            let truncated_excerpt = error_excerpt.as_deref().map(truncate_error_excerpt);

            let matched_run = match event.run_id.as_deref() {
                Some(ext_id) => running_runs
                    .iter()
                    .rev()
                    .find(|r| r.external_run_id.as_deref() == Some(ext_id)),
                None => running_runs.last(),
            };

            if let Some(existing) = matched_run {
                RunCorrelation::FinishExistingRun {
                    run_id: existing.id,
                    status,
                    failed_step: failed_step.clone(),
                    error_excerpt: truncated_excerpt,
                }
            } else {
                RunCorrelation::InsertFinishedRunWithoutStart {
                    status,
                    failed_step: failed_step.clone(),
                    error_excerpt: truncated_excerpt,
                }
            }
        }
    }
}

/// Conserve uniquement les 20 dernières lignes d'un extrait d'erreur (`ADR-0003`).
pub fn truncate_error_excerpt(excerpt: &str) -> String {
    let lines: Vec<&str> = excerpt.lines().collect();
    let start = lines.len().saturating_sub(20);
    lines[start..].join("\n")
}
