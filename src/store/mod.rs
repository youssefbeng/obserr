use std::path::Path;

use anyhow::{Context, Result};
use chrono::{DateTime, SecondsFormat, Utc};
use rusqlite::{Connection, OptionalExtension, params};

use crate::config::Config;
use crate::domain::{
    Incident, IncidentCause, IncidentNotification, IncidentStatus, IncidentTransition, IngestEvent,
    Job, JobDetails, JobWithLatestRun, MissedRun, OpenIncidentSummary, Run, RunCorrelation,
    RunStatus, evaluate_missed_run_incident, is_slot_covered_by_runs, plan_run_ingestion,
};

const MIGRATION_0001: &str = include_str!("migrations/0001_initial.sql");
const MIGRATION_0002: &str = include_str!("migrations/0002_schedules_and_missed_runs.sql");

const SELECT_JOB_COLUMNS: &str = "SELECT id, source, name, schedule_cron, grace_seconds, \
                                  timezone, owner, tags, schedule_registered_at, \
                                  last_scheduled_slot, created_at FROM jobs";

const SELECT_RUN_COLUMNS: &str = "SELECT id, job_id, external_run_id, status, started_at, \
                                  finished_at, failed_step, error_excerpt, external_url FROM runs";

const SELECT_INCIDENT_COLUMNS: &str = "SELECT id, job_id, status, cause, opened_at, \
                                       closed_at, opening_run_id, closing_run_id FROM incidents";

const SELECT_MISSED_RUN_COLUMNS: &str =
    "SELECT id, job_id, scheduled_for, detected_at FROM missed_runs";

pub struct Store {
    conn: Connection,
    last_tick_now: Option<DateTime<Utc>>,
}

impl Store {
    /// Ouvre une base SQLite sur disque et exécute les migrations embarquées.
    pub fn open(path: impl AsRef<Path>) -> Result<Self> {
        let conn = Connection::open(path.as_ref())
            .with_context(|| format!("failed to open SQLite database at {:?}", path.as_ref()))?;
        Self::init(conn)
    }

    /// Ouvre une base SQLite en mémoire (`:memory:`) pour les tests d'intégration.
    pub fn open_in_memory() -> Result<Self> {
        let conn = Connection::open_in_memory().context("failed to open in-memory SQLite")?;
        Self::init(conn)
    }

    /// Configure les pragmas SQLite (`WAL`, `foreign_keys`) et applique les migrations embarquées.
    fn init(conn: Connection) -> Result<Self> {
        conn.pragma_update(None, "journal_mode", "WAL")
            .context("failed to enable WAL mode")?;
        conn.pragma_update(None, "foreign_keys", "ON")
            .context("failed to enable foreign_keys")?;

        conn.execute_batch(MIGRATION_0001)
            .context("failed to run migration 0001")?;

        let version: i64 = conn
            .pragma_query_value(None, "user_version", |row| row.get(0))
            .unwrap_or(0);
        if version < 2 {
            let has_schedule_col: bool = conn
                .prepare("SELECT schedule_cron FROM jobs LIMIT 0")
                .is_ok();
            if !has_schedule_col {
                conn.execute_batch(MIGRATION_0002)
                    .context("failed to run migration 0002")?;
            }
            conn.pragma_update(None, "user_version", 2)
                .context("failed to update user_version pragma")?;
        }

        Ok(Self {
            conn,
            last_tick_now: None,
        })
    }

    /// Retourne le dernier horodatage `now` observé par `tick(now)`.
    pub fn last_tick_now(&self) -> Option<DateTime<Utc>> {
        self.last_tick_now
    }

    /// Vérifie que la connexion SQLite répond aux requêtes (utilisé par `/health`).
    pub fn ping(&self) -> Result<()> {
        let _: i64 = self.conn.query_row("SELECT 1", [], |row| row.get(0))?;
        Ok(())
    }

    /// Synchronise en base SQLite les `Jobs` déclarés dans la configuration TOML (création et mise à jour à chaud).
    pub fn sync_config(
        &mut self,
        config: &Config,
        registered_at: Option<DateTime<Utc>>,
    ) -> Result<()> {
        let reg_str = registered_at.as_ref().map(format_ts);
        let created_ts = registered_at
            .or(self.last_tick_now)
            .unwrap_or(DateTime::UNIX_EPOCH);
        let created_str = format_ts(&created_ts);

        let tx = self.conn.transaction()?;
        for job_cfg in &config.jobs {
            let tags_json =
                serde_json::to_string(&job_cfg.tags).unwrap_or_else(|_| "[]".to_string());
            let sql_existing =
                format!("{SELECT_JOB_COLUMNS} WHERE source = ?1 AND name = ?2 LIMIT 1");
            let existing = tx
                .query_row(
                    &sql_existing,
                    params![job_cfg.source, job_cfg.name],
                    Self::map_job_row,
                )
                .optional()?;

            match existing {
                None => {
                    let initial_reg = if job_cfg.schedule_cron.is_some() {
                        reg_str.clone()
                    } else {
                        None
                    };
                    tx.execute(
                        "INSERT INTO jobs (
                            source, name, schedule_cron, grace_seconds, timezone,
                            owner, tags, schedule_registered_at, last_scheduled_slot, created_at
                         ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, NULL, ?9)",
                        params![
                            job_cfg.source,
                            job_cfg.name,
                            job_cfg.schedule_cron,
                            job_cfg.grace_seconds,
                            job_cfg.timezone,
                            job_cfg.owner,
                            tags_json,
                            initial_reg,
                            created_str,
                        ],
                    )?;
                }
                Some(prev_job) => {
                    let (new_reg, new_last_slot) = prev_job.reconcile_schedule_state(
                        job_cfg.schedule_cron.as_deref(),
                        job_cfg.timezone.as_deref(),
                        registered_at,
                    );
                    let new_reg_str = new_reg.as_ref().map(format_ts);
                    let new_last_slot_str = new_last_slot.as_ref().map(format_ts);

                    tx.execute(
                        "UPDATE jobs
                         SET schedule_cron = ?1,
                             grace_seconds = ?2,
                             timezone = ?3,
                             owner = ?4,
                             tags = ?5,
                             schedule_registered_at = ?6,
                             last_scheduled_slot = ?7
                         WHERE id = ?8",
                        params![
                            job_cfg.schedule_cron,
                            job_cfg.grace_seconds,
                            job_cfg.timezone,
                            job_cfg.owner,
                            tags_json,
                            new_reg_str,
                            new_last_slot_str,
                            prev_job.id,
                        ],
                    )?;
                }
            }
        }
        tx.commit()?;
        Ok(())
    }

    /// Évalue à l'instant `now` tous les `Jobs` dotés d'un `Schedule`, enregistre les `Missed Runs`, ouvre les `Incidents` et purge les historiques expirés.
    pub fn tick(
        &mut self,
        now: DateTime<Utc>,
        default_timezone: &str,
        retention_days: u32,
    ) -> Result<Vec<IncidentNotification>> {
        self.last_tick_now = Some(now);
        let now_str = format_ts(&now);
        let tx = self.conn.transaction()?;

        let sql_jobs = format!("{SELECT_JOB_COLUMNS} ORDER BY id ASC");
        let jobs = {
            let mut stmt = tx.prepare(&sql_jobs)?;
            stmt.query_map([], Self::map_job_row)?
                .collect::<rusqlite::Result<Vec<_>>>()?
        };

        let mut notifications = Vec::new();

        for job in jobs {
            let Some(schedule) = job.schedule(default_timezone) else {
                continue;
            };

            let registered_at = match job.schedule_registered_at {
                Some(reg) => reg,
                None => {
                    tx.execute(
                        "UPDATE jobs SET schedule_registered_at = ?1 WHERE id = ?2",
                        params![now_str, job.id],
                    )?;
                    now
                }
            };

            let due_slots = schedule.due_slots(registered_at, job.last_scheduled_slot, now);
            if due_slots.is_empty() {
                continue;
            }

            let runs = Self::load_all_runs_for_job_in_tx(&tx, job.id)?;

            for slot in due_slots {
                let slot_str = format_ts(&slot);
                if !is_slot_covered_by_runs(&runs, slot, schedule.grace_seconds) {
                    tx.execute(
                        "INSERT INTO missed_runs (job_id, scheduled_for, detected_at)
                         VALUES (?1, ?2, ?3)
                         ON CONFLICT(job_id, scheduled_for) DO NOTHING",
                        params![job.id, slot_str, now_str],
                    )?;

                    if tx.changes() > 0 {
                        let missed_run_id = tx.last_insert_rowid();
                        let open_incident = Self::load_open_incident(&tx, job.id)?;
                        let transition =
                            evaluate_missed_run_incident(open_incident.as_ref(), &job, slot, now);
                        if let Some(notif) = Self::apply_incident_transition_in_tx(
                            &tx,
                            job.id,
                            &now_str,
                            Some(missed_run_id),
                            transition,
                        )? {
                            notifications.push(notif);
                        }
                    }
                }

                tx.execute(
                    "UPDATE jobs SET last_scheduled_slot = ?1 WHERE id = ?2",
                    params![slot_str, job.id],
                )?;
            }
        }

        // Évalue les Schedules avant de purger les Runs qui couvrent leurs créneaux.
        let cutoff = now
            .checked_sub_signed(chrono::Duration::days(i64::from(retention_days)))
            .unwrap_or(DateTime::<Utc>::MIN_UTC);
        Self::purge_history_in_tx(&tx, cutoff)?;
        tx.commit()?;
        Ok(notifications)
    }

    /// Purge les historiques expirés en préservant les Incidents ouverts et le dernier Run reçu par Job.
    fn purge_history_in_tx(tx: &rusqlite::Transaction<'_>, cutoff: DateTime<Utc>) -> Result<()> {
        // Compare les instants avec une borne stricte ; les dates nulles des Runs en cours restent exclues.
        let cutoff = format_ts(&cutoff);
        tx.execute("DELETE FROM incidents WHERE status = 'closed' AND julianday(closed_at) < julianday(?1)", [&cutoff])?;
        tx.execute(
            "DELETE FROM runs WHERE julianday(finished_at) < julianday(?1)
                    AND id NOT IN (SELECT MAX(id) FROM runs GROUP BY job_id)
                    AND id NOT IN (
                        SELECT MAX(r.id) FROM runs r
                        JOIN incidents i ON i.job_id = r.job_id
                        WHERE i.status = 'open' AND r.status IN ('failed', 'abandoned')
                        GROUP BY r.job_id
                    )",
            [&cutoff],
        )?;
        tx.execute(
            "DELETE FROM missed_runs WHERE julianday(detected_at) < julianday(?1)",
            [&cutoff],
        )?;
        Ok(())
    }

    /// Enregistre de manière transactionnelle un événement `started` ou `finished`, met à jour l'`Incident` éventuel et crée le `Job` si nécessaire.
    pub fn record_event(&mut self, event: &IngestEvent) -> Result<Option<IncidentNotification>> {
        let tx = self.conn.transaction()?;
        let ts_str = format_ts(&event.timestamp);

        tx.execute(
            "INSERT INTO jobs (source, name, created_at) VALUES (?1, ?2, ?3)
             ON CONFLICT(source, name) DO NOTHING",
            params![event.source, event.job_name, ts_str],
        )?;

        let job_id: i64 = tx.query_row(
            "SELECT id FROM jobs WHERE source = ?1 AND name = ?2",
            params![event.source, event.job_name],
            |row| row.get(0),
        )?;

        let running_runs = Self::load_running_runs_in_tx(&tx, job_id)?;
        let open_incident = Self::load_open_incident(&tx, job_id)?;
        let plan = plan_run_ingestion(&running_runs, open_incident.as_ref(), event);
        let mut inserted_finished_run_id = None;

        match &plan.correlation {
            RunCorrelation::KeepRunningRun { .. } => {}
            RunCorrelation::StartNewRun { abandon_run_ids } => {
                for &abandoned_id in abandon_run_ids {
                    tx.execute(
                        "UPDATE runs SET status = ?1, finished_at = ?2 WHERE id = ?3",
                        params![RunStatus::Abandoned.as_str(), ts_str, abandoned_id],
                    )?;
                }
                tx.execute(
                    "INSERT INTO runs (job_id, external_run_id, status, started_at, external_url)
                     VALUES (?1, ?2, ?3, ?4, ?5)",
                    params![
                        job_id,
                        event.run_id,
                        RunStatus::Running.as_str(),
                        ts_str,
                        event.external_url
                    ],
                )?;
            }
            RunCorrelation::FinishExistingRun {
                run_id,
                status,
                failed_step,
                error_excerpt,
                external_url,
            } => {
                tx.execute(
                    "UPDATE runs
                     SET status = ?1,
                         finished_at = ?2,
                         failed_step = ?3,
                         error_excerpt = ?4,
                         external_url = ?5
                     WHERE id = ?6",
                    params![
                        status.as_str(),
                        ts_str,
                        failed_step,
                        error_excerpt,
                        external_url,
                        run_id
                    ],
                )?;
            }
            RunCorrelation::InsertFinishedRunWithoutStart {
                status,
                failed_step,
                error_excerpt,
                external_url,
            } => {
                tx.execute(
                    "INSERT INTO runs (
                        job_id, external_run_id, status, finished_at,
                        failed_step, error_excerpt, external_url
                     ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
                    params![
                        job_id,
                        event.run_id,
                        status.as_str(),
                        ts_str,
                        failed_step,
                        error_excerpt,
                        external_url
                    ],
                )?;
                inserted_finished_run_id = Some(tx.last_insert_rowid());
            }
        }

        let transition = plan.into_transition(inserted_finished_run_id);
        let notification =
            Self::apply_incident_transition_in_tx(&tx, job_id, &ts_str, None, transition)?;
        tx.commit()?;
        Ok(notification)
    }

    /// Calcule l'`Incident` ouvert, le fuseau horaire effectif et le prochain créneau attendu d'un `Job` à l'instant `ref_now`.
    fn compute_job_schedule_view(
        &self,
        job: &Job,
        default_timezone: &str,
        ref_now: DateTime<Utc>,
    ) -> Result<(Option<Incident>, String, Option<DateTime<Utc>>)> {
        let open_incident = Self::load_open_incident(&self.conn, job.id)?;
        let effective_timezone = job.effective_timezone(default_timezone).to_string();
        let next_expected_slot = job
            .schedule(default_timezone)
            .and_then(|s| s.next_slot_after(ref_now));
        Ok((open_incident, effective_timezone, next_expected_slot))
    }

    /// Liste tous les `Jobs` triés par statut (`Incident ouvert`, puis `OK`, puis `Sans Schedule`) et par `(source, name)`.
    pub fn list_jobs_with_latest_run(
        &self,
        default_timezone: &str,
        ref_now: DateTime<Utc>,
    ) -> Result<Vec<JobWithLatestRun>> {
        let sql = format!("{SELECT_JOB_COLUMNS} ORDER BY source ASC, name ASC");
        let mut stmt = self.conn.prepare(&sql)?;
        let jobs_iter = stmt.query_map([], Self::map_job_row)?;

        let sql_latest = format!("{SELECT_RUN_COLUMNS} WHERE job_id = ?1 ORDER BY id DESC LIMIT 1");
        let mut results = Vec::new();
        for job_res in jobs_iter {
            let job = job_res?;
            let latest_run = self
                .conn
                .query_row(&sql_latest, params![job.id], Self::map_run_row)
                .optional()?;
            let (open_incident, effective_timezone, next_expected_slot) =
                self.compute_job_schedule_view(&job, default_timezone, ref_now)?;

            results.push(JobWithLatestRun {
                job,
                latest_run,
                open_incident,
                effective_timezone,
                next_expected_slot,
            });
        }

        results.sort_by(|a, b| {
            a.sort_rank()
                .cmp(&b.sort_rank())
                .then_with(|| a.job.source.cmp(&b.job.source))
                .then_with(|| a.job.name.cmp(&b.job.name))
        });

        Ok(results)
    }

    /// Récupère un `Job` par `(source, name)` ainsi que ses métadonnées, son prochain créneau et son historique.
    pub fn get_job_details(
        &self,
        source: &str,
        name: &str,
        default_timezone: &str,
        ref_now: DateTime<Utc>,
    ) -> Result<Option<JobDetails>> {
        let sql_job = format!("{SELECT_JOB_COLUMNS} WHERE source = ?1 AND name = ?2");
        let job_opt = self
            .conn
            .query_row(&sql_job, params![source, name], Self::map_job_row)
            .optional()?;

        let Some(job) = job_opt else {
            return Ok(None);
        };

        let (open_incident, effective_timezone, next_expected_slot) =
            self.compute_job_schedule_view(&job, default_timezone, ref_now)?;

        let sql_runs = format!("{SELECT_RUN_COLUMNS} WHERE job_id = ?1 ORDER BY id DESC");
        let mut stmt_runs = self.conn.prepare(&sql_runs)?;
        let runs = stmt_runs
            .query_map(params![job.id], Self::map_run_row)?
            .collect::<rusqlite::Result<Vec<_>>>()?;

        let sql_missed = format!(
            "{SELECT_MISSED_RUN_COLUMNS} WHERE job_id = ?1 ORDER BY scheduled_for DESC, id DESC"
        );
        let mut stmt_missed = self.conn.prepare(&sql_missed)?;
        let missed_runs = stmt_missed
            .query_map(params![job.id], Self::map_missed_run_row)?
            .collect::<rusqlite::Result<Vec<_>>>()?;

        let sql_incidents = format!("{SELECT_INCIDENT_COLUMNS} WHERE job_id = ?1 ORDER BY id DESC");
        let mut stmt_incidents = self.conn.prepare(&sql_incidents)?;
        let incidents = stmt_incidents
            .query_map(params![job.id], Self::map_incident_row)?
            .collect::<rusqlite::Result<Vec<_>>>()?;

        Ok(Some(JobDetails {
            job,
            open_incident,
            effective_timezone,
            next_expected_slot,
            runs,
            missed_runs,
            incidents,
        }))
    }

    /// Liste tous les `Incidents` actuellement au statut `open` avec le dernier extrait d'erreur et le lien vers la `Source`.
    pub fn list_open_incidents(&self) -> Result<Vec<OpenIncidentSummary>> {
        let sql = format!(
            "{SELECT_INCIDENT_COLUMNS} WHERE status = 'open' ORDER BY opened_at ASC, id ASC"
        );
        let mut stmt = self.conn.prepare(&sql)?;
        let incidents_iter = stmt.query_map([], Self::map_incident_row)?;

        let sql_job = format!("{SELECT_JOB_COLUMNS} WHERE id = ?1");
        let sql_runs = format!("{SELECT_RUN_COLUMNS} WHERE job_id = ?1 ORDER BY id DESC");
        let sql_latest_missed =
            format!("{SELECT_MISSED_RUN_COLUMNS} WHERE job_id = ?1 ORDER BY id DESC LIMIT 1");

        let mut summaries = Vec::new();
        for inc_res in incidents_iter {
            let incident = inc_res?;
            let job = self
                .conn
                .query_row(&sql_job, params![incident.job_id], Self::map_job_row)?;

            let mut stmt_runs = self.conn.prepare(&sql_runs)?;
            let runs = stmt_runs
                .query_map(params![job.id], Self::map_run_row)?
                .collect::<rusqlite::Result<Vec<_>>>()?;

            let latest_missed = self
                .conn
                .query_row(
                    &sql_latest_missed,
                    params![job.id],
                    Self::map_missed_run_row,
                )
                .optional()?;

            summaries.push(OpenIncidentSummary::from_domain(
                incident,
                job,
                &runs,
                latest_missed.as_ref(),
            ));
        }

        Ok(summaries)
    }

    /// Charge tous les `Runs` d'un `Job` dans une transaction pour vérifier la couverture des créneaux de `Schedule`.
    fn load_all_runs_for_job_in_tx(
        tx: &rusqlite::Transaction<'_>,
        job_id: i64,
    ) -> Result<Vec<Run>> {
        let sql = format!("{SELECT_RUN_COLUMNS} WHERE job_id = ?1 ORDER BY id ASC");
        let mut stmt = tx.prepare(&sql)?;
        let runs = stmt
            .query_map(params![job_id], Self::map_run_row)?
            .collect::<rusqlite::Result<Vec<_>>>()?;
        Ok(runs)
    }

    /// Charge tous les `Runs` actuellement au statut `running` pour un `Job` donné dans une transaction.
    fn load_running_runs_in_tx(tx: &rusqlite::Transaction<'_>, job_id: i64) -> Result<Vec<Run>> {
        let sql = format!(
            "{SELECT_RUN_COLUMNS} WHERE job_id = ?1 AND status = 'running' ORDER BY id ASC"
        );
        let mut stmt = tx.prepare(&sql)?;
        let runs = stmt
            .query_map(params![job_id], Self::map_run_row)?
            .collect::<rusqlite::Result<Vec<_>>>()?;
        Ok(runs)
    }

    /// Applique en base SQLite la transition d'`Incident` décidée par le domaine et retourne la notification éventuelle.
    fn apply_incident_transition_in_tx(
        tx: &rusqlite::Transaction<'_>,
        job_id: i64,
        ts_str: &str,
        opening_missed_run_id: Option<i64>,
        transition: IncidentTransition,
    ) -> Result<Option<IncidentNotification>> {
        match transition {
            IncidentTransition::None => Ok(None),
            IncidentTransition::Open {
                cause,
                opening_run_id,
                notification,
            } => {
                tx.execute(
                    "INSERT INTO incidents (
                        job_id, status, cause, opened_at, opening_run_id, opening_missed_run_id
                     ) VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
                    params![
                        job_id,
                        IncidentStatus::Open.as_str(),
                        cause.as_db_str(),
                        ts_str,
                        opening_run_id,
                        opening_missed_run_id,
                    ],
                )?;
                Ok(Some(notification))
            }
            IncidentTransition::Close {
                incident_id,
                closing_run_id,
                notification,
            } => {
                tx.execute(
                    "UPDATE incidents
                     SET status = ?1, closed_at = ?2, closing_run_id = ?3
                     WHERE id = ?4",
                    params![
                        IncidentStatus::Closed.as_str(),
                        ts_str,
                        closing_run_id,
                        incident_id
                    ],
                )?;
                Ok(Some(notification))
            }
        }
    }

    /// Charge l'`Incident` actuellement au statut `open` pour un `Job` donné.
    fn load_open_incident(conn: &Connection, job_id: i64) -> Result<Option<Incident>> {
        let sql = format!(
            "{SELECT_INCIDENT_COLUMNS} WHERE job_id = ?1 AND status = 'open' ORDER BY id DESC LIMIT 1"
        );
        let incident = conn
            .query_row(&sql, params![job_id], Self::map_incident_row)
            .optional()?;
        Ok(incident)
    }

    fn map_incident_row(row: &rusqlite::Row<'_>) -> rusqlite::Result<Incident> {
        let status_str: String = row.get(2)?;
        let cause_str: String = row.get(3)?;
        let opened_str: String = row.get(4)?;
        let closed_str: Option<String> = row.get(5)?;
        Ok(Incident {
            id: row.get(0)?,
            job_id: row.get(1)?,
            status: IncidentStatus::from_db(&status_str),
            cause: IncidentCause::from_db(&cause_str),
            opened_at: parse_ts(&opened_str),
            closed_at: closed_str.as_deref().map(parse_ts),
            opening_run_id: row.get(6)?,
            closing_run_id: row.get(7)?,
        })
    }

    fn map_job_row(row: &rusqlite::Row<'_>) -> rusqlite::Result<Job> {
        let tags_raw: Option<String> = row.get(7)?;
        let tags: Vec<String> = tags_raw
            .as_deref()
            .and_then(|s| serde_json::from_str(s).ok())
            .unwrap_or_default();
        let reg_str: Option<String> = row.get(8)?;
        let last_slot_str: Option<String> = row.get(9)?;
        let created_str: String = row.get(10)?;

        Ok(Job {
            id: row.get(0)?,
            source: row.get(1)?,
            name: row.get(2)?,
            schedule_cron: row.get(3)?,
            grace_seconds: row.get(4)?,
            timezone: row.get(5)?,
            owner: row.get(6)?,
            tags,
            schedule_registered_at: reg_str.as_deref().map(parse_ts),
            last_scheduled_slot: last_slot_str.as_deref().map(parse_ts),
            created_at: parse_ts(&created_str),
        })
    }

    fn map_run_row(row: &rusqlite::Row<'_>) -> rusqlite::Result<Run> {
        let status_str: String = row.get(3)?;
        let started_str: Option<String> = row.get(4)?;
        let finished_str: Option<String> = row.get(5)?;
        Ok(Run {
            id: row.get(0)?,
            job_id: row.get(1)?,
            external_run_id: row.get(2)?,
            status: RunStatus::from_db(&status_str),
            started_at: started_str.as_deref().map(parse_ts),
            finished_at: finished_str.as_deref().map(parse_ts),
            failed_step: row.get(6)?,
            error_excerpt: row.get(7)?,
            external_url: row.get(8)?,
        })
    }

    fn map_missed_run_row(row: &rusqlite::Row<'_>) -> rusqlite::Result<MissedRun> {
        let sched_str: String = row.get(2)?;
        let detected_str: String = row.get(3)?;
        Ok(MissedRun {
            id: row.get(0)?,
            job_id: row.get(1)?,
            scheduled_for: parse_ts(&sched_str),
            detected_at: parse_ts(&detected_str),
        })
    }
}

fn format_ts(dt: &DateTime<Utc>) -> String {
    dt.to_rfc3339_opts(SecondsFormat::Secs, true)
}

fn parse_ts(s: &str) -> DateTime<Utc> {
    DateTime::parse_from_rfc3339(s)
        .map(|dt| dt.with_timezone(&Utc))
        .unwrap_or(DateTime::UNIX_EPOCH)
}
