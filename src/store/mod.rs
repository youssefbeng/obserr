use std::path::Path;

use anyhow::{Context, Result};
use chrono::{DateTime, SecondsFormat, Utc};
use rusqlite::{Connection, OptionalExtension, params};

use crate::domain::{
    IngestEvent, Job, JobWithLatestRun, Run, RunCorrelation, RunStatus, correlate_event,
};

const MIGRATION_0001: &str = include_str!("migrations/0001_initial.sql");

const SELECT_RUN_COLUMNS: &str = "SELECT id, job_id, external_run_id, status, started_at, \
                                  finished_at, failed_step, error_excerpt, external_url FROM runs";

pub struct Store {
    conn: Connection,
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

    /// Configure les pragmas SQLite (`WAL`, `foreign_keys`) et applique le schéma initial.
    fn init(conn: Connection) -> Result<Self> {
        conn.pragma_update(None, "journal_mode", "WAL")
            .context("failed to enable WAL mode")?;
        conn.pragma_update(None, "foreign_keys", "ON")
            .context("failed to enable foreign_keys")?;
        conn.execute_batch(MIGRATION_0001)
            .context("failed to run database migrations")?;
        Ok(Self { conn })
    }

    /// Vérifie que la connexion SQLite répond aux requêtes (utilisé par `/health`).
    pub fn ping(&self) -> Result<()> {
        let _: i64 = self.conn.query_row("SELECT 1", [], |row| row.get(0))?;
        Ok(())
    }

    /// Enregistre de manière transactionnelle un événement `started` ou `finished` et crée le `Job` si nécessaire.
    pub fn record_event(&mut self, event: &IngestEvent) -> Result<()> {
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
        let correlation = correlate_event(&running_runs, event);

        match correlation {
            RunCorrelation::KeepRunningRun { .. } => {}
            RunCorrelation::StartNewRun { abandon_run_ids } => {
                for abandoned_id in abandon_run_ids {
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
            } => {
                tx.execute(
                    "UPDATE runs
                     SET status = ?1,
                         finished_at = ?2,
                         failed_step = ?3,
                         error_excerpt = ?4,
                         external_url = COALESCE(?5, external_url)
                     WHERE id = ?6",
                    params![
                        status.as_str(),
                        ts_str,
                        failed_step,
                        error_excerpt,
                        event.external_url,
                        run_id
                    ],
                )?;
            }
            RunCorrelation::InsertFinishedRunWithoutStart {
                status,
                failed_step,
                error_excerpt,
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
                        event.external_url
                    ],
                )?;
            }
        }

        tx.commit()?;
        Ok(())
    }

    /// Liste tous les `Jobs` triés par `(source, name)` accompagnés de leur dernier `Run` éventuel.
    pub fn list_jobs_with_latest_run(&self) -> Result<Vec<JobWithLatestRun>> {
        let mut stmt = self
            .conn
            .prepare("SELECT id, source, name, created_at FROM jobs ORDER BY source ASC, name ASC")?;
        let jobs_iter = stmt.query_map([], Self::map_job_row)?;

        let sql_latest = format!("{SELECT_RUN_COLUMNS} WHERE job_id = ?1 ORDER BY id DESC LIMIT 1");
        let mut results = Vec::new();
        for job_res in jobs_iter {
            let job = job_res?;
            let latest_run = self
                .conn
                .query_row(&sql_latest, params![job.id], Self::map_run_row)
                .optional()?;
            results.push(JobWithLatestRun { job, latest_run });
        }
        Ok(results)
    }

    /// Récupère un `Job` par `(source, name)` ainsi que l'historique complet de ses `Runs` du plus récent au plus ancien.
    pub fn get_job_with_runs(&self, source: &str, name: &str) -> Result<Option<(Job, Vec<Run>)>> {
        let job_opt = self
            .conn
            .query_row(
                "SELECT id, source, name, created_at FROM jobs WHERE source = ?1 AND name = ?2",
                params![source, name],
                Self::map_job_row,
            )
            .optional()?;

        let Some(job) = job_opt else {
            return Ok(None);
        };

        let sql = format!("{SELECT_RUN_COLUMNS} WHERE job_id = ?1 ORDER BY id DESC");
        let mut stmt = self.conn.prepare(&sql)?;
        let runs = stmt
            .query_map(params![job.id], Self::map_run_row)?
            .collect::<rusqlite::Result<Vec<_>>>()?;
        Ok(Some((job, runs)))
    }

    /// Charge tous les `Runs` actuellement au statut `running` pour un `Job` donné dans une transaction.
    fn load_running_runs_in_tx(tx: &rusqlite::Transaction<'_>, job_id: i64) -> Result<Vec<Run>> {
        let sql = format!("{SELECT_RUN_COLUMNS} WHERE job_id = ?1 AND status = 'running' ORDER BY id ASC");
        let mut stmt = tx.prepare(&sql)?;
        let runs = stmt
            .query_map(params![job_id], Self::map_run_row)?
            .collect::<rusqlite::Result<Vec<_>>>()?;
        Ok(runs)
    }

    fn map_job_row(row: &rusqlite::Row<'_>) -> rusqlite::Result<Job> {
        let created_str: String = row.get(3)?;
        Ok(Job {
            id: row.get(0)?,
            source: row.get(1)?,
            name: row.get(2)?,
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
}

fn format_ts(dt: &DateTime<Utc>) -> String {
    dt.to_rfc3339_opts(SecondsFormat::Secs, true)
}

fn parse_ts(s: &str) -> DateTime<Utc> {
    DateTime::parse_from_rfc3339(s)
        .map(|dt| dt.with_timezone(&Utc))
        .unwrap_or(DateTime::UNIX_EPOCH)
}
