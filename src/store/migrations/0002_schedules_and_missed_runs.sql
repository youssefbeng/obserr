ALTER TABLE jobs ADD COLUMN schedule_cron TEXT;
ALTER TABLE jobs ADD COLUMN grace_seconds INTEGER;
ALTER TABLE jobs ADD COLUMN timezone TEXT;
ALTER TABLE jobs ADD COLUMN owner TEXT;
ALTER TABLE jobs ADD COLUMN tags TEXT NOT NULL DEFAULT '[]';
ALTER TABLE jobs ADD COLUMN schedule_registered_at TEXT;
ALTER TABLE jobs ADD COLUMN last_scheduled_slot TEXT;

CREATE TABLE IF NOT EXISTS missed_runs (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    job_id INTEGER NOT NULL REFERENCES jobs(id) ON DELETE CASCADE,
    scheduled_for TEXT NOT NULL,
    detected_at TEXT NOT NULL,
    UNIQUE(job_id, scheduled_for)
);

CREATE INDEX IF NOT EXISTS idx_missed_runs_job_id ON missed_runs(job_id, scheduled_for DESC, id DESC);

ALTER TABLE incidents ADD COLUMN opening_missed_run_id INTEGER REFERENCES missed_runs(id) ON DELETE SET NULL;
