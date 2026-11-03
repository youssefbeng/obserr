CREATE TABLE IF NOT EXISTS jobs (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    source TEXT NOT NULL,
    name TEXT NOT NULL,
    created_at TEXT NOT NULL,
    UNIQUE(source, name)
);

CREATE TABLE IF NOT EXISTS runs (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    job_id INTEGER NOT NULL REFERENCES jobs(id) ON DELETE CASCADE,
    external_run_id TEXT,
    status TEXT NOT NULL,
    started_at TEXT,
    finished_at TEXT,
    failed_step TEXT,
    error_excerpt TEXT,
    external_url TEXT
);

CREATE INDEX IF NOT EXISTS idx_runs_job_id ON runs(job_id, id DESC);

CREATE TABLE IF NOT EXISTS incidents (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    job_id INTEGER NOT NULL REFERENCES jobs(id) ON DELETE CASCADE,
    status TEXT NOT NULL,
    cause TEXT NOT NULL,
    opened_at TEXT NOT NULL,
    closed_at TEXT,
    opening_run_id INTEGER REFERENCES runs(id) ON DELETE SET NULL,
    closing_run_id INTEGER REFERENCES runs(id) ON DELETE SET NULL
);

CREATE INDEX IF NOT EXISTS idx_incidents_job_id ON incidents(job_id, id DESC);
CREATE INDEX IF NOT EXISTS idx_incidents_status ON incidents(status, opened_at ASC, id ASC);
CREATE UNIQUE INDEX IF NOT EXISTS idx_incidents_one_open_per_job ON incidents(job_id) WHERE status = 'open';
