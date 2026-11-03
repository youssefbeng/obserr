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
