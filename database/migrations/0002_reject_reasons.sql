-- Section 11: document rejected records and reasons — per-reason counts on every run.
ALTER TABLE ops.pipeline_runs ADD COLUMN IF NOT EXISTS reject_reasons JSONB NOT NULL DEFAULT '{}'::jsonb;
