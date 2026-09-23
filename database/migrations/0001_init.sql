-- Phase 1 foundation: extensions, ops schema (observability, section 26) and
-- the geographic reference dimensions (section 4 / 7). Data is loaded in Phase 2.
CREATE EXTENSION IF NOT EXISTS postgis;

CREATE SCHEMA IF NOT EXISTS ops;
CREATE SCHEMA IF NOT EXISTS reference;

-- Ingestion / pipeline run ledger. One row per run, rerunnable pipelines append.
CREATE TABLE IF NOT EXISTS ops.pipeline_runs (
    run_id            BIGSERIAL PRIMARY KEY,
    source            TEXT        NOT NULL,
    pipeline_version  TEXT        NOT NULL,
    git_commit        TEXT,
    data_version      TEXT,
    started_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
    finished_at       TIMESTAMPTZ,
    rows_received     BIGINT      NOT NULL DEFAULT 0 CHECK (rows_received >= 0),
    rows_processed    BIGINT      NOT NULL DEFAULT 0 CHECK (rows_processed >= 0),
    rows_rejected     BIGINT      NOT NULL DEFAULT 0 CHECK (rows_rejected >= 0),
    status            TEXT        NOT NULL CHECK (status IN ('running', 'success', 'failed')),
    error             TEXT
);
CREATE INDEX IF NOT EXISTS pipeline_runs_source_started_idx ON ops.pipeline_runs (source, started_at DESC);

CREATE TABLE IF NOT EXISTS reference.dim_region (
    region_code  CHAR(2)  PRIMARY KEY,
    name         TEXT     NOT NULL,
    geometry     geometry(MultiPolygon, 4326)
);

CREATE TABLE IF NOT EXISTS reference.dim_department (
    department_code  VARCHAR(3) PRIMARY KEY,
    region_code      CHAR(2)    NOT NULL REFERENCES reference.dim_region (region_code),
    name             TEXT       NOT NULL,
    geometry         geometry(MultiPolygon, 4326)
);

CREATE TABLE IF NOT EXISTS reference.dim_commune (
    commune_code     CHAR(5)    PRIMARY KEY,
    department_code  VARCHAR(3) NOT NULL REFERENCES reference.dim_department (department_code),
    region_code      CHAR(2)    NOT NULL REFERENCES reference.dim_region (region_code),
    name             TEXT       NOT NULL,
    latitude         DOUBLE PRECISION CHECK (latitude BETWEEN -90 AND 90),
    longitude        DOUBLE PRECISION CHECK (longitude BETWEEN -180 AND 180),
    geometry         geometry(MultiPolygon, 4326)
);
CREATE INDEX IF NOT EXISTS dim_commune_department_idx ON reference.dim_commune (department_code);
CREATE INDEX IF NOT EXISTS dim_commune_geom_idx ON reference.dim_commune USING GIST (geometry);
CREATE INDEX IF NOT EXISTS dim_department_geom_idx ON reference.dim_department USING GIST (geometry);
CREATE INDEX IF NOT EXISTS dim_region_geom_idx ON reference.dim_region USING GIST (geometry);
