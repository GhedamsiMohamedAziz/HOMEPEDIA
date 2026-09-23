# HOMEPEDIA

DataOps + Big Data + Geospatial + AI platform for exploring and analysing the French housing market.

**Status: Phase 2 (Data) complete** — Phase 1 foundation (repository, Docker stack, PostgreSQL/PostGIS, MongoDB,
Spark cluster, CI) plus nine reproducible source connectors landing RAW and BRONZE layers.
Phases 3–8 follow the roadmap in `HOMEPEDIA_Senior_Implementation_Plan.pdf`.

## 1. Project overview

HOMEPEDIA ingests heterogeneous public datasets (DVF, INSEE, DPE, Géorisques, transport, education, a textual
source), lands them in a Parquet data lake, processes them with Spark, serves them from PostgreSQL/PostGIS and
MongoDB, and exposes housing / territorial / NLP analytics in a Streamlit application at commune, department and
region level.

## 2. Architecture

```
External sources → ingestion connectors → data lake (RAW/BRONZE/SILVER/GOLD, Parquet)
  → Spark cluster → PostgreSQL + PostGIS (structured, geospatial) / MongoDB (documents, text, NLP outputs)
  → analytics layer → Streamlit
```

| Path | Role |
|---|---|
| `homepedia/` | shared runtime: typed settings, JSON logging, DB clients, Spark session, healthcheck |
| `ingestion/` | `connectors/` framework (base class, HTTP, run ledger, Arrow helpers), `sources/` one module per source, `pipelines/run.py` CLI |
| `processing/` | Spark jobs, cleaning, transformations, feature engineering — Phase 3 |
| `analytics/` | deterministic analytical functions by domain — Phase 5 |
| `database/` | SQL migrations, schemas, seeds, Mongo init |
| `ml/` | NLP, models, evaluation — Phase 6 |
| `apps/streamlit/` | application — Phase 7 (shell only today) |
| `infrastructure/docker/` | the single application image |
| `data/{raw,bronze,silver,gold}` | local data lake (git-ignored) |
| `tests/`, `docs/`, `notebooks/` | tests, documentation, exploration |

Every Python service (Spark master, Spark worker, Jupyter, Streamlit, CLI) runs the same image
(`infrastructure/docker/app.Dockerfile`: Python 3.11 + JRE + PySpark 3.5.5) so driver and executors never drift.

## 3. Data sources

Nine connectors, catalogued with provider, URL, licence, update frequency, geographic level, temporal coverage,
format, ingestion method and reliability in [`docs/data_sources.md`](docs/data_sources.md):

| Connector | Domain | BRONZE tables |
|---|---|---|
| `geo_reference` | official codes, names, centroids, legal population, contours | `regions`, `departments`, `communes` |
| `dvf` | housing transactions (DGFiP / Etalab) | `transactions` (partitioned by year, department) |
| `insee_filosofi` | income & poverty (INSEE Filosofi 2021) | `income` |
| `insee_emploi` | employment × age × PCS (INSEE RP 2022) | `employment` |
| `dpe` | energy performance certificates (ADEME) | `certificates` |
| `georisques` | natural/technological risks per commune | `commune_risks` |
| `sncf_stations` | rail stations (WGS84 points) | `stations` |
| `education` | schools directory | `schools` |
| `grand_debat` | textual source: 2019 local-meeting reports | `meeting_reports` |

Every connector implements `extract() → write_raw() → normalize() → validate()` (`ingestion/connectors/base.py`).
RAW keeps the exact bytes under `data/raw/<source>/<data_version>/` with a `manifest.json` (URL, sha256, size,
fetch time); reruns reuse files already fetched. BRONZE is typed Parquet with canonical column names
(`commune_code`, `department_code`, `region_code`, `latitude`, `longitude`, `price`, `surface`, `date`, `year`, …)
and a `_data_version` column. Rows with nulls in declared key columns are rejected and counted per reason.
Each run writes one row to `ops.pipeline_runs` (rows received / processed / rejected, status, git commit, error).

```
make ingest                       # all sources, inside the stack
make ingest SOURCES="dvf dpe"     # subset
uv run python -m ingestion.pipelines.run geo_reference   # locally (needs Postgres reachable)
```

Scope is set in `.env`: `DEPARTMENTS`, `DVF_YEARS`, `DPE_SINCE`. Reference data and station lists are always
national.

## 4. Database schema

Migrations live in `database/migrations/NNNN_name.sql` and are applied once, in order, by `python -m database.migrate`
(tracked in `ops.schema_migrations`).

Migration `0001_init`:

- `postgis` extension.
- `ops.pipeline_runs` — one row per pipeline run: source, pipeline_version, git_commit, data_version, timestamps,
  rows received / processed / rejected, status (`running|success|failed`), error.
- `reference.dim_region`, `reference.dim_department`, `reference.dim_commune` — canonical geographic hierarchy
  keyed on official codes (`region_code`, `department_code`, `commune_code`), lat/lon with range checks,
  `MultiPolygon` geometries in EPSG:4326 with GiST indexes.

MongoDB (`database/mongo/init.js`) creates `raw_documents`, `reviews` (JSON-schema validated: `source`, `text`,
`language`, `territory.commune_code`, `sentiment ∈ [-1, 1]`, `topics[]`) and `nlp_outputs`, indexed on
`territory.commune_code` and `source`.

## 5. Data cleaning methodology

Phase 3. Rules will be implemented in `processing/cleaning/` and validated in `tests/` (schema, null rates,
duplicates, date validity, geographic validity, numeric ranges, referential integrity; rejected records logged
with reasons to `ops.pipeline_runs`).

## 6. Spark architecture

Standalone cluster in Docker: `spark-master` (UI :8080) and `spark-worker` (2 cores / 2 GB, UI :8081). Jobs are
submitted from the `cli` container with the driver inside the compose network:

```
make spark-smoke   # spark-submit --master spark://spark-master:7077 processing/spark/jobs/smoke_job.py
```

The smoke job builds a 100 000-row, 8-partition DataFrame, derives `price_m2`, aggregates per department and
writes Parquet to `data/bronze/_smoke`. It exits non-zero unless all 95 departments come back, and the completed
application is visible in the master UI. `homepedia.spark.session.build_session()` is the single SparkSession
factory (cluster URL from settings, `local[*]` in tests).

## 7. NLP / AI methodology

Phase 6.

## 8. Analytical methodology

Phase 5.

## 9. Visualisation methodology

Phase 7.

## 10. Installation

Requirements: Docker + Compose v2, [uv](https://docs.astral.sh/uv/) for local development.

```
cp .env.example .env      # adjust *_PORT if a port is already taken on your machine
make install              # local venv (unit tests, lint, type-check)
```

## 11. Local execution

```
make up            # build image, start postgres, mongo, spark-master, spark-worker, jupyter, streamlit
make migrate       # apply SQL migrations
make healthcheck   # postgres/postgis, migrations, mongo
make spark-smoke   # distributed smoke job on the cluster
make check         # all of the above + integration tests
make down          # stop (keeps volumes) — `make clean` drops them
```

| Service | URL |
|---|---|
| Streamlit | http://localhost:8501 |
| Spark master UI | http://localhost:8080 |
| Spark worker UI | http://localhost:8081 |
| JupyterLab | http://localhost:8888 |
| PostgreSQL | `localhost:${POSTGRES_PORT}` |
| MongoDB | `localhost:${MONGO_PORT}` |

Configuration is read from `.env` by both compose and `homepedia.settings.Settings` (pydantic, secrets never
appear in reprs/logs). Inside containers the hostnames/ports are overridden to the compose network.

## 12. Testing

```
make lint               # ruff + ruff format --check + mypy --strict
make test               # unit tests (no docker); the Spark test needs a JVM and skips without one
make test-integration   # end-to-end tests against the running stack
```

Unit tests cover the Arrow helpers and connector normalisation on real-data fixtures cut from DVF and the
education directory (`tests/fixtures/`); no source is mocked.

CI (`.github/workflows/ci.yml`) runs lint + unit tests, then boots the full compose stack, migrates, healthchecks,
runs the Spark smoke job on the cluster and the integration tests.

## 13. Limitations

- Data is limited to the configured departments (default 33, 31, 44) and DVF 2023–2024 / DPE since 2024;
  widen `.env` scope to cover France (DVF full-year files are ~100 MB each, DPE ~15 M rows nationally).
- BRONZE is ingested but not yet cleaned (SILVER) or aggregated (GOLD): Phase 3.
- SNCF stations carry no INSEE code; Grand Débat reports carry postal code + town only. Both are resolved to
  communes in Phase 3 (spatial join / postal-code mapping).
- Single Spark worker; scale by `docker compose up --scale spark-worker=N`.
- JupyterLab runs without a token on localhost only; do not expose it.

## 14. Future improvements

Roadmap phases 3–8 of the implementation plan (sources, data lake layers, Spark transformations, full schema,
analytics, NLP, Streamlit pages, production hardening). Data dictionary and source catalogue land in `docs/`
with Phase 2.
# HOMEPEDIA
