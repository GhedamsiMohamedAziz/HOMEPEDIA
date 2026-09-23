# HOMEPEDIA

DataOps + Big Data + Geospatial + AI platform for exploring and analysing the French housing market.

## 1. Project overview

HOMEPEDIA ingests heterogeneous public datasets (DVF transactions, INSEE income and employment, ADEME energy
certificates, Géorisques risks, rail stations, schools, a textual source), lands them in a Parquet data lake,
processes them with Spark, serves them from PostgreSQL/PostGIS and MongoDB, and exposes housing, territorial and
NLP analytics in a Streamlit application at commune, department and region level.

## 2. Architecture

```
External sources → ingestion connectors → data lake (RAW / BRONZE / SILVER / GOLD, Parquet)
  → Spark cluster → PostgreSQL + PostGIS (structured, geospatial) / MongoDB (documents, text, NLP outputs)
  → analytics layer → Streamlit
```

| Path | Role |
|---|---|
| `homepedia/` | shared runtime: typed settings, JSON logging, DB clients, Spark session factory, healthcheck |
| `ingestion/connectors/` | connector framework: base class, HTTPS download, run ledger, Arrow helpers |
| `ingestion/sources/` | one module per data source |
| `ingestion/pipelines/` | connector registry and CLI |
| `processing/cleaning/` | declarative data-quality rules (Spark) |
| `processing/transformations/` | BRONZE → SILVER standardisation per source |
| `processing/spark/jobs/` | Spark entry points (silver, gold, smoke) |
| `analytics/` | deterministic analytical functions by domain |
| `database/` | SQL migrations, Mongo init |
| `ml/` | NLP, models, evaluation |
| `apps/streamlit/` | application |
| `infrastructure/docker/` | the single application image |
| `data/{raw,bronze,silver,gold}` | local data lake (git-ignored) |
| `tests/`, `docs/`, `notebooks/` | tests, documentation, exploration |

Every Python service (Spark master, Spark worker, Jupyter, Streamlit, CLI) runs the same image
(`infrastructure/docker/app.Dockerfile`: Python 3.11 + JRE + PySpark 3.5.5) so driver and executors never drift.

Geographic identity is canonical everywhere: `commune_code`, `department_code`, `region_code` are official INSEE
codes; joins never rely on names.

## 3. Data sources

Catalogued in [`docs/data_sources.md`](docs/data_sources.md) (provider, URL, licence, update frequency, geographic
level, temporal coverage, format, ingestion method, reliability).

| Connector | Domain | BRONZE tables |
|---|---|---|
| `geo_reference` | official codes, names, centroids, legal population, postal codes, contours | `regions`, `departments`, `communes` |
| `dvf` | housing transactions (DGFiP / Etalab) | `transactions` |
| `insee_filosofi` | income & poverty (INSEE Filosofi 2021) | `income` |
| `insee_emploi` | employment × age × PCS (INSEE RP 2022) | `employment` |
| `dpe` | energy performance certificates (ADEME) | `certificates` |
| `georisques` | natural/technological risks per commune | `commune_risks` |
| `sncf_stations` | rail stations (WGS84 points) | `stations` |
| `education` | schools directory | `schools` |
| `grand_debat` | textual source: local-meeting reports | `meeting_reports` |

Each connector implements `extract → write_raw → normalize → validate` (`ingestion/connectors/base.py`). RAW keeps
the exact source bytes under `data/raw/<source>/<data_version>/` with a `manifest.json` (URL, sha256, size, fetch
time). BRONZE is typed Parquet with canonical column names and a `_data_version` column. Every run writes one row
to `ops.pipeline_runs` (rows received / processed / rejected, status, git commit, error).

Scope is set in `.env`: `DEPARTMENTS`, `DVF_YEARS`, `DPE_SINCE`. Reference data and station lists are national.

## 4. Database schema

Migrations live in `database/migrations/NNNN_name.sql`, applied once, in order, by `python -m database.migrate`
(tracked in `ops.schema_migrations`).

- `ops.pipeline_runs` — run ledger: source, pipeline_version, git_commit, data_version, timestamps, rows
  received / processed / rejected, status (`running|success|failed`), error.
- `reference.dim_region`, `reference.dim_department`, `reference.dim_commune` — geographic hierarchy keyed on
  official codes, lat/lon with range checks, `MultiPolygon` geometries in EPSG:4326 with GiST indexes.

MongoDB (`database/mongo/init.js`) holds `raw_documents`, `reviews` (JSON-schema validated: `source`, `text`,
`language`, `territory.commune_code`, `sentiment ∈ [-1, 1]`, `topics[]`) and `nlp_outputs`, indexed on
`territory.commune_code` and `source`.

## 5. Data cleaning methodology

Rules are declarative (`processing/cleaning/rules.py`) and evaluated in Spark: each rule is a named boolean
expression; rows failing any rule are written to `data/silver/_rejects/<table>/` with a `reason` column and counted
per reason in `ops.pipeline_runs`. Standard rules: price > 0, surface > 0, plausible price/m², valid
latitude/longitude, commune code present in the reference, valid dates, energy labels in A–G. DVF multi-lot
mutations are consolidated to one row per mutation and property type. See [`docs/data_dictionary.md`](docs/data_dictionary.md).

## 6. Spark architecture

Standalone cluster in Docker: `spark-master` (UI :8080) and `spark-worker` (2 cores / 2 GB, UI :8081), scaled with
`docker compose up --scale spark-worker=N`. Jobs are submitted from the `cli` container so the driver sits inside
the compose network. `homepedia.spark.session.build_session()` is the single SparkSession factory (cluster URL
from settings, `local[*]` in tests). SILVER and GOLD are PySpark DataFrame / Spark SQL jobs reading and writing
partitioned Parquet; DVF aggregation runs commune → department → region.

## 7. NLP / AI methodology

Text → language detection → cleaning → sentence segmentation → sentiment → topics → entities → aggregation by
territory (`ml/nlp/`). Outputs are stored in MongoDB and aggregated into `territory_text_insights`.

## 8. Analytical methodology

Deterministic functions in `analytics/` (price evolution, population growth, housing density, energy distribution,
climate exposure, transport accessibility) computed from GOLD tables; the application never computes metrics
itself. Pipeline: raw data → metric → indicator → visualisation.

## 9. Visualisation methodology

Streamlit with global filters shared by maps, tables and charts. Map representations follow the analytical
question (choropleth for rates, bubbles for counts, heatmaps for density).

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
make ingest        # all source connectors (SOURCES="dvf dpe" for a subset)
make silver        # BRONZE → SILVER on the Spark cluster
make gold          # SILVER → GOLD on the Spark cluster
make check         # full validation
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

Configuration is read from `.env` by both compose and `homepedia.settings.Settings` (pydantic; secrets never appear
in reprs or logs). Inside containers the hostnames and ports are overridden to the compose network.

## 12. Testing

```
make lint               # ruff + ruff format --check + mypy --strict
make test               # unit tests (no docker; Spark tests skip without a JVM)
make test-spark         # Spark tests inside the image
make test-integration   # end-to-end tests against the running stack
```

Unit tests use real-data fixtures cut from the sources (`tests/fixtures/`); nothing is mocked. CI runs lint and
unit tests, then boots the compose stack, migrates, healthchecks, runs the Spark smoke job, ingests, and runs the
integration tests.

## 13. Limitations

- Default scope is three departments (33, 31, 44), DVF 2023–2024, DPE since 2024; widen `.env` to cover France.
- INSEE suppresses income indicators for small communes (null values are kept, not imputed).
- Stations carry no INSEE code (resolved spatially); Grand Débat reports carry postal code + town only.
- Single Spark worker by default; JupyterLab runs without a token on localhost only.

## 14. Future improvements

Managed Spark (Databricks), additional sources (GTFS transit, population history, building permits), an optional
analytical assistant translating natural-language questions into deterministic analytics calls.
