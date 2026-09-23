.DEFAULT_GOAL := help
COMPOSE := docker compose
RUN     := $(COMPOSE) run --rm cli

help: ## list targets
	@grep -E '^[a-zA-Z_-]+:.*?## ' $(MAKEFILE_LIST) | awk 'BEGIN{FS=":.*?## "}{printf "  %-14s %s\n",$$1,$$2}'

.env:
	cp .env.example .env

install: ## local dev env (uv)
	uv sync

lint: ## ruff + mypy
	uv run ruff check .
	uv run ruff format --check .
	uv run mypy

test: ## unit tests (no docker needed)
	uv run pytest -q

up: .env ## start the whole stack
	$(COMPOSE) up -d --build --wait

down: ## stop the stack (keeps volumes)
	$(COMPOSE) down

clean: ## stop and drop volumes
	$(COMPOSE) down -v

migrate: .env ## apply SQL migrations
	$(RUN) python3 -m database.migrate

healthcheck: .env ## verify postgres/postgis, migrations, mongo
	$(RUN) python3 -m homepedia.healthcheck

spark-smoke: .env ## run the distributed smoke job on the compose cluster
	$(RUN) /opt/spark/bin/spark-submit --master spark://spark-master:7077 processing/spark/jobs/smoke_job.py

ingest: .env ## run all source connectors (or SOURCES="dvf dpe") inside the stack
	$(RUN) python3 -m ingestion.pipelines.run $(SOURCES)

silver: .env ## BRONZE -> SILVER on the cluster (TABLES="transactions certificates" for a subset)
	$(RUN) /opt/spark/bin/spark-submit --master spark://spark-master:7077 processing/spark/jobs/silver.py $(TABLES)

gold: .env ## SILVER -> GOLD on the cluster (TABLES=... for a subset)
	$(RUN) /opt/spark/bin/spark-submit --master spark://spark-master:7077 processing/spark/jobs/gold.py $(TABLES)

load: .env ## SILVER/GOLD -> PostgreSQL + MongoDB (TARGETS=... for a subset)
	$(RUN) python3 -m database.load $(TARGETS)
	$(RUN) python3 -m database.load_mongo

test-spark: .env ## Spark unit tests inside the image (JVM)
	$(RUN) python3 -m pytest -q tests/test_spark_smoke.py tests/test_spark_processing.py

test-integration: .env ## integration tests inside the stack
	$(RUN) python3 -m pytest -q -m integration -o addopts=""

check: up migrate healthcheck spark-smoke test-spark ingest silver gold load test-integration ## full validation: stack, migrations, spark, ingestion, integration tests

logs: ## follow stack logs
	$(COMPOSE) logs -f

.PHONY: help install lint test up down clean migrate healthcheck spark-smoke ingest silver gold load test-spark test-integration check logs
