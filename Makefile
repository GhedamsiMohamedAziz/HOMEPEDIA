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

test-integration: .env ## integration tests inside the stack
	$(RUN) python3 -m pytest -q -m integration -o addopts=""

check: up migrate healthcheck spark-smoke ingest test-integration ## full validation: stack, migrations, spark, ingestion, integration tests

logs: ## follow stack logs
	$(COMPOSE) logs -f

.PHONY: help install lint test up down clean migrate healthcheck spark-smoke ingest test-integration check logs
