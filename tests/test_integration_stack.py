"""End-to-end Phase 1 checks against the running compose stack (`make test-integration`)."""

from __future__ import annotations

import pytest

from homepedia.db.mongo import get_database, missing_collections
from homepedia.db.postgres import connect
from homepedia.settings import get_settings

pytestmark = pytest.mark.integration


def test_postgis_and_migrations_applied() -> None:
    with connect(get_settings()) as conn:
        assert conn.execute("SELECT postgis_version()").fetchone() is not None
        tables = {
            r[0]
            for r in conn.execute(
                "SELECT table_name FROM information_schema.tables WHERE table_schema='reference'"
            ).fetchall()
        }
        assert {"dim_region", "dim_department", "dim_commune"} <= tables
        # PostGIS geometry column actually works
        assert conn.execute("SELECT ST_AsText(ST_MakePoint(2.35, 48.85))").fetchone() == (
            "POINT(2.35 48.85)",
        )


def test_pipeline_runs_ledger_constraints() -> None:
    with connect(get_settings()) as conn, conn.transaction(force_rollback=True):
        conn.execute(
            "INSERT INTO ops.pipeline_runs (source, pipeline_version, status) VALUES ('t', '0.1', 'running')"
        )
        with pytest.raises(Exception, match="check constraint"), conn.transaction():
            conn.execute(
                "INSERT INTO ops.pipeline_runs (source, pipeline_version, status) VALUES ('t','0.1','bogus')"
            )


def test_mongo_collections_and_validator() -> None:
    assert missing_collections() == []
    db = get_database()
    from pymongo.errors import WriteError

    with pytest.raises(WriteError):
        db.reviews.insert_one({"source": "test"})  # missing required `text`
    res = db.reviews.insert_one(
        {"source": "test", "text": "calme", "territory": {"commune_code": "33063"}, "sentiment": 0.7}
    )
    db.reviews.delete_one({"_id": res.inserted_id})


def test_pipeline_runs_recorded_for_ingested_sources() -> None:
    with connect(get_settings()) as conn:
        rows = conn.execute(
            "SELECT source, status, rows_received, rows_processed, rows_rejected, finished_at"
            " FROM ops.pipeline_runs WHERE status <> 'running'"
        ).fetchall()
    assert rows, "no completed ingestion run recorded — run `make ingest` first"
    for source, status, received, processed, rejected, finished_at in rows:
        assert status in {"success", "failed"}, source
        assert finished_at is not None
        if status == "success":
            assert int(received) == int(processed) + int(rejected), source  # type: ignore[call-overload]
