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


def test_gold_levels_cover_every_territory() -> None:
    import pyarrow.compute as pc
    import pyarrow.parquet as pq

    root = get_settings().data_lake_root
    profile = pq.read_table(root / "gold" / "territory_profile", columns=["level", "territory_code"])
    communes = pq.read_table(root / "silver" / "communes", columns=["commune_code"]).num_rows
    counts = {
        lvl: pc.sum(pc.equal(profile["level"], lvl)).as_py() for lvl in ("commune", "department", "region")
    }
    assert counts["commune"] == communes
    assert counts["department"] == 101 and counts["region"] == 18
    keys = {
        (lvl, code)
        for lvl, code in zip(profile["level"].to_pylist(), profile["territory_code"].to_pylist(), strict=True)
    }
    assert (
        len(keys) == profile.num_rows
    )  # codes collide across levels (department 01 vs region 01), never within


def test_postgis_reference_and_facts_loaded() -> None:
    with connect(get_settings()) as conn:
        n_geom = conn.execute(
            "SELECT count(*) FROM reference.dim_commune WHERE geometry IS NOT NULL"
        ).fetchone()
        assert n_geom is not None and int(n_geom[0]) > 30000  # type: ignore[call-overload]
        # spatial query: Bordeaux's polygon contains its rail stations
        row = conn.execute(
            "SELECT count(*) FROM amenities.rail_station s"
            " JOIN reference.dim_commune c ON c.commune_code = '33063' WHERE ST_Contains(c.geometry, s.geom)"
        ).fetchone()
        assert row is not None and int(row[0]) >= 1  # type: ignore[call-overload]
        views = {
            r[0]
            for r in conn.execute(
                "SELECT table_name FROM information_schema.views WHERE table_schema = 'analytics'"
            ).fetchall()
        }
        assert {"v_commune_overview", "v_territory_geometry", "v_housing_market_latest"} <= views
        profile = conn.execute(
            "SELECT population, median_price_m2 FROM analytics.v_commune_overview"
            " WHERE territory_code = '33063'"
        ).fetchone()
        assert profile is not None and profile[0] and profile[1]


def test_mongo_reviews_loaded_with_territory() -> None:
    doc = get_database().reviews.find_one({"source": "grand_debat", "territory.commune_code": "33063"})
    assert doc is not None and doc["language"] == "fr" and len(doc["text"]) >= 20
