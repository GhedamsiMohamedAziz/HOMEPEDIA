"""SILVER/GOLD logic on tiny in-memory frames (JVM required; skipped without one)."""

from __future__ import annotations

import datetime as dt
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F  # noqa: N812

from processing.cleaning.rules import Rule, apply_rules, reject_counts, reject_duplicates
from processing.transformations.gold import housing_market, housing_price_evolution
from processing.transformations.silver import Lake, transactions


def test_apply_rules_reports_every_failed_rule(spark: SparkSession) -> None:
    df = spark.createDataFrame([(1, 10.0), (2, -1.0), (3, None), (4, 30.0)], ["id", "price"])
    clean, rejects = apply_rules(df, [Rule("price_not_positive", "price > 0"), Rule("odd_id", "id % 2 = 0")])
    assert sorted(r["id"] for r in clean.collect()) == [4]
    counts = reject_counts(rejects)
    assert counts == {"price_not_positive": 2, "odd_id": 2}
    assert {r["id"]: r["reason"] for r in rejects.collect()}[3] == "price_not_positive;odd_id"


def test_reject_duplicates_keeps_latest(spark: SparkSession) -> None:
    df = spark.createDataFrame([("a", 1, "old"), ("a", 2, "new"), ("b", 1, "only")], ["k", "v", "tag"])
    keep, dups = reject_duplicates(df, ["k"], order_by="v")
    assert {r["k"]: r["tag"] for r in keep.collect()} == {"a": "new", "b": "only"}
    assert reject_counts(dups) == {"duplicate_key": 1}


def _bronze_dvf(spark: SparkSession, root: Path) -> None:
    cols = [
        "mutation_id",
        "date",
        "mutation_type",
        "price",
        "commune_code",
        "department_code",
        "postal_code",
        "property_type_code",
        "property_type",
        "surface",
        "rooms",
        "land_surface",
        "lots",
        "longitude",
        "latitude",
        "year",
    ]
    d = dt.date(2023, 3, 1)
    rows = [
        # simple house sale
        (
            "m1",
            d,
            "Vente",
            300000.0,
            "33063",
            "33",
            "33000",
            "1",
            "Maison",
            100.0,
            4.0,
            200.0,
            0,
            -0.58,
            44.84,
            2023,
        ),
        # apartment with a dependency row and two lots: surfaces summed, dependency folded
        (
            "m2",
            d,
            "Vente",
            200000.0,
            "33063",
            "33",
            "33000",
            "2",
            "Appartement",
            30.0,
            1.0,
            None,
            2,
            -0.58,
            44.84,
            2023,
        ),
        (
            "m2",
            d,
            "Vente",
            200000.0,
            "33063",
            "33",
            "33000",
            "2",
            "Appartement",
            20.0,
            1.0,
            None,
            2,
            -0.58,
            44.84,
            2023,
        ),
        (
            "m2",
            d,
            "Vente",
            200000.0,
            "33063",
            "33",
            "33000",
            "3",
            "Dépendance",
            None,
            None,
            None,
            2,
            -0.58,
            44.84,
            2023,
        ),
        # mixed use: house + shop -> rejected
        (
            "m3",
            d,
            "Vente",
            500000.0,
            "33063",
            "33",
            "33000",
            "1",
            "Maison",
            120.0,
            5.0,
            None,
            0,
            None,
            None,
            2023,
        ),
        (
            "m3",
            d,
            "Vente",
            500000.0,
            "33063",
            "33",
            "33000",
            "4",
            "Local industriel. commercial ou assimilé",
            80.0,
            0.0,
            None,
            0,
            None,
            None,
            2023,
        ),
        # implausible price per m2 -> rejected
        (
            "m4",
            d,
            "Vente",
            1000.0,
            "33063",
            "33",
            "33000",
            "1",
            "Maison",
            100.0,
            4.0,
            None,
            0,
            None,
            None,
            2023,
        ),
        # unknown commune -> rejected
        (
            "m5",
            d,
            "Vente",
            250000.0,
            "99999",
            "99",
            "99000",
            "1",
            "Maison",
            100.0,
            4.0,
            None,
            0,
            None,
            None,
            2023,
        ),
        # land only (no housing row) -> not in output at all
        (
            "m6",
            d,
            "Vente terrain à bâtir",
            80000.0,
            "33063",
            "33",
            "33000",
            None,
            None,
            None,
            None,
            900.0,
            0,
            None,
            None,
            2023,
        ),
        # next year, for growth
        (
            "m7",
            dt.date(2024, 5, 2),
            "Vente",
            330000.0,
            "33063",
            "33",
            "33000",
            "1",
            "Maison",
            100.0,
            4.0,
            None,
            0,
            None,
            None,
            2024,
        ),
    ]
    spark.createDataFrame(rows, cols).write.mode("overwrite").parquet(
        str(root / "bronze" / "dvf" / "transactions")
    )
    spark.createDataFrame(
        [("33063", "33", "75", "Bordeaux"), ("33281", "33", "75", "Mérignac")],
        ["commune_code", "department_code", "region_code", "name"],
    ).write.mode("overwrite").parquet(str(root / "silver" / "communes"))


def test_dvf_silver_consolidation(spark: SparkSession, tmp_path: Path) -> None:
    _bronze_dvf(spark, tmp_path)
    lake = Lake(tmp_path)
    clean, rejects = transactions(spark, lake)
    got = {r["mutation_id"]: r for r in clean.collect()}
    assert set(got) == {"m1", "m2", "m7"}
    assert got["m2"]["surface"] == 50.0 and got["m2"]["units"] == 2 and got["m2"]["price_m2"] == 4000.0
    assert got["m1"]["price_m2"] == 3000.0 and got["m1"]["region_code"] == "75"
    assert got["m1"]["quarter"] == 1 and got["m1"]["month"] == 3
    assert reject_counts(rejects) == {
        "mixed_use_mutation": 1,
        "price_m2_implausible": 1,
        "commune_not_in_reference": 1,
    }


def test_gold_market_and_evolution(spark: SparkSession, tmp_path: Path) -> None:
    _bronze_dvf(spark, tmp_path)
    lake = Lake(tmp_path)
    clean, _ = transactions(spark, lake)
    clean.write.mode("overwrite").parquet(str(lake.silver_path("transactions")))

    market = {(r["year"], r["property_type"]): r for r in housing_market(spark, lake, "department").collect()}
    assert market[(2023, "all")]["transactions"] == 2
    assert market[(2023, "all")]["median_price_m2"] == 3500.0  # median of 3000 and 4000
    assert market[(2023, "Maison")]["n_surface_80_120"] == 1

    evo = {
        (r["granularity"], r["period"]): r
        for r in housing_price_evolution(spark, lake).filter(F.col("level") == "commune").collect()
    }
    assert evo[("year", "2023")]["yoy_growth"] is None
    assert evo[("year", "2024")]["yoy_growth"] == round((3300 - 3500) / 3500, 4)
    assert evo[("year", "2024")]["transactions_yoy_growth"] == -0.5
    assert evo[("quarter", "2023-Q1")]["transactions"] == 2
    assert evo[("month", "2024-05")]["median_price_m2"] == 3300.0
