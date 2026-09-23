"""BRONZE → SILVER (section 9): cleaned, standardised, one function per table.

Each function takes the SparkSession + a Lake and returns (clean, rejects). Rejects carry `reasons`.
Scope filters (e.g. "only sales") are expressed as rules too, so every dropped row is documented.
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path

import pyarrow.parquet as pq
from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F  # noqa: N812

from processing.cleaning.rules import (
    VALID_COORDS,
    Rule,
    apply_rules,
    in_reference,
    reject_duplicates,
    with_reference,
)
from processing.transformations.geo import CommuneIndex

SALE_TYPES = ("Vente", "Vente en l'état futur d'achèvement")
HOUSING_TYPES = ("Maison", "Appartement")
# piggy: plausibility bounds for €/m² — outside is a data error or a non-market transfer; tune per market
PRICE_M2_MIN, PRICE_M2_MAX = 200.0, 25_000.0


@dataclass(frozen=True)
class Lake:
    root: Path

    def bronze(self, spark: SparkSession, source: str, table: str) -> DataFrame:
        return spark.read.parquet(str(self.root / "bronze" / source / table))

    def silver(self, spark: SparkSession, table: str) -> DataFrame:
        return spark.read.parquet(str(self.root / "silver" / table))

    def silver_path(self, table: str) -> Path:
        return self.root / "silver" / table


# ----------------------------------------------------------------------------- reference
def communes(spark: SparkSession, lake: Lake) -> tuple[DataFrame, DataFrame]:
    df = lake.bronze(spark, "geo_reference", "communes").drop("_data_version")
    df = df.withColumn(
        "density_km2", F.when(F.col("surface_ha") > 0, F.col("population") / (F.col("surface_ha") / 100.0))
    )
    deps = lake.bronze(spark, "geo_reference", "departments").select(
        F.col("department_code").alias("_dep"), F.col("region_code").alias("_dep_region")
    )
    df = df.join(deps, df["department_code"] == deps["_dep"], "left")
    rules = [
        Rule("bad_commune_code", "commune_code RLIKE '^([0-9]{2}|2[AB])[0-9]{3}$'"),
        # overseas collectivities (975-989) are not departments: outside Region > Department > Commune
        Rule("department_not_in_reference", "_dep IS NOT NULL"),
        Rule("region_mismatch", "region_code = _dep_region"),
        VALID_COORDS,
    ]
    clean, rejects = apply_rules(df, rules)
    return clean.drop("_dep", "_dep_region"), rejects


def departments(spark: SparkSession, lake: Lake) -> tuple[DataFrame, DataFrame]:
    return apply_rules(lake.bronze(spark, "geo_reference", "departments").drop("_data_version"), [])


def regions(spark: SparkSession, lake: Lake) -> tuple[DataFrame, DataFrame]:
    return apply_rules(lake.bronze(spark, "geo_reference", "regions").drop("_data_version"), [])


def _ref(spark: SparkSession, lake: Lake) -> DataFrame:
    return lake.silver(spark, "communes").select("commune_code", "department_code", "region_code")


# ----------------------------------------------------------------------------- housing
def transactions(spark: SparkSession, lake: Lake) -> tuple[DataFrame, DataFrame]:
    """One row per (mutation, housing type); dependency/land rows folded in, mixed-use sales rejected."""
    raw = lake.bronze(spark, "dvf", "transactions")
    typed = raw.withColumn(
        "kind",
        F.when(F.col("property_type").isin(*HOUSING_TYPES), F.col("property_type"))
        .when(F.col("property_type").isNull() | (F.col("property_type") == "Dépendance"), F.lit(None))
        .otherwise(F.lit("other")),
    )
    w = Window.partitionBy("mutation_id")
    typed = typed.withColumn("kinds", F.array_distinct(F.collect_list("kind").over(w)))
    housing = typed.filter(F.col("kind").isin(*HOUSING_TYPES))
    agg = housing.groupBy("mutation_id", "kind").agg(
        F.first("date").alias("date"),
        F.first("year").alias("year"),
        F.first("mutation_type").alias("mutation_type"),
        F.first("price").alias("price"),
        F.first("commune_code").alias("commune_code"),
        F.first("department_code").alias("department_code"),
        F.first("postal_code").alias("postal_code"),
        F.sum("surface").alias("surface"),
        F.sum("rooms").alias("rooms"),
        F.count("*").alias("units"),
        F.sum("land_surface").alias("land_surface"),
        F.first("longitude", ignorenulls=True).alias("longitude"),
        F.first("latitude", ignorenulls=True).alias("latitude"),
        F.first("kinds").alias("kinds"),
    )
    df = (
        agg.withColumnRenamed("kind", "property_type")
        .withColumn("price_m2", F.round(F.col("price") / F.col("surface"), 2))
        .withColumn("month", F.month("date"))
        .withColumn("quarter", F.quarter("date"))
    )
    df = with_reference(df, _ref(spark, lake))
    rules = [
        Rule("not_a_sale", f"mutation_type IN ({', '.join(repr(t) for t in SALE_TYPES)})"),
        Rule("mixed_use_mutation", "NOT array_contains(kinds, 'other') AND size(kinds) = 1"),
        Rule("price_not_positive", "price > 0"),
        Rule("surface_not_positive", "surface > 0"),
        Rule("price_m2_implausible", f"price_m2 BETWEEN {PRICE_M2_MIN} AND {PRICE_M2_MAX}"),
        Rule("date_year_mismatch", "year(date) = year"),
        in_reference(),
        VALID_COORDS,
    ]
    clean, rejects = apply_rules(df, rules)
    return clean.drop("kinds", "_ref_commune_code", "mutation_type"), rejects


def certificates(spark: SparkSession, lake: Lake) -> tuple[DataFrame, DataFrame]:
    df, dups = reject_duplicates(lake.bronze(spark, "dpe", "certificates"), ["dpe_id"], order_by="date")
    df = with_reference(df, _ref(spark, lake)).withColumn("year", F.year("date"))
    rules = [
        Rule("invalid_energy_label", "energy_label IN ('A','B','C','D','E','F','G')"),
        Rule("invalid_ghg_label", "ghg_label IN ('A','B','C','D','E','F','G')"),
        Rule("surface_not_positive", "surface > 0"),
        Rule("surface_implausible", "surface <= 1000"),
        Rule("dwelling_only", "building_type IN ('maison', 'appartement')"),
        in_reference(),
    ]
    clean, rejects = apply_rules(df, rules)
    return clean.drop("_ref_commune_code"), rejects.unionByName(dups, allowMissingColumns=True)


# ----------------------------------------------------------------------------- socio-economic
INCOME_MEASURES = {
    "MED_SL": "median_income",
    "D1_SL": "income_d1",
    "D9_SL": "income_d9",
    "IR_D9_D1_SL": "interdecile_ratio",
    "PR_MD60": "poverty_rate",
    "S_HH_TAX": "taxed_households_share",
    "NUM_HH": "households",
    "NUM_PER": "persons",
}


def income(spark: SparkSession, lake: Lake) -> tuple[DataFrame, DataFrame]:
    df = lake.bronze(spark, "insee_filosofi", "income").filter(F.col("measure").isin(*INCOME_MEASURES))
    wide = df.groupBy("commune_code", "year").pivot("measure", list(INCOME_MEASURES)).agg(F.first("value"))
    for src, dst in INCOME_MEASURES.items():
        wide = wide.withColumnRenamed(src, dst)
    wide = with_reference(wide, _ref(spark, lake))
    clean, rejects = apply_rules(wide, [in_reference()])
    return clean.drop("_ref_commune_code"), rejects


def employment(spark: SparkSession, lake: Lake) -> tuple[DataFrame, DataFrame]:
    """15-64 population by status (1 employed, 2 unemployed, 1T2 active) for PCS total; rates derived."""
    df = lake.bronze(spark, "insee_emploi", "employment").filter("socio_professional_category = '_T'")
    wide = (
        df.groupBy("commune_code", "year")
        .pivot("employment_status", ["1", "2", "1T2"])
        .agg(F.sum("value"))
        .withColumnRenamed("1", "employed")
        .withColumnRenamed("2", "unemployed")
        .withColumnRenamed("1T2", "active")
        # the 2022 vintage publishes employed + active only
        .withColumn("unemployed", F.coalesce(F.col("unemployed"), F.col("active") - F.col("employed")))
        .withColumn("unemployment_rate", F.when(F.col("active") > 0, F.col("unemployed") / F.col("active")))
    )
    wide = with_reference(wide, _ref(spark, lake))
    clean, rejects = apply_rules(wide, [in_reference(), Rule("active_not_positive", "active > 0")])
    return clean.drop("_ref_commune_code"), rejects


def population(spark: SparkSession, lake: Lake) -> tuple[DataFrame, DataFrame]:
    """Census series per (commune, census year): population, dwellings by occupancy, births, deaths."""
    df = lake.bronze(spark, "insee_population", "population_series")
    key = F.when(F.col("measure") == "DWELLINGS", F.concat(F.lit("DW_"), F.col("occupancy"))).otherwise(
        F.col("measure")
    )
    names = {
        "POP": "population",
        "DW__T": "dwellings",
        "DW_DW_MAIN": "main_dwellings",
        "DW_DW_SEC_DW_OCC": "secondary_dwellings",
        "DW_DW_VAC": "vacant_dwellings",
        "BRTH": "births",
        "DEATH": "deaths",
        "SUP": "area_km2",
    }
    wide = (
        df.withColumn("k", key).groupBy("commune_code", "year").pivot("k", list(names)).agg(F.first("value"))
    )
    for src, dst in names.items():
        wide = wide.withColumnRenamed(src, dst)
    wide = wide.withColumn(
        "vacancy_rate",
        F.when(F.col("dwellings") > 0, F.round(F.col("vacant_dwellings") / F.col("dwellings"), 4)),
    )
    wide = with_reference(wide, _ref(spark, lake))
    clean, rejects = apply_rules(wide, [in_reference(), Rule("population_missing", "population IS NOT NULL")])
    return clean.drop("_ref_commune_code"), rejects


# ----------------------------------------------------------------------------- environment / amenities
def commune_risks(spark: SparkSession, lake: Lake) -> tuple[DataFrame, DataFrame]:
    df = (
        lake.bronze(spark, "georisques", "commune_risks")
        .dropDuplicates(["commune_code", "risk_code"])
        .withColumn(
            "risk_family",
            F.when(F.col("risk_code").startswith("1"), "natural")
            .when(F.col("risk_code").startswith("2"), "technological")
            .otherwise("other"),
        )
    )
    df = with_reference(df, _ref(spark, lake))
    clean, rejects = apply_rules(df, [in_reference()])
    return clean.drop("_ref_commune_code"), rejects


def schools(spark: SparkSession, lake: Lake) -> tuple[DataFrame, DataFrame]:
    df, dups = reject_duplicates(lake.bronze(spark, "education", "schools"), ["school_id"])
    df = with_reference(df, _ref(spark, lake))
    rules = [Rule("not_open", "state = 'OUVERT'"), in_reference(), VALID_COORDS]
    clean, rejects = apply_rules(df, rules)
    return clean.drop("_ref_commune_code"), rejects.unionByName(dups, allowMissingColumns=True)


def stations(spark: SparkSession, lake: Lake) -> tuple[DataFrame, DataFrame]:
    """Passenger stations resolved to a commune by point-in-polygon against the reference contours."""
    idx = _commune_index(lake, geometry=True)
    src = lake.bronze(spark, "sncf_stations", "stations")
    rows = [
        {
            **r.asDict(),
            "commune_code": idx.resolve_point(r["longitude"], r["latitude"])
            if r["longitude"] is not None and r["latitude"] is not None
            else None,
        }
        for r in src.collect()
    ]
    df, dups = reject_duplicates(
        spark.createDataFrame(rows, src.schema.add("commune_code", "string")), ["station_uic"]
    )
    df = with_reference(df, _ref(spark, lake))
    rules = [
        Rule("not_passenger_station", "passengers = 'O'"),
        Rule("commune_unresolved", "commune_code IS NOT NULL"),
        VALID_COORDS,
    ]
    clean, rejects = apply_rules(df, rules)
    return clean.drop("_ref_commune_code"), rejects.unionByName(dups, allowMissingColumns=True)


def meeting_reports(spark: SparkSession, lake: Lake) -> tuple[DataFrame, DataFrame]:
    """Grand Débat reports resolved to a commune via postal code + town name."""
    idx = _commune_index(lake, geometry=False)
    src = lake.bronze(spark, "grand_debat", "meeting_reports").drop("department_code")
    rows = [
        {**r.asDict(), "commune_code": idx.resolve_postal(r["postal_code"], r["town"])} for r in src.collect()
    ]
    df = spark.createDataFrame(rows, src.schema.add("commune_code", "string"))
    df = with_reference(df, _ref(spark, lake))
    df = df.withColumn("created_at", F.to_timestamp("created_at")).withColumn(
        "themes", F.split(F.col("themes"), ";")
    )
    rules = [
        Rule("commune_unresolved", "commune_code IS NOT NULL"),
        Rule("text_too_short", "length(text) >= 20"),
    ]
    clean, rejects = apply_rules(df, rules)
    return clean.drop("_ref_commune_code"), rejects


def _commune_index(lake: Lake, *, geometry: bool) -> CommuneIndex:
    cols = ["commune_code", "name", "postal_codes"] + (["geometry_geojson"] if geometry else [])
    t = pq.read_table(lake.root / "bronze" / "geo_reference" / "communes", columns=cols)
    rows = t.to_pylist()
    return CommuneIndex.build(
        ((r["commune_code"], r["name"], r["postal_codes"], r.get("geometry_geojson")) for r in rows),
        geometry=geometry,
    )


SilverFn = Callable[[SparkSession, Lake], tuple[DataFrame, DataFrame]]

#: execution order matters: everything after `communes` joins the commune reference
SILVER: dict[str, tuple[SilverFn, tuple[str, ...]]] = {
    "regions": (regions, ()),
    "departments": (departments, ()),
    "communes": (communes, ()),
    "transactions": (transactions, ("year", "department_code")),
    "certificates": (certificates, ("department_code",)),
    "income": (income, ()),
    "employment": (employment, ()),
    "population": (population, ()),
    "commune_risks": (commune_risks, ()),
    "schools": (schools, ()),
    "stations": (stations, ()),
    "meeting_reports": (meeting_reports, ()),
}
