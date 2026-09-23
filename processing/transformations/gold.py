"""SILVER → GOLD (section 9): business-ready tables at commune / department / region level.

housing_market_<level>   year x property_type ('all' + each): counts, median/avg price, price/m2, surfaces
housing_price_evolution  month / quarter / year series per territory with YoY, 3y, 5y growth (section 15)
climate_risk             risk exposure per commune + share of exposed communes per department / region
territory_profile        one row per territory and level: identity, housing, demographics, economy,
                         education, transport, environment, energy, text counts (section 16)
"""

from __future__ import annotations

from collections.abc import Callable

from pyspark.sql import Column, DataFrame, SparkSession, Window
from pyspark.sql import functions as F  # noqa: N812

from processing.transformations.silver import Lake

LEVELS: dict[str, str] = {"commune": "commune_code", "department": "department_code", "region": "region_code"}


# ----------------------------------------------------------------------------- housing market
def _market_metrics() -> list[Column]:
    return [
        F.count("*").alias("transactions"),
        F.round(F.median("price_m2"), 0).alias("median_price_m2"),
        F.round(F.avg("price_m2"), 0).alias("avg_price_m2"),
        F.round(F.percentile_approx("price_m2", 0.25), 0).alias("p25_price_m2"),
        F.round(F.percentile_approx("price_m2", 0.75), 0).alias("p75_price_m2"),
        F.round(F.median("price"), 0).alias("median_price"),
        F.round(F.avg("price"), 0).alias("avg_price"),
        F.round(F.median("surface"), 1).alias("median_surface"),
        F.round(F.avg("surface"), 1).alias("avg_surface"),
        F.round(F.avg("rooms"), 2).alias("avg_rooms"),
        F.sum(F.when(F.col("surface") < 40, 1).otherwise(0)).alias("n_surface_lt_40"),
        F.sum(F.when((F.col("surface") >= 40) & (F.col("surface") < 80), 1).otherwise(0)).alias(
            "n_surface_40_80"
        ),
        F.sum(F.when((F.col("surface") >= 80) & (F.col("surface") < 120), 1).otherwise(0)).alias(
            "n_surface_80_120"
        ),
        F.sum(F.when(F.col("surface") >= 120, 1).otherwise(0)).alias("n_surface_ge_120"),
    ]


def housing_market(spark: SparkSession, lake: Lake, level: str) -> DataFrame:
    """commune → department → region aggregation of cleaned transactions, by year and property type."""
    code = LEVELS[level]
    tx = lake.silver(spark, "transactions")
    by_type = tx.groupBy(code, "year", "property_type").agg(*_market_metrics())
    all_types = tx.groupBy(code, "year").agg(*_market_metrics()).withColumn("property_type", F.lit("all"))
    out = (
        by_type.unionByName(all_types)
        .withColumnRenamed(code, "territory_code")
        .withColumn("level", F.lit(level))
    )
    return out.select(
        "level",
        "territory_code",
        "year",
        "property_type",
        *[c for c in out.columns if c not in {"level", "territory_code", "year", "property_type"}],
    )


# ----------------------------------------------------------------------------- temporal
def housing_price_evolution(spark: SparkSession, lake: Lake) -> DataFrame:
    tx = lake.silver(spark, "transactions").filter("property_type IN ('Maison', 'Appartement')")
    parts = []
    for level, code in LEVELS.items():
        for granularity, sub in (
            ("month", F.col("month")),
            ("quarter", F.col("quarter")),
            ("year", F.lit(1)),
        ):
            g = (
                tx.withColumn("sub", sub)
                .groupBy(code, "year", "sub")
                .agg(
                    F.count("*").alias("transactions"),
                    F.round(F.median("price_m2"), 0).alias("median_price_m2"),
                )
                .withColumnRenamed(code, "territory_code")
                .withColumn("level", F.lit(level))
                .withColumn("granularity", F.lit(granularity))
            )
            parts.append(g)
    series = parts[0]
    for p in parts[1:]:
        series = series.unionByName(p)
    series = series.withColumn(
        "period",
        F.when(F.col("granularity") == "year", F.col("year").cast("string"))
        .when(F.col("granularity") == "quarter", F.concat(F.col("year"), F.lit("-Q"), F.col("sub")))
        .otherwise(F.concat(F.col("year"), F.lit("-"), F.lpad(F.col("sub"), 2, "0"))),
    )
    keys = ["level", "granularity", "territory_code", "sub"]
    for years, name in ((1, "yoy_growth"), (3, "growth_3y"), (5, "growth_5y")):
        prev = series.select(
            *keys, (F.col("year") + years).alias("year"), F.col("median_price_m2").alias("_prev")
        )
        series = series.join(prev, [*keys, "year"], "left").withColumn(
            name, F.round((F.col("median_price_m2") - F.col("_prev")) / F.col("_prev"), 4)
        )
        series = series.drop("_prev")
    prev_n = series.select(*keys, (F.col("year") + 1).alias("year"), F.col("transactions").alias("_prev_n"))
    series = series.join(prev_n, [*keys, "year"], "left").withColumn(
        "transactions_yoy_growth", F.round((F.col("transactions") - F.col("_prev_n")) / F.col("_prev_n"), 4)
    )
    return series.drop("_prev_n", "sub").select(
        "level",
        "territory_code",
        "granularity",
        "period",
        "year",
        "transactions",
        "median_price_m2",
        "yoy_growth",
        "growth_3y",
        "growth_5y",
        "transactions_yoy_growth",
    )


# ----------------------------------------------------------------------------- demographics
def population_evolution(spark: SparkSession, lake: Lake) -> DataFrame:
    """Census series per territory with growth vs the previous census and annualised growth (section 18)."""
    pop = lake.silver(spark, "population")
    parts = []
    for level, code in LEVELS.items():
        g = (
            pop.groupBy(code, "year")
            .agg(
                F.sum("population").alias("population"),
                F.sum("dwellings").alias("dwellings"),
                F.sum("main_dwellings").alias("main_dwellings"),
                F.sum("secondary_dwellings").alias("secondary_dwellings"),
                F.sum("vacant_dwellings").alias("vacant_dwellings"),
                F.sum("births").alias("births"),
                F.sum("deaths").alias("deaths"),
                F.count("*").alias("communes"),
            )
            .withColumnRenamed(code, "territory_code")
            .withColumn("level", F.lit(level))
        )
        parts.append(g)
    out = parts[0]
    for p in parts[1:]:
        out = out.unionByName(p)
    w = Window.partitionBy("level", "territory_code").orderBy("year")
    out = (
        out.withColumn("prev_year", F.lag("year").over(w))
        .withColumn("prev_population", F.lag("population").over(w))
        .withColumn(
            "growth", F.round((F.col("population") - F.col("prev_population")) / F.col("prev_population"), 4)
        )
        .withColumn(
            "annual_growth",
            F.round(
                F.pow(
                    F.col("population") / F.col("prev_population"), 1.0 / (F.col("year") - F.col("prev_year"))
                )
                - 1,
                5,
            ),
        )
        .withColumn(
            "vacancy_rate",
            F.when(F.col("dwellings") > 0, F.round(F.col("vacant_dwellings") / F.col("dwellings"), 4)),
        )
    )
    return out.select(
        "level",
        "territory_code",
        "year",
        "communes",
        "population",
        "prev_year",
        "prev_population",
        "growth",
        "annual_growth",
        "dwellings",
        "main_dwellings",
        "secondary_dwellings",
        "vacant_dwellings",
        "vacancy_rate",
        "births",
        "deaths",
    )


# ----------------------------------------------------------------------------- environment
RISK_FLAGS = {"flood": "11", "ground_movement": "12", "earthquake": "13", "radon": "18", "industrial": "21"}


def climate_risk(spark: SparkSession, lake: Lake) -> DataFrame:
    risks = lake.silver(spark, "commune_risks")
    aggs = [
        F.countDistinct("risk_code").alias("risk_count"),
        F.countDistinct(F.when(F.col("risk_family") == "natural", F.col("risk_code"))).alias(
            "natural_risk_count"
        ),
        F.countDistinct(F.when(F.col("risk_family") == "technological", F.col("risk_code"))).alias(
            "technological_risk_count"
        ),
        *[
            F.max(F.col("risk_code").startswith(code).cast("int")).alias(f"has_{flag}")
            for flag, code in RISK_FLAGS.items()
        ],
    ]
    per_commune = risks.groupBy("commune_code", "department_code", "region_code").agg(*aggs)
    communes = lake.silver(spark, "communes").select("commune_code", "department_code", "region_code")
    per_commune = communes.join(
        per_commune.drop("department_code", "region_code"), "commune_code", "left"
    ).fillna(0, subset=[c for c in per_commune.columns if c.endswith("_count") or c.startswith("has_")])
    commune_level = (
        per_commune.withColumn("level", F.lit("commune"))
        .withColumnRenamed("commune_code", "territory_code")
        .withColumn("communes", F.lit(1))
        .withColumn("exposed_communes_share", F.when(F.col("risk_count") > 0, 1.0).otherwise(0.0))
        .drop("department_code", "region_code")
    )
    parts = [commune_level]
    for level in ("department", "region"):
        code = LEVELS[level]
        parts.append(
            per_commune.groupBy(code)
            .agg(
                F.count("*").alias("communes"),
                F.round(F.avg("risk_count"), 2).alias("risk_count"),
                F.round(F.avg("natural_risk_count"), 2).alias("natural_risk_count"),
                F.round(F.avg("technological_risk_count"), 2).alias("technological_risk_count"),
                *[F.round(F.avg(f"has_{flag}"), 4).alias(f"has_{flag}") for flag in RISK_FLAGS],
                F.round(F.avg((F.col("risk_count") > 0).cast("double")), 4).alias("exposed_communes_share"),
            )
            .withColumnRenamed(code, "territory_code")
            .withColumn("level", F.lit(level))
        )
    out = parts[0]
    for p in parts[1:]:
        out = out.unionByName(p)
    return out.select(
        "level", "territory_code", *[c for c in out.columns if c not in {"level", "territory_code"}]
    )


# ----------------------------------------------------------------------------- territory profile
def _latest(df: DataFrame) -> DataFrame:
    """One row per commune: the most recent year available."""
    w = Window.partitionBy("commune_code").orderBy(F.col("year").desc())
    return df.withColumn("_rn", F.row_number().over(w)).filter("_rn = 1").drop("_rn")


def territory_profile(spark: SparkSession, lake: Lake) -> DataFrame:
    """Commune rows joined from every domain, then rolled up to department and region."""
    communes = lake.silver(spark, "communes").select(
        "commune_code",
        "department_code",
        "region_code",
        "name",
        "population",
        "surface_ha",
        "latitude",
        "longitude",
    )
    tx = lake.silver(spark, "transactions").filter("property_type IN ('Maison', 'Appartement')")
    row = tx.agg(F.max("year")).first()
    latest_year = int(row[0]) if row and row[0] is not None else 0
    housing = (
        tx.filter(F.col("year") == latest_year)
        .groupBy("commune_code")
        .agg(
            F.count("*").alias("transactions"),
            F.round(F.median("price_m2"), 0).alias("median_price_m2"),
            F.round(F.median("price"), 0).alias("median_price"),
            F.round(F.avg(F.when(F.col("property_type") == "Appartement", 1.0).otherwise(0.0)), 4).alias(
                "apartment_share"
            ),
        )
    )
    income = _latest(lake.silver(spark, "income")).select(
        "commune_code",
        "median_income",
        "poverty_rate",
        "interdecile_ratio",
        F.col("year").alias("income_year"),
    )
    employment = _latest(lake.silver(spark, "employment")).select(
        "commune_code",
        "active",
        "employed",
        "unemployed",
        "unemployment_rate",
        F.col("year").alias("employment_year"),
    )
    schools = (
        lake.silver(spark, "schools")
        .groupBy("commune_code")
        .agg(
            F.count("*").alias("schools"),
            F.sum((F.col("school_type") == "Ecole").cast("int")).alias("primary_schools"),
            F.sum((F.col("school_type") == "Collège").cast("int")).alias("middle_schools"),
            F.sum((F.col("school_type") == "Lycée").cast("int")).alias("high_schools"),
            F.sum((F.col("status") == "Public").cast("int")).alias("public_schools"),
        )
    )
    stations = lake.silver(spark, "stations").groupBy("commune_code").agg(F.count("*").alias("rail_stations"))
    risk = (
        climate_risk(spark, lake)
        .filter("level = 'commune'")
        .select(
            F.col("territory_code").alias("commune_code"), "risk_count", "natural_risk_count", "has_flood"
        )
    )
    dpe = (
        lake.silver(spark, "certificates")
        .groupBy("commune_code")
        .agg(
            F.count("*").alias("dpe_count"),
            F.round(F.avg(F.col("energy_label").isin("A", "B").cast("double")), 4).alias("dpe_share_ab"),
            F.round(F.avg(F.col("energy_label").isin("F", "G").cast("double")), 4).alias("dpe_share_fg"),
            F.round(F.avg("energy_kwh_m2"), 1).alias("avg_energy_kwh_m2"),
        )
    )
    texts = (
        lake.silver(spark, "meeting_reports").groupBy("commune_code").agg(F.count("*").alias("text_reports"))
    )
    demo = (
        population_evolution(spark, lake)
        .filter("level = 'commune'")
        .withColumn(
            "_rn", F.row_number().over(Window.partitionBy("territory_code").orderBy(F.col("year").desc()))
        )
        .filter("_rn = 1")
        .select(
            F.col("territory_code").alias("commune_code"),
            F.col("year").alias("census_year"),
            F.col("annual_growth").alias("population_annual_growth"),
            "dwellings",
            "main_dwellings",
            "vacant_dwellings",
            "vacancy_rate",
        )
    )

    prof = communes
    for extra in (housing, income, employment, schools, stations, risk, dpe, texts, demo):
        prof = prof.join(extra, "commune_code", "left")
    prof = prof.fillna(
        0,
        subset=[
            "transactions",
            "schools",
            "primary_schools",
            "middle_schools",
            "high_schools",
            "public_schools",
            "rail_stations",
            "risk_count",
            "natural_risk_count",
            "has_flood",
            "dpe_count",
            "text_reports",
        ],
    )
    prof = (
        prof.withColumn("density_km2", F.round(F.col("population") / (F.col("surface_ha") / 100.0), 1))
        .withColumn("housing_year", F.lit(latest_year))
        .withColumn("dwellings_per_1000", F.round(F.col("dwellings") / F.col("population") * 1000, 1))
        .withColumn(
            "transactions_per_1000_dwellings", F.round(F.col("transactions") / F.col("dwellings") * 1000, 1)
        )
    )
    commune_level = (
        prof.withColumn("level", F.lit("commune"))
        .withColumnRenamed("commune_code", "territory_code")
        .withColumn("communes", F.lit(1))
    )
    parts = [commune_level]
    names = {
        "department": lake.silver(spark, "departments").select(
            F.col("department_code").alias("territory_code"), "name"
        ),
        "region": lake.silver(spark, "regions").select(F.col("region_code").alias("territory_code"), "name"),
    }
    for level in ("department", "region"):
        code = LEVELS[level]
        market = (
            housing_market(spark, lake, level)
            .filter((F.col("year") == latest_year) & (F.col("property_type") == "all"))
            .select("territory_code", "transactions", "median_price_m2", "median_price")
        )
        apt = (
            housing_market(spark, lake, level)
            .filter((F.col("year") == latest_year) & (F.col("property_type") == "Appartement"))
            .select("territory_code", F.col("transactions").alias("_apt"))
        )
        w_income = F.sum(F.col("median_income") * F.col("population")) / F.sum(
            F.when(F.col("median_income").isNotNull(), F.col("population"))
        )
        w_poverty = F.sum(F.col("poverty_rate") * F.col("population")) / F.sum(
            F.when(F.col("poverty_rate").isNotNull(), F.col("population"))
        )
        rolled = (
            prof.groupBy(code)
            .agg(
                F.count("*").alias("communes"),
                F.sum("population").alias("population"),
                F.sum("surface_ha").alias("surface_ha"),
                F.round(F.avg("latitude"), 4).alias("latitude"),
                F.round(F.avg("longitude"), 4).alias("longitude"),
                F.round(w_income, 0).alias("median_income"),  # population-weighted mean of commune medians
                F.round(w_poverty, 2).alias("poverty_rate"),
                F.round(F.avg("interdecile_ratio"), 2).alias("interdecile_ratio"),
                F.max("income_year").alias("income_year"),
                F.sum("active").alias("active"),
                F.sum("employed").alias("employed"),
                F.sum("unemployed").alias("unemployed"),
                F.max("employment_year").alias("employment_year"),
                F.sum("schools").alias("schools"),
                F.sum("primary_schools").alias("primary_schools"),
                F.sum("middle_schools").alias("middle_schools"),
                F.sum("high_schools").alias("high_schools"),
                F.sum("public_schools").alias("public_schools"),
                F.sum("rail_stations").alias("rail_stations"),
                F.round(F.avg("risk_count"), 2).alias("risk_count"),
                F.round(F.avg("natural_risk_count"), 2).alias("natural_risk_count"),
                F.round(F.avg("has_flood"), 4).alias("has_flood"),
                F.sum("dpe_count").alias("dpe_count"),
                F.round(F.sum(F.col("dpe_share_ab") * F.col("dpe_count")) / F.sum("dpe_count"), 4).alias(
                    "dpe_share_ab"
                ),
                F.round(F.sum(F.col("dpe_share_fg") * F.col("dpe_count")) / F.sum("dpe_count"), 4).alias(
                    "dpe_share_fg"
                ),
                F.round(F.sum(F.col("avg_energy_kwh_m2") * F.col("dpe_count")) / F.sum("dpe_count"), 1).alias(
                    "avg_energy_kwh_m2"
                ),
                F.sum("text_reports").alias("text_reports"),
                F.max("census_year").alias("census_year"),
                F.sum("dwellings").alias("dwellings"),
                F.sum("main_dwellings").alias("main_dwellings"),
                F.sum("vacant_dwellings").alias("vacant_dwellings"),
            )
            .withColumn("vacancy_rate", F.round(F.col("vacant_dwellings") / F.col("dwellings"), 4))
            .withColumn("dwellings_per_1000", F.round(F.col("dwellings") / F.col("population") * 1000, 1))
            .withColumnRenamed(code, "territory_code")
            .withColumn("unemployment_rate", F.round(F.col("unemployed") / F.col("active"), 4))
            .withColumn("density_km2", F.round(F.col("population") / (F.col("surface_ha") / 100.0), 1))
            .withColumn("housing_year", F.lit(latest_year))
            .withColumn("level", F.lit(level))
            .join(names[level], "territory_code", "left")
            .join(market, "territory_code", "left")
            .join(apt, "territory_code", "left")
            .withColumn("apartment_share", F.round(F.col("_apt") / F.col("transactions"), 4))
            .drop("_apt")
            .fillna(0, subset=["transactions"])
            .withColumn(
                "transactions_per_1000_dwellings",
                F.round(F.col("transactions") / F.col("dwellings") * 1000, 1),
            )
            .join(
                population_evolution(spark, lake)
                .filter(F.col("level") == level)
                .withColumn(
                    "_rn",
                    F.row_number().over(Window.partitionBy("territory_code").orderBy(F.col("year").desc())),
                )
                .filter("_rn = 1")
                .select("territory_code", F.col("annual_growth").alias("population_annual_growth")),
                "territory_code",
                "left",
            )
        )
        if level == "department":
            rolled = rolled.join(
                lake.silver(spark, "departments").select(
                    F.col("department_code").alias("territory_code"), "region_code"
                ),
                "territory_code",
                "left",
            ).withColumn("department_code", F.col("territory_code"))
        else:
            rolled = rolled.withColumn("department_code", F.lit(None).cast("string")).withColumn(
                "region_code", F.col("territory_code")
            )
        parts.append(rolled)
    out = parts[0]
    for p in parts[1:]:
        out = out.unionByName(p, allowMissingColumns=True)
    first = [
        "level",
        "territory_code",
        "name",
        "department_code",
        "region_code",
        "communes",
        "population",
        "density_km2",
    ]
    return out.select(*first, *[c for c in out.columns if c not in first])


GoldFn = Callable[[SparkSession, Lake], DataFrame]
GOLD: dict[str, GoldFn] = {
    "housing_market_commune": lambda s, lk: housing_market(s, lk, "commune"),
    "housing_market_department": lambda s, lk: housing_market(s, lk, "department"),
    "housing_market_region": lambda s, lk: housing_market(s, lk, "region"),
    "housing_price_evolution": housing_price_evolution,
    "population_evolution": population_evolution,
    "climate_risk": climate_risk,
    "territory_profile": territory_profile,
}
