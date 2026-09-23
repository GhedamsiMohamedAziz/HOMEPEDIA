"""Declarative data-quality rules evaluated in Spark (section 11).

A Rule is a named Spark SQL boolean expression; a row is kept only if every rule is true (null counts as
failure). Rejected rows are returned with a `reasons` array so they can be written out and counted per reason.
"""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F  # noqa: N812


@dataclass(frozen=True)
class Rule:
    name: str
    condition: str  # Spark SQL expression that must be true


# reusable rules across sources
def in_reference(col: str = "commune_code") -> Rule:
    return Rule("commune_not_in_reference", f"_ref_{col} IS NOT NULL")


VALID_COORDS = Rule(
    "invalid_coordinates",
    "(latitude IS NULL AND longitude IS NULL)"
    " OR (latitude BETWEEN -90 AND 90 AND longitude BETWEEN -180 AND 180"
    " AND NOT (latitude = 0 AND longitude = 0))",
)


def apply_rules(df: DataFrame, rules: Sequence[Rule]) -> tuple[DataFrame, DataFrame]:
    """Return (clean, rejects). `rejects` carries `reasons: array<string>` and `reason: string`."""
    if not rules:
        return df, df.limit(0).withColumn("reasons", F.array().cast("array<string>")).withColumn(
            "reason", F.lit("")
        )
    flags = [F.when(~F.coalesce(F.expr(r.condition), F.lit(False)), F.lit(r.name)) for r in rules]
    tagged = df.withColumn("reasons", F.array_compact(F.array(*flags)))
    clean = tagged.filter(F.size("reasons") == 0).drop("reasons")
    rejects = tagged.filter(F.size("reasons") > 0).withColumn("reason", F.concat_ws(";", "reasons"))
    return clean, rejects


def reject_duplicates(
    df: DataFrame, keys: Sequence[str], order_by: str | None = None
) -> tuple[DataFrame, DataFrame]:
    """Keep one row per key (latest by `order_by` if given); extra rows are rejected as `duplicate_key`."""
    order = F.col(order_by).desc_nulls_last() if order_by else F.lit(1)
    w = Window.partitionBy(*keys).orderBy(order)
    tagged = df.withColumn("_rn", F.row_number().over(w))
    keep = tagged.filter("_rn = 1").drop("_rn")
    dups = (
        tagged.filter("_rn > 1")
        .drop("_rn")
        .withColumn("reasons", F.array(F.lit("duplicate_key")))
        .withColumn("reason", F.lit("duplicate_key"))
    )
    return keep, dups


def reject_counts(rejects: DataFrame) -> dict[str, int]:
    rows = rejects.select(F.explode("reasons").alias("r")).groupBy("r").count().collect()
    return {row["r"]: int(row["count"]) for row in rows}


def with_reference(df: DataFrame, communes: DataFrame, col: str = "commune_code") -> DataFrame:
    """Left-join the commune reference: exposes `_ref_<col>` (for in_reference) + region/department codes."""
    ref = communes.select(
        F.col("commune_code").alias(f"_ref_{col}"),
        F.col("department_code").alias("_ref_department_code"),
        F.col("region_code").alias("_ref_region_code"),
    )
    out = df.join(ref, df[col] == ref[f"_ref_{col}"], "left")
    if "department_code" not in df.columns:
        out = out.withColumn("department_code", F.col("_ref_department_code"))
    if "region_code" not in df.columns:
        out = out.withColumn("region_code", F.col("_ref_region_code"))
    return out.drop("_ref_department_code", "_ref_region_code")
