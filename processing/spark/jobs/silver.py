"""spark-submit processing/spark/jobs/silver.py [table ...] — BRONZE → SILVER with rejects + ledger."""

from __future__ import annotations

import sys
import time

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F  # noqa: N812

from homepedia.ledger import record_run
from homepedia.logging import configure_logging, get_logger
from homepedia.settings import Settings, get_settings
from homepedia.spark.session import build_session
from processing.cleaning.rules import reject_counts
from processing.transformations.silver import SILVER, Lake

log = get_logger("silver")


def write(df: DataFrame, path: str, partitions: tuple[str, ...]) -> None:
    writer = df.write.mode("overwrite")
    if partitions:
        writer = writer.partitionBy(*partitions)
    writer.parquet(path)


def run_table(spark: SparkSession, settings: Settings, table: str) -> dict[str, int]:
    fn, partitions = SILVER[table]
    lake = Lake(settings.data_lake_root)
    t0 = time.perf_counter()
    with record_run(
        settings, f"silver.{table}", settings.pipeline_version, lake_version(spark, lake, table)
    ) as st:
        clean, rejects = fn(spark, lake)
        clean = clean.withColumn("_processed_at", F.current_timestamp()).cache()
        rejects = rejects.cache()
        write(clean, str(lake.silver_path(table)), partitions)
        write(
            rejects.withColumn("_processed_at", F.current_timestamp()),
            str(lake.silver_path(f"_rejects/{table}")),
            (),
        )
        st.rows_processed = clean.count()
        st.rows_rejected = rejects.count()
        st.rows_received = st.rows_processed + st.rows_rejected
        st.reject_reasons = reject_counts(rejects)
        clean.unpersist()
        rejects.unpersist()
    out = {"processed": st.rows_processed, "rejected": st.rows_rejected}
    log.info(
        "silver done",
        extra={
            "extra": {
                "table": table,
                **out,
                "reasons": st.reject_reasons,
                "s": round(time.perf_counter() - t0, 1),
            }
        },
    )
    return out


def lake_version(spark: SparkSession, lake: Lake, table: str) -> str:
    """data_version of the bronze input, so a SILVER row traces back to its RAW fetch (section 27)."""
    src = {
        "transactions": ("dvf", "transactions"),
        "certificates": ("dpe", "certificates"),
        "income": ("insee_filosofi", "income"),
        "employment": ("insee_emploi", "employment"),
        "population": ("insee_population", "population_series"),
        "commune_risks": ("georisques", "commune_risks"),
        "schools": ("education", "schools"),
        "stations": ("sncf_stations", "stations"),
        "meeting_reports": ("grand_debat", "meeting_reports"),
    }.get(table, ("geo_reference", table))
    row = lake.bronze(spark, *src).select(F.max("_data_version")).first()
    return str(row[0]) if row and row[0] else "unknown"


def main(argv: list[str]) -> int:
    settings = get_settings()
    configure_logging(settings.log_level)
    tables = argv or list(SILVER)
    unknown = [t for t in tables if t not in SILVER]
    if unknown:
        log.error("unknown tables", extra={"extra": {"unknown": unknown, "known": list(SILVER)}})
        return 2
    spark = build_session(settings, app_name="homepedia-silver")
    failed = 0
    try:
        for table in tables:
            try:
                run_table(spark, settings, table)
            except (
                Exception
            ) as exc:  # the ledger has the failure; keep going so one table can't block the rest
                failed += 1
                log.error(
                    "silver failed",
                    extra={"extra": {"table": table, "error": f"{type(exc).__name__}: {exc}"}},
                )
    finally:
        spark.stop()
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
