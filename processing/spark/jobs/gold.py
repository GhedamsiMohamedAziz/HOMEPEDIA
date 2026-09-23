"""spark-submit processing/spark/jobs/gold.py [table ...] — SILVER → GOLD with ledger."""

from __future__ import annotations

import sys
import time

from pyspark.sql import functions as F  # noqa: N812

from homepedia.ledger import record_run
from homepedia.logging import configure_logging, get_logger
from homepedia.settings import get_settings
from homepedia.spark.session import build_session
from processing.transformations.gold import GOLD
from processing.transformations.silver import Lake

log = get_logger("gold")


def main(argv: list[str]) -> int:
    settings = get_settings()
    configure_logging(settings.log_level)
    tables = argv or list(GOLD)
    unknown = [t for t in tables if t not in GOLD]
    if unknown:
        log.error("unknown tables", extra={"extra": {"unknown": unknown, "known": list(GOLD)}})
        return 2
    lake = Lake(settings.data_lake_root)
    spark = build_session(settings, app_name="homepedia-gold")
    failed = 0
    try:
        for table in tables:
            t0 = time.perf_counter()
            try:
                with record_run(settings, f"gold.{table}", settings.pipeline_version, "silver") as st:
                    df = GOLD[table](spark, lake).withColumn("_processed_at", F.current_timestamp())
                    df.write.mode("overwrite").parquet(str(lake.root / "gold" / table))
                    st.rows_received = st.rows_processed = spark.read.parquet(
                        str(lake.root / "gold" / table)
                    ).count()
                log.info(
                    "gold done",
                    extra={
                        "extra": {
                            "table": table,
                            "rows": st.rows_processed,
                            "s": round(time.perf_counter() - t0, 1),
                        }
                    },
                )
            except Exception as exc:
                failed += 1
                log.error(
                    "gold failed", extra={"extra": {"table": table, "error": f"{type(exc).__name__}: {exc}"}}
                )
    finally:
        spark.stop()
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
