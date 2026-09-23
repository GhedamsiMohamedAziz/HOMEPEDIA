"""Cluster smoke job: proves distributed execution end to end and writes Parquet to BRONZE.

Submitted to the compose cluster by `make spark-smoke`; runs on local[*] in tests.
"""

from __future__ import annotations

import sys
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F  # noqa: N812

from homepedia.settings import get_settings
from homepedia.spark.session import build_session


def run(spark: SparkSession, out_dir: Path, rows: int = 100_000, partitions: int = 8) -> dict[str, int]:
    df = (
        spark.range(0, rows, numPartitions=partitions)
        .withColumn("department_code", F.lpad((F.col("id") % 95 + 1).cast("string"), 2, "0"))
        .withColumn("price", (F.col("id") % 1000 + 1) * 1000.0)
        .withColumn("surface", F.col("id") % 150 + 10.0)
        .withColumn("price_m2", F.round(F.col("price") / F.col("surface"), 2))
    )
    agg = df.groupBy("department_code").agg(
        F.count("*").alias("transactions"), F.avg("price_m2").alias("avg_price_m2")
    )
    agg.write.mode("overwrite").parquet(str(out_dir))
    return {
        "input_partitions": df.rdd.getNumPartitions(),
        "rows": rows,
        "departments": agg.count(),
        "cluster_cores": spark.sparkContext.defaultParallelism,
    }


def main() -> int:
    s = get_settings()
    spark = build_session(s, app_name="homepedia-smoke")
    try:
        stats = run(spark, s.lake_path("bronze") / "_smoke")
        print(stats)
        return 0 if stats["departments"] == 95 else 1
    finally:
        spark.stop()


if __name__ == "__main__":
    sys.exit(main())
