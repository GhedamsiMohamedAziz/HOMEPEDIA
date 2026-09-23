"""Needs a JVM (CI and the docker image); skipped otherwise via the `spark` fixture."""

from __future__ import annotations

from pathlib import Path

from pyspark.sql import SparkSession

from processing.spark.jobs.smoke_job import run


def test_smoke_job_local(spark: SparkSession, tmp_path: Path) -> None:
    stats = run(spark, tmp_path / "out", rows=10_000, partitions=4)
    assert stats["departments"] == 95
    assert stats["input_partitions"] == 4
    assert spark.read.parquet(str(tmp_path / "out")).count() == 95
