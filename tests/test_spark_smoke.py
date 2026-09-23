"""Needs a JVM. Runs in CI and inside the docker image; skipped when java is absent."""

from __future__ import annotations

import shutil
import subprocess
from pathlib import Path

import pytest


def _has_jvm() -> bool:
    java = shutil.which("java")
    if java is None:
        return False
    try:  # macOS ships a /usr/bin/java stub that exits non-zero when no runtime is installed
        return subprocess.run([java, "-version"], capture_output=True, timeout=10).returncode == 0  # noqa: S603
    except (OSError, subprocess.TimeoutExpired):
        return False


pytestmark = pytest.mark.skipif(not _has_jvm(), reason="no JVM available")


def test_smoke_job_local(tmp_path: Path) -> None:
    from pyspark.sql import SparkSession

    from processing.spark.jobs.smoke_job import run

    spark = SparkSession.builder.master("local[2]").appName("test").getOrCreate()
    try:
        stats = run(spark, tmp_path / "out", rows=10_000, partitions=4)
        assert stats["departments"] == 95
        assert stats["input_partitions"] == 4
        assert spark.read.parquet(str(tmp_path / "out")).count() == 95
    finally:
        spark.stop()
