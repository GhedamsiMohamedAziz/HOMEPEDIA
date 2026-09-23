"""SparkSession factory. Points at the compose cluster by default, local[*] for tests."""

from __future__ import annotations

from pyspark.sql import SparkSession

from homepedia.settings import Settings, get_settings


def build_session(settings: Settings | None = None, app_name: str | None = None) -> SparkSession:
    s = settings or get_settings()
    return (
        SparkSession.builder.master(s.spark_master_url)
        .appName(app_name or s.spark_app_name)
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", "8")
        .getOrCreate()
    )
