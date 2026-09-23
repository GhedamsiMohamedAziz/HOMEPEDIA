from __future__ import annotations

import shutil
import subprocess
from collections.abc import Iterator

import pytest

from homepedia.settings import Settings


@pytest.fixture
def settings() -> Settings:
    # explicit values: tests must not depend on the developer's .env
    return Settings(_env_file=None)  # type: ignore[call-arg]


def has_jvm() -> bool:
    java = shutil.which("java")
    if java is None:
        return False
    try:  # macOS ships a /usr/bin/java stub that exits non-zero when no runtime is installed
        return subprocess.run([java, "-version"], capture_output=True, timeout=10).returncode == 0  # noqa: S603
    except (OSError, subprocess.TimeoutExpired):
        return False


@pytest.fixture(scope="session")
def spark() -> Iterator[object]:
    if not has_jvm():
        pytest.skip("no JVM available")
    from pyspark.sql import SparkSession

    s = (
        SparkSession.builder.master("local[2]")
        .appName("homepedia-tests")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    yield s
    s.stop()
