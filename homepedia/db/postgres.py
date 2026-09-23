"""PostgreSQL / PostGIS access."""

from __future__ import annotations

from collections.abc import Iterator
from contextlib import contextmanager

import psycopg

from homepedia.settings import Settings, get_settings


@contextmanager
def connect(settings: Settings | None = None) -> Iterator[psycopg.Connection[tuple[object, ...]]]:
    s = settings or get_settings()
    with psycopg.connect(s.postgres_dsn, connect_timeout=5) as conn:
        yield conn


def postgis_version(settings: Settings | None = None) -> str:
    """Raises if the server is unreachable or PostGIS is not installed."""
    with connect(settings) as conn:
        row = conn.execute("SELECT postgis_version()").fetchone()
    if row is None:
        raise RuntimeError("postgis_version() returned no row")
    return str(row[0])
