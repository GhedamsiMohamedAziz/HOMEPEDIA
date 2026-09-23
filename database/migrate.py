"""Apply database/migrations/NNNN_*.sql in order, once each. Idempotent, safe to rerun.

python -m database.migrate
"""

from __future__ import annotations

import re
import sys
from pathlib import Path

import psycopg

from homepedia.db.postgres import connect
from homepedia.logging import configure_logging, get_logger
from homepedia.settings import Settings, get_settings

MIGRATIONS_DIR = Path(__file__).parent / "migrations"
_NAME_RE = re.compile(r"^(\d{4})_[a-z0-9_]+\.sql$")
log = get_logger(__name__)


def list_migrations(directory: Path = MIGRATIONS_DIR) -> list[tuple[int, Path]]:
    found: list[tuple[int, Path]] = []
    for path in sorted(directory.glob("*.sql")):
        m = _NAME_RE.match(path.name)
        if not m:
            raise ValueError(f"bad migration filename {path.name!r}: expected NNNN_snake_case.sql")
        found.append((int(m.group(1)), path))
    versions = [v for v, _ in found]
    if len(versions) != len(set(versions)):
        raise ValueError(f"duplicate migration versions: {versions}")
    return found


Conn = psycopg.Connection[tuple[object, ...]]


def apply_migrations(conn: Conn, directory: Path = MIGRATIONS_DIR) -> list[int]:
    conn.execute("CREATE SCHEMA IF NOT EXISTS ops")
    conn.execute(
        "CREATE TABLE IF NOT EXISTS ops.schema_migrations ("
        " version INTEGER PRIMARY KEY, name TEXT NOT NULL, applied_at TIMESTAMPTZ NOT NULL DEFAULT now())"
    )
    conn.commit()
    applied = {r[0] for r in conn.execute("SELECT version FROM ops.schema_migrations").fetchall()}
    newly: list[int] = []
    for version, path in list_migrations(directory):
        if version in applied:
            continue
        with conn.transaction():
            conn.execute(path.read_text(encoding="utf-8"))
            conn.execute(
                "INSERT INTO ops.schema_migrations (version, name) VALUES (%s, %s)", (version, path.name)
            )
        newly.append(version)
        log.info("migration applied", extra={"extra": {"version": version, "file": path.name}})
    return newly


def main(settings: Settings | None = None) -> int:
    s = settings or get_settings()
    configure_logging(s.log_level)
    with connect(s) as conn:
        newly = apply_migrations(conn)
    log.info("migrate done", extra={"extra": {"applied": newly}})
    return 0


if __name__ == "__main__":
    sys.exit(main())
