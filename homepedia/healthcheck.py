"""`python -m homepedia.healthcheck` — verifies every Phase 1 service. Exit 1 on any failure."""

from __future__ import annotations

import sys
from collections.abc import Callable
from dataclasses import dataclass

from homepedia.db.mongo import missing_collections
from homepedia.db.postgres import connect, postgis_version
from homepedia.logging import configure_logging, get_logger
from homepedia.settings import Settings, get_settings

log = get_logger(__name__)


@dataclass(frozen=True)
class CheckResult:
    name: str
    ok: bool
    detail: str


def check_postgres(s: Settings) -> str:
    return f"postgis {postgis_version(s)}"


def check_migrations(s: Settings) -> str:
    with connect(s) as conn:
        row = conn.execute("SELECT max(version) FROM ops.schema_migrations").fetchone()
    if row is None or row[0] is None:
        raise RuntimeError("no migration applied — run `make migrate`")
    return f"migration {row[0]} applied"


def check_mongo(s: Settings) -> str:
    missing = missing_collections(s)
    if missing:
        raise RuntimeError(f"missing collections: {missing}")
    return "collections present"


CHECKS: dict[str, Callable[[Settings], str]] = {
    "postgres": check_postgres,
    "migrations": check_migrations,
    "mongo": check_mongo,
}


def run_checks(s: Settings, checks: dict[str, Callable[[Settings], str]] = CHECKS) -> list[CheckResult]:
    results: list[CheckResult] = []
    for name, fn in checks.items():
        try:
            results.append(CheckResult(name, True, fn(s)))
        except Exception as exc:
            results.append(CheckResult(name, False, f"{type(exc).__name__}: {exc}"))
    return results


def main() -> int:
    s = get_settings()
    configure_logging(s.log_level)
    results = run_checks(s)
    for r in results:
        log.log(20 if r.ok else 40, "healthcheck", extra={"extra": r.__dict__})
    return 0 if all(r.ok for r in results) else 1


if __name__ == "__main__":
    sys.exit(main())
