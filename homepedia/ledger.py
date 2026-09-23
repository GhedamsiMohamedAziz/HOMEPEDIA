"""ops.pipeline_runs writer — every run is recorded, success or failure (section 26)."""

from __future__ import annotations

import json
import os
import subprocess
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass, field
from pathlib import Path

from homepedia.db.postgres import connect
from homepedia.settings import Settings


@dataclass
class RunStats:
    rows_received: int = 0
    rows_processed: int = 0
    rows_rejected: int = 0
    reject_reasons: dict[str, int] = field(default_factory=dict)


def git_commit() -> str | None:
    if env := os.environ.get("GIT_COMMIT"):
        return env
    try:
        out = subprocess.run(
            ["git", "rev-parse", "HEAD"],  # noqa: S607
            capture_output=True,
            text=True,
            timeout=5,
            cwd=Path(__file__).resolve().parents[2],
            check=False,
        )
    except (OSError, subprocess.TimeoutExpired):
        return None
    return out.stdout.strip() if out.returncode == 0 else None


@contextmanager
def record_run(
    settings: Settings, source: str, pipeline_version: str, data_version: str
) -> Iterator[RunStats]:
    stats = RunStats()
    with connect(settings) as conn:
        row = conn.execute(
            "INSERT INTO ops.pipeline_runs (source, pipeline_version, git_commit, data_version, status)"
            " VALUES (%s, %s, %s, %s, 'running') RETURNING run_id",
            (source, pipeline_version, git_commit(), data_version),
        ).fetchone()
        conn.commit()
    assert row is not None
    run_id = int(row[0])  # type: ignore[call-overload]
    try:
        yield stats
    except BaseException as exc:
        _finish(settings, run_id, stats, "failed", f"{type(exc).__name__}: {exc}"[:2000])
        raise
    _finish(settings, run_id, stats, "success", None)


def _finish(settings: Settings, run_id: int, stats: RunStats, status: str, error: str | None) -> None:
    with connect(settings) as conn:
        conn.execute(
            "UPDATE ops.pipeline_runs SET finished_at = now(), rows_received = %s, rows_processed = %s,"
            " rows_rejected = %s, reject_reasons = %s::jsonb, status = %s, error = %s WHERE run_id = %s",
            (
                stats.rows_received,
                stats.rows_processed,
                stats.rows_rejected,
                json.dumps(stats.reject_reasons),
                status,
                error,
                run_id,
            ),
        )
        conn.commit()
