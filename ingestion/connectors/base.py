"""SourceConnector: extract() → write_raw() → normalize() → validate() → bronze, all inside run().

RAW keeps the exact bytes plus a manifest (url, sha256, size, fetched_at). BRONZE is typed Parquet.
Row-level rejects are counted per reason and reported in ops.pipeline_runs.
"""

from __future__ import annotations

import json
import time
from abc import ABC, abstractmethod
from collections.abc import Iterator
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq

from homepedia.ledger import RunStats, record_run
from homepedia.logging import get_logger
from homepedia.settings import Settings
from ingestion.connectors.http import download

log = get_logger("ingestion")


@dataclass(frozen=True)
class RawFile:
    name: str
    url: str
    path: Path
    sha256: str
    size_bytes: int
    fetched_at: str


@dataclass(frozen=True)
class RunResult:
    source: str
    data_version: str
    tables: dict[str, int]
    stats: RunStats
    duration_s: float


class SourceConnector(ABC):
    name: str
    version: str = "1"
    #: columns that must be non-null in every normalised table; violations are rejected + counted
    not_null: dict[str, tuple[str, ...]] = {}
    partition_by: dict[str, tuple[str, ...]] = {}

    def __init__(self, settings: Settings) -> None:
        self.settings = settings
        self.data_version = datetime.now(UTC).strftime("%Y-%m-%d")

    # -- layout -------------------------------------------------------------------------------
    @property
    def raw_dir(self) -> Path:
        return self.settings.lake_path("raw") / self.name / self.data_version

    @property
    def bronze_dir(self) -> Path:
        return self.settings.lake_path("bronze") / self.name

    # -- steps --------------------------------------------------------------------------------
    @abstractmethod
    def extract(self) -> Iterator[tuple[str, str]]:
        """Yield (file name, url) pairs to fetch. Multi-request sources override write_raw instead."""

    def write_raw(self) -> list[RawFile]:
        """Download every extract() item into RAW unless already present (rerunnable)."""
        files: list[RawFile] = []
        for name, url in self.extract():
            files.append(self.fetch(name, url))
        self._write_manifest(files)
        return files

    def fetch(self, name: str, url: str) -> RawFile:
        if cached := self.cached(name):
            return cached
        dest = self.raw_dir / name
        log.info("downloading", extra={"extra": {"source": self.name, "file": name, "url": url}})
        sha = download(url, dest, self.settings.http_timeout)
        return RawFile(name, url, dest, sha, dest.stat().st_size, datetime.now(UTC).isoformat())

    @abstractmethod
    def normalize(self, files: list[RawFile]) -> dict[str, pa.Table]:
        """Parse RAW into typed tables with canonical column names (section 12)."""

    def validate(self, table_name: str, table: pa.Table, stats: RunStats) -> pa.Table:
        """Drop rows with nulls in declared key columns, counting each rejection reason."""
        stats.rows_received += table.num_rows
        keep = pa.array([True] * table.num_rows)
        for col in self.not_null.get(table_name, ()):
            if col not in table.column_names:
                raise ValueError(f"{self.name}.{table_name}: missing required column {col!r}")
            bad = pc.is_null(table[col])
            n_bad = pc.sum(bad).as_py() or 0
            if n_bad:
                stats.reject_reasons[f"{table_name}.{col}_null"] = n_bad
                keep = pc.and_(keep, pc.invert(bad))
        out = table.filter(keep)
        stats.rows_rejected += table.num_rows - out.num_rows
        stats.rows_processed += out.num_rows
        return out

    def write_bronze(self, table_name: str, table: pa.Table) -> Path:
        out = self.bronze_dir / table_name
        if out.exists():
            for p in sorted(out.rglob("*"), reverse=True):
                p.unlink() if p.is_file() else p.rmdir()
        out.mkdir(parents=True, exist_ok=True)
        table = table.append_column("_data_version", pa.array([self.data_version] * table.num_rows))
        parts = list(self.partition_by.get(table_name, ()))
        if parts:
            pq.write_to_dataset(table, out, partition_cols=parts)
        else:
            pq.write_table(table, out / "part-0.parquet")
        return out

    def run(self) -> RunResult:
        t0 = time.perf_counter()
        pipeline_version = f"{self.settings.pipeline_version}+{self.name}.{self.version}"
        with record_run(self.settings, self.name, pipeline_version, self.data_version) as stats:
            files = self.write_raw()
            counts: dict[str, int] = {}
            for table_name, table in self.normalize(files).items():
                clean = self.validate(table_name, table, stats)
                self.write_bronze(table_name, clean)
                counts[table_name] = clean.num_rows
            log.info(
                "ingestion done",
                extra={"extra": {"source": self.name, "tables": counts, **asdict(stats)}},
            )
        return RunResult(self.name, self.data_version, counts, stats, round(time.perf_counter() - t0, 1))

    # -- manifest -----------------------------------------------------------------------------
    def cached(self, name: str) -> RawFile | None:
        """RAW file already fetched for this data_version (same size as recorded) -> reuse it."""
        dest = self.raw_dir / name
        m = self._manifest().get(name)
        if not m or not dest.exists() or dest.stat().st_size != m["size_bytes"]:
            return None
        log.info("raw cached", extra={"extra": {"source": self.name, "file": name}})
        return RawFile(
            name, str(m["url"]), dest, str(m["sha256"]), int(m["size_bytes"]), str(m["fetched_at"])
        )

    @property
    def _manifest_path(self) -> Path:
        return self.raw_dir / "manifest.json"

    def _manifest(self) -> dict[str, dict[str, Any]]:
        if not self._manifest_path.exists():
            return {}
        data = json.loads(self._manifest_path.read_text())
        return {f["name"]: f for f in data["files"]}

    def _write_manifest(self, files: list[RawFile]) -> None:
        self.raw_dir.mkdir(parents=True, exist_ok=True)
        payload = {
            "source": self.name,
            "connector_version": self.version,
            "data_version": self.data_version,
            "files": [{k: v for k, v in asdict(f).items() if k != "path"} for f in files],
        }
        self._manifest_path.write_text(json.dumps(payload, indent=2))
