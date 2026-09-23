"""Connector normalisation on real-data fixtures + base-class validation/bronze/manifest logic."""

from __future__ import annotations

import json
from collections.abc import Iterator
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from homepedia.settings import Settings
from ingestion.connectors.base import RawFile, SourceConnector
from ingestion.connectors.ledger import RunStats
from ingestion.pipelines.registry import CONNECTORS
from ingestion.sources.dvf import DvfConnector
from ingestion.sources.education import EducationConnector

FIXTURES = Path(__file__).parent / "fixtures"


def raw(name: str, path: Path) -> RawFile:
    return RawFile(
        name, "https://example.invalid/" + name, path, "0" * 64, path.stat().st_size, "2026-01-01T00:00:00Z"
    )


@pytest.fixture
def lake_settings(tmp_path: Path) -> Settings:
    return Settings(_env_file=None, data_lake_root=tmp_path)  # type: ignore[call-arg]


def test_dvf_normalize(lake_settings: Settings) -> None:
    t = DvfConnector(lake_settings).normalize([raw("dvf_2023_33.csv.gz", FIXTURES / "dvf_sample.csv.gz")])[
        "transactions"
    ]
    assert t.num_rows == 200
    assert t["year"].to_pylist()[0] == 2023
    assert t["price"].type == pa.float64()
    assert t["date"].type == pa.date32()
    assert set(t["department_code"].to_pylist()) == {"33"}
    assert all(len(c) == 5 for c in t["commune_code"].to_pylist())


def test_education_normalize_strips_department_padding(lake_settings: Settings) -> None:
    t = EducationConnector(lake_settings).normalize([raw("a.csv", FIXTURES / "education_sample.csv")])[
        "schools"
    ]
    assert t.num_rows == 20
    assert set(t["department_code"].to_pylist()) <= {"33", "31", "44"}
    assert t["latitude"].type == pa.float64()


class _Fake(SourceConnector):
    name = "fake"
    not_null = {"t": ("k",)}
    partition_by = {"t": ("p",)}

    def extract(self) -> Iterator[tuple[str, str]]:
        yield from ()

    def normalize(self, files: list[RawFile]) -> dict[str, pa.Table]:
        return {}


def test_validate_rejects_null_keys_and_counts(lake_settings: Settings) -> None:
    c = _Fake(lake_settings)
    stats = RunStats()
    t = pa.table({"k": ["a", None, "c"], "p": ["x", "x", "y"]})
    out = c.validate("t", t, stats)
    assert out.num_rows == 2
    assert (stats.rows_received, stats.rows_processed, stats.rows_rejected) == (3, 2, 1)
    assert stats.reject_reasons == {"t.k_null": 1}
    with pytest.raises(ValueError, match="missing required column"):
        c.validate("t", pa.table({"p": ["x"]}), RunStats())


def test_write_bronze_partitions_and_overwrites(lake_settings: Settings) -> None:
    c = _Fake(lake_settings)
    out = c.write_bronze("t", pa.table({"k": ["a", "c"], "p": ["x", "y"]}))
    assert sorted(d.name for d in out.iterdir()) == ["p=x", "p=y"]
    c.write_bronze("t", pa.table({"k": ["z"], "p": ["q"]}))
    assert sorted(d.name for d in out.iterdir()) == ["p=q"]
    assert pq.read_table(out)["_data_version"].to_pylist() == [c.data_version]


def test_cached_uses_manifest(lake_settings: Settings) -> None:
    c = _Fake(lake_settings)
    c.raw_dir.mkdir(parents=True)
    f = c.raw_dir / "a.txt"
    f.write_text("abc")
    assert c.cached("a.txt") is None  # no manifest yet
    c._write_manifest([raw("a.txt", f)])
    assert json.loads(c._manifest_path.read_text())["files"][0]["name"] == "a.txt"
    hit = c.cached("a.txt")
    assert hit is not None and hit.path == f
    f.write_text("abcd")  # size changed -> refetch
    assert c.cached("a.txt") is None


def test_registry_is_complete_and_ordered() -> None:
    assert list(CONNECTORS) == [
        "geo_reference",
        "dvf",
        "insee_filosofi",
        "insee_emploi",
        "dpe",
        "georisques",
        "sncf_stations",
        "education",
        "grand_debat",
    ]
    assert list(CONNECTORS).index("geo_reference") < list(CONNECTORS).index("georisques")
