from __future__ import annotations

from pathlib import Path

import pyarrow as pa
import pytest

from ingestion.connectors.tables import (
    commune_department,
    project,
    read_csv,
    strip_leading_zero_dept,
)

FIXTURES = Path(__file__).parent / "fixtures"


def test_read_csv_keeps_leading_zeros_and_nulls_empty(tmp_path: Path) -> None:
    p = tmp_path / "x.csv"
    p.write_text('code;n\n"01001";\n"033";7\n')
    t = read_csv(p, delimiter=";")
    assert t["code"].to_pylist() == ["01001", "033"]
    assert t["n"].to_pylist() == [None, "7"]


def test_read_csv_gz_fixture() -> None:
    t = read_csv(FIXTURES / "dvf_sample.csv.gz")
    assert t.num_rows == 200
    assert t["code_commune"].type == pa.string()


def test_project_casts_and_renames() -> None:
    t = pa.table({"a": ["1.5", " 2 "], "d": ["2023-01-05", "2023-02-01"], "z": ["x", "y"]})
    out = project(t, {"a": ("price", pa.float64()), "d": ("date", pa.date32())})
    assert out.column_names == ["price", "date"]
    assert out["price"].to_pylist() == [1.5, 2.0]
    assert str(out["date"][0].as_py()) == "2023-01-05"


def test_project_missing_column_raises() -> None:
    with pytest.raises(ValueError, match="missing source columns"):
        project(pa.table({"a": ["1"]}), {"b": ("b", pa.string())})


def test_department_helpers() -> None:
    codes = pa.chunked_array([["033", "02A", "974", "33"]])
    assert strip_leading_zero_dept(codes).to_pylist() == ["33", "2A", "974", "33"]
    communes = pa.chunked_array([["33063", "2A004", "97411", "01001"]])
    assert commune_department(communes).to_pylist() == ["33", "2A", "974", "01"]
