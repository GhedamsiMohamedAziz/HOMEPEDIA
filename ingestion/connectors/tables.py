"""Arrow helpers shared by connectors: read CSV as strings, then rename/cast to canonical types."""

from __future__ import annotations

import gzip
from pathlib import Path

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.csv as pcsv


def read_csv(path: Path, delimiter: str = ",", columns: list[str] | None = None) -> pa.Table:
    """Read everything as UTF-8 strings (codes keep leading zeros); empty strings become null."""
    header = _header(path, delimiter)
    return pcsv.read_csv(
        path,
        parse_options=pcsv.ParseOptions(delimiter=delimiter, newlines_in_values=True),
        convert_options=pcsv.ConvertOptions(
            column_types={c: pa.string() for c in header},
            include_columns=columns,
            strings_can_be_null=True,
            auto_dict_encode=False,
            quoted_strings_can_be_null=False,
        ),
        read_options=pcsv.ReadOptions(encoding="utf8"),
    )


def _header(path: Path, delimiter: str) -> list[str]:
    opener = gzip.open if path.suffix == ".gz" else open
    with opener(path, "rt", encoding="utf-8-sig") as fh:
        line = fh.readline().rstrip("\r\n")
    return [c.strip('"') for c in line.split(delimiter)]


def project(table: pa.Table, mapping: dict[str, tuple[str, pa.DataType]]) -> pa.Table:
    """mapping: source column -> (canonical name, type). Missing sources raise; casts are strict."""
    missing = [c for c in mapping if c not in table.column_names]
    if missing:
        raise ValueError(f"missing source columns: {missing}")
    cols = []
    names = []
    for src, (dst, typ) in mapping.items():
        col = table[src]
        if pa.types.is_string(col.type) and not pa.types.is_string(typ):
            col = pc.cast(pc.utf8_trim_whitespace(col), typ)
        elif not pa.types.is_string(typ):
            col = pc.cast(col, typ)
        cols.append(col)
        names.append(dst)
    return pa.table(cols, names=names)


def strip_leading_zero_dept(col: pa.ChunkedArray) -> pa.ChunkedArray:
    """'033' -> '33', '02A' -> '2A', '974' stays: department codes are 2 chars except DOM."""
    is3 = pc.equal(pc.utf8_length(col), 3)
    starts0 = pc.starts_with(col, "0")
    return pc.if_else(pc.and_(is3, starts0), pc.utf8_slice_codeunits(col, 1), col)


def in_departments(col: pa.ChunkedArray, departments: list[str]) -> pa.ChunkedArray:
    return pc.is_in(col, value_set=pa.array(departments))


def commune_department(commune_code: pa.ChunkedArray) -> pa.ChunkedArray:
    """INSEE commune code -> department code (2A/2B for Corsica, 3 digits for DOM)."""
    first3 = pc.utf8_slice_codeunits(commune_code, 0, 3)
    first2 = pc.utf8_slice_codeunits(commune_code, 0, 2)
    return pc.if_else(pc.starts_with(commune_code, "97"), first3, first2)
