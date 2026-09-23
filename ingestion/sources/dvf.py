"""08 — DVF (Demandes de valeurs foncières), geocoded by Etalab: one csv.gz per year x department."""

from __future__ import annotations

from collections.abc import Iterator

import pyarrow as pa

from ingestion.connectors.base import RawFile, SourceConnector
from ingestion.connectors.tables import project, read_csv

BASE = "https://files.data.gouv.fr/geo-dvf/latest/csv"

COLUMNS: dict[str, tuple[str, pa.DataType]] = {
    "id_mutation": ("mutation_id", pa.string()),
    "date_mutation": ("date", pa.date32()),
    "nature_mutation": ("mutation_type", pa.string()),
    "valeur_fonciere": ("price", pa.float64()),
    "code_commune": ("commune_code", pa.string()),
    "code_departement": ("department_code", pa.string()),
    "code_postal": ("postal_code", pa.string()),
    "code_type_local": ("property_type_code", pa.string()),
    "type_local": ("property_type", pa.string()),
    "surface_reelle_bati": ("surface", pa.float64()),
    "nombre_pieces_principales": ("rooms", pa.float64()),
    "surface_terrain": ("land_surface", pa.float64()),
    "nombre_lots": ("lots", pa.int64()),
    "longitude": ("longitude", pa.float64()),
    "latitude": ("latitude", pa.float64()),
}


class DvfConnector(SourceConnector):
    name = "dvf"
    not_null = {"transactions": ("mutation_id", "date", "commune_code", "department_code")}
    partition_by = {"transactions": ("year", "department_code")}

    def extract(self) -> Iterator[tuple[str, str]]:
        for year in self.settings.dvf_years:
            for dept in self.settings.departments:
                yield f"dvf_{year}_{dept}.csv.gz", f"{BASE}/{year}/departements/{dept}.csv.gz"

    def normalize(self, files: list[RawFile]) -> dict[str, pa.Table]:
        parts = []
        for f in files:
            raw = read_csv(f.path, columns=list(COLUMNS))
            t = project(raw, COLUMNS)
            year = int(f.name.split("_")[1])
            parts.append(t.append_column("year", pa.array([year] * t.num_rows, pa.int32())))
        return {"transactions": pa.concat_tables(parts)}
