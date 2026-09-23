"""12 — Transport: SNCF station list (all of France, WGS84 points). No INSEE code in the source:
the commune is resolved spatially against the PostGIS contours in Phase 3/4."""

from __future__ import annotations

from collections.abc import Iterator

import pyarrow as pa

from ingestion.connectors.base import RawFile, SourceConnector
from ingestion.connectors.tables import project, read_csv

URL = "https://ressources.data.sncf.com/api/explore/v2.1/catalog/datasets/liste-des-gares/exports/csv?delimiter=%3B"
COLUMNS: dict[str, tuple[str, pa.DataType]] = {
    "code_uic": ("station_uic", pa.string()),
    "libelle": ("name", pa.string()),
    "fret": ("freight", pa.string()),
    "voyageurs": ("passengers", pa.string()),
    "commune": ("commune_name", pa.string()),
    "departemen": ("department_name", pa.string()),
    "x_wgs84": ("longitude", pa.float64()),
    "y_wgs84": ("latitude", pa.float64()),
}


class SncfStationsConnector(SourceConnector):
    name = "sncf_stations"
    not_null = {"stations": ("station_uic", "name", "latitude", "longitude")}

    def extract(self) -> Iterator[tuple[str, str]]:
        yield "liste-des-gares.csv", URL

    def normalize(self, files: list[RawFile]) -> dict[str, pa.Table]:
        raw = read_csv(files[0].path, delimiter=";", columns=list(COLUMNS))
        return {"stations": project(raw, COLUMNS)}
