"""07 — Geographic reference: official codes + names + centroids + population from geo.api.gouv.fr,
simplified contours (100 m) from Etalab admin-express exports. Whole of France (small)."""

from __future__ import annotations

import json
from collections.abc import Iterator
from pathlib import Path

import pyarrow as pa

from ingestion.connectors.base import RawFile, SourceConnector

GEO_API = "https://geo.api.gouv.fr"
CONTOURS = "https://etalab-datasets.geo.data.gouv.fr/contours-administratifs/2024/geojson"


class GeoReferenceConnector(SourceConnector):
    name = "geo_reference"
    version = "2"  # + postal_codes
    not_null = {
        "regions": ("region_code", "name"),
        "departments": ("department_code", "region_code", "name"),
        "communes": ("commune_code", "department_code", "region_code", "name"),
    }

    def extract(self) -> Iterator[tuple[str, str]]:
        yield "regions.json", f"{GEO_API}/regions"
        yield "departements.json", f"{GEO_API}/departements"
        yield (
            "communes.json",
            f"{GEO_API}/communes?fields=nom,code,codeDepartement,codeRegion,centre,population,surface,codesPostaux&format=json",
        )
        yield "regions-100m.geojson", f"{CONTOURS}/regions-100m.geojson"
        yield "departements-100m.geojson", f"{CONTOURS}/departements-100m.geojson"
        yield "communes-100m.geojson", f"{CONTOURS}/communes-100m.geojson"

    def normalize(self, files: list[RawFile]) -> dict[str, pa.Table]:
        by = {f.name: f.path for f in files}
        geoms = {k: _geometries(by[f"{k}-100m.geojson"]) for k in ("regions", "departements", "communes")}
        regions = json.loads(by["regions.json"].read_text())
        departements = json.loads(by["departements.json"].read_text())
        communes = json.loads(by["communes.json"].read_text())
        return {
            "regions": pa.table(
                {
                    "region_code": [r["code"] for r in regions],
                    "name": [r["nom"] for r in regions],
                    "geometry_geojson": [geoms["regions"].get(r["code"]) for r in regions],
                }
            ),
            "departments": pa.table(
                {
                    "department_code": [d["code"] for d in departements],
                    "region_code": [d["codeRegion"] for d in departements],
                    "name": [d["nom"] for d in departements],
                    "geometry_geojson": [geoms["departements"].get(d["code"]) for d in departements],
                }
            ),
            "communes": pa.table(
                {
                    "commune_code": [c["code"] for c in communes],
                    "department_code": [c.get("codeDepartement") for c in communes],
                    "region_code": [c.get("codeRegion") for c in communes],
                    "name": [c["nom"] for c in communes],
                    "population": pa.array([c.get("population") for c in communes], pa.int64()),
                    "surface_ha": pa.array([c.get("surface") for c in communes], pa.float64()),
                    "longitude": pa.array(
                        [c["centre"]["coordinates"][0] if "centre" in c else None for c in communes],
                        pa.float64(),
                    ),
                    "latitude": pa.array(
                        [c["centre"]["coordinates"][1] if "centre" in c else None for c in communes],
                        pa.float64(),
                    ),
                    "postal_codes": pa.array(
                        [c.get("codesPostaux", []) for c in communes], pa.list_(pa.string())
                    ),
                    "geometry_geojson": [geoms["communes"].get(c["code"]) for c in communes],
                }
            ),
        }


def _geometries(path: Path) -> dict[str, str]:
    fc = json.loads(path.read_text())
    return {f["properties"]["code"]: json.dumps(f["geometry"]) for f in fc["features"]}
