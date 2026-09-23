"""11 — Environment & risk: GASPAR natural/technological risks per commune (Géorisques API).
Communes of the configured departments come from the geo_reference BRONZE table."""

from __future__ import annotations

import hashlib
import json
from collections.abc import Iterator
from datetime import UTC, datetime

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq

from ingestion.connectors.base import RawFile, SourceConnector
from ingestion.connectors.http import get_json
from ingestion.connectors.tables import in_departments

API = "https://georisques.gouv.fr/api/v1/gaspar/risques"
BATCH = 10


class GeorisquesConnector(SourceConnector):
    name = "georisques"
    not_null = {"commune_risks": ("commune_code", "risk_code", "risk_label")}
    partition_by = {"commune_risks": ("department_code",)}

    def commune_codes(self) -> list[str]:
        path = self.settings.lake_path("bronze") / "geo_reference" / "communes"
        if not path.exists():
            raise RuntimeError("geo_reference must be ingested first (needs commune list)")
        t = pq.read_table(path, columns=["commune_code", "department_code"])
        t = t.filter(in_departments(t["department_code"], self.settings.departments))
        return sorted(pc.unique(t["commune_code"]).to_pylist())

    def extract(self) -> Iterator[tuple[str, str]]:
        yield "risques.jsonl", API

    def fetch(self, name: str, url: str) -> RawFile:
        if cached := self.cached(name):
            return cached
        dest = self.raw_dir / name
        dest.parent.mkdir(parents=True, exist_ok=True)
        codes = self.commune_codes()
        part = dest.with_suffix(".part")
        with part.open("w", encoding="utf-8") as fh:
            for i in range(0, len(codes), BATCH):
                chunk = ",".join(codes[i : i + BATCH])
                page = get_json(f"{url}?code_insee={chunk}&page=1&page_size=100", self.settings.http_timeout)
                for row in page["data"]:
                    fh.write(json.dumps(row, ensure_ascii=False) + "\n")
        part.replace(dest)
        sha = hashlib.sha256(dest.read_bytes()).hexdigest()
        return RawFile(name, url, dest, sha, dest.stat().st_size, datetime.now(UTC).isoformat())

    def normalize(self, files: list[RawFile]) -> dict[str, pa.Table]:
        rows = []
        for line in files[0].path.read_text(encoding="utf-8").splitlines():
            doc = json.loads(line)
            for r in doc.get("risques_detail", []):
                rows.append(
                    {
                        "commune_code": doc["code_insee"],
                        "department_code": _dept(doc["code_insee"]),
                        "commune_name": doc.get("libelle_commune"),
                        "risk_code": r.get("num_risque"),
                        "risk_label": r.get("libelle_risque_long"),
                        "seismic_zone": r.get("zone_sismicite"),
                    }
                )
        schema = pa.schema(
            [
                ("commune_code", pa.string()),
                ("department_code", pa.string()),
                ("commune_name", pa.string()),
                ("risk_code", pa.string()),
                ("risk_label", pa.string()),
                ("seismic_zone", pa.string()),
            ]
        )
        return {"commune_risks": pa.Table.from_pylist(rows, schema=schema)}


def _dept(commune_code: str) -> str:
    return commune_code[:3] if commune_code.startswith("97") else commune_code[:2]
