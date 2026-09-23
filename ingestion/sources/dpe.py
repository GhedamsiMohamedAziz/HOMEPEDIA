"""10 — DPE (energy performance certificates) for existing dwellings, ADEME data-fair API.
Paged JSON (10 000 rows/page, cursor in `next`) filtered by department and date, stored as JSONL."""

from __future__ import annotations

import hashlib
import json
from collections.abc import Iterator
from datetime import UTC, datetime
from urllib.parse import quote

import pyarrow as pa

from ingestion.connectors.base import RawFile, SourceConnector
from ingestion.connectors.http import get_json
from ingestion.connectors.tables import project

API = "https://data.ademe.fr/data-fair/api/v1/datasets/meg-83tjwtg8dyz4vv7h1dqe/lines"
FIELDS = [
    "numero_dpe",
    "date_etablissement_dpe",
    "etiquette_dpe",
    "etiquette_ges",
    "type_batiment",
    "periode_construction",
    "surface_habitable_logement",
    "conso_5_usages_par_m2_ep",
    "emission_ges_5_usages_par_m2",
    "code_insee_ban",
    "code_departement_ban",
    "code_postal_ban",
]
COLUMNS: dict[str, tuple[str, pa.DataType]] = {
    "numero_dpe": ("dpe_id", pa.string()),
    "date_etablissement_dpe": ("date", pa.date32()),
    "etiquette_dpe": ("energy_label", pa.string()),
    "etiquette_ges": ("ghg_label", pa.string()),
    "type_batiment": ("building_type", pa.string()),
    "periode_construction": ("construction_period", pa.string()),
    "surface_habitable_logement": ("surface", pa.float64()),
    "conso_5_usages_par_m2_ep": ("energy_kwh_m2", pa.float64()),
    "emission_ges_5_usages_par_m2": ("ghg_kg_m2", pa.float64()),
    "code_insee_ban": ("commune_code", pa.string()),
    "code_departement_ban": ("department_code", pa.string()),
    "code_postal_ban": ("postal_code", pa.string()),
}


class DpeConnector(SourceConnector):
    name = "dpe"
    not_null = {"certificates": ("dpe_id", "date", "commune_code", "department_code")}
    partition_by = {"certificates": ("department_code",)}

    def extract(self) -> Iterator[tuple[str, str]]:
        for dept in self.settings.departments:
            qs = quote(
                f"code_departement_ban:{dept} AND date_etablissement_dpe:[{self.settings.dpe_since} TO *]"
            )
            yield f"dpe_{dept}.jsonl", f"{API}?size=10000&qs={qs}&select={','.join(FIELDS)}&sort=numero_dpe"

    def fetch(self, name: str, url: str) -> RawFile:
        if cached := self.cached(name):
            return cached
        dest = self.raw_dir / name
        dest.parent.mkdir(parents=True, exist_ok=True)
        part = dest.with_suffix(".part")
        next_url: str | None = url
        with part.open("w", encoding="utf-8") as fh:
            while next_url:
                page = get_json(next_url, self.settings.http_timeout)
                for row in page["results"]:
                    fh.write(json.dumps(row, ensure_ascii=False) + "\n")
                next_url = page.get("next") if page["results"] else None
        part.replace(dest)
        sha = hashlib.sha256(dest.read_bytes()).hexdigest()
        return RawFile(name, url, dest, sha, dest.stat().st_size, datetime.now(UTC).isoformat())

    def normalize(self, files: list[RawFile]) -> dict[str, pa.Table]:
        rows = [json.loads(line) for f in files for line in f.path.read_text(encoding="utf-8").splitlines()]
        table = pa.Table.from_pylist([{k: r.get(k) for k in FIELDS} for r in rows])
        for col in ("surface_habitable_logement", "conso_5_usages_par_m2_ep", "emission_ges_5_usages_par_m2"):
            if col in table.column_names and pa.types.is_integer(table[col].type):
                table = table.set_column(table.column_names.index(col), col, table[col].cast(pa.float64()))
        return {"certificates": project(table, COLUMNS)}
