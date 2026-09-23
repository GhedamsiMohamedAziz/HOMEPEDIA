"""13 — Education: annuaire de l'éducation (schools directory), filtered by department server-side."""

from __future__ import annotations

from collections.abc import Iterator
from urllib.parse import quote

import pyarrow as pa

from ingestion.connectors.base import RawFile, SourceConnector
from ingestion.connectors.tables import project, read_csv, strip_leading_zero_dept

API = "https://data.education.gouv.fr/api/explore/v2.1/catalog/datasets/fr-en-annuaire-education/exports/csv"
COLUMNS: dict[str, tuple[str, pa.DataType]] = {
    "identifiant_de_l_etablissement": ("school_id", pa.string()),
    "nom_etablissement": ("name", pa.string()),
    "type_etablissement": ("school_type", pa.string()),
    "statut_public_prive": ("status", pa.string()),
    "code_commune": ("commune_code", pa.string()),
    "code_departement": ("department_code", pa.string()),
    "code_region": ("region_code", pa.string()),
    "ecole_maternelle": ("has_kindergarten", pa.string()),
    "ecole_elementaire": ("has_elementary", pa.string()),
    "voie_generale": ("has_general_track", pa.string()),
    "voie_technologique": ("has_technological_track", pa.string()),
    "voie_professionnelle": ("has_vocational_track", pa.string()),
    "appartenance_education_prioritaire": ("priority_education", pa.string()),
    "etat": ("state", pa.string()),
    "latitude": ("latitude", pa.float64()),
    "longitude": ("longitude", pa.float64()),
}


class EducationConnector(SourceConnector):
    name = "education"
    not_null = {"schools": ("school_id", "name", "commune_code", "department_code")}
    partition_by = {"schools": ("department_code",)}

    def extract(self) -> Iterator[tuple[str, str]]:
        # the source pads department codes to 3 chars ("033")
        codes = ",".join(f'"{d.zfill(3)}"' for d in self.settings.departments)
        where = quote(f"code_departement in ({codes})")
        yield "annuaire-education.csv", f"{API}?delimiter=%3B&where={where}"

    def normalize(self, files: list[RawFile]) -> dict[str, pa.Table]:
        raw = read_csv(files[0].path, delimiter=";", columns=list(COLUMNS))
        t = project(raw, COLUMNS)
        idx = t.column_names.index("department_code")
        return {
            "schools": t.set_column(idx, "department_code", strip_leading_zero_dept(t["department_code"]))
        }
