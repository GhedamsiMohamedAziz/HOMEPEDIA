"""09 / 14 — INSEE Filosofi 2021 (income, poverty) and RP 2022 employment, commune level.
Published by INSEE on data.gouv.fr in the long 'DS_' format; kept long in BRONZE."""

from __future__ import annotations

from collections.abc import Iterator

import pyarrow as pa
import pyarrow.compute as pc

from ingestion.connectors.base import RawFile, SourceConnector
from ingestion.connectors.tables import commune_department, in_departments, project, read_csv

DATASET = (
    "https://static.data.gouv.fr/resources/principaux-indicateurs-sur-la-pauvrete-en-2021-niveau-de-vie-"
    "taux-de-pauvrete-part-des-menages-imposes-et-decomposition-du-revenu-disponible-1"
)
FILOSOFI_URL = f"{DATASET}/20260415-150720/ds-filosofi-cc-2021-data.csv"
EMPLOI_URL = f"{DATASET}/20260415-151924/ds-rp-emploi-lr-comp-2022-data.csv"


class _InseeLong(SourceConnector):
    url: str
    file: str
    table: str
    measure_col: str
    extra: dict[str, tuple[str, pa.DataType]] = {}

    def extract(self) -> Iterator[tuple[str, str]]:
        yield self.file, self.url

    def normalize(self, files: list[RawFile]) -> dict[str, pa.Table]:
        raw = read_csv(files[0].path, delimiter=";")
        raw = raw.filter(pc.equal(raw["GEO_OBJECT"], "COM"))
        raw = raw.filter(in_departments(commune_department(raw["GEO"]), self.settings.departments))
        mapping: dict[str, tuple[str, pa.DataType]] = {
            "GEO": ("commune_code", pa.string()),
            self.measure_col: ("measure", pa.string()),
            **self.extra,
            "TIME_PERIOD": ("year", pa.int32()),
            "OBS_VALUE": ("value", pa.float64()),
        }
        t = project(raw, mapping)
        t = t.append_column("department_code", commune_department(t["commune_code"]))
        return {self.table: t}


class InseeFilosofiConnector(_InseeLong):
    name = "insee_filosofi"
    url = FILOSOFI_URL
    file = "ds-filosofi-cc-2021-data.csv"
    table = "income"
    measure_col = "FILOSOFI_MEASURE"
    extra = {"UNIT_MEASURE": ("unit", pa.string())}
    not_null = {"income": ("commune_code", "measure", "year")}


class InseeEmploiConnector(_InseeLong):
    name = "insee_emploi"
    url = EMPLOI_URL
    file = "ds-rp-emploi-lr-comp-2022-data.csv"
    table = "employment"
    measure_col = "RP_MEASURE"
    extra = {
        "EMPSTA_ENQ": ("employment_status", pa.string()),
        "AGE": ("age_band", pa.string()),
        "PCS": ("socio_professional_category", pa.string()),
    }
    not_null = {"employment": ("commune_code", "measure", "year", "value")}
