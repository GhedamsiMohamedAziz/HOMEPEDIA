"""09 — INSEE historical census series (population, dwellings by occupancy, births, deaths, area) since
1968, commune level, one Parquet file from the Melodi API."""

from __future__ import annotations

from collections.abc import Iterator

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq

from ingestion.connectors.base import RawFile, SourceConnector
from ingestion.connectors.tables import commune_department, in_departments

URL = "https://api.insee.fr/melodi/file/DS_RP_SERIE_HISTORIQUE_2023/PARQUET"
MEASURES = ("POP", "DWELLINGS", "BRTH", "DEATH", "SUP")


class InseePopulationConnector(SourceConnector):
    name = "insee_population"
    not_null = {"population_series": ("commune_code", "measure", "year", "value")}

    def extract(self) -> Iterator[tuple[str, str]]:
        yield "DS_RP_SERIE_HISTORIQUE_2023.parquet", URL

    def normalize(self, files: list[RawFile]) -> dict[str, pa.Table]:
        raw = pq.read_table(files[0].path).combine_chunks()
        # dictionary-encoded columns -> plain strings so filters/casts behave
        t = pa.table(
            {
                n: pc.cast(raw[n], pa.string()) if pa.types.is_dictionary(raw[n].type) else raw[n]
                for n in raw.column_names
            }
        )
        t = t.filter(pc.and_(pc.equal(t["GEO_OBJECT"], "COM"), pc.is_in(t["RP_MEASURE"], pa.array(MEASURES))))
        t = t.combine_chunks()
        t = t.filter(in_departments(commune_department(t["GEO"]), self.settings.departments)).combine_chunks()
        out = pa.table(
            {
                "commune_code": t["GEO"],
                "measure": t["RP_MEASURE"],
                "occupancy": t["OCS"],
                "year": pc.year(t["TIME_PERIOD"]).cast(pa.int32()),
                "value": t["OBS_VALUE"],
                "unit": t["UNIT_MEASURE"],
            }
        )
        return {
            "population_series": out.append_column("department_code", commune_department(out["commune_code"]))
        }
