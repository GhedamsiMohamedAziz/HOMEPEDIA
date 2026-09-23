"""15 — Textual source: Grand Débat National local-meeting reports (2019, Licence Ouverte).
Free text about local issues with postal code / department / town. Processed by the NLP pipeline
in Phase 6; matched to commune codes via postal code + town name in Phase 3."""

from __future__ import annotations

from collections.abc import Iterator

import pyarrow as pa
import pyarrow.compute as pc

from ingestion.connectors.base import RawFile, SourceConnector
from ingestion.connectors.tables import in_departments, project, read_csv

URL = "https://static.data.gouv.fr/resources/donnees-ouvertes-du-grand-debat-national/20200611-235901/cr-ril.csv"
COLUMNS: dict[str, tuple[str, pa.DataType]] = {
    "Création": ("created_at", pa.string()),
    "Titre": ("title", pa.string()),
    "Sur quel s theme s votre reunion a t elle porte": ("themes", pa.string()),
    "Comment la reunion sest elle passee": ("text_atmosphere", pa.string()),
    "Quels ont ete les constats ou les diagnostics exprimes sur les themes du debat": (
        "text_diagnostics",
        pa.string(),
    ),
    "Quelles sont les propositions qui ont emerge des discussions": ("text_proposals", pa.string()),
    "Combien de participants etaient presents": ("participants", pa.string()),
    "Code postal": ("postal_code", pa.string()),
    "Code département": ("department_code", pa.string()),
    "Ville": ("town", pa.string()),
    "Région": ("region_name", pa.string()),
}


class GrandDebatConnector(SourceConnector):
    name = "grand_debat"
    not_null = {"meeting_reports": ("created_at", "department_code", "text")}
    partition_by = {"meeting_reports": ("department_code",)}

    def extract(self) -> Iterator[tuple[str, str]]:
        yield "cr-ril.csv", URL

    def normalize(self, files: list[RawFile]) -> dict[str, pa.Table]:
        raw = read_csv(files[0].path, columns=list(COLUMNS))
        t = project(raw, COLUMNS)
        t = t.filter(in_departments(t["department_code"], self.settings.departments))
        text = pc.binary_join_element_wise(
            pc.fill_null(t["text_diagnostics"], ""), pc.fill_null(t["text_proposals"], ""), "\n"
        )
        text = pc.if_else(pc.equal(pc.utf8_trim_whitespace(text), ""), None, text)
        return {"meeting_reports": t.append_column("text", text)}
