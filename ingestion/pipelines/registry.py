"""All connectors, in dependency order (georisques needs geo_reference's commune list)."""

from __future__ import annotations

from ingestion.connectors.base import SourceConnector
from ingestion.sources.dpe import DpeConnector
from ingestion.sources.dvf import DvfConnector
from ingestion.sources.education import EducationConnector
from ingestion.sources.geo_reference import GeoReferenceConnector
from ingestion.sources.georisques import GeorisquesConnector
from ingestion.sources.grand_debat import GrandDebatConnector
from ingestion.sources.insee import InseeEmploiConnector, InseeFilosofiConnector
from ingestion.sources.insee_population import InseePopulationConnector
from ingestion.sources.transport import SncfStationsConnector

CONNECTORS: dict[str, type[SourceConnector]] = {
    c.name: c
    for c in (
        GeoReferenceConnector,
        DvfConnector,
        InseeFilosofiConnector,
        InseeEmploiConnector,
        InseePopulationConnector,
        DpeConnector,
        GeorisquesConnector,
        SncfStationsConnector,
        EducationConnector,
        GrandDebatConnector,
    )
}
