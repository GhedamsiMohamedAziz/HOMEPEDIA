"""Analytics layer against the loaded database (`make test-integration`)."""

from __future__ import annotations

import pytest

from analytics.demographics.kpis import population_growth
from analytics.environment.kpis import climate_exposure, energy_distribution
from analytics.geography.kpis import housing_density, transport_accessibility
from analytics.housing.kpis import (
    housing_kpis,
    price_evolution,
    property_type_distribution,
    surface_distribution,
)
from analytics.insights import explain, generate_insights
from analytics.profile import compare, get_profile
from analytics.repository import geometries, search_territories

pytestmark = pytest.mark.integration
BORDEAUX = "33063"


def test_housing_kpis_and_distributions() -> None:
    k = housing_kpis("commune", BORDEAUX)
    assert k is not None and k.transactions > 1000 and k.median_price_m2 and 2000 < k.median_price_m2 < 8000
    dist = property_type_distribution("commune", BORDEAUX)
    assert set(dist["property_type"]) == {"Maison", "Appartement"} and abs(dist["share"].sum() - 1) < 1e-6
    surf = surface_distribution("department", "33")
    assert len(surf) == 4 and abs(surf["share"].sum() - 1) < 1e-6
    evo = price_evolution("department", "33", "year")
    assert evo["period"].tolist() == ["2023", "2024"] and evo["growth"].iloc[1] < 0
    assert evo["index"].iloc[0] == 100.0


def test_population_density_transport_environment() -> None:
    pop = population_growth("commune", BORDEAUX)
    assert pop["year"].iloc[0] == 1968 and pop["annual_growth"].iloc[-1] is not None
    dens = housing_density("commune", BORDEAUX)
    assert dens.population_density_km2 and dens.population_density_km2 > 4000 and dens.vacancy_rate
    acc = transport_accessibility("commune", BORDEAUX)
    assert acc.rail_stations >= 1 and acc.score == 1.0 and acc.nearest_station_km is not None
    e = energy_distribution("commune", BORDEAUX)
    assert e["label"].tolist() == list("ABCDEFG") and e["n"].sum() > 1000
    assert 0 < climate_exposure("department", "33").exposure_index <= 1


def test_profile_compare_insights() -> None:
    p = get_profile("commune", BORDEAUX)
    assert p.name == "Bordeaux" and p["population"] > 200000 and p.sections["housing"]["median_price_m2"]
    table = compare("commune", [BORDEAUX, "31555", "44109"])
    assert table.shape[1] == 3 and "Median price (€/m²)" in table.index
    ins = generate_insights("commune", BORDEAUX)
    assert ins and all(i.reference_name == "Gironde" for i in ins)
    assert "Bordeaux" in explain(ins)


def test_geometries_and_search() -> None:
    fc = geometries("commune", department="33")
    assert fc["type"] == "FeatureCollection" and len(fc["features"]) > 500
    assert fc["features"][0]["geometry"]["type"] in ("Polygon", "MultiPolygon")
    hits = search_territories("bordeaux")
    assert BORDEAUX in set(hits["territory_code"])
