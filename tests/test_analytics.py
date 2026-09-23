"""Pure analytics functions (no database)."""

from __future__ import annotations

import pandas as pd

from analytics.demographics.kpis import calculate_population_growth
from analytics.environment.kpis import (
    calculate_climate_exposure,
    calculate_energy_distribution,
    energy_summary,
)
from analytics.geography.kpis import calculate_housing_density, calculate_transport_accessibility
from analytics.housing.kpis import calculate_cagr, calculate_price_evolution
from analytics.insights import Insight, compare_profiles, explain
from analytics.profile import TerritoryProfile
from analytics.repository import Level


def test_price_evolution_growth_and_index() -> None:
    s = pd.DataFrame({"period": ["2022", "2023", "2024"], "median_price_m2": [3000.0, 3300.0, 3135.0]})
    out = calculate_price_evolution(s)
    assert out["growth"].tolist()[1:] == [0.1, -0.05]
    assert out["index"].tolist() == [100.0, 110.0, 104.5]


def test_cagr() -> None:
    assert calculate_cagr(100, 121, 2) == 0.1
    assert calculate_cagr(100, 121, 0) is None
    assert calculate_cagr(0, 5, 1) is None


def test_population_growth() -> None:
    s = pd.DataFrame(
        {
            "year": [2012, 2017, 2023],
            "population": [100.0, 110.0, 121.0],
            "dwellings": [50, 55, 60],
            "vacant_dwellings": [5, 5, 6],
        }
    )
    out = calculate_population_growth(s)
    assert out["growth"].round(2).tolist()[1:] == [0.1, 0.1]
    assert round(out["annual_growth"][1], 4) == round(1.1 ** (1 / 5) - 1, 4)
    assert out["vacancy_rate"].tolist() == [0.1, 0.0909, 0.1]


def test_energy_distribution_fills_missing_labels() -> None:
    dist = calculate_energy_distribution(pd.DataFrame({"label": ["A", "C", "G"], "n": [1, 6, 3]}))
    assert dist["label"].tolist() == list("ABCDEFG")
    assert dist["n"].sum() == 10 and dist.loc[dist["label"] == "B", "n"].item() == 0
    assert energy_summary(dist) == {"share_ab": 0.1, "share_fg": 0.3}


def test_climate_exposure_index() -> None:
    row = pd.Series(
        {
            "risk_count": 3,
            "natural_risk_count": 2,
            "technological_risk_count": 1,
            "has_flood": 1,
            "has_radon": 1,
        }
    )
    ex = calculate_climate_exposure(row)
    assert ex.exposure_index == 0.4 and ex.hazards["earthquake"] == 0.0


def test_density_and_accessibility() -> None:
    d = calculate_housing_density(population=20000, dwellings=10000, area_km2=50, vacant=800)
    assert (
        d.population_density_km2,
        d.dwellings_per_km2,
        d.dwellings_per_1000_inhabitants,
        d.vacancy_rate,
    ) == (400.0, 200.0, 500.0, 0.08)
    assert calculate_housing_density(None, None, None).population_density_km2 is None
    assert calculate_transport_accessibility(2, 50000, None).score == 1.0
    a = calculate_transport_accessibility(0, 1000, 15.0)
    assert a.score == 0.5 and a.stations_per_100k == 0.0
    assert calculate_transport_accessibility(0, 1000, None).score == 0.0


def _profile(name: str, level: Level, **metrics: float) -> TerritoryProfile:
    return TerritoryProfile(
        level, name[:2], name, {"economy": dict(metrics), "identity": {"department_code": "33"}}
    )


def test_insights_are_material_directional_and_sorted() -> None:
    bordeaux = _profile(
        "Bordeaux", "commune", median_income=24870, poverty_rate=17.0, unemployment_rate=0.111
    )
    gironde = _profile(
        "Gironde", "department", median_income=24089, poverty_rate=13.5, unemployment_rate=0.101
    )
    ins = compare_profiles(bordeaux, gironde)
    assert [i.metric for i in ins] == [
        "poverty_rate"
    ]  # income +3.2 % and unemployment +9.9 % are below the 10 % threshold
    i = ins[0]
    assert isinstance(i, Insight) and i.direction == "higher" and i.assessment == "unfavourable"
    assert i.delta_pct == 0.2593 and "Gironde" in i.text
    assert explain([]).startswith("No material")
