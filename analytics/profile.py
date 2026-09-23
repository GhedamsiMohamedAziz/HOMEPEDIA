"""TerritoryProfile (section 16) and comparison mode (section 22)."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

import pandas as pd

from analytics.repository import Level, territory_profile, territory_profiles

SECTIONS: dict[str, tuple[str, ...]] = {
    "identity": (
        "name",
        "department_code",
        "region_code",
        "communes",
        "population",
        "surface_ha",
        "density_km2",
    ),
    "housing": (
        "housing_year",
        "transactions",
        "median_price_m2",
        "median_price",
        "apartment_share",
        "dwellings",
        "main_dwellings",
        "vacant_dwellings",
        "vacancy_rate",
        "dwellings_per_1000",
        "transactions_per_1000_dwellings",
    ),
    "demographics": ("census_year", "population_annual_growth"),
    "economy": (
        "median_income",
        "poverty_rate",
        "interdecile_ratio",
        "income_year",
        "active",
        "employed",
        "unemployed",
        "unemployment_rate",
        "employment_year",
    ),
    "education": ("schools", "primary_schools", "middle_schools", "high_schools", "public_schools"),
    "transport": ("rail_stations",),
    "environment": ("risk_count", "natural_risk_count", "has_flood"),
    "energy": ("dpe_count", "dpe_share_ab", "dpe_share_fg", "avg_energy_kwh_m2"),
    "text": ("text_reports",),
}

COMPARISON_METRICS: dict[str, str] = {
    "population": "Population",
    "density_km2": "Density (inh./km²)",
    "population_annual_growth": "Population growth (annual)",
    "median_price_m2": "Median price (€/m²)",
    "median_price": "Median price (€)",
    "transactions": "Transactions (latest year)",
    "apartment_share": "Apartment share",
    "vacancy_rate": "Vacancy rate",
    "median_income": "Median income (€/yr)",
    "poverty_rate": "Poverty rate (%)",
    "unemployment_rate": "Unemployment rate",
    "schools": "Schools",
    "rail_stations": "Rail stations",
    "risk_count": "Risks (count)",
    "dpe_share_fg": "DPE F-G share",
    "avg_energy_kwh_m2": "Energy use (kWh/m²/yr)",
}


@dataclass(frozen=True)
class TerritoryProfile:
    level: Level
    code: str
    name: str
    sections: dict[str, dict[str, Any]] = field(default_factory=dict)

    @classmethod
    def from_row(cls, row: pd.Series) -> TerritoryProfile:
        sections = {
            sec: {k: (None if pd.isna(row.get(k)) else row.get(k)) for k in cols if k in row.index}
            for sec, cols in SECTIONS.items()
        }
        return cls(row["level"], row["territory_code"], row["name"] or row["territory_code"], sections)

    def __getitem__(self, metric: str) -> Any:
        for sec in self.sections.values():
            if metric in sec:
                return sec[metric]
        raise KeyError(metric)


def get_profile(level: Level, code: str) -> TerritoryProfile:
    return TerritoryProfile.from_row(territory_profile(level, code))


def compare(level: Level, codes: list[str], metrics: list[str] | None = None) -> pd.DataFrame:
    """Metrics x territories table, traceable to analytics.territory_profile (section 22)."""
    df = territory_profiles(level, codes)
    if df.empty:
        raise KeyError(f"no territory found for {codes}")
    df = df.set_index("territory_code").reindex([c for c in codes if c in df["territory_code"].values])
    metrics = metrics or list(COMPARISON_METRICS)
    out = df[metrics].T
    out.columns = [f"{df.loc[c, 'name']} ({c})" for c in out.columns]
    out.index = [COMPARISON_METRICS.get(m, m) for m in out.index]
    return out
