"""Demographic KPIs: census series growth (section 18: calculate_population_growth)."""

from __future__ import annotations

import pandas as pd

from analytics.housing.kpis import calculate_cagr
from analytics.repository import Level, population_series


def calculate_population_growth(series: pd.DataFrame) -> pd.DataFrame:
    """For an ordered (year, population) series add growth vs previous census and annualised growth."""
    out = series.sort_values("year").reset_index(drop=True).copy()
    out["prev_year"] = out["year"].shift(1)
    out["prev_population"] = out["population"].shift(1)
    out["growth"] = (out["population"] / out["prev_population"] - 1).round(4)
    out["annual_growth"] = [
        calculate_cagr(p, c, y - py) if pd.notna(py) else None
        for p, c, y, py in zip(
            out["prev_population"], out["population"], out["year"], out["prev_year"], strict=True
        )
    ]
    if "dwellings" in out and "vacant_dwellings" in out:
        out["vacancy_rate"] = (out["vacant_dwellings"] / out["dwellings"]).round(4)
    return out


def population_growth(level: Level, code: str) -> pd.DataFrame:
    return calculate_population_growth(population_series(level, code))
