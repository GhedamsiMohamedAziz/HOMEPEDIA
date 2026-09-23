"""Housing KPIs (section 14) and temporal analytics (section 15). Pure functions + thin DB wrappers."""

from __future__ import annotations

from dataclasses import dataclass

import pandas as pd

from analytics.repository import Level, housing_market, price_series

SURFACE_BANDS = {
    "n_surface_lt_40": "< 40 m²",
    "n_surface_40_80": "40-80 m²",
    "n_surface_80_120": "80-120 m²",
    "n_surface_ge_120": "≥ 120 m²",
}


@dataclass(frozen=True)
class HousingKpis:
    year: int
    property_type: str
    transactions: int
    median_price_m2: float | None
    avg_price_m2: float | None
    p25_price_m2: float | None
    p75_price_m2: float | None
    median_price: float | None
    median_surface: float | None


def calculate_price_evolution(
    series: pd.DataFrame, value: str = "median_price_m2", lag: int = 1
) -> pd.DataFrame:
    """Add `growth` (vs `lag` periods earlier) and `index` (first period = 100) to an ordered series."""
    out = series.sort_values("period").reset_index(drop=True).copy()
    out["growth"] = (out[value] / out[value].shift(lag) - 1).round(4)
    base = out[value].dropna()
    out["index"] = (out[value] / base.iloc[0] * 100).round(1) if not base.empty else None
    return out


def calculate_cagr(first: float, last: float, years: float) -> float | None:
    """Compound annual growth rate; None when undefined."""
    if years <= 0 or first is None or last is None or first <= 0 or last <= 0:
        return None
    ratio: float = float(last) / float(first)
    return round(float(ratio ** (1.0 / years)) - 1, 5)


def housing_kpis(
    level: Level, code: str, year: int | None = None, property_type: str = "all"
) -> HousingKpis | None:
    df = housing_market(level, code)
    df = df[df["property_type"] == property_type]
    if year is not None:
        df = df[df["year"] == year]
    if df.empty:
        return None
    r = df.sort_values("year").iloc[-1]
    return HousingKpis(
        int(r["year"]),
        str(r["property_type"]),
        int(r["transactions"]),
        r["median_price_m2"],
        r["avg_price_m2"],
        r["p25_price_m2"],
        r["p75_price_m2"],
        r["median_price"],
        r["median_surface"],
    )


def property_type_distribution(level: Level, code: str, year: int | None = None) -> pd.DataFrame:
    df = housing_market(level, code)
    df = df[df["property_type"] != "all"]
    if year is None and not df.empty:
        year = int(df["year"].max())
    df = df[df["year"] == year][["property_type", "transactions", "median_price_m2"]].copy()
    df["share"] = (df["transactions"] / df["transactions"].sum()).round(4) if not df.empty else None
    return df.reset_index(drop=True)


def surface_distribution(
    level: Level, code: str, year: int | None = None, property_type: str = "all"
) -> pd.DataFrame:
    df = housing_market(level, code)
    df = df[df["property_type"] == property_type]
    if df.empty:
        return pd.DataFrame(columns=["band", "transactions", "share"])
    if year is None:
        year = int(df["year"].max())
    r = df[df["year"] == year].iloc[0]
    rows = [(label, int(r[col])) for col, label in SURFACE_BANDS.items()]
    out = pd.DataFrame(rows, columns=["band", "transactions"])
    out["share"] = (out["transactions"] / out["transactions"].sum()).round(4)
    return out


def price_evolution(level: Level, code: str, granularity: str = "year") -> pd.DataFrame:
    if granularity not in ("month", "quarter", "year"):
        raise ValueError("granularity must be month, quarter or year")
    return calculate_price_evolution(price_series(level, code, granularity))
