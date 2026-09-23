"""Insights layer (section 23): deterministic comparisons to the parent territory -> structured insights.

No language model is involved in computing anything; `explain()` only renders the structured insight as text,
and a future LLM explanation would consume these Insight objects, never raw data.
"""

from __future__ import annotations

from dataclasses import dataclass

from analytics.profile import TerritoryProfile, get_profile
from analytics.repository import PARENT, Level

# metric -> (label, unit, higher_is: 'better' | 'worse' | 'neutral')
METRICS: dict[str, tuple[str, str, str]] = {
    "median_price_m2": ("median price per m²", "€/m²", "neutral"),
    "median_income": ("median income", "€/yr", "better"),
    "poverty_rate": ("poverty rate", "%", "worse"),
    "unemployment_rate": ("unemployment rate", "", "worse"),
    "population_annual_growth": ("annual population growth", "", "better"),
    "vacancy_rate": ("housing vacancy rate", "", "worse"),
    "dpe_share_fg": ("share of F-G energy labels", "", "worse"),
    "density_km2": ("population density", "inh./km²", "neutral"),
    "risk_count": ("number of natural/technological risks", "", "worse"),
}
MATERIAL_DELTA = 0.10  # piggy: 10 % relative gap = worth reporting; make per-metric if users disagree


@dataclass(frozen=True)
class Insight:
    metric: str
    label: str
    value: float
    reference_value: float
    reference_name: str
    delta_pct: float  # relative gap vs reference
    direction: str  # 'higher' | 'lower'
    assessment: str  # 'favourable' | 'unfavourable' | 'neutral'
    text: str


def _fmt(v: float, unit: str) -> str:
    if unit == "":
        return f"{v * 100:.1f} %" if abs(v) < 1.5 else f"{v:,.0f}"
    if unit == "%":
        return f"{v:.1f} %"
    return f"{v:,.0f} {unit}"


def compare_profiles(profile: TerritoryProfile, reference: TerritoryProfile) -> list[Insight]:
    out: list[Insight] = []
    for metric, (label, unit, higher_is) in METRICS.items():
        try:
            v, r = profile[metric], reference[metric]
        except KeyError:
            continue
        if v is None or r is None or r == 0:
            continue
        delta = round((float(v) - float(r)) / abs(float(r)), 4)
        if abs(delta) < MATERIAL_DELTA:
            continue
        direction = "higher" if delta > 0 else "lower"
        if higher_is == "neutral":
            assessment = "neutral"
        else:
            assessment = "favourable" if (delta > 0) == (higher_is == "better") else "unfavourable"
        text = (
            f"{profile.name}: {label} is {_fmt(float(v), unit)}, {abs(delta) * 100:.0f} % {direction} than "
            f"{reference.name} ({_fmt(float(r), unit)})."
        )
        out.append(
            Insight(metric, label, float(v), float(r), reference.name, delta, direction, assessment, text)
        )
    return sorted(out, key=lambda i: -abs(i.delta_pct))


def generate_insights(level: Level, code: str) -> list[Insight]:
    """Insights for a territory against its parent (commune -> department, department -> region)."""
    profile = get_profile(level, code)
    parent_level = PARENT[level]
    if parent_level is None:
        return []
    parent_code = profile.sections["identity"][f"{parent_level}_code"]
    return compare_profiles(profile, get_profile(parent_level, parent_code))


def explain(insights: list[Insight], limit: int = 5) -> str:
    """Plain-text rendering of computed insights (the optional LLM step would start from this list)."""
    return (
        "\n".join(i.text for i in insights[:limit]) or "No material difference with the reference territory."
    )
