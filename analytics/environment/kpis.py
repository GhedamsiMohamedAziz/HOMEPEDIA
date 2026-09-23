"""Environmental and energy KPIs (section 18: calculate_energy_distribution, calculate_climate_exposure)."""

from __future__ import annotations

from dataclasses import dataclass

import pandas as pd

from analytics.repository import Level, climate_risk, energy_labels

LABELS = list("ABCDEFG")
# piggy: equal-weight exposure index over the five flagged hazards; add severity weights if needed
HAZARDS = ("has_flood", "has_ground_movement", "has_earthquake", "has_radon", "has_industrial")


def calculate_energy_distribution(counts: pd.DataFrame) -> pd.DataFrame:
    """counts: (label, n) -> every label A..G with n and share (missing labels count 0)."""
    base = pd.DataFrame({"label": LABELS}).merge(counts, on="label", how="left").fillna({"n": 0})
    base["n"] = base["n"].astype(int)
    total = int(base["n"].sum())
    base["share"] = (base["n"] / total).round(4) if total else 0.0
    return base


def energy_summary(dist: pd.DataFrame) -> dict[str, float]:
    s = dict(zip(dist["label"], dist["share"], strict=True))
    return {"share_ab": round(s["A"] + s["B"], 4), "share_fg": round(s["F"] + s["G"], 4)}


@dataclass(frozen=True)
class ClimateExposure:
    risk_count: float
    natural_risk_count: float
    technological_risk_count: float
    hazards: dict[str, float]
    exposure_index: float  # 0..1


def calculate_climate_exposure(row: pd.Series) -> ClimateExposure:
    hazards = {h.removeprefix("has_"): float(row.get(h) or 0.0) for h in HAZARDS}
    return ClimateExposure(
        float(row.get("risk_count") or 0.0),
        float(row.get("natural_risk_count") or 0.0),
        float(row.get("technological_risk_count") or 0.0),
        hazards,
        round(sum(hazards.values()) / len(hazards), 4),
    )


def energy_distribution(level: Level, code: str) -> pd.DataFrame:
    return calculate_energy_distribution(energy_labels(level, code))


def climate_exposure(level: Level, code: str) -> ClimateExposure:
    return calculate_climate_exposure(climate_risk(level, code))
