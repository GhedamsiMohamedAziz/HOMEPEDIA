"""Geographic KPIs: housing density and transport accessibility (section 18)."""

from __future__ import annotations

from dataclasses import dataclass

from analytics.repository import Level, nearest_station_km, territory_profile


@dataclass(frozen=True)
class HousingDensity:
    population_density_km2: float | None
    dwellings_per_km2: float | None
    dwellings_per_1000_inhabitants: float | None
    vacancy_rate: float | None


def calculate_housing_density(
    population: float | None, dwellings: float | None, area_km2: float | None, vacant: float | None = None
) -> HousingDensity:
    def div(a: float | None, b: float | None, scale: float = 1.0, nd: int = 1) -> float | None:
        return None if a is None or b is None or b <= 0 else round(a / b * scale, nd)

    return HousingDensity(
        div(population, area_km2),
        div(dwellings, area_km2),
        div(dwellings, population, 1000),
        div(vacant, dwellings, 1, 4),
    )


@dataclass(frozen=True)
class TransportAccessibility:
    rail_stations: int
    stations_per_100k: float | None
    nearest_station_km: float | None
    score: float  # 0 (no access) .. 1 (station in the commune)


def calculate_transport_accessibility(
    rail_stations: int, population: float | None, nearest_km: float | None
) -> TransportAccessibility:
    per_100k = None if not population else round(rail_stations / population * 100_000, 2)
    if rail_stations > 0:
        score = 1.0
    elif nearest_km is None:
        score = 0.0
    else:
        score = round(
            max(0.0, 1 - nearest_km / 30.0), 3
        )  # piggy: linear decay to 0 at 30 km; tune with GTFS data
    return TransportAccessibility(rail_stations, per_100k, nearest_km, score)


def housing_density(level: Level, code: str) -> HousingDensity:
    p = territory_profile(level, code)
    area = None if p.get("surface_ha") is None else float(p["surface_ha"]) / 100.0
    return calculate_housing_density(p.get("population"), p.get("dwellings"), area, p.get("vacant_dwellings"))


def transport_accessibility(level: Level, code: str) -> TransportAccessibility:
    p = territory_profile(level, code)
    nearest = nearest_station_km(code) if level == "commune" else None
    return calculate_transport_accessibility(int(p.get("rail_stations") or 0), p.get("population"), nearest)
