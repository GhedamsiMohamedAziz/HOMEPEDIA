"""Read-only access to the serving database for the analytics layer. All SQL lives here."""

# ruff: noqa: E501 — SQL strings read better unwrapped

from __future__ import annotations

from typing import Any, Literal

import pandas as pd

from homepedia.db.postgres import connect
from homepedia.settings import Settings, get_settings

Level = Literal["commune", "department", "region"]
LEVELS: tuple[Level, ...] = ("commune", "department", "region")
PARENT: dict[Level, Level | None] = {"commune": "department", "department": "region", "region": None}


def fetch(sql: str, params: tuple[Any, ...] = (), settings: Settings | None = None) -> pd.DataFrame:
    with connect(settings or get_settings()) as conn, conn.cursor() as cur:
        cur.execute(sql, params)
        cols = [d.name for d in cur.description or []]
        return pd.DataFrame(cur.fetchall(), columns=cols)


def check_level(level: str) -> Level:
    if level not in LEVELS:
        raise ValueError(f"level must be one of {LEVELS}, got {level!r}")
    return level


def territory_profile(level: Level, code: str) -> pd.Series:
    df = fetch(
        "SELECT * FROM analytics.territory_profile WHERE level = %s AND territory_code = %s", (level, code)
    )
    if df.empty:
        raise KeyError(f"unknown territory {level}:{code}")
    return df.iloc[0]


def territory_profiles(
    level: Level, codes: list[str] | None = None, department: str | None = None
) -> pd.DataFrame:
    sql = "SELECT * FROM analytics.territory_profile WHERE level = %s"
    params: list[Any] = [level]
    if codes:
        sql += " AND territory_code = ANY(%s)"
        params.append(codes)
    if department:
        sql += " AND department_code = %s"
        params.append(department)
    return fetch(sql + " ORDER BY territory_code", tuple(params))


def housing_market(level: Level, code: str) -> pd.DataFrame:
    return fetch(
        "SELECT * FROM analytics.housing_market WHERE level = %s AND territory_code = %s ORDER BY year, property_type",
        (level, code),
    )


def price_series(level: Level, code: str, granularity: str) -> pd.DataFrame:
    return fetch(
        "SELECT period, year, transactions, median_price_m2, yoy_growth, growth_3y, growth_5y, transactions_yoy_growth"
        " FROM analytics.housing_price_evolution WHERE level = %s AND territory_code = %s AND granularity = %s"
        " ORDER BY period",
        (level, code, granularity),
    )


def population_series(level: Level, code: str) -> pd.DataFrame:
    return fetch(
        "SELECT year, population, dwellings, main_dwellings, secondary_dwellings, vacant_dwellings, births, deaths"
        " FROM analytics.population_evolution WHERE level = %s AND territory_code = %s ORDER BY year",
        (level, code),
    )


def energy_labels(level: Level, code: str) -> pd.DataFrame:
    col = {"commune": "commune_code", "department": "department_code", "region": "region_code"}[level]
    return fetch(
        f"SELECT energy_label AS label, count(*) AS n FROM energy.dpe_certificate WHERE {col} = %s"  # noqa: S608 — fixed column map
        " GROUP BY energy_label ORDER BY energy_label",
        (code,),
    )


def climate_risk(level: Level, code: str) -> pd.Series:
    df = fetch("SELECT * FROM analytics.climate_risk WHERE level = %s AND territory_code = %s", (level, code))
    if df.empty:
        raise KeyError(f"unknown territory {level}:{code}")
    return df.iloc[0]


def nearest_station_km(commune_code: str) -> float | None:
    """Distance from the commune centroid to the closest passenger rail station (PostGIS geography)."""
    df = fetch(
        "SELECT ST_Distance(ST_SetSRID(ST_MakePoint(c.longitude, c.latitude), 4326)::geography, s.geom::geography)"
        " / 1000.0 AS km FROM reference.dim_commune c, amenities.rail_station s"
        " WHERE c.commune_code = %s AND c.latitude IS NOT NULL ORDER BY km LIMIT 1",
        (commune_code,),
    )
    return None if df.empty else float(df.iloc[0]["km"])


def geometries(level: Level, codes: list[str] | None = None, department: str | None = None) -> dict[str, Any]:
    """GeoJSON FeatureCollection for the map layer (simplified 100 m contours)."""
    sql = (
        "SELECT territory_code, name, ST_AsGeoJSON(geometry, 5)::json AS geom FROM analytics.v_territory_geometry"
        " WHERE level = %s AND geometry IS NOT NULL"
    )
    params: list[Any] = [level]
    if codes:
        sql += " AND territory_code = ANY(%s)"
        params.append(codes)
    if department and level == "commune":
        sql += " AND territory_code IN (SELECT commune_code FROM reference.dim_commune WHERE department_code = %s)"
        params.append(department)
    df = fetch(sql, tuple(params))
    return {
        "type": "FeatureCollection",
        "features": [
            {"type": "Feature", "id": r.territory_code, "properties": {"name": r.name}, "geometry": r.geom}
            for r in df.itertuples()
        ],
    }


def search_territories(query: str, limit: int = 20) -> pd.DataFrame:
    return fetch(
        "SELECT level, territory_code, name, department_code FROM analytics.territory_profile"
        " WHERE name ILIKE %s OR territory_code LIKE %s ORDER BY level DESC, population DESC NULLS LAST LIMIT %s",
        (f"%{query}%", f"{query}%", limit),
    )
