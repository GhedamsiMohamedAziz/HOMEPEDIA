"""SILVER/GOLD Parquet → PostgreSQL. `python -m database.load [target ...]`.

Each target: COPY the Parquet columns into a temp table, then INSERT ... ON CONFLICT DO UPDATE (rerunnable).
Geometries arrive as GeoJSON text and are converted with ST_GeomFromGeoJSON in the upsert.
"""

from __future__ import annotations

import io
import sys
import time
from dataclasses import dataclass, field
from pathlib import Path

import psycopg
import pyarrow as pa
import pyarrow.csv as pcsv
import pyarrow.parquet as pq

from homepedia.db.postgres import connect
from homepedia.ledger import record_run
from homepedia.logging import configure_logging, get_logger
from homepedia.settings import Settings, get_settings

log = get_logger("load")


@dataclass(frozen=True)
class Target:
    table: str  # schema.table
    parquet: str  # path under the lake root
    keys: tuple[str, ...]
    columns: tuple[str, ...]  # parquet columns == target columns (same names)
    geometry: str | None = None  # parquet column holding GeoJSON -> target `geometry` (MultiPolygon)
    filters: dict[str, str] = field(default_factory=dict)  # column -> value equality filter on the parquet


PROFILE_COLS = (
    "level",
    "territory_code",
    "name",
    "department_code",
    "region_code",
    "communes",
    "population",
    "density_km2",
    "surface_ha",
    "latitude",
    "longitude",
    "housing_year",
    "transactions",
    "median_price_m2",
    "median_price",
    "apartment_share",
    "median_income",
    "poverty_rate",
    "interdecile_ratio",
    "income_year",
    "active",
    "employed",
    "unemployed",
    "unemployment_rate",
    "employment_year",
    "schools",
    "primary_schools",
    "middle_schools",
    "high_schools",
    "public_schools",
    "rail_stations",
    "risk_count",
    "natural_risk_count",
    "has_flood",
    "dpe_count",
    "dpe_share_ab",
    "dpe_share_fg",
    "avg_energy_kwh_m2",
    "text_reports",
    "census_year",
    "population_annual_growth",
    "dwellings",
    "main_dwellings",
    "vacant_dwellings",
    "vacancy_rate",
    "dwellings_per_1000",
    "transactions_per_1000_dwellings",
)
MARKET_COLS = (
    "level",
    "territory_code",
    "year",
    "property_type",
    "transactions",
    "median_price_m2",
    "avg_price_m2",
    "p25_price_m2",
    "p75_price_m2",
    "median_price",
    "avg_price",
    "median_surface",
    "avg_surface",
    "avg_rooms",
    "n_surface_lt_40",
    "n_surface_40_80",
    "n_surface_80_120",
    "n_surface_ge_120",
)

TARGETS: dict[str, Target] = {
    "dim_region": Target(
        "reference.dim_region",
        "silver/regions",
        ("region_code",),
        ("region_code", "name"),
        "geometry_geojson",
    ),
    "dim_department": Target(
        "reference.dim_department",
        "silver/departments",
        ("department_code",),
        ("department_code", "region_code", "name"),
        "geometry_geojson",
    ),
    "dim_commune": Target(
        "reference.dim_commune",
        "silver/communes",
        ("commune_code",),
        (
            "commune_code",
            "department_code",
            "region_code",
            "name",
            "latitude",
            "longitude",
            "population",
            "surface_ha",
            "density_km2",
            "postal_codes",
        ),
        "geometry_geojson",
    ),
    "fact_transaction": Target(
        "housing.fact_transaction",
        "silver/transactions",
        ("mutation_id", "property_type"),
        (
            "mutation_id",
            "property_type",
            "date",
            "year",
            "month",
            "quarter",
            "commune_code",
            "department_code",
            "region_code",
            "postal_code",
            "price",
            "surface",
            "rooms",
            "units",
            "land_surface",
            "price_m2",
            "latitude",
            "longitude",
        ),
    ),
    "commune_income": Target(
        "socio.commune_income",
        "silver/income",
        ("commune_code", "year"),
        (
            "commune_code",
            "year",
            "median_income",
            "income_d1",
            "income_d9",
            "interdecile_ratio",
            "poverty_rate",
            "taxed_households_share",
            "households",
            "persons",
        ),
    ),
    "commune_employment": Target(
        "socio.commune_employment",
        "silver/employment",
        ("commune_code", "year"),
        ("commune_code", "year", "active", "employed", "unemployed", "unemployment_rate"),
    ),
    "commune_population": Target(
        "socio.commune_population",
        "silver/population",
        ("commune_code", "year"),
        (
            "commune_code",
            "year",
            "population",
            "dwellings",
            "main_dwellings",
            "secondary_dwellings",
            "vacant_dwellings",
            "vacancy_rate",
            "births",
            "deaths",
            "area_km2",
        ),
    ),
    "population_evolution": Target(
        "analytics.population_evolution",
        "gold/population_evolution",
        ("level", "territory_code", "year"),
        (
            "level",
            "territory_code",
            "year",
            "communes",
            "population",
            "prev_year",
            "prev_population",
            "growth",
            "annual_growth",
            "dwellings",
            "main_dwellings",
            "secondary_dwellings",
            "vacant_dwellings",
            "vacancy_rate",
            "births",
            "deaths",
        ),
    ),
    "dpe_certificate": Target(
        "energy.dpe_certificate",
        "silver/certificates",
        ("dpe_id",),
        (
            "dpe_id",
            "date",
            "year",
            "energy_label",
            "ghg_label",
            "building_type",
            "construction_period",
            "surface",
            "energy_kwh_m2",
            "ghg_kg_m2",
            "commune_code",
            "department_code",
            "region_code",
            "postal_code",
        ),
    ),
    "commune_risk": Target(
        "environment.commune_risk",
        "silver/commune_risks",
        ("commune_code", "risk_code"),
        ("commune_code", "risk_code", "risk_label", "risk_family", "seismic_zone"),
    ),
    "school": Target(
        "amenities.school",
        "silver/schools",
        ("school_id",),
        (
            "school_id",
            "name",
            "school_type",
            "status",
            "commune_code",
            "department_code",
            "region_code",
            "priority_education",
            "latitude",
            "longitude",
        ),
    ),
    "rail_station": Target(
        "amenities.rail_station",
        "silver/stations",
        ("station_uic",),
        ("station_uic", "name", "commune_code", "department_code", "region_code", "latitude", "longitude"),
    ),
    "housing_market_commune": Target(
        "analytics.housing_market",
        "gold/housing_market_commune",
        ("level", "territory_code", "year", "property_type"),
        MARKET_COLS,
    ),
    "housing_market_department": Target(
        "analytics.housing_market",
        "gold/housing_market_department",
        ("level", "territory_code", "year", "property_type"),
        MARKET_COLS,
    ),
    "housing_market_region": Target(
        "analytics.housing_market",
        "gold/housing_market_region",
        ("level", "territory_code", "year", "property_type"),
        MARKET_COLS,
    ),
    "housing_price_evolution": Target(
        "analytics.housing_price_evolution",
        "gold/housing_price_evolution",
        ("level", "territory_code", "granularity", "period"),
        (
            "level",
            "territory_code",
            "granularity",
            "period",
            "year",
            "transactions",
            "median_price_m2",
            "yoy_growth",
            "growth_3y",
            "growth_5y",
            "transactions_yoy_growth",
        ),
    ),
    "climate_risk": Target(
        "analytics.climate_risk",
        "gold/climate_risk",
        ("level", "territory_code"),
        (
            "level",
            "territory_code",
            "communes",
            "risk_count",
            "natural_risk_count",
            "technological_risk_count",
            "has_flood",
            "has_ground_movement",
            "has_earthquake",
            "has_radon",
            "has_industrial",
            "exposed_communes_share",
        ),
    ),
    "territory_profile": Target(
        "analytics.territory_profile", "gold/territory_profile", ("level", "territory_code"), PROFILE_COLS
    ),
}


def _pg_array(col: pa.ChunkedArray) -> pa.ChunkedArray:
    """list<string> -> Postgres text[] literal ('{a,b}'), quoted for safety."""
    out = []
    for v in col.to_pylist():
        out.append(None if v is None else "{" + ",".join('"' + s.replace('"', '\\"') + '"' for s in v) + "}")
    return pa.chunked_array([pa.array(out, pa.string())])


def read_target(root: Path, t: Target) -> pa.Table:
    cols = list(t.columns) + ([t.geometry] if t.geometry else [])
    table = pq.read_table(root / t.parquet, columns=cols)
    for name in table.column_names:
        col = table[name]
        if pa.types.is_list(col.type):
            table = table.set_column(table.column_names.index(name), name, _pg_array(col))
        elif pa.types.is_timestamp(col.type):
            table = table.set_column(table.column_names.index(name), name, col.cast(pa.string()))
    return table


def upsert(conn: psycopg.Connection[tuple[object, ...]], t: Target, table: pa.Table) -> int:
    cols = list(table.column_names)
    tmp = "_load_" + t.table.replace(".", "_")
    conn.execute(f"DROP TABLE IF EXISTS {tmp}")
    plain = ", ".join(f'"{c}"' for c in cols if c != t.geometry)
    # identifiers come from the static TARGETS registry, never from input
    conn.execute(f"CREATE TEMP TABLE {tmp} AS SELECT {plain} FROM {t.table} WITH NO DATA")  # noqa: S608
    if t.geometry:
        conn.execute(f'ALTER TABLE {tmp} ADD COLUMN "{t.geometry}" TEXT')
    buf = io.BytesIO()
    pcsv.write_csv(table, buf, pcsv.WriteOptions(include_header=False))
    buf.seek(0)
    quoted = ", ".join(f'"{c}"' for c in cols)
    with conn.cursor().copy(f"COPY {tmp} ({quoted}) FROM STDIN WITH (FORMAT csv, NULL '')") as cp:
        while chunk := buf.read(1 << 20):
            cp.write(chunk)
    insert_cols = [c for c in cols if c != t.geometry] + (["geometry"] if t.geometry else [])
    select_cols = [f'"{c}"' for c in cols if c != t.geometry] + (
        [f'ST_Multi(ST_SetSRID(ST_GeomFromGeoJSON("{t.geometry}"), 4326))'] if t.geometry else []
    )
    updates = ", ".join(f'"{c}" = EXCLUDED."{c}"' for c in insert_cols if c not in t.keys)
    keys = ", ".join(f'"{k}"' for k in t.keys)
    ins = ", ".join(f'"{c}"' for c in insert_cols)
    sel = ", ".join(select_cols)
    cur = conn.execute(
        f"INSERT INTO {t.table} ({ins}) SELECT {sel} FROM {tmp} ON CONFLICT ({keys}) DO UPDATE SET {updates}"  # noqa: S608
    )
    n = cur.rowcount
    conn.execute(f"DROP TABLE {tmp}")
    return n


def load(settings: Settings, name: str) -> int:
    t = TARGETS[name]
    t0 = time.perf_counter()
    with record_run(settings, f"load.{name}", settings.pipeline_version, "silver") as st:
        table = read_target(settings.data_lake_root, t)
        st.rows_received = table.num_rows
        with connect(settings) as conn:
            st.rows_processed = upsert(conn, t, table)
            conn.commit()
    log.info(
        "loaded",
        extra={"extra": {"target": name, "rows": st.rows_processed, "s": round(time.perf_counter() - t0, 1)}},
    )
    return st.rows_processed


def main(argv: list[str]) -> int:
    settings = get_settings()
    configure_logging(settings.log_level)
    names = argv or list(TARGETS)
    unknown = [n for n in names if n not in TARGETS]
    if unknown:
        log.error("unknown targets", extra={"extra": {"unknown": unknown, "known": list(TARGETS)}})
        return 2
    failed = 0
    for name in names:
        try:
            load(settings, name)
        except Exception as exc:
            failed += 1
            log.error(
                "load failed", extra={"extra": {"target": name, "error": f"{type(exc).__name__}: {exc}"}}
            )
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
