-- Section 7: structured analytical data. Dimensions + facts per domain, GOLD tables, spatial columns, indexes.
CREATE SCHEMA IF NOT EXISTS housing;
CREATE SCHEMA IF NOT EXISTS socio;
CREATE SCHEMA IF NOT EXISTS energy;
CREATE SCHEMA IF NOT EXISTS environment;
CREATE SCHEMA IF NOT EXISTS amenities;
CREATE SCHEMA IF NOT EXISTS analytics;
CREATE SCHEMA IF NOT EXISTS text;

-- ---------------------------------------------------------------- dimensions
ALTER TABLE reference.dim_commune
    ADD COLUMN IF NOT EXISTS population   BIGINT CHECK (population >= 0),
    ADD COLUMN IF NOT EXISTS surface_ha   DOUBLE PRECISION CHECK (surface_ha >= 0),
    ADD COLUMN IF NOT EXISTS density_km2  DOUBLE PRECISION,
    ADD COLUMN IF NOT EXISTS postal_codes TEXT[];

CREATE TABLE IF NOT EXISTS reference.dim_date (
    date_key     DATE PRIMARY KEY,
    year         SMALLINT NOT NULL,
    quarter      SMALLINT NOT NULL CHECK (quarter BETWEEN 1 AND 4),
    month        SMALLINT NOT NULL CHECK (month BETWEEN 1 AND 12),
    year_quarter TEXT NOT NULL,
    year_month   TEXT NOT NULL
);
INSERT INTO reference.dim_date (date_key, year, quarter, month, year_quarter, year_month)
SELECT d, EXTRACT(YEAR FROM d), EXTRACT(QUARTER FROM d), EXTRACT(MONTH FROM d),
       to_char(d, 'YYYY') || '-Q' || EXTRACT(QUARTER FROM d), to_char(d, 'YYYY-MM')
FROM generate_series(DATE '2010-01-01', DATE '2035-12-31', INTERVAL '1 day') AS g(d)
ON CONFLICT DO NOTHING;

CREATE TABLE IF NOT EXISTS reference.dim_property_type (
    property_type TEXT PRIMARY KEY,
    label_fr      TEXT NOT NULL,
    label_en      TEXT NOT NULL
);
INSERT INTO reference.dim_property_type VALUES
    ('Maison', 'Maison', 'House'), ('Appartement', 'Appartement', 'Apartment'), ('all', 'Tous types', 'All types')
ON CONFLICT DO NOTHING;

-- ---------------------------------------------------------------- housing
CREATE TABLE IF NOT EXISTS housing.fact_transaction (
    mutation_id      TEXT NOT NULL,
    property_type    TEXT NOT NULL REFERENCES reference.dim_property_type (property_type),
    date             DATE NOT NULL REFERENCES reference.dim_date (date_key),
    year             SMALLINT NOT NULL,
    month            SMALLINT NOT NULL,
    quarter          SMALLINT NOT NULL,
    commune_code     CHAR(5) NOT NULL REFERENCES reference.dim_commune (commune_code),
    department_code  VARCHAR(3) NOT NULL,
    region_code      CHAR(2) NOT NULL,
    postal_code      TEXT,
    price            DOUBLE PRECISION NOT NULL CHECK (price > 0),
    surface          DOUBLE PRECISION NOT NULL CHECK (surface > 0),
    rooms            DOUBLE PRECISION,
    units            INTEGER NOT NULL,
    land_surface     DOUBLE PRECISION,
    price_m2         DOUBLE PRECISION NOT NULL CHECK (price_m2 > 0),
    latitude         DOUBLE PRECISION,
    longitude        DOUBLE PRECISION,
    geom             geometry(Point, 4326) GENERATED ALWAYS AS
                     (CASE WHEN latitude IS NULL THEN NULL ELSE ST_SetSRID(ST_MakePoint(longitude, latitude), 4326) END) STORED,
    PRIMARY KEY (mutation_id, property_type)
);
CREATE INDEX IF NOT EXISTS fact_transaction_commune_year_idx ON housing.fact_transaction (commune_code, year);
CREATE INDEX IF NOT EXISTS fact_transaction_dept_year_idx    ON housing.fact_transaction (department_code, year);
CREATE INDEX IF NOT EXISTS fact_transaction_date_idx         ON housing.fact_transaction (date);
CREATE INDEX IF NOT EXISTS fact_transaction_geom_idx         ON housing.fact_transaction USING GIST (geom);

-- ---------------------------------------------------------------- socio-economic
CREATE TABLE IF NOT EXISTS socio.commune_income (
    commune_code           CHAR(5) NOT NULL REFERENCES reference.dim_commune (commune_code),
    year                   SMALLINT NOT NULL,
    median_income          DOUBLE PRECISION,
    income_d1              DOUBLE PRECISION,
    income_d9              DOUBLE PRECISION,
    interdecile_ratio      DOUBLE PRECISION,
    poverty_rate           DOUBLE PRECISION,
    taxed_households_share DOUBLE PRECISION,
    households             DOUBLE PRECISION,
    persons                DOUBLE PRECISION,
    PRIMARY KEY (commune_code, year)
);
CREATE TABLE IF NOT EXISTS socio.commune_employment (
    commune_code      CHAR(5) NOT NULL REFERENCES reference.dim_commune (commune_code),
    year              SMALLINT NOT NULL,
    active            DOUBLE PRECISION,
    employed          DOUBLE PRECISION,
    unemployed        DOUBLE PRECISION,
    unemployment_rate DOUBLE PRECISION CHECK (unemployment_rate BETWEEN 0 AND 1),
    PRIMARY KEY (commune_code, year)
);

-- ---------------------------------------------------------------- energy
CREATE TABLE IF NOT EXISTS energy.dpe_certificate (
    dpe_id              TEXT PRIMARY KEY,
    date                DATE NOT NULL,
    year                SMALLINT NOT NULL,
    energy_label        CHAR(1) NOT NULL CHECK (energy_label BETWEEN 'A' AND 'G'),
    ghg_label           CHAR(1) NOT NULL CHECK (ghg_label BETWEEN 'A' AND 'G'),
    building_type       TEXT NOT NULL,
    construction_period TEXT,
    surface             DOUBLE PRECISION NOT NULL CHECK (surface > 0),
    energy_kwh_m2       DOUBLE PRECISION,
    ghg_kg_m2           DOUBLE PRECISION,
    commune_code        CHAR(5) NOT NULL REFERENCES reference.dim_commune (commune_code),
    department_code     VARCHAR(3) NOT NULL,
    region_code         CHAR(2) NOT NULL,
    postal_code         TEXT
);
CREATE INDEX IF NOT EXISTS dpe_commune_idx ON energy.dpe_certificate (commune_code);
CREATE INDEX IF NOT EXISTS dpe_dept_label_idx ON energy.dpe_certificate (department_code, energy_label);

-- ---------------------------------------------------------------- environment
CREATE TABLE IF NOT EXISTS environment.commune_risk (
    commune_code CHAR(5) NOT NULL REFERENCES reference.dim_commune (commune_code),
    risk_code    TEXT NOT NULL,
    risk_label   TEXT NOT NULL,
    risk_family  TEXT NOT NULL CHECK (risk_family IN ('natural', 'technological', 'other')),
    seismic_zone TEXT,
    PRIMARY KEY (commune_code, risk_code)
);

-- ---------------------------------------------------------------- amenities
CREATE TABLE IF NOT EXISTS amenities.school (
    school_id          TEXT PRIMARY KEY,
    name               TEXT NOT NULL,
    school_type        TEXT,
    status             TEXT,
    commune_code       CHAR(5) NOT NULL REFERENCES reference.dim_commune (commune_code),
    department_code    VARCHAR(3) NOT NULL,
    region_code        CHAR(2),
    priority_education TEXT,
    latitude           DOUBLE PRECISION,
    longitude          DOUBLE PRECISION,
    geom               geometry(Point, 4326) GENERATED ALWAYS AS
                       (CASE WHEN latitude IS NULL THEN NULL ELSE ST_SetSRID(ST_MakePoint(longitude, latitude), 4326) END) STORED
);
CREATE INDEX IF NOT EXISTS school_commune_idx ON amenities.school (commune_code);
CREATE INDEX IF NOT EXISTS school_geom_idx ON amenities.school USING GIST (geom);

CREATE TABLE IF NOT EXISTS amenities.rail_station (
    station_uic     TEXT PRIMARY KEY,
    name            TEXT NOT NULL,
    commune_code    CHAR(5) NOT NULL REFERENCES reference.dim_commune (commune_code),
    department_code VARCHAR(3),
    region_code     CHAR(2),
    latitude        DOUBLE PRECISION NOT NULL,
    longitude       DOUBLE PRECISION NOT NULL,
    geom            geometry(Point, 4326) GENERATED ALWAYS AS (ST_SetSRID(ST_MakePoint(longitude, latitude), 4326)) STORED
);
CREATE INDEX IF NOT EXISTS rail_station_commune_idx ON amenities.rail_station (commune_code);
CREATE INDEX IF NOT EXISTS rail_station_geom_idx ON amenities.rail_station USING GIST (geom);

-- ---------------------------------------------------------------- GOLD (analytics)
CREATE TABLE IF NOT EXISTS analytics.housing_market (
    level             TEXT NOT NULL CHECK (level IN ('commune', 'department', 'region')),
    territory_code    TEXT NOT NULL,
    year              SMALLINT NOT NULL,
    property_type     TEXT NOT NULL REFERENCES reference.dim_property_type (property_type),
    transactions      INTEGER NOT NULL,
    median_price_m2   DOUBLE PRECISION,
    avg_price_m2      DOUBLE PRECISION,
    p25_price_m2      DOUBLE PRECISION,
    p75_price_m2      DOUBLE PRECISION,
    median_price      DOUBLE PRECISION,
    avg_price         DOUBLE PRECISION,
    median_surface    DOUBLE PRECISION,
    avg_surface       DOUBLE PRECISION,
    avg_rooms         DOUBLE PRECISION,
    n_surface_lt_40   INTEGER,
    n_surface_40_80   INTEGER,
    n_surface_80_120  INTEGER,
    n_surface_ge_120  INTEGER,
    PRIMARY KEY (level, territory_code, year, property_type)
);

CREATE TABLE IF NOT EXISTS analytics.housing_price_evolution (
    level                   TEXT NOT NULL,
    territory_code          TEXT NOT NULL,
    granularity             TEXT NOT NULL CHECK (granularity IN ('month', 'quarter', 'year')),
    period                  TEXT NOT NULL,
    year                    SMALLINT NOT NULL,
    transactions            INTEGER NOT NULL,
    median_price_m2         DOUBLE PRECISION,
    yoy_growth              DOUBLE PRECISION,
    growth_3y               DOUBLE PRECISION,
    growth_5y               DOUBLE PRECISION,
    transactions_yoy_growth DOUBLE PRECISION,
    PRIMARY KEY (level, territory_code, granularity, period)
);

CREATE TABLE IF NOT EXISTS analytics.climate_risk (
    level                    TEXT NOT NULL,
    territory_code           TEXT NOT NULL,
    communes                 INTEGER,
    risk_count               DOUBLE PRECISION,
    natural_risk_count       DOUBLE PRECISION,
    technological_risk_count DOUBLE PRECISION,
    has_flood                DOUBLE PRECISION,
    has_ground_movement      DOUBLE PRECISION,
    has_earthquake           DOUBLE PRECISION,
    has_radon                DOUBLE PRECISION,
    has_industrial           DOUBLE PRECISION,
    exposed_communes_share   DOUBLE PRECISION,
    PRIMARY KEY (level, territory_code)
);

CREATE TABLE IF NOT EXISTS analytics.territory_profile (
    level               TEXT NOT NULL,
    territory_code      TEXT NOT NULL,
    name                TEXT,
    department_code     VARCHAR(3),
    region_code         CHAR(2),
    communes            INTEGER,
    population          BIGINT,
    density_km2         DOUBLE PRECISION,
    surface_ha          DOUBLE PRECISION,
    latitude            DOUBLE PRECISION,
    longitude           DOUBLE PRECISION,
    housing_year        SMALLINT,
    transactions        INTEGER,
    median_price_m2     DOUBLE PRECISION,
    median_price        DOUBLE PRECISION,
    apartment_share     DOUBLE PRECISION,
    median_income       DOUBLE PRECISION,
    poverty_rate        DOUBLE PRECISION,
    interdecile_ratio   DOUBLE PRECISION,
    income_year         SMALLINT,
    active              DOUBLE PRECISION,
    employed            DOUBLE PRECISION,
    unemployed          DOUBLE PRECISION,
    unemployment_rate   DOUBLE PRECISION,
    employment_year     SMALLINT,
    schools             INTEGER,
    primary_schools     INTEGER,
    middle_schools      INTEGER,
    high_schools        INTEGER,
    public_schools      INTEGER,
    rail_stations       INTEGER,
    risk_count          DOUBLE PRECISION,
    natural_risk_count  DOUBLE PRECISION,
    has_flood           DOUBLE PRECISION,
    dpe_count           INTEGER,
    dpe_share_ab        DOUBLE PRECISION,
    dpe_share_fg        DOUBLE PRECISION,
    avg_energy_kwh_m2   DOUBLE PRECISION,
    text_reports        INTEGER,
    PRIMARY KEY (level, territory_code)
);
CREATE INDEX IF NOT EXISTS territory_profile_dept_idx ON analytics.territory_profile (level, department_code);

-- ---------------------------------------------------------------- analytical views (section 28: item 28)
CREATE OR REPLACE VIEW analytics.v_commune_overview AS
SELECT p.*, c.geometry
FROM analytics.territory_profile p
JOIN reference.dim_commune c ON c.commune_code = p.territory_code
WHERE p.level = 'commune';

CREATE OR REPLACE VIEW analytics.v_territory_geometry AS
SELECT 'commune' AS level, commune_code AS territory_code, name, geometry FROM reference.dim_commune
UNION ALL
SELECT 'department', department_code, name, geometry FROM reference.dim_department
UNION ALL
SELECT 'region', region_code, name, geometry FROM reference.dim_region;

CREATE OR REPLACE VIEW analytics.v_housing_market_latest AS
SELECT m.*
FROM analytics.housing_market m
JOIN (SELECT level, territory_code, max(year) AS year FROM analytics.housing_market GROUP BY 1, 2) l
  USING (level, territory_code, year);
