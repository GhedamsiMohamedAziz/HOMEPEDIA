-- Demographics: census series (SILVER) and population evolution (GOLD); profile densities.
CREATE TABLE IF NOT EXISTS socio.commune_population (
    commune_code        CHAR(5) NOT NULL REFERENCES reference.dim_commune (commune_code),
    year                SMALLINT NOT NULL,
    population          DOUBLE PRECISION,
    dwellings           DOUBLE PRECISION,
    main_dwellings      DOUBLE PRECISION,
    secondary_dwellings DOUBLE PRECISION,
    vacant_dwellings    DOUBLE PRECISION,
    vacancy_rate        DOUBLE PRECISION,
    births              DOUBLE PRECISION,
    deaths              DOUBLE PRECISION,
    area_km2            DOUBLE PRECISION,
    PRIMARY KEY (commune_code, year)
);

CREATE TABLE IF NOT EXISTS analytics.population_evolution (
    level               TEXT NOT NULL,
    territory_code      TEXT NOT NULL,
    year                SMALLINT NOT NULL,
    communes            INTEGER,
    population          DOUBLE PRECISION,
    prev_year           SMALLINT,
    prev_population     DOUBLE PRECISION,
    growth              DOUBLE PRECISION,
    annual_growth       DOUBLE PRECISION,
    dwellings           DOUBLE PRECISION,
    main_dwellings      DOUBLE PRECISION,
    secondary_dwellings DOUBLE PRECISION,
    vacant_dwellings    DOUBLE PRECISION,
    vacancy_rate        DOUBLE PRECISION,
    births              DOUBLE PRECISION,
    deaths              DOUBLE PRECISION,
    PRIMARY KEY (level, territory_code, year)
);

ALTER TABLE analytics.territory_profile
    ADD COLUMN IF NOT EXISTS census_year SMALLINT,
    ADD COLUMN IF NOT EXISTS population_annual_growth DOUBLE PRECISION,
    ADD COLUMN IF NOT EXISTS dwellings DOUBLE PRECISION,
    ADD COLUMN IF NOT EXISTS main_dwellings DOUBLE PRECISION,
    ADD COLUMN IF NOT EXISTS vacant_dwellings DOUBLE PRECISION,
    ADD COLUMN IF NOT EXISTS vacancy_rate DOUBLE PRECISION,
    ADD COLUMN IF NOT EXISTS dwellings_per_1000 DOUBLE PRECISION,
    ADD COLUMN IF NOT EXISTS transactions_per_1000_dwellings DOUBLE PRECISION;
