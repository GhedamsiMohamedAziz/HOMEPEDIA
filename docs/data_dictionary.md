# Data dictionary

Canonical keys everywhere: `commune_code` (INSEE, 5 chars, e.g. `33063`, `2A004`), `department_code` (`33`, `2A`,
`974`), `region_code` (`75`). Coordinates are WGS84 (`latitude`, `longitude`). Money in euros, surfaces in m²,
rates as fractions (0–1) unless the column name ends in `_rate` from INSEE (Filosofi `poverty_rate` is a percentage
as published). Every SILVER/GOLD table carries `_processed_at`; BRONZE carries `_data_version` (RAW fetch date).

## SILVER (`data/silver/`)

Rejected rows live in `data/silver/_rejects/<table>/` with `reasons` (array) and `reason` (`;`-joined). Reason
counts per run are in `ops.pipeline_runs.reject_reasons`.

| Table | Grain | Source | Columns | Rules (reject reasons) |
|---|---|---|---|---|
| `regions` | region | geo_reference | `region_code`, `name`, `geometry_geojson` | — |
| `departments` | department | geo_reference | `department_code`, `region_code`, `name`, `geometry_geojson` | — |
| `communes` | commune | geo_reference | `commune_code`, `department_code`, `region_code`, `name`, `population` (legal population), `surface_ha`, `density_km2`, `latitude`, `longitude` (centroid), `postal_codes` (array), `geometry_geojson` | `bad_commune_code`, `department_not_in_reference` (overseas collectivities), `region_mismatch`, `invalid_coordinates` |
| `transactions` | (mutation, property type) | dvf | `mutation_id`, `property_type` (Maison/Appartement), `date`, `year`, `month`, `quarter`, `price`, `surface` (sum of housing lots), `rooms`, `units` (housing rows folded), `land_surface`, `price_m2`, `commune_code`, `department_code`, `region_code`, `postal_code`, `latitude`, `longitude` | `not_a_sale` (only Vente / VEFA), `mixed_use_mutation` (house + shop, or house + flat), `price_not_positive`, `surface_not_positive`, `price_m2_implausible` (outside 200–25 000 €/m²), `date_year_mismatch`, `commune_not_in_reference`, `invalid_coordinates` |
| `certificates` | DPE (latest version per id) | dpe | `dpe_id`, `date`, `year`, `energy_label`, `ghg_label` (A–G), `building_type`, `construction_period`, `surface`, `energy_kwh_m2` (primary energy, 5 uses), `ghg_kg_m2`, `commune_code`, `department_code`, `region_code`, `postal_code` | `duplicate_key` (older DPE versions), `invalid_energy_label`, `invalid_ghg_label`, `surface_not_positive`, `surface_implausible` (> 1000 m²), `dwelling_only` (whole buildings excluded), `commune_not_in_reference` |
| `income` | (commune, year) | insee_filosofi | `median_income` (€/yr per consumption unit), `income_d1`, `income_d9`, `interdecile_ratio`, `poverty_rate` (%), `taxed_households_share` (%), `households`, `persons`; nulls = INSEE confidentiality suppression | `commune_not_in_reference` |
| `employment` | (commune, census year 2011/2016/2022) | insee_emploi | `active`, `employed`, `unemployed` (derived as active − employed when not published), `unemployment_rate` (fraction), ages 15–64 | `commune_not_in_reference`, `active_not_positive` |
| `population` | (commune, census year 1968…2023) | insee_population | `population`, `dwellings`, `main_dwellings`, `secondary_dwellings`, `vacant_dwellings`, `vacancy_rate`, `births`, `deaths`, `area_km2` | `commune_not_in_reference`, `population_missing` |
| `commune_risks` | (commune, risk) | georisques | `risk_code`, `risk_label`, `risk_family` (natural = codes 1x, technological = 2x), `seismic_zone`, `commune_name` | `commune_not_in_reference` |
| `schools` | school | education | `school_id` (UAI), `name`, `school_type`, `status` (Public/Privé), `has_*` flags, `priority_education`, `state`, `latitude`, `longitude` | `duplicate_key`, `not_open`, `commune_not_in_reference`, `invalid_coordinates` |
| `stations` | passenger rail station | sncf_stations | `station_uic`, `name`, `commune_name`, `department_name`, `latitude`, `longitude`, `commune_code` (point-in-polygon on the reference contours) | `duplicate_key`, `not_passenger_station`, `commune_unresolved`, `invalid_coordinates` |
| `meeting_reports` | Grand Débat local meeting report | grand_debat | `created_at`, `title`, `themes` (array), `text_atmosphere`, `text_diagnostics`, `text_proposals`, `text` (diagnostics + proposals), `participants`, `postal_code`, `town`, `commune_code` (postal code + town name) | `commune_unresolved`, `text_too_short` (< 20 chars) |

## GOLD (`data/gold/`)

All GOLD tables carry `level` (`commune` / `department` / `region`) and `territory_code`. Codes collide across
levels (department `01` vs region `01`); the key is always (`level`, `territory_code`).

| Table | Key | Columns |
|---|---|---|
| `housing_market_commune` / `_department` / `_region` | (`territory_code`, `year`, `property_type`) with `property_type` ∈ {`all`, `Maison`, `Appartement`} | `transactions`, `median_price_m2`, `avg_price_m2`, `p25_price_m2`, `p75_price_m2`, `median_price`, `avg_price`, `median_surface`, `avg_surface`, `avg_rooms`, surface distribution counts `n_surface_lt_40`, `n_surface_40_80`, `n_surface_80_120`, `n_surface_ge_120` |
| `housing_price_evolution` | (`level`, `territory_code`, `granularity` ∈ {month, quarter, year}, `period` e.g. `2024-05`, `2024-Q2`, `2024`) | `year`, `transactions`, `median_price_m2` (houses + flats), `yoy_growth`, `growth_3y`, `growth_5y` (fractions vs the same sub-period N years earlier; null when unavailable), `transactions_yoy_growth` |
| `population_evolution` | (`level`, `territory_code`, `year`) | `communes`, `population`, `prev_year`, `prev_population`, `growth` (vs previous census), `annual_growth` (CAGR), dwelling stock by occupancy, `vacancy_rate`, `births`, `deaths` |
| `climate_risk` | (`level`, `territory_code`) | commune: `risk_count`, `natural_risk_count`, `technological_risk_count`, `has_flood`, `has_ground_movement`, `has_earthquake`, `has_radon`, `has_industrial` (0/1), `exposed_communes_share` (0/1); department/region: averages of the above over communes, `communes`, `exposed_communes_share` |
| `territory_profile` | (`level`, `territory_code`) | identity `name`, `department_code`, `region_code`, `communes`, `population`, `surface_ha`, `density_km2`, `latitude`, `longitude`; housing (latest year, `housing_year`) `transactions`, `median_price_m2`, `median_price`, `apartment_share`; economy `median_income` (department/region: population-weighted mean of commune medians), `poverty_rate`, `interdecile_ratio`, `income_year`, `active`, `employed`, `unemployed`, `unemployment_rate`, `employment_year`; education `schools`, `primary_schools`, `middle_schools`, `high_schools`, `public_schools`; transport `rail_stations`; environment `risk_count`, `natural_risk_count`, `has_flood` (share at upper levels); energy `dpe_count`, `dpe_share_ab`, `dpe_share_fg`, `avg_energy_kwh_m2`; demographics `census_year`, `population_annual_growth`, `dwellings`, `main_dwellings`, `vacant_dwellings`, `vacancy_rate`, `dwellings_per_1000`, `transactions_per_1000_dwellings`; text `text_reports` |

`territory_text_insights` (sentiment, topics, entities per territory) is produced by the NLP pipeline.

## PostgreSQL

SILVER facts and GOLD tables are loaded by `python -m database.load` (upsert, rerunnable) into: `reference.dim_*`
(+ `dim_date`, `dim_property_type`), `housing.fact_transaction` (with a generated `geom` point),
`socio.commune_income`, `socio.commune_employment`, `socio.commune_population`, `energy.dpe_certificate`, `environment.commune_risk`,
`amenities.school`, `amenities.rail_station`, `analytics.housing_market` (three levels in one table),
`analytics.housing_price_evolution`, `analytics.population_evolution`, `analytics.climate_risk`, `analytics.territory_profile`. Views:
`analytics.v_commune_overview` (profile + geometry), `analytics.v_territory_geometry`, `analytics.v_housing_market_latest`.

## MongoDB

`reviews`: one document per Grand Débat report (`python -m database.load_mongo`), shape
`{source, territory: {commune_code, department_code, region_code}, text, language, title, themes[], created_at}`.
NLP outputs (`sentiment`, `topics`, entities) are added by the NLP pipeline into `nlp_outputs`.

## Analytics layer (`analytics/`)

Deterministic functions over the PostgreSQL tables; the application never computes metrics itself.

| Module | Functions |
|---|---|
| `analytics.housing.kpis` | `housing_kpis`, `property_type_distribution`, `surface_distribution`, `price_evolution` (pure: `calculate_price_evolution`, `calculate_cagr`) |
| `analytics.demographics.kpis` | `population_growth` (pure: `calculate_population_growth`) |
| `analytics.geography.kpis` | `housing_density`, `transport_accessibility` (pure: `calculate_housing_density`, `calculate_transport_accessibility`) |
| `analytics.environment.kpis` | `energy_distribution`, `climate_exposure` (pure: `calculate_energy_distribution`, `calculate_climate_exposure`) |
| `analytics.profile` | `TerritoryProfile`, `get_profile`, `compare` (comparison mode) |
| `analytics.insights` | `generate_insights` (territory vs parent, material gaps ≥ 10 %), `explain` (text rendering of computed insights) |
| `analytics.repository` | all SQL: profiles, market, series, DPE labels, risks, nearest station (PostGIS), GeoJSON geometries, search |
