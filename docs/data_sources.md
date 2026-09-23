# Source catalogue

Ingestion scope is controlled by `DEPARTMENTS`, `DVF_YEARS` and `DPE_SINCE` in `.env` (default: Gironde 33,
Haute-Garonne 31, Loire-Atlantique 44; DVF 2023–2024; DPE since 2024-01-01). Reference data and station lists are
always ingested for the whole of France. Every run is recorded in `ops.pipeline_runs`; RAW files carry a
`manifest.json` with URL, sha256, size and fetch time.

| Connector | Source / provider | URL | Licence | Update freq. | Geo level | Temporal coverage | Format | Ingestion | Reliability |
|---|---|---|---|---|---|---|---|---|---|
| `geo_reference` | Découpage administratif — geo.api.gouv.fr (INSEE COG + legal population) and Etalab contours 100 m (IGN Admin Express) | https://geo.api.gouv.fr/ , https://etalab-datasets.geo.data.gouv.fr/contours-administratifs/2024/geojson/ | Licence Ouverte 2.0 | yearly (COG) | region / department / commune | 2024 edition | JSON, GeoJSON | HTTP download, full | high — official codes, canonical join keys |
| `dvf` | Demandes de valeurs foncières géolocalisées — DGFiP, geocoded by Etalab | https://files.data.gouv.fr/geo-dvf/latest/csv/{year}/departements/{dept}.csv.gz | Licence Ouverte 2.0 | semi-annual | transaction (commune code, lat/lon) | 2019 → latest | CSV gz | HTTP download per year × department | high; Alsace-Moselle and Mayotte not covered; multi-lot mutations need dedup (Phase 3) |
| `insee_filosofi` | Filosofi 2021 — INSEE (income, poverty, disposable income decomposition) | https://static.data.gouv.fr/resources/principaux-indicateurs-sur-la-pauvrete-en-2021-…/ds-filosofi-cc-2021-data.csv | Licence Ouverte 2.0 | yearly | commune | 2021 | CSV (long, `;`) | HTTP download, filtered to scope | high; small communes are suppressed for confidentiality (null values) |
| `insee_emploi` | Recensement de la population 2022 — INSEE, employment × age × PCS | https://static.data.gouv.fr/resources/principaux-indicateurs-sur-la-pauvrete-en-2021-…/ds-rp-emploi-lr-comp-2022-data.csv | Licence Ouverte 2.0 | yearly | commune | 2022 | CSV (long, `;`) | HTTP download, filtered to scope | high |
| `dpe` | DPE logements existants (depuis juillet 2021) — ADEME / Observatoire DPE | https://data.ademe.fr/data-fair/api/v1/datasets/meg-83tjwtg8dyz4vv7h1dqe/lines | Licence Ouverte 2.0 | continuous | dwelling (commune code via BAN) | 2021-07 → today | JSON API, 10 000 rows/page | paged API, filtered by department + date | medium — declarative data, duplicates across DPE versions, address geocoding by BAN |
| `georisques` | GASPAR risks per commune — Géorisques (BRGM / MTE) | https://georisques.gouv.fr/api/v1/gaspar/risques | Licence Ouverte 2.0 | continuous | commune | current | JSON API | batched API (10 communes / call) for scope communes | high |
| `sncf_stations` | Liste des gares — SNCF Réseau | https://ressources.data.sncf.com/api/explore/v2.1/catalog/datasets/liste-des-gares/exports/csv | ODbL | irregular | point (WGS84) | current | CSV | HTTP download, full | medium — no INSEE code, commune resolved spatially in Phase 3 |
| `education` | Annuaire de l'éducation — Ministère de l'Éducation nationale | https://data.education.gouv.fr/api/explore/v2.1/catalog/datasets/fr-en-annuaire-education/exports/csv | Licence Ouverte 2.0 | daily | school (commune code, lat/lon) | current | CSV | HTTP export filtered by department | high; department codes 3-char padded in source, normalised |
| `grand_debat` | Grand Débat National 2019 — comptes-rendus des réunions locales (SIG) | https://static.data.gouv.fr/resources/donnees-ouvertes-du-grand-debat-national/20200611-235901/cr-ril.csv | Licence Ouverte 2.0 | frozen (2019) | postal code / town / department | Jan–Apr 2019 | CSV | HTTP download, filtered to scope | medium — free text, 2019 snapshot, ~40 % of reports have no text (rejected and counted) |

Planned but not yet connected: transport.data.gouv.fr GTFS feeds (local transit), INSEE population history
(legal populations series), Sitadel building permits (ADEME/SDES).
