# Project VoltStream — EV Grid Resilience Data Pipeline

An end-to-end data engineering pipeline that ingests EV charging station data and weather
data, enriches them together, and serves a business-facing "Grid Resilience" dashboard —
built on Databricks (PySpark + Delta Lake) and Power BI, following a Medallion (Bronze /
Silver / Gold) Architecture.

**Client (simulated):** VoltStream Energy Solutions, a municipal EV charging network
**Business problem:** Reactive maintenance and no visibility into how weather correlates
with station downtime and lost revenue.

## Architecture

```
Open Charge Map API ──┐
                       ├─► Bronze (raw JSON, append-only, checkpointed)
                       │        │
                       │        ▼
                       │   Silver (flattened, deduplicated, quality-checked,
                       │           SCD2 status history)
                       │        │
OpenWeatherMap API ────┼────────┤
                       │        ▼
                       │   Gold (Star Schema: Dim_Station, Dim_Weather,
                       │         Fact_Station_Status, Risk_Score,
                       │         Est_Revenue_At_Risk)
                       │        │
                       │        ▼
                       └──► Power BI dashboard (KPIs, weather correlation,
                             maintenance priority list, RLS, drill-through)
```

Orchestrated end-to-end as a single Databricks Job (`VoltStream_Full_Pipeline`), running
Bronze → Silver → Gold in guaranteed sequence, optionally on a schedule.

## Repository Contents

| File | Layer | Purpose |
|---|---|---|
| `01_bronze_ev_ingestion.py` | Bronze | Paginated ingestion from Open Charge Map, checkpointed incremental fetch, secrets-based auth |
| `02_silver_ev_cleaning.py` | Silver | Flatten/standardize/deduplicate stations, connector-level detail table, quarantine bad records, SCD2 status history |
| `03_gold_enrichment.py` | Gold | Weather-zone strategy, OpenWeatherMap ingestion, spatial join, Risk_Score, Star Schema, revenue-at-risk |
| `powerbi/` | Serving | Power BI `.pbix` — star schema model, DAX measures, dashboard visuals |

## 🔌 Data Sources

- **Open Charge Map** — EV charging station metadata (locations, connectors, operational status)
- **OpenWeatherMap** — current weather conditions (temperature, condition, wind speed)

## 🔧 Tech Stack

**Data Processing**
PySpark | Delta Lake | Databricks (Serverless compute)

**Languages & Frameworks**
Python | SQL | Databricks Workflows

**Data Sources**
Open Charge Map API | OpenWeatherMap API | REST

**Infrastructure**
Unity Catalog Volumes | Databricks Secrets | Git (Databricks Git folders)

**Visualization**
Power BI | DAX

## Tables Produced, by Layer

### Bronze
| Table | Purpose |
|---|---|
| `bronze_ev_stations` | Raw EV station JSON from Open Charge Map, one row per fetched record, unflattened, append-only |
| `bronze_ev_checkpoint` | Tracks the last successful run timestamp per region, driving incremental `modifiedsince` fetches |

### Silver
| Table | Purpose |
|---|---|
| `silver_ev_stations` | Flattened, deduplicated, current-state station table — one row per station, with `Is_Operational` and `data_quality_error` |
| `silver_ev_connections` | Exploded connector-level detail (station_id, connection_type_id, power_kw, etc.), kept separate to preserve station-level grain |
| `silver_ev_stations_history` | SCD Type 2 audit trail (`valid_from`, `valid_to`, `is_current`), recording every operational-status change over time |
| `quarantine_ev_connections` | Connector records that failed data-quality validation (negative `PowerKW`), routed here instead of dropped or failing the pipeline |
| `silver_weather` | Current weather snapshot per rounded lat/long "zone" (temperature, condition, wind speed); overwritten each run since it reflects live conditions, not history |

### Gold (Star Schema)
| Table | Purpose |
|---|---|
| `Dim_Station` | One row per station — static attributes: title, address, lat/long, `max_power_kw` |
| `Dim_Weather` | One row per weather zone — temperature, condition, wind speed, `weather_zone_id` |
| `Fact_Station_Status` | The transactional core — `station_id`, `weather_zone_id`, `Is_Operational`, `risk_score`, `Est_Revenue_At_Risk`, `Offline_Count`; appended each run to build a time series |

Only the three Gold tables are exported to Power BI — everything upstream exists to produce
them cleanly. See *Key Design Decisions* below for why each table is shaped the way it is.

### How Each Table Is Used in Power BI

| Table | Role in the dashboard |
|---|---|
| `Fact_Station_Status` | The measure source for every KPI and chart — `Total Revenue At Risk` (Card), `risk_score` (Scatter chart, Priority table), `Is_Operational` (RLS-filtered aggregations, slicer interactions) |
| `Dim_Station` | Supplies station identity/context — `title` and `address_line1` in the Maintenance Priority table, `state_or_province` for the Row-Level Security filter, `station_id` as the drill-through key |
| `Dim_Weather` | Supplies weather context — `condition` drives the Weather Condition slicer and the dynamic report title, `temp_f`/`wind_speed_mph` feed the Weather Correlation scatter chart |

`Dim_Station` and `Dim_Weather` join into `Fact_Station_Status` (Many-to-one, single-direction
cross-filter) so that filtering or slicing by station attributes or weather condition
propagates correctly into every Fact-based visual.

## Key Design Decisions

**Why raw JSON strings in Bronze, not a parsed schema?**
Bronze intentionally avoids asserting a schema on write. If the source API adds or changes a
field, ingestion doesn't break — schema enforcement is deferred to Silver, where it's a
deliberate, versioned decision rather than an accidental crash.

**Why explode `Connections` into a separate table instead of the station table?**
Exploding in place would change `silver_ev_stations`'s grain from one-row-per-station to
one-row-per-connector, inflating simple counts and duplicating every station attribute per
plug. `silver_ev_connections` keeps a normalized, station-grain-preserving design — standard
dimensional modeling practice.

**Why SCD Type 1 (MERGE) for `silver_ev_stations`, but SCD Type 2 for history?**
The current-state table should stay simple and fast for "what's true right now" queries.
Full history (needed to answer "how long was this station down?") is captured separately in
`silver_ev_stations_history`, so neither table has to compromise on its primary purpose.

**Why flag missing geolocation instead of dropping those rows?**
Dropping silently would understate non-geographic aggregations (e.g., total station counts,
revenue-at-risk totals) without anyone noticing. Flagging via `data_quality_error` lets
downstream consumers filter deliberately.

**Why fetch `Is_Operational` from OCM's live reference-data API instead of hardcoding it?**
A hardcoded status-code mapping silently goes stale if the source changes its definitions.
Pulling from `/v3/referencedata` at runtime (with a verified fallback) keeps the mapping
self-correcting.

**Why round lat/long to 1 decimal for weather zones?**
Weather is essentially uniform within an ~8-11km cell, and calling a weather API once per
station (500+ calls) would burn through free-tier rate limits for no benefit. Rounding
collapses many stations into shared zones, typically reducing calls by an order of magnitude.

## Senior-Level Challenges Implemented

- **Checkpointing** — incremental `modifiedsince` fetches via a dedicated checkpoint table
- **Data Quality Framework** — quarantine table for records failing validation (negative
  `PowerKW`), routed instead of dropped or pipeline-failing
- **SCD Type 2** — full status-change history with `valid_from` / `valid_to` / `is_current`
- **Orchestration** — Databricks Workflows job chaining all three notebooks with explicit
  task dependencies
- **Est_Revenue_At_Risk** — derived financial-impact metric on the Fact table
- **Dynamic DAX title** — Power BI report title reacts live to the weather-condition slicer
- **Row-Level Security** — a `California Manager` role restricting visibility by state
- **Drill-through** — a dedicated per-station detail page reachable from the main dashboard

## Limitations

- **Open Charge Map is community-maintained, not real-time.** Station status updates depend
  on operators or community contributors reporting changes — actual outages may not appear
  in the API immediately, so `Is_Operational` and downtime tracking reflect *reported* status,
  not necessarily *live* status.
- **`Risk_Score` is an illustrative rule-based formula, not a calibrated predictive model.**
  It combines a small set of additive business rules (weather severity, temperature extremes,
  fast-charger presence) chosen for demonstration purposes — it hasn't been validated against
  historical outage data, so it should be read as a prioritization heuristic, not a statistical
  risk prediction.
- **Weather-zone granularity (1-decimal lat/long rounding) trades precision for API-call
  efficiency.** Two stations in the same zone are assumed to experience identical weather,
  which is a reasonable approximation at this grid size but not exact.
- **Sample scale is modest** (capped via `max_records` in Bronze) — built and validated for
  correctness and pipeline design rather than large-scale performance testing.
- **Power BI is fed via CSV export through a Unity Catalog Volume**, a workaround for
  Databricks Free Edition's restriction on direct external BI connectors. A production
  deployment would connect Power BI directly to a Databricks SQL Warehouse instead.

## Author

**Kishan** — Data Engineering (learning project)
Built end-to-end as a hands-on exercise in Medallion Architecture, PySpark/Delta Lake,
API-based ingestion, dimensional modeling, and Power BI dashboarding on Databricks.

## Running the Pipeline

1. Set Databricks Secrets: `voltstream` scope with `ocm_api_key` and `owm_api_key`
2. Import all three notebooks into a Databricks Git folder
3. Run the `VoltStream_Full_Pipeline` Job (or the notebooks individually, in order)
4. Export `Fact_Station_Status`, `Dim_Station`, `Dim_Weather` to a Unity Catalog Volume as CSV
5. Load into Power BI, build/refresh the star schema relationships, refresh visuals
