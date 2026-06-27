Here's the full build spec, with the **data catalog as the centerpiece** since that's what you asked for. Working name: **TransitLake**. The whole point is that it leans on your UTC/transportation domain — you already understand GTFS, OD pairs, and Chicago transit data, so this is authentic _and_ fast for you, not a generic tutorial pipeline.

## What it proves (the recruiter signal)

A Data Engineer screen checks for the spine your current projects don't show: **orchestration, a warehouse/lakehouse, modeled transformations, and data quality.** CryptoNorm proves streaming; FairLend proves batch mining; this proves you can run a _governed, scheduled, multi-source pipeline with tested data models_ — the day job. Keywords it hits: _Dagster/Airflow, dbt, medallion architecture, dimensional modeling, DuckDB/BigQuery, Great Expectations, data quality, incremental models, lineage, orchestration, Parquet._

## The data — exactly what, and from where

You're right that it's almost all public. Here's the concrete catalog. Mix **slowly-changing reference data** (GTFS static), **high-volume time-series** (GTFS-Realtime + congestion), and **enrichment** (weather) — that mix is what makes it a real lakehouse rather than one table.

| Source                  | Dataset                                                            | What it gives you                                       | Format / Access                             | Volume                                            |
| ----------------------- | ------------------------------------------------------------------ | ------------------------------------------------------- | ------------------------------------------- | ------------------------------------------------- |
| **CTA**                 | Static GTFS (`google_transit.zip`)                                 | routes, stops, trips, `stop_times`, shapes, calendar    | ZIP of CSVs, direct download                | ~5–10M rows (`stop_times` dominates) per snapshot |
| **CTA**                 | GTFS-Realtime (Bus Tracker / Train Tracker)                        | live vehicle positions, trip updates (delays), alerts   | Protobuf / JSON, free API key               | **the volume driver** — poll every 60s            |
| **Metra**               | Static GTFS + GTFS-RT                                              | commuter-rail schedule + live positions                 | ZIP + protobuf, free key                    | adds a 2nd agency                                 |
| **Pace**                | Static GTFS                                                        | suburban bus schedule                                   | ZIP of CSVs                                 | adds a 3rd agency                                 |
| **Chicago Data Portal** | _Traffic Tracker — Historical Congestion by Segment_ (`sxs8-h27x`) | road-segment congestion every ~10 min, years of history | Socrata API (free app token), CSV/JSON      | **~290M rows** — bulk-load a slice                |
| **Chicago Data Portal** | _Average Daily Traffic Counts_ (`pfsx-4n4m`)                       | AADT per location                                       | Socrata API                                 | small reference dim                               |
| **CMAP**                | Data Hub — traffic counts, travel demand, land use                 | regional context, count locations                       | datahub.cmap.illinois.gov (CSV/GeoJSON/API) | reference + geo                                   |
| **IDOT**                | "Getting Around Illinois" / Roadway Info — AADT                    | statewide traffic counts (GIS)                          | Shapefile/CSV download                      | reference + geo                                   |
| **USDOT / BTS**         | National Transit Database — Monthly Ridership                      | ridership + service by agency                           | CSV / data.transportation.gov (Socrata)     | monthly facts                                     |
| **Open-Meteo**          | Historical daily weather (Chicago)                                 | temp, precip, snow, wind                                | JSON, **free, no key**                      | daily enrichment                                  |

A few specifics so you don't hunt:

-   **CTA static GTFS**: `https://www.transitchicago.com/downloads/sch_data/google_transit.zip` — re-download daily; each snapshot is a dated bronze partition.
-   **GTFS-RT** uses the `gtfs-realtime-bindings` Python package to parse protobuf into `VehiclePosition`/`TripUpdate` records.
-   **Socrata** (Chicago portal + USDOT) uses the `sodapy` client with a free app token; supports paged pulls and `$where` time filters for incremental loads.
-   **Mobility Database** (`mobilitydatabase.org`) is the canonical catalog if you want to add more agencies/feeds cleanly.
-   Verify current dataset IDs/endpoints when you start — portals occasionally rename, but those IDs are the long-standing ones.

## How you actually hit "50M+ rows" (honestly)

Don't fake it — accumulate it:

-   **GTFS-RT vehicle positions**, polled every 60s across CTA's ~1,800 active buses + trains ≈ **1–2M rows/day**. Two weeks of polling ≈ 20–30M rows, and it grows forever (that's the point of a lakehouse).
-   **Historical congestion** (`sxs8-h27x`) is ~290M rows on its own — bulk-load a **multi-month slice** (e.g., 60M rows) into bronze once.
-   Static GTFS `stop_times` across 3 agencies ≈ a few million per daily snapshot.

So the bullet's "50M+ rows over X sources" is real and conservative once the RT poller has run for a couple weeks.

## Architecture — a medallion lakehouse

```
  Dagster (schedules + assets + sensors + asset checks)
        │
  ┌─────▼─────────────────────────────────────────────────────────────┐
  │ BRONZE (raw, immutable)   land exactly as received, dated partitions │
  │   gtfs_static/*.parquet · gtfs_rt/vehicle_positions/dt=.../*.parquet  │
  │   socrata/congestion/* · ntd/* · weather/*                           │
  ├──────────────────────────────────────────────────────────────────────┤
  │ SILVER (conformed)  parse protobuf, unzip GTFS, typecast, dedupe,     │
  │   conform schemas + keys across agencies                             │
  ├──────────────────────────────────────────────────────────────────────┤
  │ GOLD (dbt marts)  dims + facts + analytics marts                     │
  │   dim_route/stop/agency/segment/date · fact_vehicle_position,        │
  │   fact_trip_delay, fact_congestion, fact_ridership · OTP/hotspot marts│
  └──────────────────────────────────────────────────────────────────────┘
        │                         │                          │
   Great Expectations        dbt tests              Streamlit / Evidence
   (bronze→silver gates)   (in-warehouse)               dashboard
```

The lake is **Parquet** on disk (or MinIO for an S3 look); **DuckDB** is the query engine; **dbt-duckdb** does the modeling. That's a legit, zero-cost local lakehouse.

## Tech stack (with rationale)

| Layer          | Pick                                                               | Why / alternative                                                                                                                         |
| -------------- | ------------------------------------------------------------------ | ----------------------------------------------------------------------------------------------------------------------------------------- |
| Orchestration  | **Dagster**                                                        | Software-defined _assets_ map 1:1 to lakehouse tables; better DX, lineage, and asset checks than Airflow for a portfolio. _Alt: Airflow._ |
| Lake storage   | **Parquet** on disk → **MinIO** (stretch)                          | Columnar, partitioned; MinIO gives the S3 story.                                                                                          |
| Query engine   | **DuckDB**                                                         | Fast, embedded, free; queries Parquet directly. _Cloud alt: BigQuery (dbt-bigquery, 1TB/mo free)._                                        |
| Transformation | **dbt** (dbt-duckdb)                                               | Staging→marts, tests, docs, lineage — the data-eng standard. Gets you to 40+ models.                                                      |
| Ingestion      | Python + `sodapy`, `gtfs-realtime-bindings`, `requests`            | Or **dlt** (data-load-tool) as a stretch for declarative EL.                                                                              |
| Data quality   | **Great Expectations** (bronze/silver) + **dbt tests** (warehouse) | Two-layer: validate on ingest _and_ in-warehouse.                                                                                         |
| Dashboard      | **Streamlit** or **Evidence.dev**                                  | Streamlit = you know Python; Evidence = BI-as-code (SQL+markdown), very impressive for data-eng.                                          |
| CI/CD          | **GitHub Actions** + **sqlfluff**                                  | Lint SQL, `dbt build`, `dbt test`, GE checkpoint on every PR.                                                                             |

## Repo structure

```
transitlake/
├── README.md                  # architecture, lineage diagram, data dictionary, dashboard GIF
├── pyproject.toml
├── dagster_proj/
│   ├── assets/{bronze,silver}.py   # software-defined assets per source
│   ├── resources.py                # duckdb, socrata, http clients
│   ├── schedules.py · sensors.py   # daily static pull + 60s RT poll
│   └── definitions.py
├── ingestion/
│   ├── gtfs_static.py · gtfs_rt.py
│   ├── socrata.py (congestion, ADT, NTD)
│   ├── cmap.py · idot.py · weather.py
├── lake/ (gitignored)  bronze/  silver/
├── dbt/
│   ├── models/staging/        # stg_cta__stop_times, stg_socrata__congestion, ...
│   ├── models/intermediate/   # int_trip_delays, int_segment_congestion
│   ├── models/marts/          # dim_*, fact_*, mart_otp_by_route, mart_congestion_hotspots
│   ├── tests/ · macros/ · seeds/
│   └── dbt_project.yml
├── great_expectations/        # suites + checkpoints
├── dashboard/                 # Streamlit or Evidence
├── .github/workflows/{ci.yml, daily-ingest.yml}
└── docs/{data_dictionary.md, sources.md, lineage.png}
```

## Milestone plan (~2 weeks)

**Phase 0 — Scaffold (Day 1).** Dagster + DuckDB + dbt skeleton; pick agencies (CTA + Metra + Pace); free API keys (CTA, Socrata app token). _Demoable: `dagster dev` shows the asset graph._

**Phase 1 — Bronze ingestion (Days 2–4).** GTFS static download/unzip → dated parquet; GTFS-RT poller on a 60s Dagster schedule; Socrata pulls (congestion slice, ADT, NTD); Open-Meteo daily. Everything lands raw and partitioned. _Demoable: bronze fills, RT row count climbs each minute._

**Phase 2 — Silver (Days 5–6).** Parse protobuf → tabular; conform schemas and keys across agencies; typecast, dedupe, dedupe RT snapshots. Dagster bronze→silver assets. _Demoable: clean conformed tables in DuckDB._

**Phase 3 — dbt marts (Days 7–9).** Staging models per source; dimensions (`dim_route/stop/agency/segment/date`); facts (`fact_vehicle_position`, `fact_trip_delay`, `fact_congestion`, `fact_ridership`); analytics marts (on-time performance by route, congestion hotspots by segment×hour, delay-vs-weather). Push past **40 models**. _Demoable: `dbt docs` lineage graph._

**Phase 4 — Data quality (Days 10–11).** dbt tests (`not_null`, `unique`, `relationships`, `accepted_values`, freshness) + Great Expectations suites on bronze→silver + Dagster asset checks. Target **100+ checks**. _Demoable: a bad row fails a check and blocks the run._

**Phase 5 — Dashboard + CI + polish (Days 12–14).** Streamlit/Evidence dashboard (OTP trends, congestion heatmap, delay-vs-weather); GitHub Actions (sqlfluff + dbt build/test + GE); README with lineage diagram and data dictionary. _Demoable: live dashboard + green CI._

**Stretch (each = a resume line):** BigQuery variant; MinIO object store; **incremental** dbt models + partitioning; **SCD2** dimensions; `dlt` for declarative ingestion; dbt exposures + Soda Cloud.

## The data model + the analyses (the "so what")

Don't just move data — answer questions. Your gold marts should drive:

-   **Transit on-time performance** by route/agency/hour (from `fact_trip_delay`)
-   **Congestion hotspots** by segment × time-of-day (from `fact_congestion`)
-   **Delay ↔ weather** correlation (join delays to Open-Meteo)
-   **Delay ↔ road congestion** — where bus delays coincide with segment congestion (your multi-modal UTC edge)
-   **Ridership vs service levels** (NTD)

That last cluster is your differentiator: most data-eng portfolios stop at "I loaded data." Yours _answers transportation questions_ because of your domain — lean into it in the README.

## Metrics to capture (for the bullet)

-   dbt model count (**40+**), data-quality checks (**100+**), rows (**50M+**), sources (**6–7**), agencies (**3**), pipeline freshness/runtime.

## Exact resume bullets (for `data.tex`)

-   Built a **Dagster**-orchestrated **lakehouse** ingesting daily **GTFS** + **GTFS-RT** + traffic feeds from **7 sources**, modeled into **40+ dbt** marts over **50M+ rows**.
-   Enforced **100+** data-quality checks (**dbt tests** + **Great Expectations**) across a bronze/silver/gold **DuckDB** lake, surfacing on-time-performance and congestion marts on a dashboard.

**Placement:** `data.tex` currently runs FairLend Miners, CryptoNorm, GreenPipe. **Swap GreenPipe → TransitLake** — GreenPipe is the least data-eng of the three. That gives you a complete data-eng trio: **batch mining** (FairLend) + **streaming** (CryptoNorm) + **orchestrated lakehouse** (TransitLake). That's a genuinely strong, gap-free data-engineering story.

## Pitfalls to avoid

-   **GTFS-RT is the volume engine — start the poller on Day 2.** Rows accumulate over time; if you start it last you won't have the counts. It's the one thing you can't backfill cheaply.
-   **Partition bronze by date from the start** (`dt=YYYY-MM-DD`), or incremental models and reprocessing become painful.
-   **dbt tests are the headline DQ signal** — `relationships` tests proving referential integrity between facts and dims is exactly what reviewers look for. Don't ship marts without them.
-   **Make a failure demoable.** A GE/dbt check that _catches a bad row and blocks the pipeline_ is your interview moment — same as the drift demo in GalaxyServe.
-   **Respect rate limits & ToS** (Socrata app token, CTA key) — cache raw pulls in bronze so you never re-hit the API for the same data.
-   **Don't boil the ocean on sources.** CTA + congestion + weather alone is a complete story; Metra/Pace/CMAP/IDOT/NTD are additive. Get the thin end-to-end loop working on 2–3 sources before adding more.
