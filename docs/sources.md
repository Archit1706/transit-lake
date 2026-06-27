# Data sources

All sources are public. Keys are free. Raw pulls are cached in `lake/bronze/` so
the same data is never re-fetched (respects rate limits / ToS).

| Source | Dataset | Access | Key? | Bronze path |
| --- | --- | --- | --- | --- |
| CTA | Static GTFS (`google_transit.zip`) | ZIP of CSVs, direct download | no | `cta/gtfs_static/dt=…/` |
| CTA | Bus real-time positions (BusTime `getvehicles`) | JSON API | **yes** (Bus Tracker) | `cta/gtfs_rt/vehicle_positions/dt=…/` |
| CTA | Train real-time positions (Train Tracker `ttpositions`) | JSON API | **yes** (Train Tracker) | `cta/train_tracker/positions/dt=…/` (raw JSON) + `cta/gtfs_rt/train_positions_pb/dt=…/` (GTFS-RT protobuf) |
| Chicago Data Portal | Historical congestion by segment (`sxs8-h27x`, ~275M rows) | Socrata API | optional token | `socrata/congestion/` |
| Chicago Data Portal | Average Daily Traffic counts (`pfsx-4n4m`) | Socrata API | optional token | `socrata/adt/` |
| Open-Meteo | Daily historical weather (Chicago) | JSON | no | `open_meteo/weather_daily/` |

## Keys (`.env`)

See [.env.example](../.env.example). Register at:
- CTA Bus Tracker — <https://www.transitchicago.com/developers/bustracker/>
- CTA Train Tracker — <https://www.transitchicago.com/developers/traintracker/>
- Socrata app token — Chicago portal developer settings (lifts throttling; data
  is still reachable keyless, just rate-limited).

## Honest notes / deviations from a "textbook" GTFS-RT pipeline

- **CTA has no public GTFS-Realtime protobuf feed for buses.** The official bus
  real-time source is the **BusTime JSON API** (`getvehicles`). It carries the
  same payload a GTFS-RT `VehiclePosition` would (lat/lon, heading, route, trip,
  delay flag, timestamp), so it is the volume driver here — just JSON, not
  protobuf. Covering all ~126 routes costs ~13 calls/poll; a BusTime key is
  capped (~10k req/day), so the poller defaults to a 120s interval
  (`CTA_RT_POLL_SECONDS`), which still lands ~1M+ rows/day.
- **Trains** come from the **Train Tracker** `ttpositions` API — all 8 rail lines
  in a single call, with an `isDly` delay flag. CTA publishes no native train
  protobuf either, so each poll is **normalized to a canonical GTFS-RT
  `FeedMessage`** (`VehiclePosition` entities) with `gtfs-realtime-bindings` and
  landed as a `.pb` bronze artifact; silver **decodes that protobuf** back with
  the same bindings (`silver_train_positions`), so the protobuf is on the critical
  path, not decorative. See `ingestion/gtfs_rt_protobuf.py`. One mapping note:
  `VehiclePosition` has no schedule-delay field, so the `isDly` boolean rides
  `congestion_level` (CONGESTION vs RUNNING_SMOOTHLY) and decodes back; headsign
  isn't a `VehiclePosition` field, so it is null for trains in silver. Existing
  train JSON history was re-encoded via `scripts/backfill_train_protobuf.py`.
- **Congestion `sxs8-h27x`** is the 2011–2018 historical segment series. The live
  RT feed and this slice don't share a calendar window, so the multi-modal
  `mart_delay_vs_congestion` correlates on **hour-of-day profile**, not date.
