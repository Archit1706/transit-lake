# Data dictionary

Medallion layers: **bronze** (raw parquet, immutable, dated) → **silver**
(conformed/typed DuckDB tables) → **gold** (dbt dims/facts/marts).

## Silver (conformed)

| Table | Grain | Key columns |
| --- | --- | --- |
| `silver_routes` | one CTA bus route | `route_id`, `route_type`, names, colors |
| `silver_stops` | one stop | `stop_id`, `stop_lat/lon`, `stop_name` |
| `silver_trips` | one scheduled trip | `trip_id`, `route_id`, `service_id`, `direction_id` |
| `silver_stop_times` | one scheduled stop event | `trip_id`, `stop_id`, `arrival_sec`, `stop_sequence` |
| `silver_vehicle_positions` | one RT report (bus+train) | `mode`, `vehicle_id`, `route_id`, `lat/lon`, `is_delayed`, `report_ts` |
| `silver_congestion` | one segment×time observation | `segment_id`, `report_ts`, `speed_mph` |
| `silver_weather` | one day | `weather_date`, temps, precip, snow, wind |

## Gold — dimensions

| Dimension | Grain | Surrogate key |
| --- | --- | --- |
| `dim_date` | day (2018–2026) | `date_key` (YYYYMMDD) |
| `dim_agency` | agency | `agency` |
| `dim_mode` | mode (`bus`/`train`) | `mode` |
| `dim_route` | route × mode (bus + rail) | `route_sk` |
| `dim_stop` | stop | `stop_sk` |
| `dim_segment` | road segment | `segment_sk` |

## Gold — facts

| Fact | Grain | Foreign keys |
| --- | --- | --- |
| `fact_vehicle_position` | one RT report | `route_sk` → dim_route, `date_key` → dim_date |
| `fact_trip_delay` | one delayed RT report | `route_sk`, `date_key` |
| `fact_congestion` | one segment×time obs | `segment_sk` → dim_segment, `date_key` |
| `fact_weather_daily` | one day | `date_key` → dim_date |

## Gold — analytics marts

| Mart | Question it answers |
| --- | --- |
| `mart_otp_by_route` | Which routes have the worst on-time performance? |
| `mart_otp_by_mode_hour` | How does OTP vary by mode and hour? |
| `mart_otp_by_route_hour` | Route × hour OTP heatmap |
| `mart_congestion_hotspots` | Which road segments are slowest? |
| `mart_segment_speed_profile` | Segment × hour speed (heatmap source) |
| `mart_congestion_by_hour` | City-wide road speed by hour |
| `mart_delay_vs_weather` | Do delays rise with precip/snow? |
| `mart_delay_vs_congestion` | Transit delay vs road speed by hour-of-day |
| `mart_fleet_activity_by_route` | Daily peak vehicles + report volume per route |
| `mart_daily_delay_trend` | Delay-rate trend per mode, with weekday context |
| `mart_route_summary` | One row per route: attributes + headline OTP |
| `mart_busiest_stops` | Busiest stops by scheduled departures |
| `mart_route_service_levels` | Scheduled trips/stop-events per route |
| `mart_pipeline_freshness` | Latest timestamp + row count per source |
