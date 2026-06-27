"""Silver-layer assets: conform raw bronze Parquet into clean, typed DuckDB tables.

Silver reads bronze Parquet directly via DuckDB's read_parquet, applies types,
dedupes, and conforms keys. The headline conform is silver_vehicle_positions,
which unions CTA bus (BusTime) and train (Train Tracker) real-time feeds into one
schema with a shared mode/agency/route/delay vocabulary.

dbt staging models build on these tables (see dbt sources).
"""
import dagster as dg
from dagster import AssetExecutionContext
from dagster_duckdb import DuckDBResource

from ingestion import config

SILVER_GROUP = "silver"


def _glob(*parts: str) -> str:
    """A DuckDB-friendly (forward-slash) glob under the bronze root."""
    return (config.BRONZE.joinpath(*parts)).as_posix()


def _latest_static_dir(agency: str) -> str:
    """Path glob for the most recent static GTFS snapshot of an agency."""
    base = config.BRONZE / agency / "gtfs_static"
    snapshots = sorted(p for p in base.glob("dt=*") if p.is_dir())
    if not snapshots:
        raise dg.Failure(f"No static GTFS snapshot found for {agency}")
    return snapshots[-1].as_posix()


def _build(context: AssetExecutionContext, duckdb: DuckDBResource, table: str, sql: str) -> dg.MaterializeResult:
    with duckdb.get_connection() as conn:
        conn.execute(f"CREATE OR REPLACE TABLE {table} AS {sql}")
        rows = conn.execute(f"SELECT count(*) FROM {table}").fetchone()[0]
    context.log.info(f"{table}: {rows:,} rows")
    return dg.MaterializeResult(metadata={"rows": rows, "table": table})


# --- GTFS reference dimensions (from the latest static snapshot) -----------

@dg.asset(group_name=SILVER_GROUP, deps=["cta_gtfs_static_bronze"], kinds={"duckdb"})
def silver_routes(context: AssetExecutionContext, duckdb: DuckDBResource) -> dg.MaterializeResult:
    src = _latest_static_dir("cta")
    return _build(context, duckdb, "silver_routes", f"""
        SELECT 'CTA' AS agency, route_id,
               route_short_name, route_long_name,
               TRY_CAST(route_type AS INTEGER) AS route_type,
               route_color, route_text_color
        FROM read_parquet('{src}/routes.parquet')
    """)


@dg.asset(group_name=SILVER_GROUP, deps=["cta_gtfs_static_bronze"], kinds={"duckdb"})
def silver_stops(context: AssetExecutionContext, duckdb: DuckDBResource) -> dg.MaterializeResult:
    src = _latest_static_dir("cta")
    return _build(context, duckdb, "silver_stops", f"""
        SELECT 'CTA' AS agency, stop_id, stop_code, stop_name,
               TRY_CAST(stop_lat AS DOUBLE) AS stop_lat,
               TRY_CAST(stop_lon AS DOUBLE) AS stop_lon,
               TRY_CAST(location_type AS INTEGER) AS location_type,
               parent_station,
               TRY_CAST(wheelchair_boarding AS INTEGER) AS wheelchair_boarding
        FROM read_parquet('{src}/stops.parquet')
    """)


@dg.asset(group_name=SILVER_GROUP, deps=["cta_gtfs_static_bronze"], kinds={"duckdb"})
def silver_trips(context: AssetExecutionContext, duckdb: DuckDBResource) -> dg.MaterializeResult:
    src = _latest_static_dir("cta")
    return _build(context, duckdb, "silver_trips", f"""
        SELECT 'CTA' AS agency, route_id, service_id, trip_id,
               direction AS direction_name,
               TRY_CAST(direction_id AS INTEGER) AS direction_id,
               block_id, shape_id, schd_trip_id,
               TRY_CAST(wheelchair_accessible AS INTEGER) AS wheelchair_accessible
        FROM read_parquet('{src}/trips.parquet')
    """)


@dg.asset(group_name=SILVER_GROUP, deps=["cta_gtfs_static_bronze"], kinds={"duckdb"})
def silver_stop_times(context: AssetExecutionContext, duckdb: DuckDBResource) -> dg.MaterializeResult:
    """stop_times typed; GTFS times can exceed 24h so keep the string + a seconds-after-midnight int."""
    src = _latest_static_dir("cta")
    return _build(context, duckdb, "silver_stop_times", f"""
        SELECT 'CTA' AS agency, trip_id, stop_id,
               arrival_time, departure_time,
               -- seconds after service-day midnight (handles 25:xx:xx)
               CAST(split_part(arrival_time, ':', 1) AS BIGINT) * 3600
                 + CAST(split_part(arrival_time, ':', 2) AS BIGINT) * 60
                 + CAST(split_part(arrival_time, ':', 3) AS BIGINT) AS arrival_sec,
               TRY_CAST(stop_sequence AS INTEGER) AS stop_sequence,
               stop_headsign, pickup_type,
               TRY_CAST(shape_dist_traveled AS DOUBLE) AS shape_dist_traveled
        FROM read_parquet('{src}/stop_times.parquet')
    """)


# --- Conformed real-time positions (bus + train) ---------------------------

@dg.asset(
    group_name=SILVER_GROUP,
    deps=["cta_vehicle_positions_bronze", "cta_train_positions_bronze"],
    kinds={"duckdb"},
)
def silver_vehicle_positions(context: AssetExecutionContext, duckdb: DuckDBResource) -> dg.MaterializeResult:
    """Union CTA bus + train real-time feeds into one conformed, deduped table.

    Dedup keeps one row per (mode, vehicle_id, report_ts) — repeated polls re-report
    the same timestamped position until the vehicle next moves.
    """
    bus = _glob("cta", "gtfs_rt", "vehicle_positions", "dt=*", "*.parquet")
    train = _glob("cta", "train_tracker", "positions", "dt=*", "*.parquet")
    return _build(context, duckdb, "silver_vehicle_positions", f"""
        WITH unioned AS (
            SELECT 'CTA' AS agency, 'bus' AS mode,
                   vid AS vehicle_id, rt AS route_id, des AS headsign,
                   lat, lon, hdg AS heading, dly AS is_delayed,
                   tmstmp AS report_ts, _ingested_at
            FROM read_parquet('{bus}')
            UNION ALL
            SELECT 'CTA' AS agency, 'train' AS mode,
                   rn AS vehicle_id, rt AS route_id, destNm AS headsign,
                   lat, lon, heading, isDly AS is_delayed,
                   tmstmp AS report_ts, _ingested_at
            FROM read_parquet('{train}')
        )
        SELECT * FROM unioned
        -- drop not-yet-positioned vehicles (trains report 0/0 until GPS locks)
        WHERE lat BETWEEN 41.0 AND 43.0
          AND lon BETWEEN -89.0 AND -87.0
        QUALIFY row_number() OVER (
            PARTITION BY mode, vehicle_id, report_ts ORDER BY _ingested_at
        ) = 1
    """)


# --- Road congestion + weather ---------------------------------------------

@dg.asset(group_name=SILVER_GROUP, deps=["chicago_congestion_bronze"], kinds={"duckdb"})
def silver_congestion(context: AssetExecutionContext, duckdb: DuckDBResource) -> dg.MaterializeResult:
    src = _glob("socrata", "congestion", "*.parquet")
    return _build(context, duckdb, "silver_congestion", f"""
        SELECT TRY_CAST(segment_id AS INTEGER) AS segment_id,
               CAST(time AS TIMESTAMP) AS report_ts,
               TRY_CAST(speed AS INTEGER) AS speed_mph,
               street, direction, from_street, to_street,
               TRY_CAST(length AS DOUBLE) AS length_mi,
               TRY_CAST(bus_count AS INTEGER) AS bus_count,
               TRY_CAST(message_count AS INTEGER) AS message_count,
               TRY_CAST(hour AS INTEGER) AS hour,
               TRY_CAST(day_of_week AS INTEGER) AS day_of_week,
               TRY_CAST(month AS INTEGER) AS month,
               TRY_CAST(start_latitude AS DOUBLE) AS start_lat,
               TRY_CAST(start_longitude AS DOUBLE) AS start_lon,
               TRY_CAST(end_latitude AS DOUBLE) AS end_lat,
               TRY_CAST(end_longitude AS DOUBLE) AS end_lon
        FROM read_parquet('{src}')
        WHERE speed IS NOT NULL AND TRY_CAST(speed AS INTEGER) >= 0
        QUALIFY row_number() OVER (PARTITION BY segment_id, time ORDER BY _ingested_at) = 1
    """)


@dg.asset(group_name=SILVER_GROUP, deps=["weather_bronze"], kinds={"duckdb"})
def silver_weather(context: AssetExecutionContext, duckdb: DuckDBResource) -> dg.MaterializeResult:
    src = _glob("open_meteo", "weather_daily", "*.parquet")
    return _build(context, duckdb, "silver_weather", f"""
        SELECT CAST(date AS DATE) AS weather_date,
               temperature_2m_max AS temp_max_c,
               temperature_2m_min AS temp_min_c,
               precipitation_sum  AS precip_mm,
               rain_sum           AS rain_mm,
               snowfall_sum       AS snow_cm,
               wind_speed_10m_max AS wind_kmh,
               wind_gusts_10m_max AS gust_kmh
        FROM read_parquet('{src}')
        QUALIFY row_number() OVER (PARTITION BY date ORDER BY _ingested_at DESC) = 1
    """)
