"""Create empty silver tables so `dbt build` can run in CI without the data lake.

CI has no bronze parquet, so the Dagster-built silver tables don't exist. This
creates them empty with the exact columns the dbt staging models expect. dbt then
compiles every model and runs all tests against 0-row tables (vacuously green),
which validates SQL + the DAG + test definitions on every PR.

Run:  uv run python -m scripts.ci_bootstrap_silver
"""
import duckdb

from ingestion import config

DDL = {
    "silver_routes": """
        agency VARCHAR, route_id VARCHAR, route_short_name VARCHAR,
        route_long_name VARCHAR, route_type INTEGER, route_color VARCHAR,
        route_text_color VARCHAR
    """,
    "silver_stops": """
        agency VARCHAR, stop_id VARCHAR, stop_code VARCHAR, stop_name VARCHAR,
        stop_lat DOUBLE, stop_lon DOUBLE, location_type INTEGER,
        parent_station VARCHAR, wheelchair_boarding INTEGER
    """,
    "silver_trips": """
        agency VARCHAR, route_id VARCHAR, service_id VARCHAR, trip_id VARCHAR,
        direction_name VARCHAR, direction_id INTEGER, block_id VARCHAR,
        shape_id VARCHAR, schd_trip_id VARCHAR, wheelchair_accessible INTEGER
    """,
    "silver_stop_times": """
        agency VARCHAR, trip_id VARCHAR, stop_id VARCHAR, arrival_time VARCHAR,
        departure_time VARCHAR, arrival_sec BIGINT, stop_sequence INTEGER,
        stop_headsign VARCHAR, pickup_type VARCHAR, shape_dist_traveled DOUBLE
    """,
    "silver_vehicle_positions": """
        agency VARCHAR, mode VARCHAR, vehicle_id VARCHAR, route_id VARCHAR,
        headsign VARCHAR, lat DOUBLE, lon DOUBLE, heading INTEGER,
        is_delayed BOOLEAN, report_ts TIMESTAMP, _ingested_at TIMESTAMP
    """,
    "silver_congestion": """
        segment_id INTEGER, report_ts TIMESTAMP, speed_mph INTEGER, street VARCHAR,
        direction VARCHAR, from_street VARCHAR, to_street VARCHAR, length_mi DOUBLE,
        bus_count INTEGER, message_count INTEGER, hour INTEGER, day_of_week INTEGER,
        month INTEGER, start_lat DOUBLE, start_lon DOUBLE, end_lat DOUBLE, end_lon DOUBLE
    """,
    "silver_weather": """
        weather_date DATE, temp_max_c DOUBLE, temp_min_c DOUBLE, precip_mm DOUBLE,
        rain_mm DOUBLE, snow_cm DOUBLE, wind_kmh DOUBLE, gust_kmh DOUBLE
    """,
}


def main() -> None:
    config.LAKE_ROOT.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect(str(config.DUCKDB_PATH))
    try:
        for table, cols in DDL.items():
            con.execute(f"CREATE TABLE IF NOT EXISTS {table} ({cols})")
        print(f"bootstrapped {len(DDL)} empty silver tables at {config.DUCKDB_PATH}")
    finally:
        con.close()


if __name__ == "__main__":
    main()
