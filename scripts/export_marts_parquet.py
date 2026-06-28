"""Export the gold marts the Next.js frontend needs as Parquet files.

The frontend queries these client-side with DuckDB-WASM (SQL in the browser), so
this is the analog of export_marts_snapshot.py for the web app. Parquet (not a
native .duckdb) avoids any storage-format version mismatch with the wasm build
and keeps the files small.

Run:  uv run python -m scripts.export_marts_parquet
"""
from pathlib import Path

import duckdb

REPO = Path(__file__).resolve().parent.parent
SRC = REPO / "lake" / "transitlake.duckdb"
OUT = REPO / "frontend" / "public" / "data"

# name -> SELECT against the marts. `fleet_latest` is the freshest position per vehicle.
EXPORTS = {
    "pipeline_freshness": "SELECT * FROM main_marts.mart_pipeline_freshness",
    "otp_by_route": "SELECT * FROM main_marts.mart_otp_by_route",
    "otp_by_mode_hour": "SELECT * FROM main_marts.mart_otp_by_mode_hour",
    "congestion_hotspots": "SELECT * FROM main_marts.mart_congestion_hotspots ORDER BY congestion_rank LIMIT 200",
    "congestion_by_hour": "SELECT * FROM main_marts.mart_congestion_by_hour",
    "delay_vs_weather": "SELECT * FROM main_marts.mart_delay_vs_weather",
    "delay_vs_congestion": "SELECT * FROM main_marts.mart_delay_vs_congestion",
    "fleet_latest": """
        SELECT agency, mode, route_id, route_name, lat, lon, heading, is_delayed, report_ts
        FROM main_marts.fact_vehicle_position
        QUALIFY row_number() OVER (PARTITION BY mode, vehicle_id ORDER BY report_ts DESC) = 1
    """,
}


def main() -> None:
    if not SRC.exists():
        raise SystemExit(f"Source lake not found: {SRC} (build it first with dbt build)")
    OUT.mkdir(parents=True, exist_ok=True)

    with duckdb.connect(str(SRC), read_only=True) as con:
        for name, sql in EXPORTS.items():
            dest = (OUT / f"{name}.parquet").as_posix()
            con.execute(f"COPY ({sql}) TO '{dest}' (FORMAT PARQUET)")
            rows = con.execute(f"SELECT count(*) FROM ({sql})").fetchone()[0]
            kb = (OUT / f"{name}.parquet").stat().st_size / 1000
            print(f"  {name:22s} {rows:>7,} rows  {kb:>7.1f} KB")
    print(f"wrote {len(EXPORTS)} parquet files to {OUT}")


if __name__ == "__main__":
    main()
