"""Bronze-layer assets: land raw source data, immutable, partitioned by date.

The thin end-to-end loop covers CTA (static + real-time), Chicago congestion +
traffic counts, and weather. Each asset delegates to the `ingestion` package and
reports row counts as Dagster metadata.
"""
import datetime as dt

import dagster as dg
from dagster import AssetExecutionContext

from ingestion import config, gtfs_rt, gtfs_static, socrata, weather

# Daily partitions so every dated snapshot lands under dt=YYYY-MM-DD.
daily_partitions = dg.DailyPartitionsDefinition(start_date="2026-06-01")

BRONZE_GROUP = "bronze"


@dg.asset(group_name=BRONZE_GROUP, partitions_def=daily_partitions, kinds={"python", "parquet"})
def cta_gtfs_static_bronze(context: AssetExecutionContext) -> dg.MaterializeResult:
    """CTA static GTFS (routes, stops, trips, stop_times, shapes, ...) — dated snapshot."""
    snapshot = dt.date.fromisoformat(context.partition_key)
    result = gtfs_static.ingest_static("cta", config.CTA_GTFS_STATIC_URL, snapshot)
    tables = result["tables"]
    return dg.MaterializeResult(
        metadata={
            "snapshot_date": result["snapshot_date"],
            "stop_times_rows": tables.get("stop_times", 0),
            "total_rows": sum(tables.values()),
            "tables": dg.MetadataValue.json(tables),
            "path": result["path"],
        }
    )


@dg.asset(group_name=BRONZE_GROUP, partitions_def=daily_partitions, kinds={"python", "parquet"})
def cta_vehicle_positions_bronze(context: AssetExecutionContext) -> dg.MaterializeResult:
    """CTA live vehicle positions (BusTime) — one poll appended per materialization."""
    result = gtfs_rt.poll_once()
    context.log.info(f"Polled {result['routes']} routes, {result['rows']} vehicles")
    return dg.MaterializeResult(
        metadata={
            "vehicle_rows": result["rows"],
            "routes_polled": result["routes"],
            "path": result["path"],
        }
    )


@dg.asset(group_name=BRONZE_GROUP, partitions_def=daily_partitions, kinds={"python", "parquet"})
def weather_bronze(context: AssetExecutionContext) -> dg.MaterializeResult:
    """Open-Meteo daily weather for Chicago for the partition date (archive lags ~5 days)."""
    day = dt.date.fromisoformat(context.partition_key)
    result = weather.ingest(day, day)
    return dg.MaterializeResult(metadata={"rows": result["rows"], "date": result["start"], "path": result["path"]})


@dg.asset(group_name=BRONZE_GROUP, kinds={"python", "parquet"})
def chicago_congestion_bronze(context: AssetExecutionContext) -> dg.MaterializeResult:
    """Chicago historical congestion-by-segment slice (Socrata sxs8-h27x), bulk-loaded once."""
    result = socrata.ingest_congestion(where="time >= '2018-01-01'", max_rows=2_000_000)
    context.log.info(f"Loaded {result['rows']} congestion rows in {result['parts']} parts")
    return dg.MaterializeResult(metadata={"rows": result["rows"], "parts": result["parts"], "path": result["path"]})


@dg.asset(group_name=BRONZE_GROUP, kinds={"python", "parquet"})
def chicago_adt_bronze(context: AssetExecutionContext) -> dg.MaterializeResult:
    """Average Daily Traffic counts (Socrata pfsx-4n4m) — small reference dimension."""
    result = socrata.ingest_adt()
    return dg.MaterializeResult(metadata={"rows": result["rows"], "path": result["path"]})
