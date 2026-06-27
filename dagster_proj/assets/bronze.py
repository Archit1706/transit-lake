"""Bronze-layer assets: land raw source data, immutable, partitioned by date.

Phase 0 ships these as the asset-graph skeleton for the thin end-to-end loop
(CTA static + Chicago congestion + weather). Each body is filled in Phase 1
with the real ingestion code from the `ingestion/` package.
"""
import dagster as dg
from dagster import AssetExecutionContext

# Daily partitions so every bronze snapshot lands under dt=YYYY-MM-DD.
daily_partitions = dg.DailyPartitionsDefinition(start_date="2026-06-01")

BRONZE_GROUP = "bronze"


@dg.asset(group_name=BRONZE_GROUP, partitions_def=daily_partitions, kinds={"python", "parquet"})
def cta_gtfs_static_bronze(context: AssetExecutionContext) -> dg.MaterializeResult:
    """CTA static GTFS (routes, stops, trips, stop_times, ...) — one dated snapshot."""
    context.log.info("Phase 0 skeleton — ingestion wired up in Phase 1.")
    return dg.MaterializeResult(metadata={"status": "scaffold"})


@dg.asset(group_name=BRONZE_GROUP, kinds={"python", "parquet"})
def chicago_congestion_bronze(context: AssetExecutionContext) -> dg.MaterializeResult:
    """Chicago historical congestion-by-segment slice (Socrata sxs8-h27x)."""
    context.log.info("Phase 0 skeleton — ingestion wired up in Phase 1.")
    return dg.MaterializeResult(metadata={"status": "scaffold"})


@dg.asset(group_name=BRONZE_GROUP, partitions_def=daily_partitions, kinds={"python", "parquet"})
def weather_bronze(context: AssetExecutionContext) -> dg.MaterializeResult:
    """Open-Meteo daily weather for Chicago (keyless enrichment)."""
    context.log.info("Phase 0 skeleton — ingestion wired up in Phase 1.")
    return dg.MaterializeResult(metadata={"status": "scaffold"})
