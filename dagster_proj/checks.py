"""Dagster asset checks — the orchestration-level DQ gates.

Two flavors:
  * GE-backed checks run a Great Expectations suite (ingestion.quality) against a
    bronze/silver dataset and pass only if the whole suite passes.
  * Native checks run a single DuckDB assertion (a violation count that must be 0)
    against a silver table.

A failing check surfaces red on the asset in the Dagster UI and can block
downstream materialization — the "bad row blocks the pipeline" demo.
"""
import glob

import dagster as dg
import pandas as pd
from dagster_duckdb import DuckDBResource

from ingestion import config, quality


def _latest(*parts: str) -> str:
    matches = sorted(glob.glob(config.BRONZE.joinpath(*parts).as_posix()))
    if not matches:
        raise dg.Failure(f"No bronze files for {parts}")
    return matches[-1]


def _ge_result_to_check(name: str, df: pd.DataFrame) -> dg.AssetCheckResult:
    r = quality.validate(name, df)
    failed = [x["expectation"] for x in r["results"] if not x["success"]]
    return dg.AssetCheckResult(
        passed=r["success"],
        severity=dg.AssetCheckSeverity.ERROR,
        metadata={
            "suite": name,
            "expectations_total": r["total"],
            "expectations_passed": r["passed"],
            "failed": dg.MetadataValue.json(failed),
        },
    )


# --- GE-backed checks on bronze ingest -------------------------------------

@dg.asset_check(asset="cta_vehicle_positions_bronze", name="ge_bronze_vehicle_positions", blocking=True)
def ge_bronze_vehicle_positions() -> dg.AssetCheckResult:
    df = pd.read_parquet(_latest("cta", "gtfs_rt", "vehicle_positions", "dt=*", "*.parquet"))
    return _ge_result_to_check("bronze_vehicle_positions", df)


@dg.asset_check(asset="cta_train_positions_bronze", name="ge_bronze_train_positions", blocking=True)
def ge_bronze_train_positions() -> dg.AssetCheckResult:
    df = pd.read_parquet(_latest("cta", "train_tracker", "positions", "dt=*", "*.parquet"))
    return _ge_result_to_check("bronze_train_positions", df)


# --- GE-backed checks on silver --------------------------------------------

@dg.asset_check(asset="silver_congestion", name="ge_silver_congestion")
def ge_silver_congestion(duckdb: DuckDBResource) -> dg.AssetCheckResult:
    with duckdb.get_connection() as conn:
        df = conn.execute("SELECT * FROM silver_congestion").df()
    return _ge_result_to_check("silver_congestion", df)


@dg.asset_check(asset="silver_weather", name="ge_silver_weather")
def ge_silver_weather(duckdb: DuckDBResource) -> dg.AssetCheckResult:
    with duckdb.get_connection() as conn:
        df = conn.execute("SELECT * FROM silver_weather").df()
    return _ge_result_to_check("silver_weather", df)


# --- Native DuckDB assertion checks on silver ------------------------------

def _zero_violations(duckdb: DuckDBResource, table: str, where: str, desc: str) -> dg.AssetCheckResult:
    with duckdb.get_connection() as conn:
        bad = conn.execute(f"SELECT count(*) FROM {table} WHERE {where}").fetchone()[0]
    return dg.AssetCheckResult(
        passed=bad == 0,
        severity=dg.AssetCheckSeverity.ERROR,
        metadata={"violations": int(bad), "rule": desc},
    )


@dg.asset_check(asset="silver_vehicle_positions", name="positions_coords_valid", blocking=True)
def positions_coords_valid(duckdb: DuckDBResource) -> dg.AssetCheckResult:
    return _zero_violations(duckdb, "silver_vehicle_positions",
                            "lat NOT BETWEEN 41 AND 43 OR lon NOT BETWEEN -89 AND -87",
                            "all positions within Chicago bounds")


@dg.asset_check(asset="silver_vehicle_positions", name="positions_vehicle_id_not_null")
def positions_vehicle_id_not_null(duckdb: DuckDBResource) -> dg.AssetCheckResult:
    return _zero_violations(duckdb, "silver_vehicle_positions", "vehicle_id IS NULL",
                            "vehicle_id never null")


@dg.asset_check(asset="silver_vehicle_positions", name="positions_mode_valid")
def positions_mode_valid(duckdb: DuckDBResource) -> dg.AssetCheckResult:
    return _zero_violations(duckdb, "silver_vehicle_positions", "mode NOT IN ('bus', 'train')",
                            "mode is bus or train")


@dg.asset_check(asset="silver_vehicle_positions", name="positions_no_duplicate_reports")
def positions_no_duplicate_reports(duckdb: DuckDBResource) -> dg.AssetCheckResult:
    with duckdb.get_connection() as conn:
        dups = conn.execute("""
            SELECT count(*) FROM (
                SELECT mode, vehicle_id, report_ts, count(*) c
                FROM silver_vehicle_positions GROUP BY 1, 2, 3 HAVING count(*) > 1
            )
        """).fetchone()[0]
    return dg.AssetCheckResult(passed=dups == 0, metadata={"duplicate_keys": int(dups)})


@dg.asset_check(asset="silver_routes", name="routes_unique")
def routes_unique(duckdb: DuckDBResource) -> dg.AssetCheckResult:
    with duckdb.get_connection() as conn:
        dups = conn.execute(
            "SELECT count(*) FROM (SELECT route_id FROM silver_routes GROUP BY 1 HAVING count(*) > 1)"
        ).fetchone()[0]
    return dg.AssetCheckResult(passed=dups == 0, metadata={"duplicate_route_ids": int(dups)})


@dg.asset_check(asset="silver_stops", name="stops_coords_valid")
def stops_coords_valid(duckdb: DuckDBResource) -> dg.AssetCheckResult:
    return _zero_violations(duckdb, "silver_stops",
                            "stop_lat NOT BETWEEN 41 AND 43 OR stop_lon NOT BETWEEN -89 AND -87",
                            "all stops within Chicago bounds")


@dg.asset_check(asset="silver_congestion", name="congestion_speed_nonneg")
def congestion_speed_nonneg(duckdb: DuckDBResource) -> dg.AssetCheckResult:
    return _zero_violations(duckdb, "silver_congestion", "speed_mph < 0",
                            "speed never negative")


@dg.asset_check(asset="silver_weather", name="weather_date_unique")
def weather_date_unique(duckdb: DuckDBResource) -> dg.AssetCheckResult:
    with duckdb.get_connection() as conn:
        dups = conn.execute(
            "SELECT count(*) FROM (SELECT weather_date FROM silver_weather GROUP BY 1 HAVING count(*) > 1)"
        ).fetchone()[0]
    return dg.AssetCheckResult(passed=dups == 0, metadata={"duplicate_dates": int(dups)})
