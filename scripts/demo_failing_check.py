"""Demo: a bad row trips a blocking data-quality check.

Inserts an out-of-bounds vehicle position (lat/lon = 0,0 — off the coast of
Africa, not Chicago) into the silver table, then runs the blocking Dagster asset
check `positions_coords_valid`. The check fails — in a real run that halts every
downstream asset. The bad row is removed afterwards so the lake stays clean.

Run:  uv run python -m scripts.demo_failing_check
"""
import duckdb

from dagster_proj import checks
from dagster_proj.resources import duckdb_resource
from ingestion import config

BAD_VID = "DEMO_BAD_ROW"


def _insert_bad_row() -> None:
    con = duckdb.connect(str(config.DUCKDB_PATH))
    try:
        con.execute(
            """
            INSERT INTO silver_vehicle_positions
                (agency, mode, vehicle_id, route_id, headsign, lat, lon, heading,
                 is_delayed, report_ts, _ingested_at)
            VALUES ('CTA', 'bus', ?, '99', 'NOWHERE', 0.0, 0.0, 0,
                    false, now()::timestamp, now()::timestamp)
            """,
            [BAD_VID],
        )
    finally:
        con.close()


def _cleanup() -> None:
    con = duckdb.connect(str(config.DUCKDB_PATH))
    try:
        con.execute("DELETE FROM silver_vehicle_positions WHERE vehicle_id = ?", [BAD_VID])
    finally:
        con.close()


def main() -> None:
    print("1. Running blocking check on clean data...")
    before = checks.positions_coords_valid(duckdb_resource)
    print(f"   passed={before.passed}  violations={before.metadata['violations'].value}")

    print("2. Injecting a bad row (lat/lon = 0,0)...")
    _insert_bad_row()
    try:
        after = checks.positions_coords_valid(duckdb_resource)
        print(f"   passed={after.passed}  violations={after.metadata['violations'].value}")
        assert after.passed is False, "expected the check to FAIL on the bad row"
        print("   -> blocking check FAILED as expected; downstream assets would not run.")
    finally:
        print("3. Removing the bad row...")
        _cleanup()
        final = checks.positions_coords_valid(duckdb_resource)
        print(f"   passed={final.passed}  (lake restored)")


if __name__ == "__main__":
    main()
