"""Great Expectations validation suites for the bronze→silver gates.

Each suite is a list of expectations run against a pandas frame via GE's ephemeral
context. `validate(name, df)` returns a compact result dict the Dagster asset checks
consume. This is the ingest-time DQ layer; dbt tests are the in-warehouse layer.
"""
from __future__ import annotations

import os

os.environ.setdefault("TQDM_DISABLE", "1")  # silence GE metric progress bars

from typing import Any

import pandas as pd

import great_expectations as gx
from great_expectations import expectations as gxe

# Chicago bounding box (generous) for coordinate sanity checks.
LAT_MIN, LAT_MAX = 41.0, 43.0
LON_MIN, LON_MAX = -89.0, -87.0

# name -> list of expectations. Columns match the bronze/silver schemas.
SUITES: dict[str, list[gx.expectations.Expectation]] = {
    "bronze_vehicle_positions": [
        gxe.ExpectTableRowCountToBeBetween(min_value=1),
        gxe.ExpectColumnValuesToNotBeNull(column="vid"),
        gxe.ExpectColumnValuesToNotBeNull(column="rt"),
        gxe.ExpectColumnValuesToNotBeNull(column="tmstmp"),
        gxe.ExpectColumnValuesToBeBetween(column="lat", min_value=LAT_MIN, max_value=LAT_MAX),
        gxe.ExpectColumnValuesToBeBetween(column="lon", min_value=LON_MIN, max_value=LON_MAX),
        gxe.ExpectColumnValuesToBeBetween(column="hdg", min_value=0, max_value=360),
    ],
    "bronze_train_positions": [
        gxe.ExpectTableRowCountToBeBetween(min_value=1),
        gxe.ExpectColumnValuesToNotBeNull(column="rn"),
        gxe.ExpectColumnValuesToNotBeNull(column="rt"),
        # Trains just leaving a terminal report lat/lon=0 until GPS locks; tolerate a few.
        gxe.ExpectColumnValuesToBeBetween(column="lat", min_value=LAT_MIN, max_value=LAT_MAX, mostly=0.8),
        gxe.ExpectColumnValuesToBeBetween(column="lon", min_value=LON_MIN, max_value=LON_MAX, mostly=0.8),
        gxe.ExpectColumnValuesToBeInSet(column="rt", value_set=["red", "blue", "brn", "g", "org", "p", "pink", "y"]),
    ],
    "silver_congestion": [
        gxe.ExpectTableRowCountToBeBetween(min_value=1),
        gxe.ExpectColumnValuesToNotBeNull(column="segment_id"),
        gxe.ExpectColumnValuesToNotBeNull(column="report_ts"),
        gxe.ExpectColumnValuesToBeBetween(column="speed_mph", min_value=0, max_value=80),
        gxe.ExpectColumnValuesToBeBetween(column="hour", min_value=0, max_value=23),
    ],
    "silver_weather": [
        gxe.ExpectColumnValuesToNotBeNull(column="weather_date"),
        gxe.ExpectColumnValuesToBeBetween(column="temp_max_c", min_value=-40, max_value=50),
        gxe.ExpectColumnValuesToBeBetween(column="precip_mm", min_value=0, max_value=500),
    ],
}


def validate(name: str, df: pd.DataFrame) -> dict[str, Any]:
    """Run the named suite against df; return {success, total, passed, results:[...]}."""
    if name not in SUITES:
        raise KeyError(f"Unknown suite: {name}")

    ctx = gx.get_context(mode="ephemeral")
    asset = ctx.data_sources.add_pandas(name).add_dataframe_asset(name)
    batch_def = asset.add_batch_definition_whole_dataframe("batch")
    suite = ctx.suites.add(gx.ExpectationSuite(name=name))
    for exp in SUITES[name]:
        suite.add_expectation(exp)

    result = batch_def.get_batch(batch_parameters={"dataframe": df}).validate(suite)
    results = [
        {
            "expectation": r.expectation_config.type,
            "column": r.expectation_config.kwargs.get("column"),
            "success": r.success,
        }
        for r in result.results
    ]
    return {
        "success": result.success,
        "total": len(results),
        "passed": sum(1 for r in results if r["success"]),
        "results": results,
    }


def suite_size(name: str) -> int:
    return len(SUITES[name])
