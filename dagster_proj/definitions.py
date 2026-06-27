"""Dagster code location entrypoint: collect assets, resources, schedules, sensors."""
from __future__ import annotations

import dagster as dg

from dagster_proj.assets import bronze
from dagster_proj.resources import duckdb_resource

bronze_assets = dg.load_assets_from_modules([bronze])

defs = dg.Definitions(
    assets=bronze_assets,
    resources={
        "duckdb": duckdb_resource,
    },
)
