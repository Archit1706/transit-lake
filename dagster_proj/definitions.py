"""Dagster code location entrypoint: collect assets, resources, schedules, sensors."""
import dagster as dg

from dagster_proj import checks
from dagster_proj.assets import bronze, silver
from dagster_proj.resources import duckdb_resource
from dagster_proj.schedules import (
    daily_static_job,
    daily_static_schedule,
    rt_poll_job,
    rt_poll_schedule,
)

bronze_assets = dg.load_assets_from_modules([bronze])
silver_assets = dg.load_assets_from_modules([silver])
asset_checks = dg.load_asset_checks_from_modules([checks])

defs = dg.Definitions(
    assets=[*bronze_assets, *silver_assets],
    asset_checks=asset_checks,
    jobs=[rt_poll_job, daily_static_job],
    schedules=[rt_poll_schedule, daily_static_schedule],
    resources={
        "duckdb": duckdb_resource,
    },
)
