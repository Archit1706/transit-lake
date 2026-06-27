"""Dagster code location entrypoint: collect assets, resources, schedules, sensors."""
import dagster as dg

from dagster_proj.assets import bronze
from dagster_proj.resources import duckdb_resource
from dagster_proj.schedules import (
    daily_static_job,
    daily_static_schedule,
    rt_poll_job,
    rt_poll_schedule,
)

bronze_assets = dg.load_assets_from_modules([bronze])

defs = dg.Definitions(
    assets=bronze_assets,
    jobs=[rt_poll_job, daily_static_job],
    schedules=[rt_poll_schedule, daily_static_schedule],
    resources={
        "duckdb": duckdb_resource,
    },
)
