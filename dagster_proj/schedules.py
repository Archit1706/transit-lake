"""Schedules: the daily static pull and the frequent real-time poll.

The RT poll defaults to every 2 minutes — covering all ~126 CTA routes costs ~13
BusTime calls per poll, and a key's ~10k/day cap makes 2-minute polling the
fastest sustainable cadence (see ingestion/gtfs_rt.py).
"""
import dagster as dg

from dagster_proj.assets.bronze import (
    cta_gtfs_static_bronze,
    cta_train_positions_bronze,
    cta_vehicle_positions_bronze,
    weather_bronze,
)

# Real-time positions (bus + train): poll on a short interval (the volume driver).
rt_poll_job = dg.define_asset_job(
    "rt_poll_job",
    selection=[cta_vehicle_positions_bronze, cta_train_positions_bronze],
    partitions_def=cta_vehicle_positions_bronze.partitions_def,
)


@dg.schedule(job=rt_poll_job, cron_schedule="*/2 * * * *")
def rt_poll_schedule(context: dg.ScheduleEvaluationContext):
    """Every 2 minutes, poll bus + train positions into today's partition."""
    today = context.scheduled_execution_time.strftime("%Y-%m-%d")
    return dg.RunRequest(partition_key=today)


# Daily static refresh: new GTFS snapshot + that day's weather.
daily_static_job = dg.define_asset_job(
    "daily_static_job",
    selection=[cta_gtfs_static_bronze, weather_bronze],
    partitions_def=cta_gtfs_static_bronze.partitions_def,
)


@dg.schedule(job=daily_static_job, cron_schedule="0 6 * * *")
def daily_static_schedule(context: dg.ScheduleEvaluationContext):
    """Each morning at 06:00, refresh the static snapshot + weather for today."""
    today = context.scheduled_execution_time.strftime("%Y-%m-%d")
    return dg.RunRequest(partition_key=today)
