"""Open-Meteo daily historical weather for Chicago (keyless enrichment).

Lands one Parquet file per ingest covering a requested date range, used in gold
to correlate transit delays and road congestion against weather.
"""
from __future__ import annotations

import datetime as dt
from typing import Any

import polars as pl
import requests

from ingestion import config

DAILY_VARS = [
    "temperature_2m_max",
    "temperature_2m_min",
    "precipitation_sum",
    "rain_sum",
    "snowfall_sum",
    "wind_speed_10m_max",
    "wind_gusts_10m_max",
]
REQUEST_TIMEOUT = 60


def fetch_daily(start: dt.date, end: dt.date) -> pl.DataFrame:
    """Fetch daily weather for [start, end] inclusive."""
    params = {
        "latitude": config.CHICAGO_LAT,
        "longitude": config.CHICAGO_LON,
        "start_date": start.isoformat(),
        "end_date": end.isoformat(),
        "daily": ",".join(DAILY_VARS),
        "timezone": "America/Chicago",
    }
    resp = requests.get(config.OPEN_METEO_ARCHIVE_URL, params=params, timeout=REQUEST_TIMEOUT)
    resp.raise_for_status()
    daily = resp.json()["daily"]

    frame = pl.DataFrame(daily).with_columns(
        pl.col("time").str.to_date().alias("date")
    ).drop("time")
    return frame.with_columns(pl.lit(dt.datetime.now()).alias("_ingested_at"))


def ingest(start: dt.date, end: dt.date) -> dict[str, Any]:
    """Fetch and land a date range of daily weather to bronze."""
    frame = fetch_daily(start, end)
    out_dir = config.bronze_path("open_meteo", "weather_daily")
    out = out_dir / f"weather_{start:%Y%m%d}_{end:%Y%m%d}.parquet"
    frame.write_parquet(out)
    return {"rows": frame.height, "start": start.isoformat(), "end": end.isoformat(), "path": str(out)}


if __name__ == "__main__":
    # Backfill ~2 years of history by default. Open-Meteo archive lags ~5 days.
    today = dt.date.today()
    print(ingest(today - dt.timedelta(days=730), today - dt.timedelta(days=5)))
