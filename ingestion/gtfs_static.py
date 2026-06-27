"""Static GTFS ingestion: download an agency's google_transit.zip, unzip, and land
each table (routes, stops, trips, stop_times, ...) as Parquet in a dated bronze
partition.

Bronze keeps every column as a string — GTFS is all text on the wire and some ids
carry leading zeros — so silver owns typecasting. stop_times dominates volume
(millions of rows per snapshot).
"""
from __future__ import annotations

import datetime as dt
import io
import zipfile
from typing import Any

import polars as pl
import requests

from ingestion import config

REQUEST_TIMEOUT = 120


def download_zip(url: str) -> bytes:
    """Stream-download a GTFS zip into memory."""
    resp = requests.get(url, timeout=REQUEST_TIMEOUT, stream=True)
    resp.raise_for_status()
    return resp.content


def ingest_static(agency: str, url: str, snapshot_date: dt.date | None = None) -> dict[str, Any]:
    """Download + unzip one agency's static GTFS into bronze/<agency>/gtfs_static/dt=.../*.parquet."""
    snapshot_date = snapshot_date or dt.date.today()
    raw = download_zip(url)
    now = dt.datetime.now()

    part_dir = config.bronze_path(agency, "gtfs_static", f"dt={snapshot_date:%Y-%m-%d}")
    tables: dict[str, int] = {}

    with zipfile.ZipFile(io.BytesIO(raw)) as zf:
        for name in zf.namelist():
            if not name.endswith(".txt"):
                continue
            table = name[:-4]  # strip .txt
            with zf.open(name) as fh:
                # infer_schema_length=0 -> read every column as Utf8 (land as received).
                frame = pl.read_csv(fh.read(), infer_schema_length=0)
            frame = frame.with_columns(pl.lit(now).alias("_ingested_at"))
            frame.write_parquet(part_dir / f"{table}.parquet")
            tables[table] = frame.height

    return {"agency": agency, "snapshot_date": snapshot_date.isoformat(), "tables": tables, "path": str(part_dir)}


if __name__ == "__main__":
    print(ingest_static("cta", config.CTA_GTFS_STATIC_URL))
