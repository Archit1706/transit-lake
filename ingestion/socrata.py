"""Socrata ingestion for the Chicago Data Portal.

Two datasets for the thin loop:
  - Historical congestion by segment (sxs8-h27x, ~275M rows) -> bulk-load a bounded
    slice as the high-volume road fact. Paged, one Parquet part file per page.
  - Average Daily Traffic counts (pfsx-4n4m) -> small reference dimension.

Works without an app token (throttled); set SOCRATA_APP_TOKEN to lift limits.
"""
from __future__ import annotations

import datetime as dt
from typing import Any, Iterator

import polars as pl
from sodapy import Socrata

from ingestion import config

PAGE_SIZE = 50_000
TIMEOUT = 120


def get_client() -> Socrata:
    return Socrata(config.SOCRATA_DOMAIN, config.SOCRATA_APP_TOKEN, timeout=TIMEOUT)


def _paged(
    client: Socrata,
    dataset_id: str,
    *,
    where: str | None = None,
    order: str | None = None,
    max_rows: int | None = None,
) -> Iterator[list[dict[str, Any]]]:
    """Yield successive pages of records until exhausted or max_rows reached."""
    offset = 0
    while True:
        remaining = None if max_rows is None else max_rows - offset
        if remaining is not None and remaining <= 0:
            return
        limit = PAGE_SIZE if remaining is None else min(PAGE_SIZE, remaining)
        page = client.get(dataset_id, where=where, order=order, limit=limit, offset=offset)
        if not page:
            return
        yield page
        offset += len(page)
        if len(page) < limit:
            return


def _flatten(records: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Drop nested Socrata point objects so the frame stays flat/columnar."""
    out = []
    for r in records:
        out.append({k: v for k, v in r.items() if not isinstance(v, dict)})
    return out


def ingest_congestion(
    where: str | None = None,
    max_rows: int | None = 2_000_000,
) -> dict[str, Any]:
    """Bulk-load a slice of historical segment congestion into bronze part files."""
    client = get_client()
    out_dir = config.bronze_path("socrata", "congestion")
    now = dt.datetime.now()
    total, part = 0, 0
    try:
        for page in _paged(client, config.SOCRATA_CONGESTION_ID, where=where,
                            order="time", max_rows=max_rows):
            frame = pl.DataFrame(_flatten(page), infer_schema_length=None).with_columns(
                pl.lit(now).alias("_ingested_at")
            )
            frame.write_parquet(out_dir / f"congestion_part_{part:05d}.parquet")
            total += frame.height
            part += 1
    finally:
        client.close()
    return {"rows": total, "parts": part, "path": str(out_dir)}


def ingest_adt() -> dict[str, Any]:
    """Load the small Average Daily Traffic counts reference table."""
    client = get_client()
    try:
        records = client.get(config.SOCRATA_ADT_ID, limit=PAGE_SIZE)
    finally:
        client.close()
    frame = pl.DataFrame(_flatten(records), infer_schema_length=None).with_columns(
        pl.lit(dt.datetime.now()).alias("_ingested_at")
    )
    out_dir = config.bronze_path("socrata", "adt")
    frame.write_parquet(out_dir / "adt.parquet")
    return {"rows": frame.height, "path": str(out_dir)}


if __name__ == "__main__":
    print("ADT:", ingest_adt())
    # Bounded smoke slice; raise max_rows / add a time $where for the real bulk load.
    print("Congestion:", ingest_congestion(where="time >= '2018-01-01'", max_rows=100_000))
