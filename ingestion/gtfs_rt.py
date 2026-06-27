"""CTA real-time vehicle positions via the BusTime API.

CTA does not publish a public GTFS-Realtime protobuf feed for buses; the official
real-time source is the BusTime JSON API (`getvehicles`). It carries the same
payload a GTFS-RT VehiclePosition would (lat/lon, heading, route, trip, delay
flag, timestamp), so this is our volume driver — polled on a short interval, each
poll appended as a Parquet file under a dated bronze partition.

Rate limit: a BusTime key is capped (historically ~10,000 requests/day). Covering
all ~126 routes takes ~13 calls per poll, so:
    60s  poll -> ~18,700 req/day  (exceeds a 10k cap)
    120s poll -> ~9,400  req/day  (safe under 10k)
Set CTA_RT_POLL_SECONDS accordingly; default 120s stays under the cap while still
landing ~1M+ rows/day.
"""
from __future__ import annotations

import datetime as dt
from typing import Any

import polars as pl
import requests

from ingestion import config

BUSTIME_BASE = "https://www.ctabustracker.com/bustime/api/v2"
ROUTES_PER_CALL = 10  # BusTime getvehicles accepts up to 10 routes per request
REQUEST_TIMEOUT = 30

# Explicit schema so empty polls still write a well-typed Parquet file.
VEHICLE_SCHEMA = {
    "vid": pl.Utf8,          # vehicle id
    "tmstmp": pl.Datetime,   # report timestamp
    "lat": pl.Float64,
    "lon": pl.Float64,
    "hdg": pl.Int32,         # heading, degrees
    "pid": pl.Int64,         # pattern id
    "rt": pl.Utf8,           # route
    "des": pl.Utf8,          # destination
    "pdist": pl.Int64,       # distance into pattern (feet)
    "dly": pl.Boolean,       # delayed flag
    "tatripid": pl.Utf8,
    "origtatripno": pl.Utf8,
    "tablockid": pl.Utf8,
    "zone": pl.Utf8,
    "_ingested_at": pl.Datetime,
}


def _get(endpoint: str, params: dict[str, Any]) -> dict[str, Any]:
    params = {"key": config.CTA_BUS_API_KEY, "format": "json", **params}
    resp = requests.get(f"{BUSTIME_BASE}/{endpoint}", params=params, timeout=REQUEST_TIMEOUT)
    resp.raise_for_status()
    return resp.json().get("bustime-response", {})


def get_all_routes() -> list[str]:
    """Return every active CTA bus route id."""
    body = _get("getroutes", {})
    return [r["rt"] for r in body.get("routes", [])]


def _chunk(items: list[str], size: int) -> list[list[str]]:
    return [items[i : i + size] for i in range(0, len(items), size)]


def fetch_vehicles(routes: list[str]) -> list[dict[str, Any]]:
    """Fetch live vehicle positions for the given routes, batching ≤10 per call."""
    records: list[dict[str, Any]] = []
    for batch in _chunk(routes, ROUTES_PER_CALL):
        body = _get("getvehicles", {"rt": ",".join(batch), "tmres": "s"})
        # BusTime returns {"error": [...]} for batches with no active vehicles — skip quietly.
        records.extend(body.get("vehicle", []))
    return records


def _to_frame(records: list[dict[str, Any]], ingested_at: dt.datetime) -> pl.DataFrame:
    rows = []
    for v in records:
        rows.append(
            {
                "vid": v.get("vid"),
                "tmstmp": _parse_ts(v.get("tmstmp")),
                "lat": _f(v.get("lat")),
                "lon": _f(v.get("lon")),
                "hdg": _i(v.get("hdg")),
                "pid": v.get("pid"),
                "rt": v.get("rt"),
                "des": v.get("des"),
                "pdist": v.get("pdist"),
                "dly": bool(v.get("dly", False)),
                "tatripid": v.get("tatripid"),
                "origtatripno": v.get("origtatripno"),
                "tablockid": v.get("tablockid"),
                "zone": v.get("zone") or None,
                "_ingested_at": ingested_at,
            }
        )
    return pl.DataFrame(rows, schema=VEHICLE_SCHEMA)


def _parse_ts(s: str | None) -> dt.datetime | None:
    if not s:
        return None
    return dt.datetime.strptime(s, "%Y%m%d %H:%M:%S")


def _f(s: Any) -> float | None:
    try:
        return float(s)
    except (TypeError, ValueError):
        return None


def _i(s: Any) -> int | None:
    try:
        return int(s)
    except (TypeError, ValueError):
        return None


def poll_once() -> dict[str, Any]:
    """Run one poll cycle: fetch all vehicles, write a dated bronze Parquet file.

    Returns metadata (row count, file path) for the Dagster asset.
    """
    now = dt.datetime.now()
    routes = get_all_routes()
    records = fetch_vehicles(routes)
    frame = _to_frame(records, ingested_at=now)

    part_dir = config.bronze_path("cta", "gtfs_rt/vehicle_positions", f"dt={now:%Y-%m-%d}")
    out = part_dir / f"vehicles_{now:%Y%m%dT%H%M%S}.parquet"
    frame.write_parquet(out)

    return {"rows": frame.height, "routes": len(routes), "path": str(out)}


if __name__ == "__main__":
    print(poll_once())
