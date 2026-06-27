"""Normalise CTA Train Tracker JSON into canonical GTFS-Realtime protobuf.

CTA publishes no native GTFS-RT feed for trains — only the proprietary Train
Tracker JSON API. This module encodes that JSON into a real GTFS-RT `FeedMessage`
(VehiclePosition entities) using the official `gtfs-realtime-bindings`, lands the
serialized `.pb` as the canonical bronze artifact, and decodes it back with the
same bindings in silver. So the protobuf is on the critical path, not decorative.

Field mapping (Train Tracker -> GTFS-RT VehiclePosition):
    rn         -> entity.id / vehicle.vehicle.id
    rt         -> vehicle.trip.route_id
    lat/lon    -> vehicle.position.latitude/longitude
    heading    -> vehicle.position.bearing
    nextStpId  -> vehicle.stop_id
    isApp      -> current_status (INCOMING_AT vs IN_TRANSIT_TO)
    isDly      -> congestion_level (CONGESTION vs RUNNING_SMOOTHLY) — VehiclePosition
                  has no schedule-delay field, so the boolean rides congestion_level
    prdt       -> vehicle.timestamp (POSIX)
"""
from __future__ import annotations

import datetime as dt
from typing import Any, Iterable

from google.transit import gtfs_realtime_pb2 as gtfsrt

VP = gtfsrt.VehiclePosition
_EPOCH_UTC = dt.timezone.utc


def _to_epoch(ts: dt.datetime | None) -> int | None:
    if ts is None:
        return None
    return int(ts.replace(tzinfo=_EPOCH_UTC).timestamp())


def _from_epoch(secs: int) -> dt.datetime:
    return dt.datetime.fromtimestamp(secs, _EPOCH_UTC).replace(tzinfo=None)


def encode_trains(trains: Iterable[dict[str, Any]], header_ts: dt.datetime) -> bytes:
    """Encode normalised train dicts into a serialized GTFS-RT FeedMessage.

    Each train dict carries: id, route_id, lat, lon, bearing, is_delayed,
    is_approaching, stop_id, report_ts (datetime|None). Out-of-range coordinates
    (trains report 0/0 until GPS locks) are skipped.
    """
    feed = gtfsrt.FeedMessage()
    feed.header.gtfs_realtime_version = "2.0"
    feed.header.incrementality = gtfsrt.FeedHeader.FULL_DATASET
    feed.header.timestamp = _to_epoch(header_ts) or 0

    for t in trains:
        lat, lon = t.get("lat"), t.get("lon")
        if lat is None or lon is None or not (41.0 <= lat <= 43.0 and -89.0 <= lon <= -87.0):
            continue
        entity = feed.entity.add()
        entity.id = str(t.get("id") or "")
        vp = entity.vehicle
        vp.vehicle.id = str(t.get("id") or "")
        vp.trip.route_id = str(t.get("route_id") or "")
        vp.position.latitude = float(lat)
        vp.position.longitude = float(lon)
        if t.get("bearing") is not None:
            vp.position.bearing = float(t["bearing"])
        if t.get("stop_id"):
            vp.stop_id = str(t["stop_id"])
        vp.current_status = VP.INCOMING_AT if t.get("is_approaching") else VP.IN_TRANSIT_TO
        vp.congestion_level = VP.CONGESTION if t.get("is_delayed") else VP.RUNNING_SMOOTHLY
        epoch = _to_epoch(t.get("report_ts"))
        if epoch:
            vp.timestamp = epoch

    return feed.SerializeToString()


def decode_feed(raw: bytes) -> list[dict[str, Any]]:
    """Decode a serialized GTFS-RT FeedMessage into flat vehicle-position records."""
    feed = gtfsrt.FeedMessage()
    feed.ParseFromString(raw)
    records: list[dict[str, Any]] = []
    for entity in feed.entity:
        if not entity.HasField("vehicle"):
            continue
        vp = entity.vehicle
        records.append(
            {
                "vehicle_id": vp.vehicle.id or None,
                "route_id": vp.trip.route_id or None,
                "lat": vp.position.latitude,
                "lon": vp.position.longitude,
                "heading": int(vp.position.bearing) if vp.position.HasField("bearing") else None,
                "is_delayed": vp.congestion_level == VP.CONGESTION,
                "stop_id": vp.stop_id or None,
                "report_ts": _from_epoch(vp.timestamp) if vp.HasField("timestamp") else None,
            }
        )
    return records


def normalize_live(raw: dict[str, Any]) -> dict[str, Any]:
    """Map a raw Train Tracker JSON record to the normalised encode input."""
    def _f(v: Any) -> float | None:
        try:
            return float(v)
        except (TypeError, ValueError):
            return None

    prdt = raw.get("prdt")
    return {
        "id": raw.get("rn"),
        "route_id": raw.get("rt"),
        "lat": _f(raw.get("lat")),
        "lon": _f(raw.get("lon")),
        "bearing": _f(raw.get("heading")),
        "is_approaching": raw.get("isApp") == "1",
        "is_delayed": raw.get("isDly") == "1",
        "stop_id": raw.get("nextStpId"),
        "report_ts": dt.datetime.fromisoformat(prdt) if prdt else None,
    }
