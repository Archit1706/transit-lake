"""One-time backfill: re-encode the existing Train Tracker JSON history into
canonical GTFS-RT protobuf, so silver's protobuf path keeps the full train
history rather than starting fresh.

Reads every bronze train JSON parquet, encodes a matching `.pb` next to it under
gtfs_rt/train_positions_pb, and skips any that already exist (idempotent).

Run:  uv run python -m scripts.backfill_train_protobuf
"""
import datetime as dt

import polars as pl

from ingestion import config
from ingestion import gtfs_rt_protobuf as pb


def _row_to_norm(r: dict) -> dict:
    return {
        "id": r.get("rn"),
        "route_id": r.get("rt"),
        "lat": r.get("lat"),
        "lon": r.get("lon"),
        "bearing": r.get("heading"),
        "is_approaching": bool(r.get("isApp")),
        "is_delayed": bool(r.get("isDly")),
        "stop_id": r.get("nextStpId"),
        "report_ts": r.get("tmstmp"),
    }


def main() -> None:
    json_files = sorted(config.BRONZE.glob("cta/train_tracker/positions/dt=*/*.parquet"))
    written = skipped = 0
    for jf in json_files:
        stamp = jf.stem.replace("trains_", "")  # YYYYMMDDTHHMMSS
        ts = dt.datetime.strptime(stamp, "%Y%m%dT%H%M%S")
        pb_dir = config.bronze_path("cta", "gtfs_rt/train_positions_pb", f"dt={ts:%Y-%m-%d}")
        pb_out = pb_dir / f"trains_{stamp}.pb"
        if pb_out.exists():
            skipped += 1
            continue
        rows = pl.read_parquet(jf).to_dicts()
        raw = pb.encode_trains((_row_to_norm(r) for r in rows), header_ts=ts)
        pb_out.write_bytes(raw)
        written += 1
    print(f"backfill complete: {written} written, {skipped} already present, {len(json_files)} total")


if __name__ == "__main__":
    main()
