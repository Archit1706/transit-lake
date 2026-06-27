"""Standalone real-time poller loop for local accumulation.

Polls CTA bus + train positions every CTA_RT_POLL_SECONDS (default 120s) and lands
each poll as a dated bronze Parquet file. This is the lightweight alternative to
running `dagster dev` with rt_poll_schedule enabled — handy for leaving rows
accumulating in the background. Errors in one cycle are logged and the loop
continues.

Run:  uv run python -m ingestion.poller
"""
from __future__ import annotations

import datetime as dt
import os
import time

from ingestion import gtfs_rt

INTERVAL = int(os.getenv("CTA_RT_POLL_SECONDS", "120"))


def _log(msg: str) -> None:
    print(f"[{dt.datetime.now():%Y-%m-%d %H:%M:%S}] {msg}", flush=True)


def main() -> None:
    _log(f"poller starting, interval={INTERVAL}s")
    while True:
        cycle_start = time.monotonic()
        try:
            bus = gtfs_rt.poll_once()
            _log(f"bus: {bus['rows']} vehicles, {bus['routes']} routes")
        except Exception as exc:  # noqa: BLE001 - keep the loop alive
            _log(f"bus poll FAILED: {exc!r}")
        try:
            train = gtfs_rt.poll_trains_once()
            _log(f"train: {train['rows']} trains")
        except Exception as exc:  # noqa: BLE001
            _log(f"train poll FAILED: {exc!r}")

        elapsed = time.monotonic() - cycle_start
        time.sleep(max(0, INTERVAL - elapsed))


if __name__ == "__main__":
    main()
