"""Central configuration: lake paths and credentials, loaded from the environment.

Both the Dagster project and standalone ingestion scripts import from here so
there is a single source of truth for where bronze/silver live and which keys exist.
"""
from __future__ import annotations

import os
from pathlib import Path

from dotenv import load_dotenv

# Load .env from the repo root (one level above this file's package).
REPO_ROOT = Path(__file__).resolve().parent.parent
load_dotenv(REPO_ROOT / ".env")

# --- Lake layout -----------------------------------------------------------
LAKE_ROOT = Path(os.getenv("TRANSITLAKE_ROOT", REPO_ROOT / "lake")).resolve()
BRONZE = LAKE_ROOT / "bronze"
SILVER = LAKE_ROOT / "silver"
DUCKDB_PATH = LAKE_ROOT / "transitlake.duckdb"


def bronze_path(source: str, dataset: str, *parts: str) -> Path:
    """Return (and create) a bronze partition dir, e.g. bronze_path('cta', 'gtfs_static', 'dt=2026-06-26')."""
    p = BRONZE.joinpath(source, dataset, *parts)
    p.mkdir(parents=True, exist_ok=True)
    return p


# --- Credentials -----------------------------------------------------------
CTA_BUS_API_KEY = os.getenv("CTA_BUS_API_KEY", "")
CTA_TRAIN_API_KEY = os.getenv("CTA_TRAIN_API_KEY", "")
SOCRATA_APP_TOKEN = os.getenv("SOCRATA_APP_TOKEN") or None  # sodapy accepts None
METRA_GTFS_RT_USER = os.getenv("METRA_GTFS_RT_USER", "")
METRA_GTFS_RT_PASS = os.getenv("METRA_GTFS_RT_PASS", "")

# --- Static source endpoints (no key required) ----------------------------
CTA_GTFS_STATIC_URL = "https://www.transitchicago.com/downloads/sch_data/google_transit.zip"
PACE_GTFS_STATIC_URL = "https://www.pacebus.com/sites/default/files/2024-01/GTFS.zip"
METRA_GTFS_STATIC_URL = "https://schedules.metrarail.com/gtfs/schedule.zip"

SOCRATA_DOMAIN = "data.cityofchicago.org"
SOCRATA_CONGESTION_ID = "sxs8-h27x"   # Historical congestion by segment
SOCRATA_ADT_ID = "pfsx-4n4m"          # Average Daily Traffic Counts

OPEN_METEO_ARCHIVE_URL = "https://archive-api.open-meteo.com/v1/archive"
CHICAGO_LAT, CHICAGO_LON = 41.8781, -87.6298
