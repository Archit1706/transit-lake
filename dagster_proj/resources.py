"""Dagster resources: the DuckDB warehouse handle shared across assets."""
from __future__ import annotations

from dagster_duckdb import DuckDBResource

from ingestion import config

# A single embedded DuckDB file is the query engine over the Parquet lake.
duckdb_resource = DuckDBResource(database=str(config.DUCKDB_PATH))
