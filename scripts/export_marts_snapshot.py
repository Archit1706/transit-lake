"""Export the gold marts to a small, committable DuckDB snapshot for deployment.

The full lake (lake/transitlake.duckdb, ~140 MB) is gitignored and needs a live
pipeline. The dashboard only reads the gold schema (~114k rows, a few MB), so this
copies just `main_marts.*` into dashboard/marts.duckdb — keeping the same schema
name so the app's queries work unchanged. Commit the result; the deployed app
falls back to it when the live lake isn't present.

Run:  uv run python -m scripts.export_marts_snapshot
"""
from pathlib import Path

import duckdb

REPO = Path(__file__).resolve().parent.parent
SRC = REPO / "lake" / "transitlake.duckdb"
OUT = REPO / "dashboard" / "marts.duckdb"


def main() -> None:
    if not SRC.exists():
        raise SystemExit(f"Source lake not found: {SRC} (build it first with dbt build)")

    with duckdb.connect(str(SRC), read_only=True) as src:
        tables = [
            r[0]
            for r in src.execute(
                "select table_name from information_schema.tables "
                "where table_schema = 'main_marts' order by table_name"
            ).fetchall()
        ]

    OUT.unlink(missing_ok=True)
    out = duckdb.connect(str(OUT))
    try:
        out.execute(f"ATTACH '{SRC.as_posix()}' AS lake_src (READ_ONLY)")
        out.execute("CREATE SCHEMA IF NOT EXISTS main_marts")
        for t in tables:
            out.execute(f"CREATE OR REPLACE TABLE main_marts.{t} AS SELECT * FROM lake_src.main_marts.{t}")
        out.execute("DETACH lake_src")
    finally:
        out.close()

    size_mb = OUT.stat().st_size / 1_000_000
    print(f"wrote {OUT} — {len(tables)} tables, {size_mb:.1f} MB")


if __name__ == "__main__":
    main()
