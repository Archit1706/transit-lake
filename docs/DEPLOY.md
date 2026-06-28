# Deploying the dashboard (free)

The full lakehouse (Dagster daemon + real-time poller + ~140 MB warehouse) is
local-first. The **dashboard is read-only over the gold marts** (~114k rows), so
it deploys free as a small committed snapshot — `dashboard/marts.duckdb` (~11 MB).

The app picks its database in this order ([dashboard/app.py](../dashboard/app.py)):
1. `TRANSITLAKE_DB` env var, if set
2. the live local lake `lake/transitlake.duckdb`, if present (local dev)
3. the bundled `dashboard/marts.duckdb` snapshot (deployments)

## Refresh the snapshot

The deployed dashboard shows data frozen at export time. To update it:

```bash
uv run python -m scripts.export_marts_snapshot   # rewrites dashboard/marts.duckdb
git add dashboard/marts.duckdb && git commit -m "data: refresh marts snapshot" && git push
```

Streamlit Cloud redeploys automatically on push.

## Streamlit Community Cloud

1. Push to GitHub (done — `Archit1706/transit-lake`).
2. Go to <https://share.streamlit.io> → sign in with GitHub.
3. **New app** → pick repo `Archit1706/transit-lake`, branch `master`,
   main file path `dashboard/app.py`.
4. Deploy. It installs from [`requirements.txt`](../requirements.txt) (dashboard-only
   deps — not the full pipeline stack) and serves the app at a public URL.

No secrets are needed: the app reads the bundled snapshot, hits no APIs.

## Next.js frontend on Vercel (free)

The repo also ships a Next.js dashboard in [`frontend/`](../frontend) that runs
**SQL in the browser with DuckDB-WASM** over Parquet exports of the marts — a
fully static site, no backend.

1. Export the marts to Parquet (writes `frontend/public/data/*.parquet`):
   ```bash
   uv run python -m scripts.export_marts_parquet
   git add frontend/public/data && git commit -m "data: refresh frontend marts" && git push
   ```
2. Go to <https://vercel.com/new> → import `Archit1706/transit-lake`.
3. Set **Root Directory = `frontend`** (Next.js auto-detected). No env vars needed.
4. Deploy. Vercel redeploys on every push.

The DuckDB-WASM runtime loads from a CDN at runtime; the Parquet files are served
as static assets. Same refresh model as the Streamlit snapshot — data is frozen
at export time.

## Other platforms

- **Hugging Face Spaces** — create a *Streamlit* Space, push this repo; add the
  same `requirements.txt`. More storage headroom if the snapshot grows.
- **Render** — a free web service with start command
  `streamlit run dashboard/app.py --server.port $PORT --server.address 0.0.0.0`.
  Works, but the free tier sleeps after ~15 min idle (slow cold starts).
