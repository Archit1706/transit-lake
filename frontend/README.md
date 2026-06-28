# TransitLake — Next.js frontend

A static Next.js dashboard for the TransitLake gold marts. It loads Parquet
exports and runs **SQL entirely in the browser with DuckDB-WASM** — no backend.

## Data

The marts are exported to `public/data/*.parquet` by the Python pipeline:

```bash
# from the repo root
uv run python -m scripts.export_marts_parquet
```

`lib/duck.ts` registers those Parquet files with DuckDB-WASM and exposes views;
the page queries them client-side. To refresh the deployed data, re-run the export
and commit `frontend/public/data/`.

## Develop / build

```bash
npm install
npm run dev      # http://localhost:3000
npm run build    # static production build
```

## Deploy (Vercel, free)

Import the repo at <https://vercel.com/new>, set **Root Directory = `frontend`**
(Next.js is auto-detected), deploy. No env vars or secrets needed — the app is
fully static and the DuckDB-WASM runtime loads from a CDN. See
[../docs/DEPLOY.md](../docs/DEPLOY.md).

## Stack

Next.js (App Router) · DuckDB-WASM · Recharts · MapLibre GL (free Carto basemap) · Tailwind.
