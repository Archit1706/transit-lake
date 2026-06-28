// DuckDB-WASM data layer: initialise once, register the marts Parquet files, and
// run SQL entirely in the browser. Each Parquet export is exposed as a view named
// after the file, so callers write plain `SELECT ... FROM otp_by_route`.
import * as duckdb from "@duckdb/duckdb-wasm";

const DATASETS = [
  "pipeline_freshness",
  "otp_by_route",
  "otp_by_mode_hour",
  "congestion_hotspots",
  "congestion_by_hour",
  "delay_vs_weather",
  "delay_vs_congestion",
  "fleet_latest",
] as const;

let dbPromise: Promise<duckdb.AsyncDuckDB> | null = null;

async function initDB(): Promise<duckdb.AsyncDuckDB> {
  const bundles = duckdb.getJsDelivrBundles();
  const bundle = await duckdb.selectBundle(bundles);
  const workerUrl = URL.createObjectURL(
    new Blob([`importScripts("${bundle.mainWorker}");`], { type: "text/javascript" }),
  );
  const worker = new Worker(workerUrl);
  const db = new duckdb.AsyncDuckDB(new duckdb.VoidLogger(), worker);
  await db.instantiate(bundle.mainModule, bundle.pthreadWorker);
  URL.revokeObjectURL(workerUrl);

  const base = typeof window !== "undefined" ? window.location.origin : "";
  const conn = await db.connect();
  for (const name of DATASETS) {
    const url = `${base}/data/${name}.parquet`;
    await db.registerFileURL(name + ".parquet", url, duckdb.DuckDBDataProtocol.HTTP, false);
    await conn.query(`CREATE OR REPLACE VIEW ${name} AS SELECT * FROM read_parquet('${name}.parquet')`);
  }
  await conn.close();
  return db;
}

export function getDB(): Promise<duckdb.AsyncDuckDB> {
  if (!dbPromise) dbPromise = initDB();
  return dbPromise;
}

// Convert Arrow rows to plain JSON, coercing BigInt (counts) to Number for charts.
function normalize<T>(rows: Record<string, unknown>[]): T[] {
  return rows.map((r) => {
    const out: Record<string, unknown> = {};
    for (const [k, v] of Object.entries(r)) out[k] = typeof v === "bigint" ? Number(v) : v;
    return out as T;
  });
}

export async function query<T = Record<string, unknown>>(sql: string): Promise<T[]> {
  const db = await getDB();
  const conn = await db.connect();
  try {
    const res = await conn.query(sql);
    return normalize<T>(res.toArray().map((row) => row.toJSON() as Record<string, unknown>));
  } finally {
    await conn.close();
  }
}
