"use client";

import { useEffect, useState } from "react";
import dynamic from "next/dynamic";
import {
  Bar,
  BarChart,
  CartesianGrid,
  Cell,
  Line,
  LineChart,
  ResponsiveContainer,
  Tooltip,
  XAxis,
  YAxis,
} from "recharts";
import { query } from "@/lib/duck";
import type { FleetPoint } from "@/components/FleetMap";

const FleetMap = dynamic(() => import("@/components/FleetMap"), { ssr: false });

const BUS = "#4FC3F7";
const TRAIN = "#FF7043";
const ACCENT = "#66BB6A";

type Freshness = { source: string; row_count: number };
type OtpRoute = { route_name: string; mode: string; reports: number; otp: number };
type ByHour = { report_hour: number; avg_speed_mph: number };
type Hotspot = {
  segment_id: number; street: string; from_street: string; to_street: string;
  avg_speed_mph: number; worst_hour: number; congestion_rank: number;
};

function Panel({ title, children }: { title: string; children: React.ReactNode }) {
  return (
    <div className="rounded-xl border border-white/10 bg-white/[0.02] p-5">
      <h2 className="mb-4 text-sm font-semibold uppercase tracking-wide text-neutral-400">{title}</h2>
      {children}
    </div>
  );
}

export default function Home() {
  const [loading, setLoading] = useState(true);
  const [err, setErr] = useState<string | null>(null);
  const [fresh, setFresh] = useState<Freshness[]>([]);
  const [otp, setOtp] = useState<OtpRoute[]>([]);
  const [speedByHour, setSpeedByHour] = useState<ByHour[]>([]);
  const [hotspots, setHotspots] = useState<Hotspot[]>([]);
  const [fleet, setFleet] = useState<FleetPoint[]>([]);

  useEffect(() => {
    (async () => {
      try {
        setFresh(await query<Freshness>("SELECT source, CAST(row_count AS INT) row_count FROM pipeline_freshness"));
        setOtp(
          await query<OtpRoute>(`
            SELECT route_name, mode, CAST(total_reports AS INT) reports, CAST(on_time_rate AS DOUBLE) otp
            FROM otp_by_route WHERE total_reports >= 20 ORDER BY on_time_rate ASC LIMIT 15`),
        );
        setSpeedByHour(
          await query<ByHour>("SELECT CAST(report_hour AS INT) report_hour, CAST(avg_speed_mph AS DOUBLE) avg_speed_mph FROM congestion_by_hour ORDER BY report_hour"),
        );
        setHotspots(
          await query<Hotspot>(`
            SELECT CAST(segment_id AS INT) segment_id, street, from_street, to_street,
                   CAST(avg_speed_mph AS DOUBLE) avg_speed_mph, CAST(worst_hour AS INT) worst_hour,
                   CAST(congestion_rank AS INT) congestion_rank
            FROM congestion_hotspots ORDER BY congestion_rank LIMIT 12`),
        );
        setFleet(
          await query<FleetPoint>(`
            SELECT mode, route_id, CAST(lat AS DOUBLE) lat, CAST(lon AS DOUBLE) lon, is_delayed
            FROM fleet_latest`),
        );
      } catch (e) {
        setErr(String(e));
      } finally {
        setLoading(false);
      }
    })();
  }, []);

  const metric = (s: string) => fresh.find((f) => f.source === s)?.row_count ?? 0;

  return (
    <main className="mx-auto max-w-7xl px-6 py-10">
      <header className="mb-8">
        <h1 className="text-3xl font-bold tracking-tight">🚆 TransitLake</h1>
        <p className="mt-1 text-neutral-400">
          Chicago multi-modal transit lakehouse — querying the gold marts with{" "}
          <span className="text-neutral-200">DuckDB-WASM in your browser</span>.
        </p>
      </header>

      {err && <div className="rounded-lg border border-red-500/40 bg-red-500/10 p-4 text-red-300">{err}</div>}
      {loading && !err && <div className="animate-pulse text-neutral-400">Booting DuckDB-WASM and loading marts…</div>}

      {!loading && !err && (
        <>
          <div className="mb-8 grid grid-cols-2 gap-4 md:grid-cols-4">
            {[
              ["Vehicle positions", metric("vehicle_positions")],
              ["Congestion rows", metric("congestion")],
              ["Weather days", metric("weather")],
              ["Live fleet (latest)", fleet.length],
            ].map(([label, val]) => (
              <div key={label as string} className="rounded-xl border border-white/10 bg-white/[0.02] p-5">
                <div className="text-2xl font-bold" style={{ color: ACCENT }}>
                  {(val as number).toLocaleString()}
                </div>
                <div className="mt-1 text-sm text-neutral-400">{label as string}</div>
              </div>
            ))}
          </div>

          <div className="grid grid-cols-1 gap-6 lg:grid-cols-2">
            <Panel title="Live fleet — latest CTA positions">
              <FleetMap points={fleet} />
              <p className="mt-3 text-xs text-neutral-500">
                <span style={{ color: BUS }}>● bus</span> &nbsp; <span style={{ color: TRAIN }}>● rail</span>
                &nbsp; — rail decoded from GTFS-RT protobuf.
              </p>
            </Panel>

            <Panel title="Worst on-time performance by route">
              <ResponsiveContainer width="100%" height={460}>
                <BarChart data={otp} layout="vertical" margin={{ left: 24 }}>
                  <CartesianGrid horizontal={false} stroke="#ffffff14" />
                  <XAxis type="number" domain={[0, 1]} tick={{ fill: "#9aa0a6", fontSize: 11 }} />
                  <YAxis type="category" dataKey="route_name" width={120} tick={{ fill: "#9aa0a6", fontSize: 11 }} />
                  <Tooltip
                    contentStyle={{ background: "#0E1117", border: "1px solid #ffffff22", borderRadius: 8 }}
                    formatter={(v) => [(Number(v) * 100).toFixed(1) + "%", "on-time"]}
                  />
                  <Bar dataKey="otp" radius={[0, 3, 3, 0]}>
                    {otp.map((d, i) => (
                      <Cell key={i} fill={d.mode === "train" ? TRAIN : BUS} />
                    ))}
                  </Bar>
                </BarChart>
              </ResponsiveContainer>
            </Panel>

            <Panel title="City-wide average road speed by hour">
              <ResponsiveContainer width="100%" height={300}>
                <LineChart data={speedByHour}>
                  <CartesianGrid stroke="#ffffff14" />
                  <XAxis dataKey="report_hour" tick={{ fill: "#9aa0a6", fontSize: 11 }} />
                  <YAxis tick={{ fill: "#9aa0a6", fontSize: 11 }} />
                  <Tooltip contentStyle={{ background: "#0E1117", border: "1px solid #ffffff22", borderRadius: 8 }} />
                  <Line type="monotone" dataKey="avg_speed_mph" stroke={ACCENT} strokeWidth={2} dot={{ r: 2 }} />
                </LineChart>
              </ResponsiveContainer>
            </Panel>

            <Panel title="Top congestion hotspots">
              <div className="overflow-x-auto">
                <table className="w-full text-left text-sm">
                  <thead className="text-neutral-500">
                    <tr>
                      <th className="py-1 pr-2">#</th>
                      <th className="py-1 pr-2">Street</th>
                      <th className="py-1 pr-2">Segment</th>
                      <th className="py-1 pr-2 text-right">mph</th>
                      <th className="py-1 text-right">Worst hr</th>
                    </tr>
                  </thead>
                  <tbody>
                    {hotspots.map((h) => (
                      <tr key={h.segment_id} className="border-t border-white/5">
                        <td className="py-1.5 pr-2 text-neutral-500">{h.congestion_rank}</td>
                        <td className="py-1.5 pr-2 text-neutral-200">{h.street}</td>
                        <td className="py-1.5 pr-2 text-neutral-400">{h.from_street} → {h.to_street}</td>
                        <td className="py-1.5 pr-2 text-right tabular-nums">{h.avg_speed_mph.toFixed(1)}</td>
                        <td className="py-1.5 text-right tabular-nums">{h.worst_hour}:00</td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            </Panel>
          </div>

          <footer className="mt-10 text-center text-xs text-neutral-600">
            Static marts exported from the DuckDB lakehouse · queried client-side with DuckDB-WASM ·{" "}
            <a className="underline hover:text-neutral-400" href="https://github.com/Archit1706/transit-lake">
              source
            </a>
          </footer>
        </>
      )}
    </main>
  );
}
