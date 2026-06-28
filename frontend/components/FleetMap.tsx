"use client";

import { useMemo } from "react";
import Map, { Layer, Source } from "react-map-gl/maplibre";
import "maplibre-gl/dist/maplibre-gl.css";

export type FleetPoint = {
  mode: string;
  route_id: string;
  lat: number;
  lon: number;
  is_delayed: boolean;
};

// Free, token-less dark basemap.
const STYLE = "https://basemaps.cartocdn.com/gl/dark-matter-gl-style/style.json";

export default function FleetMap({ points }: { points: FleetPoint[] }) {
  const geojson = useMemo(
    () => ({
      type: "FeatureCollection" as const,
      features: points
        .filter((p) => Number.isFinite(p.lat) && Number.isFinite(p.lon))
        .map((p) => ({
          type: "Feature" as const,
          geometry: { type: "Point" as const, coordinates: [p.lon, p.lat] },
          properties: { mode: p.mode, delayed: p.is_delayed ? 1 : 0 },
        })),
    }),
    [points],
  );

  return (
    <div className="h-[460px] w-full overflow-hidden rounded-xl border border-white/10">
      <Map
        initialViewState={{ longitude: -87.66, latitude: 41.86, zoom: 9.6 }}
        mapStyle={STYLE}
        attributionControl={false}
      >
        <Source id="fleet" type="geojson" data={geojson}>
          <Layer
            id="fleet-points"
            type="circle"
            paint={{
              "circle-radius": ["interpolate", ["linear"], ["zoom"], 8, 1.8, 13, 4.5],
              "circle-color": [
                "case",
                ["==", ["get", "mode"], "train"],
                "#FF7043",
                "#4FC3F7",
              ],
              "circle-opacity": 0.8,
              "circle-stroke-width": ["case", ["==", ["get", "delayed"], 1], 1.2, 0],
              "circle-stroke-color": "#FBC02D",
            }}
          />
        </Source>
      </Map>
    </div>
  );
}
