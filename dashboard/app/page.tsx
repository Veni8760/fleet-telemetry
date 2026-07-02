"use client";

import { useEffect, useRef, useState } from "react";
import type { Map as LeafletMap, CircleMarker } from "leaflet";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Badge } from "@/components/ui/badge";

type Pos = {
  car_id: string;
  lat: number;
  lng: number;
  speed: number;
  battery_pct: number;
};

const API = process.env.NEXT_PUBLIC_API_BASE ?? "http://localhost:8081";

export default function Page() {
  const [count, setCount] = useState(0);
  const mapEl = useRef<HTMLDivElement>(null);

  useEffect(() => {
    let map: LeafletMap | null = null;
    let timer: ReturnType<typeof setInterval> | null = null;
    let cancelled = false;
    const markers: Record<string, CircleMarker> = {};

    (async () => {
      const L = (await import("leaflet")).default;
      if (cancelled || !mapEl.current) return;
      map = L.map(mapEl.current).setView([37.7749, -122.4194], 12);
      L.tileLayer("https://tile.openstreetmap.org/{z}/{x}/{y}.png", {
        attribution: "© OpenStreetMap",
        maxZoom: 19,
      }).addTo(map);

      const tick = async () => {
        try {
          const res = await fetch(`${API}/api/positions`);
          const cars: Pos[] = await res.json();
          setCount(cars.length);
          for (const c of cars) {
            const ll: [number, number] = [c.lat, c.lng];
            const label = `${c.car_id} · ${c.speed.toFixed(0)} mph · ${c.battery_pct.toFixed(0)}%`;
            const existing = markers[c.car_id];
            if (existing) {
              existing.setLatLng(ll).setPopupContent(label);
            } else if (map) {
              markers[c.car_id] = L.circleMarker(ll, {
                radius: 6,
                color: "#ef4444",
                fillColor: "#ef4444",
                fillOpacity: 0.9,
                weight: 1,
              })
                .bindPopup(label)
                .addTo(map);
            }
          }
        } catch {
          // API not up yet — retry next tick
        }
      };
      await tick();
      timer = setInterval(tick, 1000);
    })();

    return () => {
      cancelled = true;
      if (timer) clearInterval(timer);
      if (map) map.remove();
    };
  }, []);

  return (
    <main className="relative h-screen w-screen">
      <div ref={mapEl} data-testid="map" className="absolute inset-0" />
      <Card className="absolute left-4 top-4 z-[1000] w-56 gap-2 py-4">
        <CardHeader className="px-4">
          <CardTitle className="text-sm font-medium text-muted-foreground">
            Fleet Telemetry
          </CardTitle>
        </CardHeader>
        <CardContent className="px-4">
          <div className="flex items-center gap-2">
            <Badge
              variant="secondary"
              data-testid="car-count"
              className="text-base"
            >
              {count}
            </Badge>
            <span className="text-sm text-muted-foreground">cars live</span>
          </div>
        </CardContent>
      </Card>
    </main>
  );
}
