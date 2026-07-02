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

type Alert = {
  car_id: string;
  ts: number;
  type: string;
  message: string;
};

const API = process.env.NEXT_PUBLIC_API_BASE ?? "http://localhost:8082";

export default function Page() {
  const [count, setCount] = useState(0);
  const [alerts, setAlerts] = useState<Alert[]>([]);
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
          const ar = await fetch(`${API}/api/alerts`);
          setAlerts(await ar.json());
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

      <Card
        data-testid="alerts-panel"
        className="absolute right-4 top-4 z-[1000] flex max-h-[80vh] w-80 flex-col gap-2 py-4"
      >
        <CardHeader className="px-4">
          <CardTitle className="flex items-center gap-2 text-sm font-medium text-muted-foreground">
            Live Alerts
            <Badge variant={alerts.length ? "destructive" : "secondary"} data-testid="alert-count">
              {alerts.length}
            </Badge>
          </CardTitle>
        </CardHeader>
        <CardContent className="flex-1 overflow-y-auto px-4">
          {alerts.length === 0 ? (
            <p className="text-sm text-muted-foreground">No alerts.</p>
          ) : (
            <ul className="flex flex-col gap-2" data-testid="alert-list">
              {alerts.map((a, i) => (
                <li key={`${a.car_id}-${a.ts}-${i}`} className="flex flex-col gap-1 border-b pb-2 last:border-0">
                  <div className="flex items-center justify-between gap-2">
                    <Badge variant={a.type === "LOW_BATTERY" ? "secondary" : "destructive"}>
                      {a.type}
                    </Badge>
                    <span className="font-mono text-xs text-muted-foreground">{a.car_id}</span>
                  </div>
                  <span className="text-xs">{a.message}</span>
                </li>
              ))}
            </ul>
          )}
        </CardContent>
      </Card>
    </main>
  );
}
