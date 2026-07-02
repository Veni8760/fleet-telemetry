# Working Notes — Gate Evidence

Source of truth: `fleet-telemetry-design.md`. This file holds the proof each Gate actually passed.

## Phase 0 — Skeleton & Infra ✅
- Stack: Go 1.26, protoc 35.1, `apache/kafka:3.9.0` (KRaft, no ZK), `postgis/postgis:16-3.4`, `redis:7-alpine`.
- Kafka listeners: `HOST://localhost:9092` (host processes), `DOCKER://kafka:29092` (in-compose).
- Kafka client: `segmentio/kafka-go` (per §9). Topic `telemetry` created with 6 partitions (per §9 default).
- **Gate — message round-trips through Kafka:**
  - `go run ./simulator` → `produced car_id=hello-0 to topic=telemetry`
  - `go run ./ingest`    → `consumed car_id=hello-0 speed=42.0 battery=87.0 partition=4 offset=0`
  - Protobuf marshal/unmarshal verified end-to-end.

## Local port map (this machine — native Postgres owns 5432, ad-platform owns 8080/3000)
- Kafka `localhost:9092` · Postgres `localhost:5433` · Redis `localhost:6380`
- ingest HTTP `:8081` · dashboard `:3001`. All env-overridable.

## Phase 1 — MVP Spine ✅
- simulator: N goroutines (default `FLEET_SIZE=10`, `EMIT_RATE_HZ=1`), Tier-1 random walk around SF, Protobuf → `telemetry` keyed by car_id.
- ingest: consume → Postgres `telemetry` table, `INSERT ... ON CONFLICT (car_id,ts) DO NOTHING` (idempotent). Serves `GET /api/positions` (latest per car via `DISTINCT ON`).
- dashboard: Next.js 16 + Tailwind v4 + **shadcn** (Card/Badge overlay); Leaflet map polls `/api/positions` every 1s.
- **Gate — cars move on map + persisted in Postgres:**
  - Postgres: `select count(*),count(distinct car_id)` → `3680 | 10`.
  - `/api/positions` → HTTP 200 JSON, 10 cars.
  - Playwright @ localhost:3001: 10 `path.leaflet-interactive` markers, **all 10 changed `d` over 3.5s** (movement), badge reads `10`. Screenshot: `tasks/phase1-fleet-map.png`.

## Phase 2 — Query the Fleet ✅
- gRPC contract `FleetService` (Snapshot/Query/GeoQuery) in `proto/telemetry.proto`.
- ingest now also writes Redis hot state: hash `fleet:latest` field=car_id value=raw protobuf bytes (reuses Kafka payload, no re-marshal). Dropped ingest's HTTP server — query-api owns reads.
- query-api (`:8082` REST, `:9090` gRPC): snapshot + speed/battery filter from Redis; bbox geo via **PostGIS** (`ST_MakeEnvelope`/`ST_Contains` over `DISTINCT ON (car_id)` latest). Dashboard repointed to `:8082`.
- **Gate — filter + geo return correct sets; 1k smooth (all verified at FLEET_SIZE=1000):**
  - Smoothness: `/api/positions` ~30ms; hot-state age median 0.7s / max 1.7s → ingest keeps up at 1000 msg/s.
  - Filter (data frozen, exact set compare vs recomputed-from-snapshot):
    - `speed>=60 AND battery<=15` (gate) → 0==0 (batteries not yet drained; correct empty).
    - `speed>=60` → 31==31 · `battery<=80` → 507==507 · `speed>=30 AND battery<=85` → 224==224. All exact set match, all satisfy predicate.
  - Geo bbox (central SF) → PostGIS 417 == snapshot-bbox 417, all inside box.
  - gRPC (`tools/grpcprobe`): Snapshot=1000, Query(60)=31, GeoQuery=417 — identical to REST.
  - Dashboard: 1000 Leaflet markers, badge `1000`, 802 moved over 3.5s. Screenshot: `tasks/phase2-1k-fleet.png` (Playwright out dir).
