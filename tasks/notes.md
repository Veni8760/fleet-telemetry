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

## Phase 3 — Scale & Analytics ✅
- `loadgen`: batched burst producer (CARS/COUNT/WORKERS), ~300–450k msg/s to `telemetry` across 10k car_ids.
- ingest: added Parquet sink (`parquet-go`), env `PARQUET_DIR`/`PARQUET_FLUSH_ROWS`, flush per N rows to `telemetry-<pid>-<seq>.parquet`.
- `analytics`: embedded **DuckDB** (`go-duckdb/v2`, CGo) over `read_parquet('data/parquet/*.parquet')`.
- **Gate A — lag spikes under 10k load, recovers as replicas share partitions** (`kafka-consumer-groups --describe`):
  - loadgen burst, 0 consumers → **total lag 462,136** across 6 partitions.
  - 1 replica → owns all 6 partitions.
  - 4 replicas (one group) → partitions rebalanced across members; lag drained **462k → 341k → 247k → 63k → 794** (recovered).
  - Gotcha: killed `go run` replicas ghost the group until session timeout; `docker compose restart kafka` clears stale members. Prebuilt binary + background task keeps a replica alive reliably.
- **Gate B — DuckDB aggregates over historical Parquet:**
  - 12 parquet files → `rows=120000 cars=9941`, speed avg=40.5/max=80.0, battery avg=49.7/min=0.0, plus top-5 by avg speed.
  - Verified no sink loss: controlled 50k burst with a single live consumer → exactly 5 files (10k/file).

## Phase 4 — Stream Processing & Alerts ✅
- proto: added `Alert{car_id, ts, type, message, value}`; new `alerts` topic (3 partitions).
- ingest `detector`: rolling per-car state + rules — OVERHEAT (motor_temp>120), LOW_BATTERY (battery<15 for ≥5 consecutive readings), FAULT_CODE (any code present); 15s per-(car,type) cooldown; emits Alert protobuf to `alerts`.
- query-api: consumes `alerts` into an in-memory ring (last 100), serves `GET /api/alerts`.
- simulator: fault-injection control `POST :8090/inject?car=<id>` / `/clear` → that car emits motor_temp 130 + `OVERHEAT` fault code.
- dashboard: shadcn Card "Live Alerts" feed (Badge per type: destructive/secondary), polls `/api/alerts`.
- ops note: killed kafka-go consumers ghost their group (survives even kafka restart until session timeout), blocking offset reset. Fix applied: ingest `CONSUMER_GROUP` env + `StartOffset: LastOffset` so a fresh group consumes live (history stays in Postgres/Parquet).
- **Gate — inject fault → alert live on dashboard:**
  - `POST :8090/inject?car=car-3` → within ~1s `/api/alerts` shows `{car-3, OVERHEAT, "motor temp 130°C", 130}`.
  - Playwright @ :3001: alerts panel lists `car-3 OVERHEAT` + `car-3 FAULT_CODE`, count badge = 28.

## Phase 5 — Observability ✅
- Prometheus `/metrics` on every Go service: ingest (`:2112` — `ingest_messages_total`, `ingest_alerts_total{type}`), simulator (`:8090` — `simulator_messages_total`), query-api (`:8082` — `queryapi_requests_total{endpoint}`).
- compose: `kafka-exporter` (consumer lag), `prometheus` (:9091, `extra_hosts host-gateway` to scrape host Go services), `grafana` (:3002, anonymous admin, provisioned datasource + dashboard). Configs in `deploy/`.
- Prometheus alert rule `HighConsumerLag` (`sum(kafka_consumergroup_lag{topic="telemetry"}) > 5000 for 15s`).
- ingest throughput fixes: `CommitInterval=1s` (batch offset commits) + alerts writer `Async:true` (don't block consume loop).
- **Gate — Grafana shows live metrics; lag reacts to load:**
  - All 4 scrape targets UP; data flows Grafana→Prometheus proxy (throughput 50/s, lag, alerts by type). Dashboard renders 4 panels. Screenshot: `tasks/phase5-grafana.png`.
  - Load reaction (single ingest): baseline lag **782** → 200k burst → spike **200,228** → steady drain → recovered **~737**.
  - Alert `HighConsumerLag` observed **firing** (value 162254 > 5000) during the spike.
- measurement gotcha: `rate(counter[1m])` is garbage across process restarts (counter resets); use absolute-value deltas over a timed window. Also cleared junk 2M-row telemetry backlog (`TRUNCATE`) + recreated the topic for a clean baseline; `synchronous_commit=off` set on the dev DB.
- **BIG gotcha (root cause of most Phase 5 confusion):** a Phase 2 `go run ./simulator` (FLEET_SIZE=1000) survived every `pkill` because its exe lives at a `go-build/...` path, not `exe/simulator`. It kept producing 1000 msg/s for hours, inflating lag/throughput/car-counts. Kill strays with `pkill -f "go run ./simulator"` AND `pkill -f "go-build.*/simulator"`.

## Phase 6 — Live Dashboard Polish ✅
- query-api: `GET /api/stream` SSE endpoint pushes `{cars, alerts}` every 1s (text/event-stream, flush per frame).
- dashboard: replaced polling with `EventSource(/api/stream)`; markers updated imperatively from pushes; added shadcn **chart** (Recharts) — battery% and speed histograms computed from the live snapshot.
- **Gate — real-time push (no polling) + charts reflect live state:**
  - Playwright @ :3001: 200 markers, **165 moved in 4s**, badge=200; **0** requests to `/api/positions`/`/api/alerts` (polling gone); 26 chart bars rendered.
  - Server confirms: `queryapi_requests_total{endpoint="stream"}=1` (browser SSE connection), `positions`=none. Screenshot: `tasks/phase6-sse-charts.png`.
