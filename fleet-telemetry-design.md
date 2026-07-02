# Fleet Telemetry Platform — Design Doc

> Status: **design locked, pre-implementation.** This doc is the source of truth for
> architecture and phasing. Track all work as the phase checkboxes in §5 — no separate
> epic/ticket system.

---

## 1. Goal

A simulated EV / robotaxi fleet streaming live telemetry to a cloud-native backend that
**ingests, processes, stores, queries, visualizes, and alerts**. Built as a portfolio
flagship to demonstrate the distributed-systems / streaming / cloud-native skills that
Tesla-style backend & data-platform internships ask for.

**Skills this is designed to make load-bearing (genuinely needed, not bolted on):**
Kafka / event streaming · time-series telemetry at scale · gRPC + REST microservices ·
PostgreSQL + Redis · Prometheus/Grafana observability · Docker + Kubernetes · Go · TypeScript/Next.js.

**How we work:** explain concepts before coding · build in vertical slices · prove each
phase works before moving on.

---

## 2. Non-Goals (explicitly cut — protects scope)

These were considered and **deliberately deferred or dropped**. They are garnish on top of a
complete system, not part of it. Re-add only as named stretches (§8), never woven through the spine.

- **Real-road routing** (OSRM / Valhalla, Tier 2/3 realism) — demonstrates zero distributed-systems
  skill. Tier 1 synthetic movement only.
- **Java/Spring service** — CourtSync already proves Java. Adding it here is resume-driven
  overhead unless explicitly wanted as a cross-language exercise. Stretch only.
- **Python / ML anomaly detection** — anomaly starts as a threshold rule in Go. Stretch only.
- **Cloud deployment** — local k8s first. Cloud is a later promotion, not a starting point.
- **Replaying real datasets** (Porto taxi, T-Drive) — not needed; synthetic is industry-standard
  for telemetry.

---

## 3. Locked Decisions

| Decision | Choice | Why |
|---|---|---|
| Backend language | **All Go** | Tesla's #1 backend lang; the main new skill; goroutines fit simulating concurrent cars. |
| Service shape | **A few small Go services** (not a monolith, not artificial micro-split) | The pipeline has genuinely independent concerns + scaling profiles — justified separation. |
| Data | **Tier 1 synthetic** (random-walk + waypoints) | You can't get a real fleet feed; synthetic is the standard. |
| Time-series store | **Plain Postgres** | Handles simulated volume easily. Timescale is a Postgres *extension* — add later if range-scans hurt. |
| Hot/live state | **Redis** | Fast "current state per car" lookups for the live map. |
| Fleet scale | **~1,000 cars, configurable** | Forces real partitioning/batching lessons; still laptop-friendly. Build at 10–50, demo at 1k. |
| Dashboard | **Next.js / TypeScript** | Already known. |
| Live transport | **SSE** (server→client) | One-directional; simpler than WebSocket for "push map updates." |
| Deployment | **Local Kubernetes (kind/minikube)** | Full k8s story for $0. Cloud is a stretch. |
| Frontend↔backend | **gRPC (service-to-service) + REST/SSE (browser)** | gRPC for the contract story; REST/SSE because browsers speak it natively. |

---

## 3.1 Current build constraints (locked)

- **All Go** across every service (simulator, ingest, query-api, analytics, loadgen). No Python for now.
- **Local-only:** everything runs via `docker compose` (dev) → local `kind` (Phase 7). Nothing requires a
  cloud account, external service, or manual setup from the operator.
- **Deferred on purpose:** Python, CI/CD, automated test suites, MCP, cloud deploy, and cluster big-data
  tools (Spark/Airflow) — the big-data story is covered locally by DuckDB + Parquet instead.

---

## 4. Architecture

```
                         ┌─────────────┐
                         │  simulator  │  N goroutines = N cars
                         │   (Go)      │  emits telemetry JSON
                         └──────┬──────┘
                                │ produce (key = car_id)
                          ┌─────▼─────┐
                          │   Kafka   │  topic: telemetry (partitioned)
                          │           │  topic: alerts (phase 3)
                          └─────┬─────┘
                                │ consume
                       ┌────────▼────────┐
                       │ ingest-consumer │  (Go)
                       │  - write Postgres (history)
                       │  - update Redis (hot state)
                       │  - anomaly rules → alerts topic (phase 4)
                       └───┬─────────┬───┘
                  write     │         │   update
                ┌───────────▼──┐  ┌───▼──────────┐
                │  Postgres    │  │    Redis     │
                │ (telemetry,  │  │ car:{id}:latest
                │  cars)       │  │  hot state)  │
                └───────┬──────┘  └──────┬───────┘
                        │ query          │ read
                    ┌───▼────────────────▼───┐
                    │      query-api (Go)     │  gRPC + REST + SSE
                    │  "query the fleet"      │
                    └───────────┬─────────────┘
                                │ REST / SSE
                       ┌────────▼─────────┐
                       │  Next.js dash    │  live map + charts
                       └──────────────────┘

   Prometheus scrapes /metrics on every Go service → Grafana dashboards (phase 4)
   All of the above runs in Docker Compose (dev) → kind k8s (deploy, phase 6)
```

### Go services
1. **simulator** — spawns one goroutine per car; each does Tier-1 movement and emits a telemetry
   message at a configurable rate to Kafka, keyed by `car_id`.
2. **ingest-consumer** — consumes `telemetry`; writes history to Postgres, writes Parquet for cold
   analytics, updates Redis hot state; (phase 4) runs anomaly rules and emits to `alerts`. Runs as
   multiple replicas in one consumer group (phase 3).
3. **query-api** — serves "query the fleet": current fleet snapshot (from Redis), filtered queries
   (e.g. speed>60 AND battery<15), geo queries via PostGIS, and recent history (from Postgres).
   Exposes gRPC **and** REST, plus SSE for live dashboard pushes.
4. **analytics** (phase 3) — reads telemetry Parquet with embedded DuckDB; computes fleet-wide batch
   rollups. No server, no cluster.
5. **loadgen** (phase 3) — drives the fleet to ~10k cars to exercise consumer scaling and lag.

### Data shapes
**Telemetry message** (Protobuf on Kafka — same shapes as the gRPC contract):
```
car_id, ts, lat, lng, speed, heading, battery_pct, motor_temp, odometer, gear, fault_codes[]
```
**Postgres**
- `telemetry(car_id, ts, lat, lng, speed, heading, battery_pct, motor_temp, odometer, gear, fault_codes)`
  — indexed on `(car_id, ts)`; primary read pattern is "recent history for a car" and "time-range scans".
- `cars(car_id, model, created_at, ...)` — fleet metadata.

**Redis**
- `car:{id}:latest` → most recent telemetry per car (powers the live map without hammering Postgres).

**Kafka**
- `telemetry` — keyed by `car_id` so a car's events stay ordered within a partition; ~6 partitions
  so you actually exercise multi-partition consumption.
- `alerts` (phase 4) — anomaly events.

**Parquet + DuckDB** (phase 3, cold analytics)
- ingest writes telemetry to Parquet files; a Go job queries them with embedded DuckDB for fleet-wide
  batch rollups — large-scale columnar processing, no server, no cluster.

**PostGIS** (phase 2)
- Postgres extension enabling geospatial queries ("cars in a bounding box / near a point").

---

## 5. Phasing (vertical slices)

Each phase is a **complete vertical slice** with a **Gate** ("prove it works") that must pass
before the next starts. The one-line *Learn* note is the point of the phase. Track work as the
checkboxes below.

### Phase 0 — Skeleton & Infra
- [x] Repo layout, Go modules, Docker Compose (Kafka + Postgres + Redis).
- [x] Shared Protobuf telemetry schema in `/proto` (used on Kafka and by gRPC).
- [x] One hello-world Go service that produces a message and another that consumes it.
- **Gate:** a message round-trips through Kafka locally.
- **Learn:** Kafka producer/consumer basics; Protobuf schema; Compose wiring.

### Phase 1 — MVP Spine
- [ ] simulator: 10 cars, Tier-1 random-walk movement → `telemetry` topic (Protobuf).
- [ ] ingest-consumer: consume → write Postgres; dedupe writes on `(car_id, ts)` (idempotent).
- [ ] minimal HTTP endpoint (on ingest-consumer for now) serving latest positions from Postgres.
- [ ] minimal Next.js map that polls that endpoint and renders live car positions.
- **Gate:** cars visibly move on a map in the browser; data is persisted in Postgres.
- **Learn:** the full producer→broker→consumer→store→UI loop; why streaming exists.

### Phase 2 — Query the Fleet
- [ ] query-api: gRPC + REST; current fleet snapshot from Redis; filtered queries (speed>X, battery<Y).
- [ ] ingest-consumer also updates Redis hot state.
- [ ] PostGIS on Postgres: geospatial queries ("cars in this bounding box / near this point").
- [ ] dashboard reads live positions from the API; scale simulator toward ~1,000 cars (config knob).
- **Gate:** a filter query ("cars >60 mph AND battery <15%") and a geo query ("cars in this box") return the correct sets; 1k cars run smoothly.
- **Learn:** gRPC vs REST tradeoffs; hot vs cold state; geospatial querying; Kafka partitioning under load.

### Phase 3 — Scale & Analytics
- [ ] run 3–4 `ingest-consumer` replicas in one Kafka consumer group; observe partition rebalancing.
- [ ] Go load-test harness (`loadgen`) that drives the fleet to ~10k cars.
- [ ] ingest also writes telemetry to Parquet; a Go `analytics` job queries it with embedded **DuckDB** for fleet-wide rollups (no server, no cluster).
- **Gate:** consumer lag (via `kafka-consumer-groups` describe / logs) spikes under 10k-car load and recovers as replicas share partitions; a DuckDB batch query returns fleet-wide aggregates over historical Parquet. (Grafana visualizes the lag later in Phase 5.)
- **Learn:** consumer groups, partition rebalancing, backpressure/lag; batch (cold) vs stream (hot) analytics; large-scale columnar processing without a cluster.

### Phase 4 — Stream Processing & Alerts
- [ ] rolling aggregates + anomaly rules (Go) in the consumer (e.g. overheating, sustained low battery).
- [ ] emit to `alerts` topic; dashboard surfaces live alerts.
- **Gate:** inject a fault in the simulator → alert appears live on the dashboard.
- **Learn:** stateful stream processing, windowing/aggregation, event-driven alerts.

### Phase 5 — Observability
- [ ] Prometheus `/metrics` on every Go service; Grafana dashboards (throughput, consumer lag, alert counts).
- [ ] one basic alerting rule (e.g. consumer lag too high).
- **Gate:** Grafana shows live pipeline metrics; consumer lag is visible and reacts to load.
- **Learn:** the metrics→dashboard→alert loop; what to actually measure in a pipeline.

### Phase 6 — Live Dashboard Polish
- [ ] replace polling with **SSE** for real-time map updates.
- [ ] charts: battery distribution, fleet speed histogram, alerts feed.
- **Gate:** map updates push in real time without polling; charts reflect live fleet state.
- **Learn:** server-push transports; building a real-time frontend over a stream.

### Phase 7 — Kubernetes
- [ ] k8s manifests (Deployments, Services, ConfigMaps/Secrets) for the whole stack.
- [ ] deploy to local **kind**; document the demo runbook.
- **Gate:** `kubectl get pods` all green; the full demo works in-cluster.
- **Learn:** containerization → orchestration; k8s primitives; config/secret management.

---

## 6. Definition of Done (the "stop here and it's a flagship" line)

**Spine through Phase 7** = a complete, deployed, demoable distributed system that hits 8+ of the
target skill bullets with nothing bolted on. That is "done." Everything in §8 is optional upside.

---

## 7. Cross-cutting conventions

- **Config over hardcoding:** fleet size, emit rate, partition count, DB/Redis URLs all via env/config.
- **One repo, clear top-level dirs** per service (`/simulator`, `/ingest`, `/query-api`, `/analytics`,
  `/loadgen`, `/dashboard`, `/deploy`); shared Protobuf schema + Go types in `/proto`.
- **Prove-it gates are mandatory** — no starting phase N+1 until phase N's gate passes.

---

## 8. Stretches (post-spine)

**Actionable now** (local, all Go, no cloud, no operator-in-the-loop):
- **Elasticsearch** — full-text search over fault codes/events + geo search. Scope it to search only;
  Postgres/PostGIS already covers structured + geo filters, so this is for the search keyword and
  full-text. Costs a ~1GB JVM container. **Lean fallback: skip it — PostGIS is already in the spine.**
- **Dead-letter queue** — malformed telemetry → `telemetry.dlq` topic with retry. Streaming robustness.
- **Replay mode** — replay recorded history back through Kafka at Nx speed from an offset. Great demo;
  proves offset / consumer-position understanding.

**Parked** (need another language, cloud, or setup deferred by decision):
- **MCP server over query-api** — natural-language fleet queries. High-signal for Integration Platforms;
  revisit when wanted.
- **Python anomaly/ML service** — model instead of threshold rules. Deferred (staying all-Go).
- **Java/Spring reimpl of query-api** — "one contract, two stacks" talking point. Optional.
- **Cloud k8s deploy** (GKE/EKS) — public URL. Deferred (local kind only).
- **Tier 2 real-road routing** (OSRM/Valhalla) — cosmetic realism; low priority.

---

## 9. Defaults locked (tune later if needed)

- **Emit rate:** 1 msg/sec per car. **Partitions:** 6 on `telemetry`.
- **Map library:** Leaflet (zero build config, plain tiles).
- **Kafka Go client:** `segmentio/kafka-go` (no CGo, simplest).
- **Phase order:** scale/analytics → alerts → observability (so lag is real before Grafana draws it).
