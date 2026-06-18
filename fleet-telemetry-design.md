# Fleet Telemetry Platform — Design Doc

> Status: **design locked, pre-implementation.** This doc is the source of truth for
> architecture and phasing. It is structured so each **Phase** below maps cleanly to an
> **Epic**, and each phase's checklist items map to **tickets**.

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
                       │  - anomaly rules → alerts topic (phase 3)
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
2. **ingest-consumer** — consumes `telemetry`; writes history to Postgres, updates Redis hot
   state; (phase 3) runs anomaly rules and emits to `alerts`.
3. **query-api** — serves "query the fleet": current fleet snapshot (from Redis), filtered queries
   (e.g. speed>60 AND battery<15), and recent history (from Postgres). Exposes gRPC **and** REST,
   plus SSE for live dashboard pushes.

### Data shapes
**Telemetry message** (JSON on Kafka):
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
- `alerts` (phase 3) — anomaly events.

---

## 5. Phasing (vertical slices → Epics)

Each phase is a **complete vertical slice** with a **Gate** ("prove it works") that must pass
before the next phase starts. The one-line *Learn* note is the point of the phase.

### Phase 0 — Skeleton & Infra  *(Epic: Foundations)*
- [ ] Repo layout, Go modules, Docker Compose (Kafka + Postgres + Redis).
- [ ] One hello-world Go service that produces a message and another that consumes it.
- **Gate:** a message round-trips through Kafka locally.
- **Learn:** Kafka producer/consumer basics; Compose wiring.

### Phase 1 — MVP Spine  *(Epic: End-to-end telemetry)*
- [ ] simulator: 10 cars, Tier-1 random-walk movement → `telemetry` topic.
- [ ] ingest-consumer: consume → write Postgres.
- [ ] minimal Next.js map that polls a REST endpoint and renders live car positions.
- **Gate:** cars visibly move on a map in the browser; data is persisted in Postgres.
- **Learn:** the full producer→broker→consumer→store→UI loop; why streaming exists.

### Phase 2 — Query the Fleet  *(Epic: Fleet query API)*
- [ ] query-api: gRPC + REST; current fleet snapshot from Redis; filtered queries (speed>X, battery<Y).
- [ ] ingest-consumer also updates Redis hot state.
- [ ] dashboard reads live positions from the API; scale simulator toward ~1,000 cars (config knob).
- **Gate:** a filter query ("cars >60 mph AND battery <15%") returns the correct set; 1k cars run smoothly.
- **Learn:** gRPC vs REST tradeoffs; hot vs cold state; Kafka partitioning under real-ish load.

### Phase 3 — Stream Processing & Alerts  *(Epic: Real-time processing)*
- [ ] rolling aggregates + anomaly rules in the consumer (e.g. overheating, sustained low battery).
- [ ] emit to `alerts` topic; dashboard surfaces live alerts.
- **Gate:** inject a fault in the simulator → alert appears live on the dashboard.
- **Learn:** stateful stream processing, windowing/aggregation, event-driven alerts.

### Phase 4 — Observability  *(Epic: Metrics & monitoring)*
- [ ] Prometheus `/metrics` on every Go service; Grafana dashboards (throughput, consumer lag, alert counts).
- [ ] one basic alerting rule (e.g. consumer lag too high).
- **Gate:** Grafana shows live pipeline metrics; consumer lag is visible and reacts to load.
- **Learn:** the metrics→dashboard→alert loop; what to actually measure in a pipeline.

### Phase 5 — Live Dashboard Polish  *(Epic: Real-time UX)*
- [ ] replace polling with **SSE** for real-time map updates.
- [ ] charts: battery distribution, fleet speed histogram, alerts feed.
- **Gate:** map updates push in real time without polling; charts reflect live fleet state.
- **Learn:** server-push transports; building a real-time frontend over a stream.

### Phase 6 — Kubernetes  *(Epic: Cloud-native deploy)*
- [ ] k8s manifests (Deployments, Services, ConfigMaps/Secrets) for the whole stack.
- [ ] deploy to local **kind**; document the demo runbook.
- **Gate:** `kubectl get pods` all green; the full demo works in-cluster.
- **Learn:** containerization → orchestration; k8s primitives; config/secret management.

---

## 6. Definition of Done (the "stop here and it's a flagship" line)

**Spine through Phase 6** = a complete, deployed, demoable distributed system that hits 8+ of the
target skill bullets with nothing bolted on. That is "done." Everything in §8 is optional upside.

---

## 7. Cross-cutting conventions

- **Config over hardcoding:** fleet size, emit rate, partition count, DB/Redis URLs all via env/config.
- **One repo, clear top-level dirs** per service (`/simulator`, `/ingest`, `/query-api`, `/dashboard`,
  `/deploy`); shared Go types in one place.
- **Prove-it gates are mandatory** — no starting phase N+1 until phase N's gate passes.

---

## 8. Stretches (post-spine, pick what you want — each a standalone Epic)

- **MCP server over query-api** *(the cherry)* — natural-language fleet queries ("show me cars
  overheating in Texas"). ~Cheap, high-signal for the Integration Platforms role.
- **Java/Spring reimpl of query-api** — same gRPC contract, second stack → "one contract, two stacks"
  talking point. Only if you *want* the cross-language exercise.
- **Python anomaly/ML service** — replace threshold rules with a model; hits the data/ML bullet.
- **Tier 2 real-road routing** (OSRM/Valhalla) — cosmetic realism; low priority.
- **Cloud k8s deploy** (GKE/EKS) — public URL; promote from kind when you want a live link.

---

## 9. Open items to confirm before ticketing

- Exact emit rate per car (e.g. 1 msg/sec) and partition count (default 6) — tune in Phase 2.
- Map library for the dashboard (e.g. MapLibre/Leaflet) — decide in Phase 1.
- Whether Phase 4 observability comes *before* Phase 3 alerts (some prefer metrics first) — minor ordering call.
