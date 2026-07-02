# Fleet Telemetry Platform

Simulated EV / robotaxi fleet streaming live telemetry to a cloud-native backend that
**ingests, processes, stores, queries, visualizes, and alerts** in real time.

A portfolio flagship for distributed-systems / streaming / cloud-native backend work — built
around the rare problem domain where Kafka, stream processing, and Kubernetes are *genuinely
necessary* rather than bolted on.

> **Status:** 🚧 Building. **Phases 0–2 complete** — 1,000-car live fleet with a
> gRPC+REST query-api (Redis hot state, PostGIS geo queries) behind the Next.js map.
> Full architecture, decisions, and phasing live in the [design doc](./fleet-telemetry-design.md).

## What it is

A device simulator spins up thousands of virtual cars (GPS, speed, battery %, motor temp, fault
codes) that stream telemetry through Kafka into a Go processing pipeline, time-series + hot
storage, and a live Next.js dashboard — with full Prometheus/Grafana observability, deployed to
Kubernetes.

## Architecture

```
simulator (Go) → Kafka → ingest-consumer (Go) → Postgres + Redis
                                                      │
                                          query-api (Go, gRPC + REST + SSE)
                                                      │
                                            Next.js dashboard (live map + charts)

  + Prometheus/Grafana observability   ·   Docker Compose (dev) → Kubernetes/kind (deploy)
```

Full diagram, data shapes, and the rationale behind every choice: [design doc](./fleet-telemetry-design.md).

## Tech stack

| Layer | Choice |
|---|---|
| Backend | **Go** (simulator, ingest, query API) |
| Streaming | **Apache Kafka** |
| Storage | **PostgreSQL** (history) + **Redis** (hot state) |
| API | **gRPC + REST + SSE** |
| Frontend | **Next.js / TypeScript** (live map + charts) |
| Observability | **Prometheus + Grafana** |
| Infra | **Docker Compose** (dev) → **Kubernetes / kind** (deploy) |

## Roadmap (epics)

| Phase | Epic | Outcome |
|---|---|---|
| 0 | Foundations | Repo, Docker Compose (Kafka/Postgres/Redis), Go produce↔consume round-trip |
| 1 | End-to-end telemetry | simulator → Kafka → Postgres → live map |
| 2 | Fleet query API | gRPC+REST, Redis hot state, filtered queries, scale to ~1k cars |
| 3 | Scale & analytics | consumer-group scaling, 10k-car loadgen, DuckDB/Parquet rollups |
| 4 | Stream processing | rolling aggregates + live anomaly alerts |
| 5 | Observability | Prometheus metrics + Grafana dashboards |
| 6 | Real-time UX | SSE live map + charts |
| 7 | Cloud-native deploy | Kubernetes (kind) |

**Stretch:** MCP natural-language fleet queries · Java/Spring query-api variant · Python ML anomaly · cloud k8s.

## Quickstart

```bash
# 1. Bring up infra (Kafka :9092 · Postgres :5433 · Redis :6380)
docker compose up -d

# 2. Start the pipeline (each in its own terminal)
go run ./ingest        # consume telemetry -> Postgres history + Redis hot state
go run ./query-api     # REST :8082 + gRPC :9090 — snapshot/filter (Redis), geo (PostGIS)
FLEET_SIZE=1000 go run ./simulator   # random-walking fleet -> Kafka (env: FLEET_SIZE, EMIT_RATE_HZ)

# 3. Live map
cd dashboard && npm install && npm run dev   # http://localhost:3001
```

Query examples: `:8082/api/positions` · `:8082/api/query?min_speed=60&max_battery=15` ·
`:8082/api/geo?min_lat=37.75&min_lng=-122.45&max_lat=37.80&max_lng=-122.40`.

Requires Go 1.26+, Node 20+, Docker. `protoc` only if regenerating `/proto` (generated Go is committed).
Host ports avoid a native Postgres (5432) and another local project (8080/3000); all are env-overridable.

## How it's built

Each phase is a complete **vertical slice** with a "prove it works" gate — built in order, no
phase starts until the previous one's gate passes. Gate evidence is tracked in
[`tasks/notes.md`](./tasks/notes.md).

## Docs

- [Design doc](./fleet-telemetry-design.md) — architecture, locked decisions, phasing
- [Project handoff](./fleet-telemetry-handoff.md) — origin & context
