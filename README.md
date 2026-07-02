# Fleet Telemetry Platform

Simulated EV / robotaxi fleet streaming live telemetry to a cloud-native backend that
**ingests, processes, stores, queries, visualizes, and alerts** in real time.

A portfolio flagship for distributed-systems / streaming / cloud-native backend work — built
around the rare problem domain where Kafka, stream processing, and Kubernetes are *genuinely
necessary* rather than bolted on.

> **Status:** 🚧 Building. **Phase 0 (Foundations) complete** — Kafka round-trip proven.
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
# 1. Bring up infra (Kafka + Postgres + Redis)
docker compose up -d

# 2. Round-trip a telemetry message through Kafka (Phase 0 gate)
go run ./simulator     # produces one Telemetry protobuf message
go run ./ingest        # consumes and prints it (Ctrl-C to stop)
```

Requires Go 1.26+, Docker, and `protoc` only if regenerating `/proto` (generated Go is committed).

## How it's built

Each phase is a complete **vertical slice** with a "prove it works" gate — built in order, no
phase starts until the previous one's gate passes. Gate evidence is tracked in
[`tasks/notes.md`](./tasks/notes.md).

## Docs

- [Design doc](./fleet-telemetry-design.md) — architecture, locked decisions, phasing
- [Project handoff](./fleet-telemetry-handoff.md) — origin & context
