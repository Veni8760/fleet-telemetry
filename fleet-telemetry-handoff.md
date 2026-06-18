# Handoff: Fleet Telemetry Platform — project planning

> Paste this whole file into a new chat to continue the design conversation with full context.

## TL;DR
I'm a CS student (Java/Spring beginner, comfortable with TypeScript/Next.js, only light Go
exposure). I'm planning a **new portfolio project: a simulated EV-fleet telemetry platform**,
built specifically to learn the distributed-systems / streaming / cloud-native tools that
Tesla-style backend & data-platform internships ask for. We're still in **design/brainstorm** —
no code written yet. Below is everything decided so far and what's still open.

**How I like to work:** explain concepts before writing code (I'm learning), build in vertical
slices (not whole-backend-then-frontend), and prove each phase actually works before moving on.

## Why this project (the goal)
Targeting Tesla SWE internships (Data Platforms, AI Data Infrastructure, Integration Platforms,
Fleetnet, Factory Software, Vehicle Engineering Automation). The recurring skills across those
listings — and what this project is designed to make **load-bearing** (genuinely needed, not
bolted on):

- **Event streaming / messaging:** Kafka, queues, stream processing
- **Telemetry / data-at-scale:** car-to-cloud, "query the fleet in real time," time-series data
- **gRPC + REST microservices**
- **PostgreSQL** + complex SQL (views, functions); also Redis, Elasticsearch
- **Observability:** metrics, monitoring, alerting (Prometheus / Grafana)
- **Docker + Kubernetes + CI/CD**
- **Languages:** Go + Python (backend), TypeScript / React / Next.js (frontend)
- **AI / LLM tooling + MCP** (Integration Platforms)

## What it is
A simulated fleet of EVs / robotaxis streaming live telemetry to a cloud backend that ingests,
processes, stores, queries, visualizes, and alerts.

**Pipeline:**
- **Device simulator** — generates telemetry for thousands of virtual cars (GPS lat/lng, speed,
  heading, battery %, motor temp, odometer, gear, fault codes); emits to Kafka.
- **Kafka** — the ingestion backbone (the star; where you feel *why* streaming exists).
- **Stream processor** — anomaly detection, rolling aggregates.
- **Storage** — time-series + relational (Postgres / TimescaleDB), Redis for hot/live state.
- **"Query the fleet" API** — gRPC + REST (e.g. "show me all cars doing >60 mph with battery <15%").
- **Next.js dashboard** — live map + charts (WebSocket / SSE).
- **Observability** — Prometheus + Grafana, metrics + alerting.
- **Deployment** — Docker Compose → Kubernetes.
- *(Optional)* **MCP server** — let an LLM query the fleet in natural language ("show me cars
  overheating in Texas") — the Tesla Integration Platforms flavor.

## Decisions made so far
- **Domain:** EV fleet / robotaxis (live map + "show me cars doing X" queries).
- **Stack:** Polyglot, **Go-first**.
  - **Go** → device simulator + ingest + stream processor (+ query API). Reason: Go is Tesla's
    #1 backend language and the main new skill to learn; goroutines are ideal for simulating
    thousands of concurrent cars.
  - **Java/Spring** → the fleet-query API service in a later phase. Reason: leverage existing
    knowledge and create a real Go↔Java gRPC/Kafka seam (good portfolio piece).
  - **Next.js / TypeScript** → dashboard (already known).
  - **Python** → optional later, for anomaly/ML (hits the data/ML bullet).
  - NOTE: receptive to this split but it has not been hard-confirmed — re-confirm.
- **Data source:** **SIMULATED** — you generate it; you do NOT download a static dataset as the
  core. This is the industry-standard approach for telemetry (you can't get a real fleet feed).
  Tiered realism:
  - **Tier 1 (MVP):** synthetic random-walk / waypoint movement, zero external deps.
  - **Tier 2:** route cars along real roads via OpenStreetMap + a local routing engine (OSRM / Valhalla).
  - **Tier 3:** replay a real public GPS dataset (e.g. **Porto taxi** ~1.7M trips, or Microsoft
    **T-Drive** Beijing taxis) and synthesize the EV-specific fields (battery/temp/faults) on top.
  - *(Stretch for literally-real live data: poll a transit agency's **GTFS-Realtime** feed — real
    real-time, but buses not EVs.)*
- **Build approach:** vertical slices, phased. MVP first (simulator → Kafka → consumer → store →
  simple live map), then add realism, then stream processing/anomaly, then observability, then
  Kubernetes, then the Java query service, then optional MCP.

## Open questions (decide these next)
1. **Realism level / timing:** is realistic map movement (Tier 2/3) a portfolio goal, or is Tier-1
   synthetic fine so effort goes to the backend/streaming/k8s side?
2. **Final stack confirm:** Go-first + Java query service + Next.js dashboard + optional Python — good?
3. **Scope / timeline:** how many weeks, and how far to take it (stop after observability? include
   k8s? include the MCP layer?).
4. **Time-series store:** plain Postgres vs TimescaleDB vs something else.
5. **Exact MVP slice:** define the precise first vertical slice to build.

## Context: relationship to the old project ("CourtSync")
This is a **new flagship project**, pivoting from a prior learning project — *CourtSync*, a
volleyball drop-in **microservices** app (Java/Spring + Next.js + Kafka + gRPC + hosted Supabase
Postgres + Docker Compose). CourtSync taught the fundamentals (Spring Boot, gRPC, Kafka basics,
Postgres + Flyway migrations, Docker) but does **not** exercise the telemetry / streaming /
observability / Kubernetes cluster that Tesla roles emphasize. Plan: **keep CourtSync as-is** (a
solid full-stack + microservices portfolio piece — it directly supports the Vehicle Engineering
Automation role), and build this telemetry platform as the new flagship.

Reality check that drove this: microservices are overkill for a volleyball app (it should be a
monolith); they're justified here only as a *learning vehicle*. A fleet telemetry platform is the
rare project where Kafka/streaming/microservices are genuinely necessary (asymmetric load,
millions of devices, real-time processing) — which is exactly why it's a better learning rig.

## Next step
Continue the design conversation: resolve the open questions above, then produce a phased design,
then an implementation plan. Remember: explain before coding, vertical slices, prove each phase works.
