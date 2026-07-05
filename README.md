# Fleet Telemetry Platform

A simulated EV fleet streaming live telemetry through Kafka into a Go pipeline, time-series +
hot storage, and a live Next.js dashboard — with Prometheus/Grafana observability, runnable on
Docker Compose or a local Kubernetes cluster.

## Architecture

```
simulator (Go) → Kafka → ingest-consumer (Go) → Postgres + Redis
                                                      │
                                          query-api (Go, gRPC + REST + SSE)
                                                      │
                                            Next.js dashboard (live map + charts)

  + Prometheus/Grafana observability   ·   Docker Compose (dev) → Kubernetes/kind (deploy)
```

## Tech stack

| Layer | Choice |
|---|---|
| Backend | **Go** (simulator, ingest, query API) |
| Streaming | **Apache Kafka** |
| Storage | **PostgreSQL** (history) + **Redis** (hot state) |
| API | **gRPC + REST + SSE** |
| Frontend | **Next.js / TypeScript** (live map + charts) |
| Observability | **Prometheus + Grafana** (+ kafka-exporter for consumer lag) |
| Infra | **Docker Compose** (dev) → **Kubernetes / kind** (deploy) |

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

Observability: Prometheus at `localhost:9091`, Grafana at `localhost:3002` (anonymous). Go
services expose `/metrics` (ingest `:2112`, simulator `:8090`, query-api `:8082`).

Requires Go 1.26+, Node 20+, Docker. `protoc` only if regenerating `/proto` (generated Go is
committed). Host ports avoid a native Postgres (5432) and another local project (8080/3000); all
are env-overridable.

### Scale & analytics

```bash
# Run several consumers in one group (watch partitions rebalance):
for i in 1 2 3 4; do PARQUET_DIR=data/parquet go run ./ingest & done
CARS=10000 COUNT=300000 go run ./loadgen           # stress the group -> lag spikes, then recovers
docker exec fleet-kafka /opt/kafka/bin/kafka-consumer-groups.sh \
  --bootstrap-server localhost:9092 --describe --group ingest   # inspect lag/assignment
go run ./analytics                                 # embedded DuckDB rollups over Parquet
```

### Live alerts

With `ingest`, `query-api`, `simulator`, and the dashboard running, inject a fault and watch it
appear on the map's alerts panel:

```bash
curl -X POST 'localhost:8090/inject?car=car-3'     # car-3 overheats -> OVERHEAT alert live
curl -X POST 'localhost:8090/clear?car=car-3'      # back to normal
```

## Kubernetes (local kind)

The whole stack also runs in a local Kubernetes cluster. Images are built locally and loaded
into [kind](https://kind.sigs.k8s.io/) — no registry needed.

```bash
# 1. Build the four app images (quote the :local tag)
docker build --build-arg SERVICE=simulator  -t "fleet/simulator:local"  .
docker build --build-arg SERVICE=ingest     -t "fleet/ingest:local"     .
docker build --build-arg SERVICE=query-api  -t "fleet/query-api:local"  .
docker build -t "fleet/dashboard:local" dashboard/

# 2. Cluster + load images + apply manifests
kind create cluster --name fleet
for i in simulator ingest query-api dashboard; do kind load docker-image "fleet/$i:local" --name fleet; done
kubectl apply -f deploy/k8s/
kubectl get pods -n fleet -w        # wait for all 7 Running (app pods CrashLoop until infra is up, then settle)

# 3. Demo: port-forward the API and the dashboard
kubectl port-forward -n fleet svc/query-api 8082:8082 &
kubectl port-forward -n fleet svc/dashboard 3001:3000 &
open http://localhost:3001         # live map + alerts + charts, streamed over SSE

# inject a fault (simulator control) and watch the alert appear live:
kubectl port-forward -n fleet deploy/simulator 8090:8090 &
curl -X POST 'localhost:8090/inject?car=car-42'
```

Manifests: `deploy/k8s/00-config.yaml` (Namespace + ConfigMap + Secret), `10-infra.yaml`
(Kafka KRaft / Postgres+PostGIS / Redis), `20-apps.yaml` (ingest / query-api / simulator / dashboard).
Teardown: `kind delete cluster --name fleet`.

## Docs

- [Design doc](./fleet-telemetry-design.md) — architecture, decisions, data shapes
