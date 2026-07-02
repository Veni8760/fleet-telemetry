# Fleet Telemetry — Progress & Resume

Source of truth: `fleet-telemetry-design.md`. Gate evidence: `tasks/notes.md`.

## Done (committed on `build/fleet-telemetry`)
- [x] **Phase 0 — Skeleton & Infra** — Kafka round-trip proven.
- [x] **Phase 1 — MVP Spine** — simulator → Kafka → Postgres → live Next.js/shadcn map.
- [x] **Phase 2 — Query the Fleet** — query-api (gRPC+REST), Redis hot state, PostGIS geo, 1k cars.
- [x] **Phase 3 — Scale & Analytics** — consumer-group scaling (lag 462k→794), loadgen, DuckDB/Parquet rollups.
- [x] **Phase 4 — Stream Processing & Alerts** — rolling anomaly rules → alerts topic → live on dashboard.
- [x] **Phase 5 — Observability** — Prometheus /metrics on every service, kafka-exporter, Grafana dashboard, lag alert rule.
- [x] **Phase 6 — Live Dashboard Polish** — SSE (no polling) + battery/speed charts (shadcn/Recharts).

## TODO — resume here tomorrow

### Phase 7 — Kubernetes (local kind)  ← START HERE
- [ ] Dockerfiles: one multi-stage Go image (build arg SERVICE) for simulator/ingest/query-api (all pure Go, CGO off); Next.js image for dashboard (build with `NEXT_PUBLIC_API_BASE=http://localhost:8082`).
- [ ] k8s manifests in `deploy/k8s/`: Namespace; **ConfigMap** (KAFKA_BROKERS=kafka:9092, DATABASE_URL, REDIS_ADDR); **Secret** (postgres creds); Deployments+Services for kafka (KRaft), postgres (postgis), redis, ingest, query-api, simulator, dashboard.
- [ ] `kind create cluster`; `kind load docker-image` for the 4 app images; `kubectl apply -f deploy/k8s/`.
- [ ] Demo runbook in README (port-forward query-api :8082 and dashboard :3001).
- **Gate:** `kubectl get pods` all green; full demo works in-cluster (port-forward → map shows moving cars + alerts + charts).
- Optional/stretch: also run prometheus/grafana/kafka-exporter in-cluster (already proven in compose; not required by the gate).

### Setup already done for Phase 7
- `kind v0.32.0` installed via `go install sigs.k8s.io/kind@latest` (binary at `$(go env GOPATH)/bin/kind`; add that to PATH).
- `kubectl v1.34.1` present. Docker running.

## Environment notes (important for resume)
- **Local host ports remapped** (a native Postgres owns 5432; another project owns 8080/3000):
  Kafka `9092`, Postgres `5433`, Redis `6380`, ingest metrics `2112`, query-api `8082`(REST)/`9090`(gRPC),
  simulator control+metrics `8090`, dashboard `3001`, Prometheus `9091`, Grafana `3002`, kafka-exporter `9308`.
- **Run the stack (compose/dev):** `docker compose up -d` then `go run ./ingest`, `go run ./query-api`,
  `FLEET_SIZE=200 go run ./simulator`, and `cd dashboard && npm run dev`.
- **Stray-process gotcha:** `go run ./x` leaves a compiled child at a `go-build/...` path that survives
  naive `pkill`. Kill with `pkill -f "go run ./simulator"` AND `pkill -f "go-build.*/simulator"`.
  Prefer prebuilt binaries (`go build -o /tmp/xbin ./x`) for long-running background procs.
- **Measurement gotcha:** `rate(counter[1m])` is wrong across restarts (counter resets) — use absolute deltas.
- Dev DB tweak applied in the pgdata volume: `synchronous_commit=off`.
