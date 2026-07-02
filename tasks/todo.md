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

- [x] **Phase 7 — Kubernetes (local kind)** — 4 images (1 multi-stage Go + Next.js standalone), manifests in `deploy/k8s/` (Namespace/ConfigMap/Secret, Kafka/Postgres/Redis infra, 4 app Deployments), `kind` cluster. **Gate passed:** all 7 pods green; in-cluster demo = 200 cars on live map + injected OVERHEAT alerts + charts (`tasks/phase7-k8s-dashboard.png`). Runbook in README.

## All phases complete 🎉

Nothing left in the core roadmap. Optional stretch ideas (not required by any gate):
- Observability in-cluster (prometheus/grafana/kafka-exporter) — already proven in compose.
- MCP natural-language fleet queries · Java/Spring query-api variant · Python ML anomaly · cloud k8s.

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
