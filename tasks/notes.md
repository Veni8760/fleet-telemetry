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
