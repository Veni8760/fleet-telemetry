// ingest consumes the telemetry topic, persists history to Postgres (idempotent on
// (car_id, ts)), and updates Redis hot state (fleet:latest hash, raw protobuf bytes
// per car). query-api owns all reads. Phase 4 adds anomaly rules -> alerts topic.
package main

import (
	"context"
	"log"
	"os"

	telemetrypb "fleet-telemetry/proto/gen"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redis/go-redis/v9"
	"github.com/segmentio/kafka-go"
	"google.golang.org/protobuf/proto"
)

const schema = `
CREATE TABLE IF NOT EXISTS telemetry (
  car_id      text             NOT NULL,
  ts          bigint           NOT NULL,
  lat         double precision,
  lng         double precision,
  speed       double precision,
  heading     double precision,
  battery_pct double precision,
  motor_temp  double precision,
  odometer    double precision,
  gear        text,
  fault_codes text[],
  PRIMARY KEY (car_id, ts)
);
CREATE INDEX IF NOT EXISTS telemetry_car_ts ON telemetry (car_id, ts DESC);`

const insertSQL = `
INSERT INTO telemetry
  (car_id, ts, lat, lng, speed, heading, battery_pct, motor_temp, odometer, gear, fault_codes)
VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11)
ON CONFLICT (car_id, ts) DO NOTHING`

// hotKey is the Redis hash of latest telemetry per car (field=car_id, value=protobuf bytes).
const hotKey = "fleet:latest"

func main() {
	broker := getenv("KAFKA_BROKERS", "localhost:9092")
	dbURL := getenv("DATABASE_URL", "postgres://fleet:fleet@localhost:5433/fleet")
	redisAddr := getenv("REDIS_ADDR", "localhost:6380")

	ctx := context.Background()
	pool, err := pgxpool.New(ctx, dbURL)
	if err != nil {
		log.Fatalf("pg connect: %v", err)
	}
	defer pool.Close()
	if _, err := pool.Exec(ctx, schema); err != nil {
		log.Fatalf("schema: %v", err)
	}

	rdb := redis.NewClient(&redis.Options{Addr: redisAddr})
	defer rdb.Close()

	r := kafka.NewReader(kafka.ReaderConfig{
		Brokers: []string{broker},
		Topic:   "telemetry",
		GroupID: "ingest",
	})
	defer r.Close()

	log.Printf("ingest: consuming topic=telemetry group=ingest broker=%s pg=ok redis=%s", broker, redisAddr)
	for {
		m, err := r.ReadMessage(ctx)
		if err != nil {
			log.Fatalf("read: %v", err)
		}
		var t telemetrypb.Telemetry
		if err := proto.Unmarshal(m.Value, &t); err != nil {
			log.Printf("bad message partition=%d offset=%d: %v", m.Partition, m.Offset, err)
			continue
		}
		if _, err := pool.Exec(ctx, insertSQL,
			t.CarId, t.Ts, t.Lat, t.Lng, t.Speed, t.Heading,
			t.BatteryPct, t.MotorTemp, t.Odometer, t.Gear, t.FaultCodes); err != nil {
			log.Printf("insert %s: %v", t.CarId, err)
		}
		// Hot state: store the raw Kafka bytes (already protobuf) keyed by car_id.
		if err := rdb.HSet(ctx, hotKey, t.CarId, m.Value).Err(); err != nil {
			log.Printf("redis hset %s: %v", t.CarId, err)
		}
	}
}

func getenv(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}
