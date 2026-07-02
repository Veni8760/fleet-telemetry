// ingest consumes the telemetry topic, persists history to Postgres (idempotent on
// (car_id, ts)), and serves latest positions over HTTP for the dashboard.
// Phase 2 adds Redis hot state; Phase 4 adds anomaly rules -> alerts topic.
package main

import (
	"context"
	"encoding/json"
	"log"
	"net/http"
	"os"

	telemetrypb "fleet-telemetry/proto/gen"

	"github.com/jackc/pgx/v5/pgxpool"
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

func main() {
	broker := getenv("KAFKA_BROKERS", "localhost:9092")
	dbURL := getenv("DATABASE_URL", "postgres://fleet:fleet@localhost:5433/fleet")
	httpAddr := getenv("HTTP_ADDR", ":8081")

	ctx := context.Background()
	pool, err := pgxpool.New(ctx, dbURL)
	if err != nil {
		log.Fatalf("pg connect: %v", err)
	}
	defer pool.Close()
	if _, err := pool.Exec(ctx, schema); err != nil {
		log.Fatalf("schema: %v", err)
	}

	go serveHTTP(pool, httpAddr)

	r := kafka.NewReader(kafka.ReaderConfig{
		Brokers: []string{broker},
		Topic:   "telemetry",
		GroupID: "ingest",
	})
	defer r.Close()

	log.Printf("ingest: consuming topic=telemetry group=ingest broker=%s http=%s", broker, httpAddr)
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
	}
}

type position struct {
	CarID      string  `json:"car_id"`
	Ts         int64   `json:"ts"`
	Lat        float64 `json:"lat"`
	Lng        float64 `json:"lng"`
	Speed      float64 `json:"speed"`
	Heading    float64 `json:"heading"`
	BatteryPct float64 `json:"battery_pct"`
	MotorTemp  float64 `json:"motor_temp"`
}

const latestSQL = `
SELECT DISTINCT ON (car_id) car_id, ts, lat, lng, speed, heading, battery_pct, motor_temp
FROM telemetry
ORDER BY car_id, ts DESC`

func serveHTTP(pool *pgxpool.Pool, addr string) {
	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.Write([]byte("ok"))
	})
	mux.HandleFunc("/api/positions", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Access-Control-Allow-Origin", "*")
		rows, err := pool.Query(r.Context(), latestSQL)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		defer rows.Close()
		out := []position{}
		for rows.Next() {
			var p position
			if err := rows.Scan(&p.CarID, &p.Ts, &p.Lat, &p.Lng, &p.Speed, &p.Heading, &p.BatteryPct, &p.MotorTemp); err != nil {
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			}
			out = append(out, p)
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(out)
	})
	log.Printf("http listening on %s", addr)
	log.Fatal(http.ListenAndServe(addr, mux))
}

func getenv(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}
