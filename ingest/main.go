// Phase 0: hello-world consumer. Reads Telemetry protobuf messages and prints them.
// Phase 1 extends this to write Postgres; Phase 2 updates Redis; Phase 4 emits alerts.
package main

import (
	"context"
	"log"
	"os"

	telemetrypb "fleet-telemetry/proto/gen"

	"github.com/segmentio/kafka-go"
	"google.golang.org/protobuf/proto"
)

func main() {
	broker := getenv("KAFKA_BROKERS", "localhost:9092")
	r := kafka.NewReader(kafka.ReaderConfig{
		Brokers: []string{broker},
		Topic:   "telemetry",
		GroupID: "ingest",
	})
	defer r.Close()

	log.Printf("ingest consuming topic=telemetry group=ingest broker=%s", broker)
	for {
		m, err := r.ReadMessage(context.Background())
		if err != nil {
			log.Fatalf("read: %v", err)
		}
		var t telemetrypb.Telemetry
		if err := proto.Unmarshal(m.Value, &t); err != nil {
			log.Printf("bad message partition=%d offset=%d: %v", m.Partition, m.Offset, err)
			continue
		}
		log.Printf("consumed car_id=%s speed=%.1f battery=%.1f partition=%d offset=%d",
			t.CarId, t.Speed, t.BatteryPct, m.Partition, m.Offset)
	}
}

func getenv(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}
