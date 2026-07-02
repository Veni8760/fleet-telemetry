// loadgen blasts a large burst of synthetic telemetry across ~10k car_ids to stress the
// consumer group (backlog -> lag -> recovery as replicas share partitions). Produces COUNT
// messages then exits. Topic is assumed to exist (created by simulator/Phase 0).
package main

import (
	"context"
	"log"
	"math/rand"
	"os"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	telemetrypb "fleet-telemetry/proto/gen"

	"github.com/segmentio/kafka-go"
	"google.golang.org/protobuf/proto"
)

func main() {
	broker := getenv("KAFKA_BROKERS", "localhost:9092")
	cars := atoi(getenv("CARS", "10000"))
	total := atoi(getenv("COUNT", "300000"))
	workers := atoi(getenv("WORKERS", "8"))
	const batch = 500

	w := &kafka.Writer{
		Addr:         kafka.TCP(broker),
		Topic:        "telemetry",
		Balancer:     &kafka.Hash{},
		BatchSize:    batch,
		BatchTimeout: 10 * time.Millisecond,
		RequiredAcks: kafka.RequireOne,
	}
	defer w.Close()

	log.Printf("loadgen: producing %d messages across %d cars via %d workers", total, cars, workers)
	var produced atomic.Int64
	per := total / workers
	var wg sync.WaitGroup
	start := time.Now()
	for id := 0; id < workers; id++ {
		wg.Add(1)
		go func(seed int) {
			defer wg.Done()
			rng := rand.New(rand.NewSource(int64(seed) + 1))
			msgs := make([]kafka.Message, 0, batch)
			flush := func() {
				if len(msgs) == 0 {
					return
				}
				if err := w.WriteMessages(context.Background(), msgs...); err != nil {
					log.Printf("worker %d write: %v", seed, err)
				}
				produced.Add(int64(len(msgs)))
				msgs = msgs[:0]
			}
			for i := 0; i < per; i++ {
				carID := "car-" + strconv.Itoa(rng.Intn(cars))
				b, _ := proto.Marshal(&telemetrypb.Telemetry{
					CarId:      carID,
					Ts:         time.Now().UnixMilli(),
					Lat:        37.7749 + (rng.Float64()-0.5)*0.2,
					Lng:        -122.4194 + (rng.Float64()-0.5)*0.2,
					Speed:      rng.Float64() * 80,
					Heading:    rng.Float64() * 360,
					BatteryPct: rng.Float64() * 100,
					MotorTemp:  40 + rng.Float64()*40,
					Gear:       "D",
				})
				msgs = append(msgs, kafka.Message{Key: []byte(carID), Value: b})
				if len(msgs) == batch {
					flush()
				}
			}
			flush()
		}(id)
	}
	wg.Wait()
	n := produced.Load()
	dt := time.Since(start).Seconds()
	log.Printf("loadgen: produced %d messages in %.1fs (%.0f msg/s)", n, dt, float64(n)/dt)
}

func getenv(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}

func atoi(s string) int {
	n, err := strconv.Atoi(s)
	if err != nil {
		log.Fatalf("bad int %q: %v", s, err)
	}
	return n
}
