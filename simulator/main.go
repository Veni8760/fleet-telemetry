// simulator spawns one goroutine per car; each does Tier-1 random-walk movement
// and emits a Telemetry protobuf message to Kafka at a configurable rate, keyed by car_id.
package main

import (
	"context"
	"log"
	"math"
	"math/rand"
	"net"
	"os"
	"strconv"
	"sync"
	"time"

	telemetrypb "fleet-telemetry/proto/gen"

	"github.com/segmentio/kafka-go"
	"google.golang.org/protobuf/proto"
)

func main() {
	broker := getenv("KAFKA_BROKERS", "localhost:9092")
	topic := "telemetry"
	fleet, err := strconv.Atoi(getenv("FLEET_SIZE", "10"))
	if err != nil {
		log.Fatalf("FLEET_SIZE: %v", err)
	}
	rateHz, err := strconv.ParseFloat(getenv("EMIT_RATE_HZ", "1"), 64)
	if err != nil {
		log.Fatalf("EMIT_RATE_HZ: %v", err)
	}
	ensureTopic(broker, topic, 6)

	w := &kafka.Writer{
		Addr:         kafka.TCP(broker),
		Topic:        topic,
		Balancer:     &kafka.Hash{}, // key (car_id) -> stable partition
		BatchTimeout: 50 * time.Millisecond,
	}
	defer w.Close()

	log.Printf("simulator: fleet=%d rate=%.2fHz broker=%s", fleet, rateHz, broker)
	var wg sync.WaitGroup
	for i := 0; i < fleet; i++ {
		wg.Add(1)
		go func(n int) {
			defer wg.Done()
			runCar(w, n, rateHz)
		}(i)
	}
	wg.Wait()
}

type car struct {
	id       string
	lat, lng float64
	speed    float64 // mph
	heading  float64 // degrees
	battery  float64 // pct
	odo      float64 // miles
}

func runCar(w *kafka.Writer, n int, rateHz float64) {
	c := &car{
		id:      "car-" + strconv.Itoa(n),
		lat:     37.7749 + (rand.Float64()-0.5)*0.08, // scattered around San Francisco
		lng:     -122.4194 + (rand.Float64()-0.5)*0.08,
		speed:   rand.Float64() * 40,
		heading: rand.Float64() * 360,
		battery: 60 + rand.Float64()*40,
	}
	interval := time.Duration(float64(time.Second) / rateHz)
	t := time.NewTicker(interval)
	defer t.Stop()
	for range t.C {
		c.step(interval.Seconds())
		b, err := proto.Marshal(c.telemetry())
		if err != nil {
			log.Printf("marshal %s: %v", c.id, err)
			continue
		}
		if err := w.WriteMessages(context.Background(), kafka.Message{Key: []byte(c.id), Value: b}); err != nil {
			log.Printf("write %s: %v", c.id, err)
		}
	}
}

// step advances the car by dt seconds of Tier-1 random-walk movement.
func (c *car) step(dt float64) {
	c.heading = math.Mod(c.heading+(rand.Float64()-0.5)*40+360, 360)
	c.speed = clamp(c.speed+(rand.Float64()-0.5)*10, 0, 80)
	distM := c.speed * 0.44704 * dt // mph -> m/s -> meters
	rad := c.heading * math.Pi / 180
	c.lat += distM * math.Cos(rad) / 111320
	c.lng += distM * math.Sin(rad) / (111320 * math.Cos(c.lat*math.Pi/180))
	c.odo += distM / 1609.34
	c.battery = clamp(c.battery-(0.005+c.speed*0.0005)*dt, 0, 100)
}

func (c *car) telemetry() *telemetrypb.Telemetry {
	return &telemetrypb.Telemetry{
		CarId:      c.id,
		Ts:         time.Now().UnixMilli(),
		Lat:        c.lat,
		Lng:        c.lng,
		Speed:      c.speed,
		Heading:    c.heading,
		BatteryPct: c.battery,
		MotorTemp:  40 + c.speed*0.35, // rises with speed
		Odometer:   c.odo,
		Gear:       "D",
	}
}

func ensureTopic(broker, topic string, partitions int) {
	conn, err := kafka.Dial("tcp", broker)
	if err != nil {
		log.Fatalf("dial: %v", err)
	}
	defer conn.Close()
	controller, err := conn.Controller()
	if err != nil {
		log.Fatalf("controller: %v", err)
	}
	cc, err := kafka.Dial("tcp", net.JoinHostPort(controller.Host, strconv.Itoa(controller.Port)))
	if err != nil {
		log.Fatalf("dial controller: %v", err)
	}
	defer cc.Close()
	// Idempotent: already-exists is fine.
	_ = cc.CreateTopics(kafka.TopicConfig{Topic: topic, NumPartitions: partitions, ReplicationFactor: 1})
}

func getenv(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}

func clamp(v, lo, hi float64) float64 {
	return max(lo, min(hi, v))
}
