// simulator spawns one goroutine per car; each does Tier-1 random-walk movement
// and emits a Telemetry protobuf message to Kafka at a configurable rate, keyed by car_id.
package main

import (
	"context"
	"log"
	"math"
	"math/rand"
	"net"
	"net/http"
	"os"
	"strconv"
	"sync"
	"time"

	telemetrypb "fleet-telemetry/proto/gen"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/segmentio/kafka-go"
	"google.golang.org/protobuf/proto"
)

// faulty holds car_ids currently injected with an overheat fault (car_id -> true).
var faulty sync.Map

var metricProduced = promauto.NewCounter(prometheus.CounterOpts{
	Name: "simulator_messages_total", Help: "Telemetry messages produced.",
})

// serveControl exposes fault injection for the demo:
//
//	curl -X POST 'localhost:8090/inject?car=car-3'   -> car-3 starts emitting OVERHEAT
//	curl -X POST 'localhost:8090/clear?car=car-3'    -> back to normal
func serveControl(addr string) {
	mux := http.NewServeMux()
	mux.HandleFunc("/inject", func(w http.ResponseWriter, r *http.Request) {
		car := r.URL.Query().Get("car")
		faulty.Store(car, true)
		log.Printf("injected fault: %s", car)
		w.Write([]byte("injected " + car))
	})
	mux.HandleFunc("/clear", func(w http.ResponseWriter, r *http.Request) {
		faulty.Delete(r.URL.Query().Get("car"))
		w.Write([]byte("cleared"))
	})
	mux.Handle("/metrics", promhttp.Handler())
	log.Printf("simulator: fault-injection control + metrics on %s", addr)
	log.Fatal(http.ListenAndServe(addr, mux))
}

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
	go serveControl(getenv("SIM_ADDR", ":8090"))

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
			continue
		}
		metricProduced.Inc()
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
	motorTemp := 40 + c.speed*0.35 // normal: rises with speed
	var faults []string
	if _, ok := faulty.Load(c.id); ok {
		motorTemp = 130 // injected overheat, above the 120°C alert threshold
		faults = []string{"OVERHEAT"}
	}
	return &telemetrypb.Telemetry{
		CarId:      c.id,
		Ts:         time.Now().UnixMilli(),
		Lat:        c.lat,
		Lng:        c.lng,
		Speed:      c.speed,
		Heading:    c.heading,
		BatteryPct: c.battery,
		MotorTemp:  motorTemp,
		Odometer:   c.odo,
		Gear:       "D",
		FaultCodes: faults,
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
