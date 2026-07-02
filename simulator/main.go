// Phase 0: hello-world producer. Emits one Telemetry protobuf message to Kafka.
// Phase 1 replaces this with N-goroutine random-walk fleet.
package main

import (
	"context"
	"log"
	"net"
	"os"
	"strconv"
	"time"

	telemetrypb "fleet-telemetry/proto/gen"

	"github.com/segmentio/kafka-go"
	"google.golang.org/protobuf/proto"
)

func main() {
	broker := getenv("KAFKA_BROKERS", "localhost:9092")
	topic := "telemetry"
	ensureTopic(broker, topic, 6)

	w := &kafka.Writer{
		Addr:     kafka.TCP(broker),
		Topic:    topic,
		Balancer: &kafka.Hash{}, // key (car_id) -> stable partition
	}
	defer w.Close()

	msg := &telemetrypb.Telemetry{
		CarId:      "hello-0",
		Ts:         time.Now().UnixMilli(),
		Lat:        37.7749,
		Lng:        -122.4194,
		Speed:      42,
		Heading:    90,
		BatteryPct: 87,
		MotorTemp:  55,
		Odometer:   1234,
		Gear:       "D",
	}
	b, err := proto.Marshal(msg)
	if err != nil {
		log.Fatalf("marshal: %v", err)
	}
	if err := w.WriteMessages(context.Background(), kafka.Message{Key: []byte(msg.CarId), Value: b}); err != nil {
		log.Fatalf("write: %v", err)
	}
	log.Printf("produced car_id=%s to topic=%s", msg.CarId, topic)
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
	if err := cc.CreateTopics(kafka.TopicConfig{Topic: topic, NumPartitions: partitions, ReplicationFactor: 1}); err != nil {
		log.Fatalf("create topic: %v", err)
	}
}

func getenv(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}
