// query-api serves "query the fleet": current snapshot + speed/battery filters from Redis
// hot state, and bounding-box geo queries via PostGIS. Exposes gRPC and REST (SSE in Phase 6).
package main

import (
	"context"
	"encoding/json"
	"log"
	"net"
	"net/http"
	"os"
	"strconv"
	"sync"

	telemetrypb "fleet-telemetry/proto/gen"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/redis/go-redis/v9"
	"github.com/segmentio/kafka-go"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"
)

const hotKey = "fleet:latest"

var metricRequests = promauto.NewCounterVec(prometheus.CounterOpts{
	Name: "queryapi_requests_total", Help: "REST requests by endpoint.",
}, []string{"endpoint"})

type server struct {
	telemetrypb.UnimplementedFleetServiceServer
	rdb    *redis.Client
	pool   *pgxpool.Pool
	alerts *alertStore
}

// alertStore keeps the most recent alerts in memory (newest first) for the dashboard feed.
type alertStore struct {
	mu  sync.Mutex
	buf []*telemetrypb.Alert
	max int
}

func (a *alertStore) add(al *telemetrypb.Alert) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.buf = append([]*telemetrypb.Alert{al}, a.buf...)
	if len(a.buf) > a.max {
		a.buf = a.buf[:a.max]
	}
}

func (a *alertStore) list() []*telemetrypb.Alert {
	a.mu.Lock()
	defer a.mu.Unlock()
	out := make([]*telemetrypb.Alert, len(a.buf))
	copy(out, a.buf)
	return out
}

// consumeAlerts streams the alerts topic into the store (only new alerts on a fresh group).
func consumeAlerts(broker string, store *alertStore) {
	r := kafka.NewReader(kafka.ReaderConfig{
		Brokers:     []string{broker},
		Topic:       "alerts",
		GroupID:     "query-api",
		StartOffset: kafka.LastOffset,
	})
	defer r.Close()
	for {
		m, err := r.ReadMessage(context.Background())
		if err != nil {
			log.Printf("alerts read: %v", err)
			return
		}
		var al telemetrypb.Alert
		if err := proto.Unmarshal(m.Value, &al); err != nil {
			continue
		}
		store.add(&al)
	}
}

func main() {
	dbURL := getenv("DATABASE_URL", "postgres://fleet:fleet@localhost:5433/fleet")
	redisAddr := getenv("REDIS_ADDR", "localhost:6380")
	grpcAddr := getenv("GRPC_ADDR", ":9090")
	httpAddr := getenv("HTTP_ADDR", ":8082")
	broker := getenv("KAFKA_BROKERS", "localhost:9092")

	ctx := context.Background()
	pool, err := pgxpool.New(ctx, dbURL)
	if err != nil {
		log.Fatalf("pg connect: %v", err)
	}
	defer pool.Close()
	if _, err := pool.Exec(ctx, "CREATE EXTENSION IF NOT EXISTS postgis"); err != nil {
		log.Fatalf("postgis extension: %v", err)
	}

	s := &server{
		rdb:    redis.NewClient(&redis.Options{Addr: redisAddr}),
		pool:   pool,
		alerts: &alertStore{max: 100},
	}
	defer s.rdb.Close()

	// Live alerts feed: consume the alerts topic into memory.
	go consumeAlerts(broker, s.alerts)

	// gRPC
	go func() {
		lis, err := net.Listen("tcp", grpcAddr)
		if err != nil {
			log.Fatalf("grpc listen: %v", err)
		}
		gs := grpc.NewServer()
		telemetrypb.RegisterFleetServiceServer(gs, s)
		log.Printf("query-api: gRPC on %s", grpcAddr)
		log.Fatal(gs.Serve(lis))
	}()

	// REST
	s.serveHTTP(httpAddr)
}

// --- core queries (shared by gRPC + REST) ---

func (s *server) snapshot(ctx context.Context) ([]*telemetrypb.Telemetry, error) {
	vals, err := s.rdb.HGetAll(ctx, hotKey).Result()
	if err != nil {
		return nil, err
	}
	out := make([]*telemetrypb.Telemetry, 0, len(vals))
	for _, v := range vals {
		var t telemetrypb.Telemetry
		if err := proto.Unmarshal([]byte(v), &t); err != nil {
			continue // skip corrupt entry rather than fail the whole query
		}
		out = append(out, &t)
	}
	return out, nil
}

func filter(cars []*telemetrypb.Telemetry, minSpeed, maxBattery float64) []*telemetrypb.Telemetry {
	out := make([]*telemetrypb.Telemetry, 0, len(cars))
	for _, c := range cars {
		if c.Speed < minSpeed {
			continue
		}
		if maxBattery > 0 && c.BatteryPct > maxBattery {
			continue
		}
		out = append(out, c)
	}
	return out
}

const geoSQL = `
SELECT car_id, ts, lat, lng, speed, heading, battery_pct, motor_temp, odometer, gear, fault_codes
FROM (SELECT DISTINCT ON (car_id) * FROM telemetry ORDER BY car_id, ts DESC) latest
WHERE ST_Contains(
  ST_MakeEnvelope($1, $2, $3, $4, 4326),           -- xmin=min_lng, ymin=min_lat, xmax=max_lng, ymax=max_lat
  ST_SetSRID(ST_MakePoint(lng, lat), 4326)
)`

func (s *server) geo(ctx context.Context, minLat, minLng, maxLat, maxLng float64) ([]*telemetrypb.Telemetry, error) {
	rows, err := s.pool.Query(ctx, geoSQL, minLng, minLat, maxLng, maxLat)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []*telemetrypb.Telemetry{}
	for rows.Next() {
		var t telemetrypb.Telemetry
		if err := rows.Scan(&t.CarId, &t.Ts, &t.Lat, &t.Lng, &t.Speed, &t.Heading,
			&t.BatteryPct, &t.MotorTemp, &t.Odometer, &t.Gear, &t.FaultCodes); err != nil {
			return nil, err
		}
		out = append(out, &t)
	}
	return out, rows.Err()
}

// --- gRPC ---

func (s *server) Snapshot(ctx context.Context, _ *telemetrypb.SnapshotRequest) (*telemetrypb.Fleet, error) {
	cars, err := s.snapshot(ctx)
	return &telemetrypb.Fleet{Cars: cars}, err
}

func (s *server) Query(ctx context.Context, req *telemetrypb.QueryRequest) (*telemetrypb.Fleet, error) {
	cars, err := s.snapshot(ctx)
	if err != nil {
		return nil, err
	}
	return &telemetrypb.Fleet{Cars: filter(cars, req.MinSpeed, req.MaxBattery)}, nil
}

func (s *server) GeoQuery(ctx context.Context, req *telemetrypb.GeoQueryRequest) (*telemetrypb.Fleet, error) {
	cars, err := s.geo(ctx, req.MinLat, req.MinLng, req.MaxLat, req.MaxLng)
	return &telemetrypb.Fleet{Cars: cars}, err
}

// --- REST ---

type carJSON struct {
	CarID      string  `json:"car_id"`
	Ts         int64   `json:"ts"`
	Lat        float64 `json:"lat"`
	Lng        float64 `json:"lng"`
	Speed      float64 `json:"speed"`
	Heading    float64 `json:"heading"`
	BatteryPct float64 `json:"battery_pct"`
	MotorTemp  float64 `json:"motor_temp"`
}

type alertJSON struct {
	CarID   string  `json:"car_id"`
	Ts      int64   `json:"ts"`
	Type    string  `json:"type"`
	Message string  `json:"message"`
	Value   float64 `json:"value"`
}

func toJSON(cars []*telemetrypb.Telemetry) []carJSON {
	out := make([]carJSON, len(cars))
	for i, c := range cars {
		out[i] = carJSON{c.CarId, c.Ts, c.Lat, c.Lng, c.Speed, c.Heading, c.BatteryPct, c.MotorTemp}
	}
	return out
}

func (s *server) serveHTTP(addr string) {
	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) { w.Write([]byte("ok")) })
	mux.Handle("/metrics", promhttp.Handler())

	mux.HandleFunc("/api/positions", func(w http.ResponseWriter, r *http.Request) {
		metricRequests.WithLabelValues("positions").Inc()
		cars, err := s.snapshot(r.Context())
		writeCars(w, cars, err)
	})

	mux.HandleFunc("/api/query", func(w http.ResponseWriter, r *http.Request) {
		metricRequests.WithLabelValues("query").Inc()
		cars, err := s.snapshot(r.Context())
		if err != nil {
			writeCars(w, nil, err)
			return
		}
		writeCars(w, filter(cars, qf(r, "min_speed"), qf(r, "max_battery")), nil)
	})

	mux.HandleFunc("/api/alerts", func(w http.ResponseWriter, _ *http.Request) {
		metricRequests.WithLabelValues("alerts").Inc()
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Content-Type", "application/json")
		als := s.alerts.list()
		out := make([]alertJSON, len(als))
		for i, a := range als {
			out[i] = alertJSON{a.CarId, a.Ts, a.Type, a.Message, a.Value}
		}
		json.NewEncoder(w).Encode(out)
	})

	mux.HandleFunc("/api/geo", func(w http.ResponseWriter, r *http.Request) {
		metricRequests.WithLabelValues("geo").Inc()
		cars, err := s.geo(r.Context(), qf(r, "min_lat"), qf(r, "min_lng"), qf(r, "max_lat"), qf(r, "max_lng"))
		writeCars(w, cars, err)
	})

	log.Printf("query-api: REST on %s", addr)
	log.Fatal(http.ListenAndServe(addr, mux))
}

func writeCars(w http.ResponseWriter, cars []*telemetrypb.Telemetry, err error) {
	w.Header().Set("Access-Control-Allow-Origin", "*")
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(toJSON(cars))
}

// qf parses a float query param; missing/blank -> 0.
func qf(r *http.Request, key string) float64 {
	f, _ := strconv.ParseFloat(r.URL.Query().Get(key), 64)
	return f
}

func getenv(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}
