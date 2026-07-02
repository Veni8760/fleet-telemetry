// Throwaway gRPC smoke client for the Phase 2 gate: calls all three FleetService RPCs.
package main

import (
	"context"
	"log"
	"time"

	telemetrypb "fleet-telemetry/proto/gen"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func main() {
	conn, err := grpc.NewClient("localhost:9090", grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("dial: %v", err)
	}
	defer conn.Close()
	c := telemetrypb.NewFleetServiceClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	snap, err := c.Snapshot(ctx, &telemetrypb.SnapshotRequest{})
	if err != nil {
		log.Fatalf("Snapshot: %v", err)
	}
	log.Printf("gRPC Snapshot: %d cars", len(snap.Cars))

	q, err := c.Query(ctx, &telemetrypb.QueryRequest{MinSpeed: 60})
	if err != nil {
		log.Fatalf("Query: %v", err)
	}
	ok := true
	for _, car := range q.Cars {
		if car.Speed < 60 {
			ok = false
		}
	}
	log.Printf("gRPC Query(min_speed=60): %d cars, allSatisfy=%v", len(q.Cars), ok)

	g, err := c.GeoQuery(ctx, &telemetrypb.GeoQueryRequest{MinLat: 37.75, MinLng: -122.45, MaxLat: 37.80, MaxLng: -122.40})
	if err != nil {
		log.Fatalf("GeoQuery: %v", err)
	}
	inside := true
	for _, car := range g.Cars {
		if car.Lat < 37.75 || car.Lat > 37.80 || car.Lng < -122.45 || car.Lng > -122.40 {
			inside = false
		}
	}
	log.Printf("gRPC GeoQuery(bbox): %d cars, allInside=%v", len(g.Cars), inside)
}
