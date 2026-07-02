// analytics runs fleet-wide batch rollups over the cold Parquet telemetry using embedded
// DuckDB — large-scale columnar processing, no server, no cluster.
package main

import (
	"database/sql"
	"fmt"
	"log"
	"os"
	"path/filepath"

	_ "github.com/marcboeker/go-duckdb/v2"
)

func main() {
	dir := getenv("PARQUET_DIR", "data/parquet")
	glob := filepath.Join(dir, "*.parquet")

	files, _ := filepath.Glob(glob)
	if len(files) == 0 {
		log.Fatalf("no parquet files in %s — run ingest with PARQUET_DIR=%s first", dir, dir)
	}
	log.Printf("analytics: reading %d parquet file(s) from %s", len(files), dir)

	db, err := sql.Open("duckdb", "")
	if err != nil {
		log.Fatalf("open duckdb: %v", err)
	}
	defer db.Close()

	// Fleet-wide rollup.
	var rows, cars int64
	var avgSpeed, maxSpeed, avgBattery, minBattery float64
	err = db.QueryRow(fmt.Sprintf(`
		SELECT count(*), count(DISTINCT car_id),
		       avg(speed), max(speed), avg(battery_pct), min(battery_pct)
		FROM read_parquet('%s')`, glob)).
		Scan(&rows, &cars, &avgSpeed, &maxSpeed, &avgBattery, &minBattery)
	if err != nil {
		log.Fatalf("rollup query: %v", err)
	}
	fmt.Println("=== fleet-wide rollup (DuckDB over Parquet) ===")
	fmt.Printf("rows=%d  cars=%d\n", rows, cars)
	fmt.Printf("speed:   avg=%.1f  max=%.1f mph\n", avgSpeed, maxSpeed)
	fmt.Printf("battery: avg=%.1f  min=%.1f %%\n", avgBattery, minBattery)

	// Top 5 fastest cars by average speed.
	top, err := db.Query(fmt.Sprintf(`
		SELECT car_id, avg(speed) AS avg_speed, count(*) AS samples
		FROM read_parquet('%s')
		GROUP BY car_id ORDER BY avg_speed DESC LIMIT 5`, glob))
	if err != nil {
		log.Fatalf("top query: %v", err)
	}
	defer top.Close()
	fmt.Println("=== top 5 cars by avg speed ===")
	for top.Next() {
		var id string
		var avg float64
		var n int64
		if err := top.Scan(&id, &avg, &n); err != nil {
			log.Fatalf("scan: %v", err)
		}
		fmt.Printf("  %-10s avg=%.1f mph  (%d samples)\n", id, avg, n)
	}
}

func getenv(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}
