package integration

import (
	"context"
	"fmt"
	"sort"
	"testing"

	"github.com/cobaltdb/cobaltdb/pkg/engine"
)

func TestParallelSelectSameAsSequential(t *testing.T) {
	ctx := context.Background()

	// Open with parallel enabled (default uses NumCPU)
	db, err := engine.Open(":memory:", engine.DefaultOptions())
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	_, err = db.Exec(ctx, `CREATE TABLE nums (id INTEGER PRIMARY KEY, val INTEGER)`)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// Insert enough rows to trigger parallel (default threshold 1000)
	for i := 1; i <= 2000; i++ {
		_, err = db.Exec(ctx, `INSERT INTO nums VALUES (?, ?)`, i, i%100)
		if err != nil {
			t.Fatalf("Failed to insert: %v", err)
		}
	}

	// Query with parallel
	parRows, err := db.Query(ctx, `SELECT val FROM nums WHERE val > 10`)
	if err != nil {
		t.Fatalf("Parallel query failed: %v", err)
	}
	defer parRows.Close()

	var parResults []int
	for parRows.Next() {
		var v int
		if err := parRows.Scan(&v); err != nil {
			t.Fatalf("Scan failed: %v", err)
		}
		parResults = append(parResults, v)
	}

	// Re-open with parallel disabled to get sequential results
	db2, err := engine.Open(":memory:", &engine.Options{
		ParallelWorkers:   0,
		ParallelThreshold: 1000,
	})
	if err != nil {
		t.Fatalf("Failed to open sequential db: %v", err)
	}
	defer db2.Close()

	_, err = db2.Exec(ctx, `CREATE TABLE nums (id INTEGER PRIMARY KEY, val INTEGER)`)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}
	for i := 1; i <= 2000; i++ {
		_, err = db2.Exec(ctx, `INSERT INTO nums VALUES (?, ?)`, i, i%100)
		if err != nil {
			t.Fatalf("Failed to insert: %v", err)
		}
	}

	seqRows, err := db2.Query(ctx, `SELECT val FROM nums WHERE val > 10`)
	if err != nil {
		t.Fatalf("Sequential query failed: %v", err)
	}
	defer seqRows.Close()

	var seqResults []int
	for seqRows.Next() {
		var v int
		if err := seqRows.Scan(&v); err != nil {
			t.Fatalf("Scan failed: %v", err)
		}
		seqResults = append(seqResults, v)
	}

	if len(parResults) != len(seqResults) {
		t.Fatalf("Row count differs: parallel=%d sequential=%d", len(parResults), len(seqResults))
	}

	// Sort both results since parallel may change order
	sort.Ints(parResults)
	sort.Ints(seqResults)
	for i := range parResults {
		if parResults[i] != seqResults[i] {
			t.Fatalf("Row %d differs: parallel=%d sequential=%d", i, parResults[i], seqResults[i])
		}
	}
}

func TestParallelGroupBySameAsSequential(t *testing.T) {
	ctx := context.Background()

	db, err := engine.Open(":memory:", engine.DefaultOptions())
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	_, err = db.Exec(ctx, `CREATE TABLE sales (id INTEGER PRIMARY KEY, region TEXT, amount INTEGER)`)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	for i := 1; i <= 2000; i++ {
		region := fmt.Sprintf("region-%d", i%10)
		_, err = db.Exec(ctx, `INSERT INTO sales VALUES (?, ?, ?)`, i, region, i%100)
		if err != nil {
			t.Fatalf("Failed to insert: %v", err)
		}
	}

	parRows, err := db.Query(ctx, `SELECT region, COUNT(*), SUM(amount) FROM sales GROUP BY region`)
	if err != nil {
		t.Fatalf("Parallel GROUP BY failed: %v", err)
	}
	defer parRows.Close()

	parResults := make(map[string][2]int64)
	for parRows.Next() {
		var region string
		var cnt, sum int64
		if err := parRows.Scan(&region, &cnt, &sum); err != nil {
			t.Fatalf("Scan failed: %v", err)
		}
		parResults[region] = [2]int64{cnt, sum}
	}

	// Sequential comparison
	db2, err := engine.Open(":memory:", &engine.Options{
		ParallelWorkers:   0,
		ParallelThreshold: 1000,
	})
	if err != nil {
		t.Fatalf("Failed to open sequential db: %v", err)
	}
	defer db2.Close()

	_, err = db2.Exec(ctx, `CREATE TABLE sales (id INTEGER PRIMARY KEY, region TEXT, amount INTEGER)`)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}
	for i := 1; i <= 2000; i++ {
		region := fmt.Sprintf("region-%d", i%10)
		_, err = db2.Exec(ctx, `INSERT INTO sales VALUES (?, ?, ?)`, i, region, i%100)
		if err != nil {
			t.Fatalf("Failed to insert: %v", err)
		}
	}

	seqRows, err := db2.Query(ctx, `SELECT region, COUNT(*), SUM(amount) FROM sales GROUP BY region`)
	if err != nil {
		t.Fatalf("Sequential GROUP BY failed: %v", err)
	}
	defer seqRows.Close()

	seqResults := make(map[string][2]int64)
	for seqRows.Next() {
		var region string
		var cnt, sum int64
		if err := seqRows.Scan(&region, &cnt, &sum); err != nil {
			t.Fatalf("Scan failed: %v", err)
		}
		seqResults[region] = [2]int64{cnt, sum}
	}

	if len(parResults) != len(seqResults) {
		t.Fatalf("Group count differs: parallel=%d sequential=%d", len(parResults), len(seqResults))
	}
	for k, pv := range parResults {
		sv, ok := seqResults[k]
		if !ok {
			t.Fatalf("Missing group %s in sequential results", k)
		}
		if pv[0] != sv[0] || pv[1] != sv[1] {
			t.Fatalf("Group %s differs: parallel=(%d,%d) sequential=(%d,%d)", k, pv[0], pv[1], sv[0], sv[1])
		}
	}
}

func TestParallelThresholdRespected(t *testing.T) {
	ctx := context.Background()

	// Small table should stay sequential even with parallel enabled
	db, err := engine.Open(":memory:", &engine.Options{
		ParallelWorkers:   4,
		ParallelThreshold: 100,
	})
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	_, err = db.Exec(ctx, `CREATE TABLE small (id INTEGER PRIMARY KEY, val INTEGER)`)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}
	for i := 1; i <= 10; i++ {
		_, err = db.Exec(ctx, `INSERT INTO small VALUES (?, ?)`, i, i)
		if err != nil {
			t.Fatalf("Failed to insert: %v", err)
		}
	}

	rows, err := db.Query(ctx, `SELECT val FROM small`)
	if err != nil {
		t.Fatalf("Query failed: %v", err)
	}
	defer rows.Close()

	var count int
	for rows.Next() {
		var v int
		if err := rows.Scan(&v); err != nil {
			t.Fatalf("Scan failed: %v", err)
		}
		count++
	}
	if count != 10 {
		t.Fatalf("Expected 10 rows, got %d", count)
	}
}
