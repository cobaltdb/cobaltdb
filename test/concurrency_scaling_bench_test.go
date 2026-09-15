package test

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cobaltdb/cobaltdb/pkg/engine"
)

// setupScalingDB creates an in-memory database seeded with rows for the
// concurrency scaling benchmarks.
func setupScalingDB(tb testing.TB, rows int) *engine.DB {
	tb.Helper()
	db, err := engine.Open(":memory:", nil)
	if err != nil {
		tb.Fatalf("open: %v", err)
	}
	if _, err := db.Exec(context.Background(), "CREATE TABLE bench (id INT PRIMARY KEY, val INT, name TEXT)"); err != nil {
		tb.Fatalf("create: %v", err)
	}
	for i := 0; i < rows; i++ {
		if _, err := db.Exec(context.Background(), "INSERT INTO bench (id, val, name) VALUES (?, ?, ?)",
			i, i*7%1000, fmt.Sprintf("row-%d", i)); err != nil {
			tb.Fatalf("insert %d: %v", i, err)
		}
	}
	return db
}

// measureReadThroughput runs concurrent point SELECTs for a fixed duration and
// returns operations per second.
func measureReadThroughput(tb testing.TB, db *engine.DB, workers int, dur time.Duration) float64 {
	var ops int64
	var wg sync.WaitGroup
	stop := make(chan struct{})

	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(seed int) {
			defer wg.Done()
			i := seed
			for {
				select {
				case <-stop:
					return
				default:
				}
				rows, err := db.Query(context.Background(), "SELECT val FROM bench WHERE id = ?", i%1000)
				if err != nil {
					tb.Errorf("query: %v", err)
					return
				}
				for rows.Next() {
					var v int
					_ = rows.Scan(&v)
				}
				_ = rows.Close()
				atomic.AddInt64(&ops, 1)
				i += 7
			}
		}(w * 131)
	}

	start := time.Now()
	time.Sleep(dur)
	close(stop)
	wg.Wait()
	elapsed := time.Since(start)

	return float64(atomic.LoadInt64(&ops)) / elapsed.Seconds()
}

// TestReadScaling reports concurrent read throughput at increasing worker
// counts. It is informational: it prints a scaling table rather than asserting
// a hard threshold, so it documents the lock behaviour of the read path
// without becoming flaky on shared CI machines.
func TestReadScaling(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping scaling measurement in -short mode")
	}
	db := setupScalingDB(t, 1000)
	defer func() { _ = db.Close() }()

	const dur = 500 * time.Millisecond
	var base float64
	for _, workers := range []int{1, 2, 4, 8} {
		ops := measureReadThroughput(t, db, workers, dur)
		if workers == 1 {
			base = ops
		}
		t.Logf("workers=%-2d  %10.0f ops/sec  scaling=%.2fx", workers, ops, ops/base)
	}
}

// BenchmarkConcurrentReads measures read throughput under the default parallel
// benchmark harness.
func BenchmarkConcurrentReads(b *testing.B) {
	db := setupScalingDB(b, 1000)
	defer func() { _ = db.Close() }()

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			rows, err := db.Query(context.Background(), "SELECT val FROM bench WHERE id = ?", i%1000)
			if err != nil {
				b.Errorf("query: %v", err)
				return
			}
			for rows.Next() {
				var v int
				_ = rows.Scan(&v)
			}
			_ = rows.Close()
			i += 7
		}
	})
}
