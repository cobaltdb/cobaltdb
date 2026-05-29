package engine

import (
	"context"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func BenchmarkAdHocSmallTxn(b *testing.B) {
	dir := b.TempDir()
	opts := DefaultOptions()
	opts.Scheduler.EnableScheduler = false
	opts.Maintenance.EnableAutoCheckpoint = false
	opts.Maintenance.EnableAutoVacuum = false
	db, err := Open(filepath.Join(dir, "bench.db"), opts)
	if err != nil {
		b.Fatalf("open db: %v", err)
	}
	defer db.Close()

	ctx := context.Background()
	_, _ = db.Exec(ctx, "CREATE TABLE bench (id INT PRIMARY KEY, n INT)")

	args := make([]interface{}, 2)
	args[1] = 1

	// Pre-box all integer arguments so the hot loop does not allocate.
	boxed := make([]interface{}, b.N)
	for i := range boxed {
		boxed[i] = i
	}

	b.ResetTimer()
	var count atomic.Int64
	var wg sync.WaitGroup
	start := time.Now()
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < b.N; i++ {
			tx, err := db.Begin(ctx)
			if err != nil {
				continue
			}
			args[0] = boxed[i]
			if _, err := tx.Exec(ctx, "INSERT INTO bench VALUES (?, ?)", args...); err != nil {
				_ = tx.Rollback()
				continue
			}
			if err := tx.Commit(); err != nil {
				continue
			}
			count.Add(1)
		}
	}()
	wg.Wait()
	elapsed := time.Since(start)
	b.ReportMetric(float64(count.Load())/elapsed.Seconds(), "ops/sec")
}
