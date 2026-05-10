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
	opts.EnableScheduler = false
	opts.EnableAutoCheckpoint = false
	opts.EnableAutoVacuum = false
	db, err := Open(filepath.Join(dir, "bench.db"), opts)
	if err != nil {
		b.Fatalf("open db: %v", err)
	}
	defer db.Close()

	ctx := context.Background()
	_, _ = db.Exec(ctx, "CREATE TABLE bench (id INT PRIMARY KEY, n INT)")

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
			if _, err := tx.Exec(ctx, "INSERT INTO bench VALUES (?, ?)", i, 1); err != nil {
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
