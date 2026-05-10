package engine

import (
	"context"
	"fmt"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
)

func BenchmarkConcurrentWritersScale(b *testing.B) {
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

	for _, workers := range []int{1, 2, 4, 8, 16} {
		b.Run(fmt.Sprintf("workers=%d", workers), func(b *testing.B) {
			if _, err := db.Exec(ctx, "DROP TABLE IF EXISTS bench"); err != nil {
				b.Fatalf("drop table: %v", err)
			}
			if _, err := db.Exec(ctx, "CREATE TABLE bench (id INT PRIMARY KEY, n INT)"); err != nil {
				b.Fatalf("create table: %v", err)
			}

			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				var wg sync.WaitGroup
				var firstErr atomic.Pointer[error]
				perWorker := 100
				for w := 0; w < workers; w++ {
					wg.Add(1)
					go func(id, base int) {
						defer wg.Done()
						tx, err := db.Begin(ctx)
						if err != nil {
							firstErr.CompareAndSwap(nil, &err)
							return
						}
						for j := 0; j < perWorker; j++ {
							key := base + id*perWorker + j
							if _, err := tx.Exec(ctx, "INSERT INTO bench VALUES (?, ?)", key, id); err != nil {
								firstErr.CompareAndSwap(nil, &err)
								return
							}
						}
						if err := tx.Commit(); err != nil {
							firstErr.CompareAndSwap(nil, &err)
							return
						}
					}(w, i*workers*perWorker)
				}
				wg.Wait()
				if p := firstErr.Load(); p != nil {
					b.Fatalf("worker error: %v", *p)
				}
			}
			b.ReportMetric(float64(workers*100*b.N)/b.Elapsed().Seconds(), "ops/sec")
		})
	}
}

// BenchmarkConcurrentWritersScaleSmallTxn uses 1-row explicit transactions so
// that the adaptive commit locking uses sharded locks (≤32 shards) instead of
// that every transaction touches ≤32 unique shards. This measures the small-transaction
// parallelism the sharded path was designed for.
func BenchmarkConcurrentWritersScaleSmallTxn(b *testing.B) {
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

	for _, workers := range []int{1, 2, 4, 8, 16} {
		b.Run(fmt.Sprintf("workers=%d", workers), func(b *testing.B) {
			if _, err := db.Exec(ctx, "DROP TABLE IF EXISTS bench"); err != nil {
				b.Fatalf("drop table: %v", err)
			}
			if _, err := db.Exec(ctx, "CREATE TABLE bench (id INT PRIMARY KEY, n INT)"); err != nil {
				b.Fatalf("create table: %v", err)
			}

			perWorker := 1
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				var wg sync.WaitGroup
				var firstErr atomic.Pointer[error]
				for w := 0; w < workers; w++ {
					wg.Add(1)
					go func(id, base int) {
						defer wg.Done()
						tx, err := db.Begin(ctx)
						if err != nil {
							firstErr.CompareAndSwap(nil, &err)
							return
						}
						for j := 0; j < perWorker; j++ {
							key := base + id*perWorker + j
							if _, err := tx.Exec(ctx, "INSERT INTO bench VALUES (?, ?)", key, id); err != nil {
								firstErr.CompareAndSwap(nil, &err)
								return
							}
						}
						if err := tx.Commit(); err != nil {
							firstErr.CompareAndSwap(nil, &err)
							return
						}
					}(w, i*workers*perWorker)
				}
				wg.Wait()
				if p := firstErr.Load(); p != nil {
					b.Fatalf("worker error: %v", *p)
				}
			}
			b.ReportMetric(float64(workers*perWorker*b.N)/b.Elapsed().Seconds(), "ops/sec")
		})
	}
}
