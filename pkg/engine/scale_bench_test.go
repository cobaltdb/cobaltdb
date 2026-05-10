package engine

import (
	"context"
	"fmt"
	"path/filepath"
	"sync"
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
			var wg sync.WaitGroup
			start := make(chan struct{})
			for w := 0; w < workers; w++ {
				wg.Add(1)
				go func(id int) {
					defer wg.Done()
					<-start
					base := id * 1_000_000
					count := b.N / workers
					if id < b.N%workers {
						count++
					}
					for i := 0; i < count; i++ {
						tx, err := db.Begin(ctx)
						if err != nil {
							continue
						}
						for j := 0; j < 100; j++ {
							key := base + i*100 + j
							if _, err := tx.Exec(ctx, "INSERT INTO bench VALUES (?, ?)", key, id); err != nil {
								_ = tx.Rollback()
								continue
							}
						}
						if err := tx.Commit(); err != nil {
							continue
						}
					}
				}(w)
			}
			close(start)
			wg.Wait()
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

			b.ResetTimer()
			var wg sync.WaitGroup
			start := make(chan struct{})
			for w := 0; w < workers; w++ {
				wg.Add(1)
				go func(id int) {
					defer wg.Done()
					<-start
					base := id * 1_000_000
					count := b.N / workers
					if id < b.N%workers {
						count++
					}
					for i := 0; i < count; i++ {
						tx, err := db.Begin(ctx)
						if err != nil {
							continue
						}
						if _, err := tx.Exec(ctx, "INSERT INTO bench VALUES (?, ?)", base+i, id); err != nil {
							_ = tx.Rollback()
							continue
						}
						if err := tx.Commit(); err != nil {
							continue
						}
					}
				}(w)
			}
			close(start)
			wg.Wait()
			b.ReportMetric(float64(b.N)/b.Elapsed().Seconds(), "ops/sec")
		})
	}
}
