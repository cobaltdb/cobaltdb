package engine

import (
	"context"
	"fmt"
	"path/filepath"
	"sort"
	"sync"
	"sync/atomic"
	"testing"
	"time"
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

func BenchmarkWriteLatencyUnderReaders(b *testing.B) {
	for _, readers := range []int{0, 4, 16} {
		b.Run(fmt.Sprintf("readers=%d", readers), func(b *testing.B) {
			db := setupBenchDB(b)
			defer db.Close()

			ctx := context.Background()
			if _, err := db.Exec(ctx, "CREATE TABLE bench_latency (id INT PRIMARY KEY, n INT)"); err != nil {
				b.Fatalf("create table: %v", err)
			}
			for i := 0; i < 1000; i++ {
				if _, err := db.Exec(ctx, "INSERT INTO bench_latency VALUES (?, ?)", i, i%10); err != nil {
					b.Fatalf("seed row: %v", err)
				}
			}

			var stop atomic.Bool
			var readErrors atomic.Int64
			var wg sync.WaitGroup
			for r := 0; r < readers; r++ {
				wg.Add(1)
				go func(id int) {
					defer wg.Done()
					for i := 0; !stop.Load(); i++ {
						rows, err := db.Query(ctx, "SELECT COUNT(*) FROM bench_latency WHERE n >= ?", (id+i)%10)
						if err != nil {
							readErrors.Add(1)
							continue
						}
						var count int64
						if rows.Next() {
							if err := rows.Scan(&count); err != nil {
								readErrors.Add(1)
							}
						}
						_ = rows.Close()
					}
				}(r)
			}

			latencies := make([]int64, b.N)
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				start := time.Now()
				if _, err := db.Exec(ctx, "INSERT INTO bench_latency VALUES (?, ?)", 1_000_000+i, i%10); err != nil {
					b.Fatalf("insert: %v", err)
				}
				latencies[i] = time.Since(start).Nanoseconds()
			}
			b.StopTimer()

			stop.Store(true)
			wg.Wait()
			if readErrors.Load() > 0 {
				b.Fatalf("background reader errors: %d", readErrors.Load())
			}

			sort.Slice(latencies, func(i, j int) bool { return latencies[i] < latencies[j] })
			b.ReportMetric(float64(b.N)/b.Elapsed().Seconds(), "writes/sec")
			b.ReportMetric(float64(percentileLatency(latencies, 50))/float64(time.Millisecond), "p50_ms")
			b.ReportMetric(float64(percentileLatency(latencies, 95))/float64(time.Millisecond), "p95_ms")
			b.ReportMetric(float64(percentileLatency(latencies, 99))/float64(time.Millisecond), "p99_ms")
			b.ReportMetric(float64(latencies[len(latencies)-1])/float64(time.Millisecond), "max_ms")
		})
	}
}

func percentileLatency(sorted []int64, percentile int) int64 {
	if len(sorted) == 0 {
		return 0
	}
	idx := (len(sorted)*percentile + 99) / 100
	if idx <= 0 {
		idx = 1
	}
	if idx > len(sorted) {
		idx = len(sorted)
	}
	return sorted[idx-1]
}
