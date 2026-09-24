package optimizer

import (
	"sync"
	"testing"
	"time"

	"github.com/cobaltdb/cobaltdb/pkg/query"
)

// TestOptimizerConcurrentStatsAccess exercises the Optimizer from concurrent
// reader and writer goroutines, the way the engine uses it: EXPLAIN calls
// SelectBestIndex/GetTableStatistics from query handlers while
// UpdateTableStatistics replaces statistics from other callers.
//
// Regression: Optimizer guarded none of its stats map access, so a reader
// lookup racing a writer map-assign was a data race (fatal
// "concurrent map read and map write" in production; reported by -race).
// Run under the race detector for this test to guard the fix:
//
//	go test -race ./pkg/optimizer/ -run TestOptimizerConcurrentStatsAccess
func TestOptimizerConcurrentStatsAccess(t *testing.T) {
	stats := &Statistics{TableStats: map[string]*TableStatistics{
		"t": {TableName: "t", RowCount: 1000, IndexStats: map[string]*IndexStatistics{
			"idx_t_id": {IndexName: "idx_t_id", TableName: "t", Columns: []string{"id"}, Selectivity: 0.01},
		}},
	}}
	opt := New(DefaultConfig(), stats)
	where := &query.Identifier{Name: "id"}

	stop := make(chan struct{})
	var wg sync.WaitGroup

	for r := 0; r < 2; r++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
				}
				_ = opt.SelectBestIndex("t", where)
				_ = opt.GetTableStatistics("t")
			}
		}()
	}

	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; ; i++ {
			select {
			case <-stop:
				return
			default:
			}
			opt.UpdateStatistics("t", &TableStatistics{
				TableName: "t",
				RowCount:  int64(1000 + i%7),
				IndexStats: map[string]*IndexStatistics{
					"idx_t_id": {IndexName: "idx_t_id", TableName: "t", Columns: []string{"id"}, Selectivity: 0.01},
				},
			})
		}
	}()

	time.Sleep(100 * time.Millisecond)
	close(stop)
	wg.Wait()

	// After the concurrent phase the last snapshot must be visible.
	final := opt.GetTableStatistics("t")
	if final == nil || final.RowCount < 1000 || final.RowCount > 1006 {
		t.Fatalf("final statistics snapshot invalid: %+v", final)
	}
}
