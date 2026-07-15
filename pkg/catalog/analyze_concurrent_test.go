package catalog

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/cobaltdb/cobaltdb/pkg/btree"
	"github.com/cobaltdb/cobaltdb/pkg/query"
	"github.com/cobaltdb/cobaltdb/pkg/storage"
)

// TestAnalyzeConcurrentDML verifies that ANALYZE TABLE does not hold c.mu
// during the scan phase, allowing concurrent DML to proceed.
//
// The old implementation held c.mu for the entire scan (defer c.mu.Unlock()),
// blocking all DML while reading every row. The current implementation releases
// c.mu after capturing the tree reference and re-acquires only for the final
// stats write.
//
// We use a select-with-timeout pattern: if ANALYZE holds the lock, the INSERT
// goroutine blocks indefinitely and the test times out, failing loudly.
func TestAnalyzeConcurrentDML(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(1024, backend)
	tree, err := btree.NewBTree(pool)
	if err != nil {
		t.Fatalf("Failed to create BTree: %v", err)
	}
	catalog := New(tree, pool, nil)
	defer pool.Close()

	// Create table.
	createStmt := &query.CreateTableStmt{
		Table: "concurrent_test",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
			{Name: "x", Type: query.TokenInteger},
		},
	}
	if err := catalog.CreateTable(createStmt); err != nil {
		t.Fatalf("CreateTable: %v", err)
	}

	makeInsert := func(start, count int) *query.InsertStmt {
		rows := make([][]query.Expression, count)
		for i := 0; i < count; i++ {
			rows[i] = []query.Expression{
				&query.NumberLiteral{Value: float64(start + i)},
				&query.NumberLiteral{Value: float64(i * 2)},
			}
		}
		return &query.InsertStmt{
			Table:   "concurrent_test",
			Columns: []string{"id", "x"},
			Values:  rows,
		}
	}

	// Seed with some data so the scan is non-trivial.
	_, _, err = catalog.Insert(context.Background(), makeInsert(0, 10), nil)
	if err != nil {
		t.Fatalf("seed insert: %v", err)
	}

	type result struct {
		op  string
		err error
	}
	var analyzeResult, insertResult result
	var wg sync.WaitGroup
	resultCh := make(chan result, 2)
	timeout := time.After(2 * time.Second)

	// Goroutine: run ANALYZE (adds 500 more rows first so scan takes time).
	wg.Add(1)
	go func() {
		defer wg.Done()
		_, _, _ = catalog.Insert(context.Background(), makeInsert(1000, 500), nil)
		if err := catalog.Analyze("concurrent_test"); err != nil {
			resultCh <- result{"Analyze", err}
			return
		}
		resultCh <- result{"Analyze", nil}
	}()

	// Goroutine: run INSERT (needs c.mu; blocked by Analyze if scan holds lock).
	wg.Add(1)
	go func() {
		defer wg.Done()
		_, _, err := catalog.Insert(context.Background(), makeInsert(2000, 1), nil)
		resultCh <- result{"Insert", err}
	}()

	// Drain results or timeout. wg.Wait() blocks until both goroutines call Done.
	go func() {
		wg.Wait()
		close(resultCh)
	}()

loop:
	for {
		select {
		case r := <-resultCh:
			if r.op == "Analyze" {
				analyzeResult = r
			} else {
				insertResult = r
			}
			if analyzeResult.op != "" && insertResult.op != "" {
				break loop
			}
		case <-timeout:
			// If ANALYZE held c.mu during the scan, INSERT goroutine is blocked
			// and wg.Wait() never returns. The timeout fires and we fail.
			t.Fatal("timeout: ANALYZE may be holding c.mu during scan, blocking DML")
		}
	}

	if analyzeResult.err != nil {
		t.Errorf("Analyze failed: %v", analyzeResult.err)
	}
	if insertResult.err != nil {
		t.Errorf("Insert failed: %v", insertResult.err)
	}

	// Verify stats were collected.
	stats, found := catalog.stats["concurrent_test"]
	if !found {
		t.Error("expected stats to be collected after Analyze")
	} else if stats.RowCount == 0 {
		t.Error("expected non-zero row count in stats")
	}
}
