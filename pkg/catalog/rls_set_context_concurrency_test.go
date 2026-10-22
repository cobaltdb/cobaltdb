package catalog

import (
	"context"
	"sync"
	"testing"

	"github.com/cobaltdb/cobaltdb/pkg/query"
	"github.com/cobaltdb/cobaltdb/pkg/security"
)

// Exercise SetRLSContext concurrently with the Select path that reads rlsCtx.
// Both paths must share cat.mu so the race detector reports no unsynchronized
// access, including when RLS is enabled for the selected table.
func TestSetRLSContextConcurrentWithSelect(t *testing.T) {
	parsed, err := query.Parse("SELECT id FROM t")
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	stmt, ok := parsed.(*query.SelectStmt)
	if !ok {
		t.Fatalf("expected *query.SelectStmt, got %T", parsed)
	}

	cat := newEmptyCatalog()
	cat.enableRLS = true
	cat.rlsManager = security.NewManager()
	cat.tables["t"] = &TableDef{Name: "t", Columns: []ColumnDef{{Name: "id", Type: "INTEGER"}}}
	cat.rlsManager.EnableTable("t")

	const goroutines = 4
	const iterations = 500

	var wg sync.WaitGroup
	for g := 0; g < goroutines; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			for i := 0; i < iterations; i++ {
				if g%2 == 0 {
					cat.SetRLSContext(context.Background())
				} else {
					// Reader, reaches cat.rlsCtx under the read lock.
					_, _, _ = cat.Select(stmt, nil)
				}
			}
		}(g)
	}
	wg.Wait()
}
