package catalog

import (
	"context"
	"sync"
	"testing"

	"github.com/cobaltdb/cobaltdb/pkg/query"
	"github.com/cobaltdb/cobaltdb/pkg/security"
)

// TestRLSCtxConcurrentSelectWithContextAndSelect probes the catalog's shared
// rlsCtx field from both paths at once, under -race:
//
//   - SelectWithContext WRITES cat.rlsCtx (catalog_select.go:92) under the
//     exclusive lock, then restores it via defer (catalog_select.go:93).
//   - Select READS cat.rlsCtx through selectNeedsFullRowsForRLS, called at
//     catalog_core.go:828, and again through applySelectPostProcess.
//
// The read at catalog_core.go:828 executes BEFORE the cat.mu.RUnlock() at
// catalog_core.go:1085, so it should still hold the lock and be mutually
// excluded from the write. A "DATA RACE" report falsifies that; a clean run
// confirms it.
//
// selectNeedsFullRowsForRLS short-circuits before touching cat.rlsCtx unless RLS
// is enabled for the table, so RLS is switched on here - otherwise the probe
// would be vacuous.
func TestRLSCtxConcurrentSelectWithContextAndSelect(t *testing.T) {
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
	const iterations = 300

	var wg sync.WaitGroup
	for g := 0; g < goroutines; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			for i := 0; i < iterations; i++ {
				if g%2 == 0 {
					// Writer path.
					_, _, _ = cat.SelectWithContext(context.Background(), stmt, nil)
				} else {
					// Reader path.
					_, _, _ = cat.Select(stmt, nil)
				}
			}
		}(g)
	}
	wg.Wait()
}
