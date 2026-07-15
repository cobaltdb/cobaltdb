package engine

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"
)

// TestConcAudit_OnlineIndexBuildVsDML is a concurrency regression test.
//
// Root cause: the buffered/MVCC DML paths execute UPDATE/DELETE while holding
// only Catalog.mu.RLock (shared). When a DELETE's index maintenance fails
// (which a concurrent online index build can provoke), the non-buffered path
// calls rollbackAppliedDeleteEntries -> rebuildTableIndexesLocked, which does a
// MAP WRITE `c.indexTrees[idxName] = newTree` (pkg/catalog/catalog_index.go:525)
// under only the read lock. A concurrent UPDATE reads the same map
// (`range c.indexTrees` at pkg/catalog/catalog_update.go:2073). Concurrent map
// read+write is a data race and, without the race detector, an unrecoverable
// "fatal error: concurrent map read and map write" that crashes the process.
// It also produces index/base divergence (rows missing from the secondary
// index -> indexed equality queries return wrong/empty results).
//
//	Run with: CGO_ENABLED=1 go test -race ./pkg/engine/ \
//	  -run TestConcAudit_OnlineIndexBuildVsDML -v
func TestConcAudit_OnlineIndexBuildVsDML(t *testing.T) {
	ctx := context.Background()
	db, err := Open(":memory:", &Options{})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	mustExec(t, db, "CREATE TABLE t (id INTEGER PRIMARY KEY, k INTEGER)")
	// >1000 rows so CREATE INDEX runs as a background (online) build.
	const N = 6000
	for i := 1; i <= N; i++ {
		if _, err := db.Exec(ctx, "INSERT INTO t (id, k) VALUES (?, ?)", i, i); err != nil {
			t.Fatalf("seed insert: %v", err)
		}
	}

	var wg sync.WaitGroup
	// Online index build (background populate; no Catalog.mu held).
	wg.Add(1)
	go func() {
		defer wg.Done()
		_, _ = db.Exec(ctx, "CREATE INDEX idx_k ON t (k)")
	}()
	// Concurrent UPDATEs of k on a disjoint id range.
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 1; i <= 2000; i++ {
			_, _ = db.Exec(ctx, "UPDATE t SET k = ? WHERE id = ?", i+1000000, i)
		}
	}()
	// Concurrent DELETEs on another disjoint range (provokes the rollback ->
	// rebuildTableIndexesLocked map write under RLock).
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 3001; i <= 4000; i++ {
			_, _ = db.Exec(ctx, "DELETE FROM t WHERE id = ?", i)
		}
	}()
	wg.Wait()
	time.Sleep(300 * time.Millisecond) // let the background build finish

	// Invariant: the secondary index must agree with the base table. Compare an
	// index-preferring equality query against ground truth from a full dump.
	truth := map[int64]int64{}
	for _, r := range queryRows(t, db, "SELECT id, k FROM t") {
		truth[toI64(r[0])] = toI64(r[1])
	}
	kToIDs := map[int64][]int64{}
	for id, k := range truth {
		kToIDs[k] = append(kToIDs[k], id)
	}
	mismatches := 0
	for k := range kToIDs {
		got := queryRows(t, db, fmt.Sprintf("SELECT id FROM t WHERE k = %d", k))
		if len(got) != len(kToIDs[k]) {
			mismatches++
			if mismatches <= 10 {
				t.Errorf("k=%d: index-driven query returned %d rows, base has %d (ids=%v)",
					k, len(got), len(kToIDs[k]), kToIDs[k])
			}
		}
	}
	if mismatches > 0 {
		t.Errorf("total index/base divergences: %d", mismatches)
	}
}

func toI64(v interface{}) int64 {
	switch x := v.(type) {
	case int64:
		return x
	case int:
		return int64(x)
	case int32:
		return int64(x)
	case float64:
		return int64(x)
	default:
		return 0
	}
}
