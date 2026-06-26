package engine

import (
	"context"
	"testing"
)

// TestRecursiveCTEUnionDeduplicates verifies that a recursive CTE using UNION
// (DISTINCT) deduplicates and terminates on a cyclic graph, instead of looping
// until the depth limit. Before the fix, UNION was treated as UNION ALL.
func TestRecursiveCTEUnionTerminatesOnCycle(t *testing.T) {
	db, err := Open(":memory:", &Options{CoreStorage: CoreStorage{InMemory: true, CacheSize: 256}})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()
	ctx := context.Background()

	if _, err := db.Exec(ctx, `CREATE TABLE edges (cur INTEGER, nxt INTEGER)`); err != nil {
		t.Fatalf("create: %v", err)
	}
	// A 2-cycle: 1 -> 2 -> 1.
	if _, err := db.Exec(ctx, `INSERT INTO edges VALUES (1, 2)`); err != nil {
		t.Fatalf("insert: %v", err)
	}
	if _, err := db.Exec(ctx, `INSERT INTO edges VALUES (2, 1)`); err != nil {
		t.Fatalf("insert: %v", err)
	}

	rows, err := db.Query(ctx, `
		WITH RECURSIVE reach(n) AS (
			SELECT 1
			UNION
			SELECT edges.nxt FROM edges JOIN reach ON edges.cur = reach.n
		) SELECT n FROM reach ORDER BY n`)
	if err != nil {
		t.Fatalf("recursive UNION query errored (likely hit depth limit): %v", err)
	}
	defer rows.Close()

	var got []int64
	for rows.Next() {
		var n int64
		if err := rows.Scan(&n); err != nil {
			t.Fatalf("scan: %v", err)
		}
		got = append(got, n)
	}
	// UNION DISTINCT over the cycle reaches exactly {1, 2}.
	if len(got) != 2 || got[0] != 1 || got[1] != 2 {
		t.Fatalf("expected [1 2], got %v", got)
	}
}
