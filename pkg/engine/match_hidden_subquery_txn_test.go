package engine

import (
	"context"
	"fmt"
	"testing"
)

// Regression for the hasSubqueriesInExpr/MatchExpr gap: a subquery hidden
// inside `MATCH(content) AGAINST ((SELECT ...))` was invisible to
// hasSubqueries(stmt), so the parallel scan gate in scanTableRows was bypassed
// and the WHERE — including the pattern subquery, which
// evaluateMatchExprLocked evaluates per-row — ran on ParallelSelectRows
// worker goroutines. Transaction state is goroutine-keyed
// (Catalog.getCurrentTxn), so the pattern was computed from committed data
// only: the FTS predicate silently used the wrong pattern and the same
// in-transaction query returned different rows depending on whether ORDER BY
// forced the serial path. The AGAINST-subquery form must take the serial path
// (hasSubqueries true) and observe the transaction's own uncommitted writes.
func TestMatchAgainstSubqueryPatternReadYourWritesOnParallelPath(t *testing.T) {
	ctx := context.Background()
	// Explicit Workers>1 and a small threshold so the scan is
	// deterministically parallel-eligible regardless of host core count.
	db, err := Open(":memory:", &Options{
		ParallelQuery: ParallelQueryConfig{Workers: 4, Threshold: 10},
	})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = db.Close() }()

	mustExec(t, db, `CREATE TABLE t1 (id INTEGER PRIMARY KEY, content VARCHAR(50))`)
	mustExec(t, db, `CREATE TABLE t2 (v VARCHAR(50))`)
	// Committed t2 pattern value: visible to every goroutine.
	mustExec(t, db, `INSERT INTO t2 VALUES ('zebra')`)

	// Seed committed t1 rows: 20 'zebra pack', 10 'apple pack'.
	for _, seed := range []struct {
		start, end int
		word       string
	}{{1, 20, "zebra"}, {21, 30, "apple"}} {
		sql := "INSERT INTO t1 VALUES "
		for id := seed.start; id <= seed.end; id++ {
			if id != seed.start {
				sql += ","
			}
			sql += fmt.Sprintf("(%d,'%s pack')", id, seed.word)
		}
		mustExec(t, db, sql)
	}

	tx, err := db.Begin(ctx)
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	// Uncommitted writes: the read-your-writes pattern source in t2, and one
	// pending apple t1 row so the outer scan takes the pairs path
	// (parallel-eligible) instead of the serial single-tree fast path.
	if _, err := tx.Exec(ctx, `INSERT INTO t2 VALUES ('apple')`); err != nil {
		t.Fatalf("txn insert t2: %v", err)
	}
	if _, err := tx.Exec(ctx, `INSERT INTO t1 VALUES (31, 'apple pack fresh')`); err != nil {
		t.Fatalf("txn insert t1: %v", err)
	}

	count := func(q string) int {
		t.Helper()
		rows, err := tx.Query(ctx, q)
		if err != nil {
			t.Fatalf("query %q: %v", q, err)
		}
		defer rows.Close()
		n := 0
		for rows.Next() {
			n++
		}
		return n
	}

	const matchSubq = `SELECT id FROM t1 WHERE MATCH(content) AGAINST ((SELECT v FROM t2 ORDER BY v ASC LIMIT 1))`
	const want = 11 // 10 committed apple rows + 1 pending apple row

	if got := count(matchSubq); got != want {
		t.Fatalf("read-your-writes violated on parallel path: AGAINST pattern subquery returned %d rows, want %d (pattern evaluated against committed data)", got, want)
	}
	if got := count(matchSubq + " ORDER BY id"); got != want {
		t.Fatalf("serial control diverged: %d rows, want %d", got, want)
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("commit: %v", err)
	}
	postRows, err := db.Query(ctx, matchSubq)
	if err != nil {
		t.Fatalf("post-commit query: %v", err)
	}
	got := 0
	for postRows.Next() {
		got++
	}
	postRows.Close()
	if got != want {
		t.Fatalf("post-commit count diverged: %d rows, want %d", got, want)
	}
}
