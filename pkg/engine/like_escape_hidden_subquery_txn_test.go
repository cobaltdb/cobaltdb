package engine

import (
	"context"
	"fmt"
	"testing"
)

// Regression for the hasSubqueriesInExpr/LikeExpr.Escape gap: a subquery
// hidden inside `... LIKE <pattern> ESCAPE (SELECT ...)` was invisible to
// hasSubqueriesInExpr — its LikeExpr case walked only Expr and Pattern — so
// the parallel scan gate in scanTableRows was bypassed and EvalLike's ESCAPE
// expression, including the subquery, ran on ParallelSelectRows worker
// goroutines. Transaction state is goroutine-keyed (Catalog.getCurrentTxn),
// so the escape character was computed from committed data only and the LIKE
// predicate silently used the wrong escape semantics. The ESCAPE-subquery
// form must take the serial path (hasSubqueries true) and observe the
// transaction's own uncommitted writes.
func TestLikeEscapeSubqueryReadYourWritesOnParallelPath(t *testing.T) {
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

	mustExec(t, db, `CREATE TABLE t1 (id INTEGER PRIMARY KEY, s VARCHAR(50))`)
	mustExec(t, db, `CREATE TABLE t2 (v VARCHAR(50))`)
	// Committed t2 escape value 'y': under 'y', the pattern 'y%' means
	// escaped-% → literal "%" → matches only the exact "%" row (id 100).
	// The uncommitted 'x' sorts first under ASC LIMIT 1, so the
	// read-your-writes escape is 'x': under 'x', 'y%' means literal "y" +
	// wildcard "%" → matches the 12 committed y-rows (ids 1-12).
	mustExec(t, db, `INSERT INTO t2 VALUES ('y')`)

	for id := 1; id <= 12; id++ {
		mustExec(t, db, fmt.Sprintf(`INSERT INTO t1 VALUES (%d, 'yrow')`, id))
	}
	mustExec(t, db, `INSERT INTO t1 VALUES (100, '%')`)

	tx, err := db.Begin(ctx)
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	// Uncommitted writes: the read-your-writes escape source in t2 ('x'), and
	// one pending t1 row so the outer scan takes the pairs path
	// (parallel-eligible) instead of the serial single-tree fast path. The
	// pending row matches neither escape reading.
	if _, err := tx.Exec(ctx, `INSERT INTO t2 VALUES ('x')`); err != nil {
		t.Fatalf("txn insert t2: %v", err)
	}
	if _, err := tx.Exec(ctx, `INSERT INTO t1 VALUES (101, 'zrow')`); err != nil {
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

	const likeSubq = `SELECT id FROM t1 WHERE s LIKE 'y%' ESCAPE (SELECT v FROM t2 ORDER BY v ASC LIMIT 1)`
	const want = 12 // the 12 committed y-rows, matched under the uncommitted escape 'x'

	if got := count(likeSubq); got != want {
		t.Fatalf("read-your-writes violated on parallel path: ESCAPE subquery returned %d rows, want %d (escape character evaluated against committed data)", got, want)
	}
	if got := count(likeSubq + " ORDER BY id"); got != want {
		t.Fatalf("serial control diverged: %d rows, want %d", got, want)
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("commit: %v", err)
	}
	// Post-commit the subquery legitimately sees committed data only:
	// ORDER BY v ASC LIMIT 1 now yields 'x' (committed), so the same 12
	// rows must match.
	postRows, err := db.Query(ctx, likeSubq)
	if err != nil {
		t.Fatalf("post-commit query: %v", err)
	}
	defer postRows.Close()
	n := 0
	for postRows.Next() {
		n++
	}
	if n != want {
		t.Fatalf("post-commit count diverged: %d rows, want %d", n, want)
	}
}
