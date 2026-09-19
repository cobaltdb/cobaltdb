package engine

import (
	"context"
	"fmt"
	"testing"
	"time"
)

// Regression for the hasSubqueriesInExpr/IntervalExpr gap: a subquery hidden
// inside `INTERVAL -(SELECT ...) DAY` was invisible to hasSubqueries(stmt),
// so the parallel scan gate (!hasSubqueries) in scanTableRows was bypassed
// and the WHERE — including the subquery — was evaluated on
// ParallelSelectRows worker goroutines. Transaction state is goroutine-keyed
// (Catalog.getCurrentTxn), so the subquery lost read-your-writes and silently
// evaluated against committed data only: the same in-transaction query
// returned different rows depending on whether ORDER BY forced the serial
// path. The interval-wrapped form must take the serial path (hasSubqueries
// true) and observe the transaction's own uncommitted writes.
func TestIntervalWrappedSubqueryReadYourWritesOnParallelPath(t *testing.T) {
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

	mustExec(t, db, `CREATE TABLE t1 (id INTEGER PRIMARY KEY, d VARCHAR(10))`)
	mustExec(t, db, `CREATE TABLE t2 (v INTEGER)`)
	// Committed t2 value: visible to every goroutine.
	mustExec(t, db, `INSERT INTO t2 VALUES (1)`)

	base := time.Date(2018, 1, 1, 0, 0, 0, 0, time.UTC)
	for start := 1; start <= 30; start += 10 {
		sql := "INSERT INTO t1 VALUES "
		for id := start; id < start+10; id++ {
			if id != start {
				sql += ","
			}
			d := base.AddDate(0, 0, id-1).Format("2006-01-02")
			sql += fmt.Sprintf("(%d,'%s')", id, d)
		}
		mustExec(t, db, sql)
	}

	tx, err := db.Begin(ctx)
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	// Uncommitted writes: the read-your-writes target in t2, and a pending
	// t1 row so the outer scan takes the pairs path (parallel-eligible)
	// instead of the serial single-tree fast path.
	if _, err := tx.Exec(ctx, `INSERT INTO t2 VALUES (5000)`); err != nil {
		t.Fatalf("txn insert t2: %v", err)
	}
	if _, err := tx.Exec(ctx, `INSERT INTO t1 VALUES (31, '2020-06-01')`); err != nil {
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

	const intervalSubq = `SELECT id FROM t1 WHERE d > DATE_ADD('2030-01-01', INTERVAL -(SELECT v FROM t2 ORDER BY v DESC LIMIT 1) DAY)`
	const want = 31

	if got := count(intervalSubq); got != want {
		t.Fatalf("read-your-writes violated on parallel path: interval-wrapped subquery returned %d rows, want %d (subquery evaluated against committed data)", got, want)
	}
	if got := count(intervalSubq + " ORDER BY id"); got != want {
		t.Fatalf("serial control diverged: %d rows, want %d", got, want)
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("commit: %v", err)
	}
	postRows, err := db.Query(ctx, intervalSubq)
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
