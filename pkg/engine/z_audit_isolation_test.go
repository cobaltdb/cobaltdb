package engine

import (
	"context"
	"testing"
)

// TestAuditIsolationLevelIsReadCommitted documents the EFFECTIVE isolation
// level: reads consult the live B-tree rather than a per-transaction snapshot,
// so a value committed by another transaction after this one began IS visible
// on a re-read (a non-repeatable read). This is Read Committed behavior, not
// the Snapshot Isolation implied by the default txn options. The test asserts
// the real behavior so the documented Known Limitation stays honest; if a future
// change wires up true snapshot reads, update both.
func TestAuditIsolationLevelIsReadCommitted(t *testing.T) {
	ctx := context.Background()
	db := mustOpenMem(t)
	defer db.Close()

	mustExec(t, db, "CREATE TABLE t (id INTEGER PRIMARY KEY, v INTEGER)")
	mustExec(t, db, "INSERT INTO t VALUES (1, 10)")

	tx, err := db.Begin(ctx)
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	defer tx.Rollback()

	read := func() int64 {
		rows, err := tx.Query(ctx, "SELECT v FROM t WHERE id = 1")
		if err != nil {
			t.Fatalf("tx query: %v", err)
		}
		defer rows.Close()
		if !rows.Next() {
			t.Fatal("no row")
		}
		var v int64
		if err := rows.Scan(&v); err != nil {
			t.Fatalf("scan: %v", err)
		}
		return v
	}

	if got := read(); got != 10 {
		t.Fatalf("first read = %d, want 10", got)
	}

	// A concurrent autocommit write commits while tx is open.
	if _, err := db.Exec(ctx, "UPDATE t SET v = 20 WHERE id = 1"); err != nil {
		t.Fatalf("concurrent update: %v", err)
	}

	// Under the effective Read Committed level, tx sees the new value.
	got := read()
	if got != 20 {
		t.Logf("re-read = %d — snapshot isolation now in effect; update the Known Limitation in CLAUDE.md", got)
	}
	// The engine's contract today is Read Committed; assert it so the docs match.
	if got != 20 {
		t.Fatalf("expected effective Read Committed (re-read = 20), got %d; behavior changed — reconcile docs", got)
	}
}
