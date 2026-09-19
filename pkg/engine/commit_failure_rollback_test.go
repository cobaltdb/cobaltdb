package engine

import (
	"context"
	"strings"
	"testing"
)

// TestExplicitCommitRollsBackOnFlushFailure pins the failed-COMMIT contract
// for explicit transactions: when the commit window's FlushTableTrees refuses
// (the btree overflow-page format cap), COMMIT must return the error AND roll
// the transaction back. Pre-fix, the transaction stayed active — every later
// BEGIN failed "transaction already in progress" (the wedge behind the
// 628,791-error cascade in the 2026-09-19 round-7 log) and later statements
// ran inside the zombie transaction with phantom buffered rows. This mirrors
// the autocommit path's failed-commit rollback (database.go) and preserves the
// replTxnFlush-after-commit case: there the data IS committed and the txn is
// no longer active, so the IsTransactionActive guard skips the rollback.
func TestExplicitCommitRollsBackOnFlushFailure(t *testing.T) {
	db, err := Open(":memory:", nil)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = db.Close() }()
	ctx := context.Background()

	mustExec(t, db, "CREATE TABLE big (id INTEGER PRIMARY KEY, payload TEXT)")

	// Control: a healthy transaction round-trip.
	mustExec(t, db, "BEGIN")
	mustExec(t, db, "INSERT INTO big VALUES (1, 'small')")
	mustExec(t, db, "COMMIT")

	// Txn A: 150 x 30KB rows (~4.5MB serialized) cross the btree flush
	// overflow-page cap (4,153,440 bytes = 1018 overflow-page IDs with zero
	// remaining root data space). This COMMIT succeeds because the window
	// flushes the tree BEFORE applying buffered writes — the oversized rows
	// enter the shards only afterwards.
	bigPayload := strings.Repeat("x", 30000)
	mustExec(t, db, "BEGIN")
	for id := 100; id < 250; id++ {
		if _, err := db.Exec(ctx, "INSERT INTO big VALUES (?, ?)", id, bigPayload); err != nil {
			t.Fatalf("big INSERT %d: %v", id, err)
		}
	}
	mustExec(t, db, "COMMIT")

	// Txn B: any later COMMIT flushes the now-oversized tree and must fail
	// with the format-cap refusal — and roll back instead of wedging.
	mustExec(t, db, "BEGIN")
	mustExec(t, db, "INSERT INTO big VALUES (2, 'probe')")
	if _, err := db.Exec(ctx, "COMMIT"); err == nil || !strings.Contains(err.Error(), "fit in the root page") {
		t.Fatalf("expected the flush-refusal error from COMMIT, got: %v", err)
	}
	if _, err := db.Exec(ctx, "BEGIN"); err != nil {
		t.Fatalf("failed COMMIT left the transaction wedged; BEGIN: %v", err)
	}
	mustExec(t, db, "ROLLBACK")

	// The rolled-back probe write must be gone; the committed big rows remain.
	var n int
	if err := db.QueryRow(ctx, "SELECT COUNT(*) FROM big").Scan(&n); err != nil {
		t.Fatalf("count: %v", err)
	}
	if n != 151 {
		t.Fatalf("expected 151 rows (1 control + 150 committed big rows), got %d", n)
	}

	// Repeated clean recovery: a second doomed COMMIT must fail and roll back
	// again, leaving the connection usable — no wedge on repeated failures.
	mustExec(t, db, "BEGIN")
	mustExec(t, db, "INSERT INTO big VALUES (3, 'probe2')")
	if _, err := db.Exec(ctx, "COMMIT"); err == nil || !strings.Contains(err.Error(), "fit in the root page") {
		t.Fatalf("second COMMIT expected the flush refusal, got: %v", err)
	}
	if _, err := db.Exec(ctx, "BEGIN"); err != nil {
		t.Fatalf("second failed COMMIT also wedged the transaction; BEGIN: %v", err)
	}
	mustExec(t, db, "ROLLBACK")
}
