package engine

import (
	"context"
	"testing"
)

// TestReplaceEvictionRestoredOnRollback pins that an INSERT OR REPLACE which
// evicts an existing row (via PRIMARY KEY or a declared UNIQUE column) inside
// an explicit transaction records an undo entry for the eviction, so ROLLBACK
// restores the evicted row. Before the fix, only insertRowIndexes' REPLACE
// branch (secondary-index conflict) recorded the undo; the PK-conflict and
// legacy unique-column REPLACE branches hard-deleted the evicted row with no
// undo, so ROLLBACK permanently lost it.
func TestReplaceEvictionRestoredOnRollback(t *testing.T) {
	db, err := Open(":memory:", nil)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = db.Close() }()
	ctx := context.Background()

	// --- Case 1: REPLACE evicting via PRIMARY KEY conflict. ---
	mustExec(t, db, `CREATE TABLE rt (id INTEGER PRIMARY KEY, u TEXT)`)
	mustExec(t, db, `INSERT INTO rt VALUES (1, 'v')`)
	mustExec(t, db, `BEGIN`)
	mustExec(t, db, `INSERT OR REPLACE INTO rt (id, u) VALUES (1, 'other')`)
	mustExec(t, db, `ROLLBACK`)

	var count int
	var u string
	if err := db.QueryRow(ctx, `SELECT COUNT(*), COALESCE(MAX(u), '') FROM rt`).Scan(&count, &u); err != nil {
		t.Fatalf("count after rollback (pk replace): %v", err)
	}
	if count != 1 || u != "v" {
		t.Fatalf("FAIL: PK-REPLACE evicted row permanently lost after ROLLBACK: got count=%d u=%q, want count=1 u='v'", count, u)
	}

	// Harness control: autocommit REPLACE is permanent by definition.
	mustExec(t, db, `INSERT OR REPLACE INTO rt (id, u) VALUES (1, 'other')`)
	if err := db.QueryRow(ctx, `SELECT u FROM rt WHERE id = 1`).Scan(&u); err != nil {
		t.Fatalf("autocommit replace control: %v", err)
	}
	if u != "other" {
		t.Fatalf("FAIL: autocommit replace control got u=%q, want 'other'", u)
	}

	// --- Case 2: REPLACE evicting via a declared UNIQUE column. ---
	mustExec(t, db, `CREATE TABLE ru (id INTEGER PRIMARY KEY, u TEXT UNIQUE)`)
	mustExec(t, db, `INSERT INTO ru VALUES (1, 'v')`)
	mustExec(t, db, `BEGIN`)
	mustExec(t, db, `INSERT OR REPLACE INTO ru (id, u) VALUES (2, 'v')`)
	mustExec(t, db, `ROLLBACK`)

	if err := db.QueryRow(ctx, `SELECT COUNT(*) FROM ru`).Scan(&count); err != nil {
		t.Fatalf("count after rollback (unique replace): %v", err)
	}
	if count != 1 {
		t.Fatalf("FAIL: unique-REPLACE evicted row permanently lost after ROLLBACK: got count=%d, want 1", count)
	}
	if err := db.QueryRow(ctx, `SELECT u FROM ru`).Scan(&u); err != nil {
		t.Fatalf("read restored row: %v", err)
	}
	if u != "v" {
		t.Fatalf("FAIL: restored row is not the evicted one: got u=%q, want 'v'", u)
	}

	// Harness control: a plain txn insert still rolls back cleanly.
	mustExec(t, db, `BEGIN`)
	mustExec(t, db, `INSERT INTO ru VALUES (9, 'z')`)
	mustExec(t, db, `ROLLBACK`)
	if err := db.QueryRow(ctx, `SELECT COUNT(*) FROM ru`).Scan(&count); err != nil {
		t.Fatalf("count after plain rollback: %v", err)
	}
	if count != 1 {
		t.Fatalf("FAIL: plain insert rollback broken: got count=%d, want 1", count)
	}
}
