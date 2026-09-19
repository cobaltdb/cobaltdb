package engine

import (
	"context"
	"testing"
)

// TestAlterAddCheckIgnoresTombstones pins that ALTER TABLE ADD CHECK validates
// only LIVE rows. Deletes are soft (MVCC tombstones stay in the tree), so a
// previously-deleted row that violates the new CHECK must not block the ALTER.
// Before the fix, the committed-tree scan used decodeRow (tombstones included)
// while the pending-write pass correctly used decodeLiveRow, so the tombstone
// failed checkRowConstraints and the ALTER was rejected on an empty table.
func TestAlterAddCheckIgnoresTombstones(t *testing.T) {
	db, err := Open(":memory:", nil)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = db.Close() }()
	ctx := context.Background()

	mustExec(t, db, `CREATE TABLE ct (x INTEGER)`)
	mustExec(t, db, `INSERT INTO ct VALUES (5)`)
	mustExec(t, db, `DELETE FROM ct`) // soft delete: tombstone remains in the tree

	// Probe: the new CHECK must validate against live rows only. The table
	// has zero live rows, so the ALTER must succeed even though the
	// tombstoned row (x=5) violates CHECK (x > 10).
	if _, err := db.Exec(ctx, `ALTER TABLE ct ADD CONSTRAINT ck CHECK (x > 10)`); err != nil {
		t.Fatalf("FAIL: ADD CHECK blocked by a soft-deleted row: %v", err)
	}

	// Enforcement control: the CHECK must still reject violating live rows.
	if _, err := db.Exec(ctx, `INSERT INTO ct VALUES (5)`); err == nil {
		t.Fatalf("FAIL: CHECK (x > 10) accepted a violating live row after ALTER")
	}

	// Compliant row passes.
	mustExec(t, db, `INSERT INTO ct VALUES (20)`)

	// Harness control on a fresh table: ALTER succeeds with a compliant
	// live row, and still rejects a violating one.
	mustExec(t, db, `CREATE TABLE ct2 (x INTEGER)`)
	mustExec(t, db, `INSERT INTO ct2 VALUES (30)`)
	if _, err := db.Exec(ctx, `ALTER TABLE ct2 ADD CONSTRAINT ck2 CHECK (x > 10)`); err != nil {
		t.Fatalf("control ALTER failed with a compliant live row: %v", err)
	}
	if _, err := db.Exec(ctx, `INSERT INTO ct2 VALUES (1)`); err == nil {
		t.Fatalf("FAIL: control CHECK accepted a violating row")
	}
}
