package engine

import (
	"context"
	"strings"
	"testing"
)

// TestDMLWhereEvaluationErrorsPropagate pins that a WHERE clause whose
// evaluation errors (division by zero here) fails the DML statement on every
// scan path. Before the fix, the DELETE index path and the UPDATE pending-row
// passes swallowed evaluateWhere errors (`if err != nil || !matched`), so the
// statement silently matched zero rows while the identical statement on a
// full-scan path failed loudly.
func TestDMLWhereEvaluationErrorsPropagate(t *testing.T) {
	db, err := Open(":memory:", nil)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = db.Close() }()
	ctx := context.Background()

	// --- Case 1: UPDATE pending pass (buffered txn path). ---
	mustExec(t, db, `CREATE TABLE wt (a TEXT, b INTEGER)`)
	mustExec(t, db, `BEGIN`)
	mustExec(t, db, `INSERT INTO wt VALUES ('v', 1)`)

	// Harness control: a valid WHERE on the pending row works.
	res, err := db.Exec(ctx, `UPDATE wt SET b = b + 1 WHERE a = 'v' AND 1/1 = 1`)
	if err != nil {
		t.Fatalf("control update failed: %v", err)
	}
	if res.RowsAffected != 1 {
		t.Fatalf("control update: got %d rows, want 1", res.RowsAffected)
	}

	// Probe: an erroring WHERE must fail the statement, not silently
	// skip the pending row.
	_, err = db.Exec(ctx, `UPDATE wt SET b = b + 1 WHERE a = 'v' AND 1/0 = 1`)
	if err == nil {
		t.Fatalf("FAIL: UPDATE swallowed WHERE evaluation error on the pending pass (silent zero-row success)")
	}
	if !strings.Contains(err.Error(), "WHERE evaluation error") {
		t.Fatalf("FAIL: UPDATE error %q lacks the WHERE evaluation error context", err)
	}
	mustExec(t, db, `ROLLBACK`)

	// --- Case 2: DELETE index path. ---
	mustExec(t, db, `CREATE TABLE dt (a TEXT, b INTEGER)`)
	mustExec(t, db, `CREATE INDEX idx_dt_a ON dt(a)`)
	mustExec(t, db, `INSERT INTO dt VALUES ('v', 1), ('v', 2)`)

	// Probe: the index path re-checks the full WHERE per row; the error
	// must fail the statement instead of silently matching nothing.
	if _, err := db.Exec(ctx, `DELETE FROM dt WHERE a = 'v' AND 1/0 = 1`); err == nil {
		t.Fatalf("FAIL: DELETE swallowed WHERE evaluation error on the index path (silent zero-row success)")
	} else if !strings.Contains(err.Error(), "WHERE evaluation error") {
		t.Fatalf("FAIL: DELETE error %q lacks the WHERE evaluation error context", err)
	}

	// Rows must be untouched (the failed statement applied nothing).
	var count int
	if err := db.QueryRow(ctx, `SELECT COUNT(*) FROM dt`).Scan(&count); err != nil {
		t.Fatalf("count after failed delete: %v", err)
	}
	if count != 2 {
		t.Fatalf("FAIL: rows changed by the failed DELETE: got %d, want 2", count)
	}

	// Harness control: the same index path with a valid WHERE deletes both.
	mustExec(t, db, `DELETE FROM dt WHERE a = 'v' AND 1/1 = 1`)
	if err := db.QueryRow(ctx, `SELECT COUNT(*) FROM dt`).Scan(&count); err != nil {
		t.Fatalf("count after valid delete: %v", err)
	}
	if count != 0 {
		t.Fatalf("FAIL: control delete left %d rows, want 0", count)
	}
}
