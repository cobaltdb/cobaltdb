package engine

import (
	"context"
	"strings"
	"testing"
)

// TestInsteadOfTriggerWhereErrorsPropagate pins the round-21 contract for the
// INSTEAD OF trigger paths: a WHERE clause that errors mid-evaluation must
// fail the statement, not silently masquerade as "no rows matched".
// Regression for catalog_ddl.go executeInsteadOfUpdateTrigger /
// executeInsteadOfDeleteTrigger swallowing evaluateWhere errors.
func TestInsteadOfTriggerWhereErrorsPropagate(t *testing.T) {
	db, err := Open(":memory:", nil)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = db.Close() }()
	ctx := context.Background()

	mustExec(t, db, `CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT)`)
	mustExec(t, db, `INSERT INTO t VALUES (1, 'a'), (2, 'b')`)
	// Explicit column list: a SELECT * view yields synthetic column_0/1 names
	// in the INSTEAD OF path, which would break the valid-WHERE controls.
	mustExec(t, db, `CREATE VIEW tv AS SELECT id, v FROM t`)
	mustExec(t, db, `CREATE TABLE fired (tag TEXT)`)
	mustExec(t, db, `CREATE TRIGGER itrg INSTEAD OF UPDATE ON tv BEGIN INSERT INTO fired VALUES ('u'); END`)
	mustExec(t, db, `CREATE TRIGGER dtrg INSTEAD OF DELETE ON tv BEGIN INSERT INTO fired VALUES ('d'); END`)

	countFired := func() int {
		t.Helper()
		var n int
		if err := db.QueryRow(ctx, `SELECT COUNT(*) FROM fired`).Scan(&n); err != nil {
			t.Fatalf("count fired: %v", err)
		}
		return n
	}

	// Probe A: UPDATE through the INSTEAD OF trigger with an erroring WHERE.
	_, err = db.Exec(ctx, `UPDATE tv SET v = 'x' WHERE 1/0 = 1`)
	if err == nil || !strings.Contains(err.Error(), "WHERE evaluation error") {
		t.Fatalf("FAIL: UPDATE swallowed WHERE evaluation error on the INSTEAD OF path (err=%v)", err)
	}
	if got := countFired(); got != 0 {
		t.Fatalf("FAIL: erroring UPDATE fired the trigger %d time(s)", got)
	}

	// Control A: a valid WHERE still routes through the trigger.
	mustExec(t, db, `UPDATE tv SET v = 'x' WHERE id = 1`)
	if got := countFired(); got != 1 {
		t.Fatalf("FAIL: valid-WHERE UPDATE did not fire the trigger (fired=%d)", got)
	}

	// Probe B: DELETE through the INSTEAD OF trigger with an erroring WHERE.
	_, err = db.Exec(ctx, `DELETE FROM tv WHERE 1/0 = 1`)
	if err == nil || !strings.Contains(err.Error(), "WHERE evaluation error") {
		t.Fatalf("FAIL: DELETE swallowed WHERE evaluation error on the INSTEAD OF path (err=%v)", err)
	}
	if got := countFired(); got != 1 {
		t.Fatalf("FAIL: erroring DELETE fired the trigger (fired=%d)", got)
	}

	// Control B: a valid WHERE fires the DELETE trigger exactly once.
	mustExec(t, db, `DELETE FROM tv WHERE id = 2`)
	if got := countFired(); got != 2 {
		t.Fatalf("FAIL: valid-WHERE DELETE did not fire the trigger (fired=%d)", got)
	}
}
