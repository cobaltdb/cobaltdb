package engine

import (
	"context"
	"strings"
	"testing"
)

// TestInsteadOfInsertValueErrorsPropagate pins the main-path contract on the
// INSTEAD OF INSERT trigger path: buildInsertRow (catalog_insert.go) fails an
// INSERT whose VALUES expression errors ("failed to evaluate value for
// column ..."), so the INSTEAD OF executor must not silently build the NEW
// row with NULL and fire the trigger anyway.
// Regression for catalog_ddl.go executeInsteadOfTrigger swallowing
// evaluateExpression errors.
func TestInsteadOfInsertValueErrorsPropagate(t *testing.T) {
	db, err := Open(":memory:", nil)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = db.Close() }()
	ctx := context.Background()

	mustExec(t, db, `CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT)`)
	mustExec(t, db, `CREATE TABLE fired (tag TEXT)`)
	// Explicit column list: a SELECT * view yields synthetic column_0/1 names
	// in the INSTEAD OF path (see the round-1 regression test).
	mustExec(t, db, `CREATE VIEW iv AS SELECT id, v FROM t`)
	mustExec(t, db, `CREATE TRIGGER itrg INSTEAD OF INSERT ON iv BEGIN INSERT INTO fired VALUES ('i'); END`)

	countFired := func() int {
		t.Helper()
		var n int
		if err := db.QueryRow(ctx, `SELECT COUNT(*) FROM fired`).Scan(&n); err != nil {
			t.Fatalf("count fired: %v", err)
		}
		return n
	}

	// Control: the main INSERT path fails an erroring VALUES expression.
	_, err = db.Exec(ctx, `INSERT INTO t VALUES (1, 1/0)`)
	if err == nil || !strings.Contains(err.Error(), "failed to evaluate value") {
		t.Fatalf("FAIL: main INSERT path did not propagate the value error (err=%v)", err)
	}

	// Probe: the INSTEAD OF INSERT path must propagate the same class of error
	// instead of silently firing the trigger with a NULL-filled NEW row.
	_, err = db.Exec(ctx, `INSERT INTO iv VALUES (1, 1/0)`)
	if err == nil || !strings.Contains(err.Error(), "failed to evaluate value") {
		t.Fatalf("FAIL: INSTEAD OF INSERT swallowed the value error (err=%v)", err)
	}
	if got := countFired(); got != 0 {
		t.Fatalf("FAIL: erroring INSERT fired the trigger %d time(s)", got)
	}

	// Control: a valid INSERT through the trigger still fires exactly once.
	mustExec(t, db, `INSERT INTO iv VALUES (3, 'ok')`)
	if got := countFired(); got != 1 {
		t.Fatalf("FAIL: valid INSTEAD OF INSERT did not fire the trigger (fired=%d)", got)
	}
}
