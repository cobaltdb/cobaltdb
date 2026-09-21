package engine

import (
	"context"
	"fmt"
	"strings"
	"testing"
)

// TestInsteadOfUpdateSetErrorsPropagate pins the value-materialization contract
// for INSTEAD OF UPDATE triggers: a SET expression that cannot be evaluated
// must fail the statement instead of silently firing the trigger with a
// NULL-filled NEW row. Mirrors the main UPDATE path ("failed to evaluate SET
// expression for column '%s'": catalog_update.go) and the sibling fixes for
// INSTEAD OF INSERT VALUES (ns-2) and INSTEAD OF WHERE clauses (ns-1); the SET
// site in executeInsteadOfUpdateTrigger was the remaining swallow in the
// INSTEAD OF family.
func TestInsteadOfUpdateSetErrorsPropagate(t *testing.T) {
	ctx := context.Background()
	db, err := Open(":memory:", &Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	exec := func(sql string) error {
		_, e := db.Exec(ctx, sql)
		return e
	}
	mustExec := func(sql string) {
		t.Helper()
		if e := exec(sql); e != nil {
			t.Fatalf("exec %q: %v", sql, e)
		}
	}

	mustExec("CREATE TABLE base (id INTEGER PRIMARY KEY, v TEXT, w INTEGER)")
	mustExec("INSERT INTO base VALUES (1, 'x', 10)")
	// The fixture MUST use an explicit-column view: a star view yields
	// synthesized column names in getColumnsForTableOrView.
	mustExec("CREATE VIEW tv AS SELECT id, v, w FROM base")
	mustExec("CREATE TABLE audit (id INTEGER, newv TEXT)")
	mustExec("CREATE TRIGGER trg INSTEAD OF UPDATE ON tv BEGIN INSERT INTO audit VALUES (OLD.id, NEW.v); END")

	// Control: a satisfiable SET fires the trigger with the new value.
	if e := exec("UPDATE tv SET v = 'ok' WHERE id = 1"); e != nil {
		t.Fatalf("control update failed: %v", e)
	}
	var n1 int
	if e := db.QueryRow(ctx, "SELECT COUNT(*) FROM audit").Scan(&n1); e != nil || n1 != 1 {
		t.Fatalf("control: trigger did not fire (audit=%d err=%v)", n1, e)
	}
	var v1 interface{}
	if e := db.QueryRow(ctx, "SELECT newv FROM audit WHERE id=1").Scan(&v1); e != nil || fmt.Sprint(v1) != "ok" {
		t.Fatalf("control: wrong audit value: %v err=%v", v1, e)
	}

	// Probe: an un-evaluatable SET expression must fail the UPDATE.
	updateErr := exec("UPDATE tv SET v = 1/0 WHERE id = 1")
	if updateErr == nil {
		t.Fatal("UPDATE whose SET expression errors (division by zero) succeeded; trigger silently fired with NULL-filled NEW row")
	}
	if !strings.Contains(updateErr.Error(), "failed to evaluate SET expression for column 'v'") {
		t.Fatalf("UPDATE error %q does not carry the SET-expression error family", updateErr)
	}

	// Audit unchanged by the failed statement.
	var n2 int
	if e := db.QueryRow(ctx, "SELECT COUNT(*) FROM audit").Scan(&n2); e != nil || n2 != 1 {
		t.Fatalf("audit changed after failed UPDATE: audit=%d err=%v", n2, e)
	}
}
