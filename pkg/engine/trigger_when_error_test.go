package engine

import (
	"context"
	"strings"
	"testing"
)

// TestTriggerWhenEvaluationErrorFailsStatement pins the value-materialization
// contract for trigger WHEN conditions: a WHEN expression that cannot be
// evaluated must fail the triggering statement instead of silently skipping
// the trigger (which would drop the trigger's side effects without a trace).
// This mirrors the body-error path in executeTriggers (which propagates) and
// the INSTEAD OF WHERE fixes; the WHEN branch was the remaining swallow in
// that function.
func TestTriggerWhenEvaluationErrorFailsStatement(t *testing.T) {
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

	mustExec("CREATE TABLE t (id INTEGER PRIMARY KEY, d INTEGER)")
	mustExec("CREATE TABLE audit (id INTEGER)")
	mustExec("CREATE TRIGGER trg AFTER INSERT ON t WHEN (100 / NEW.d > 10) BEGIN INSERT INTO audit VALUES (NEW.id); END")

	// Control: a satisfiable WHEN fires the trigger.
	if e := exec("INSERT INTO t VALUES (1, 5)"); e != nil {
		t.Fatalf("control insert with satisfiable WHEN failed: %v", e)
	}
	var n1 int
	if e := db.QueryRow(ctx, "SELECT COUNT(*) FROM audit").Scan(&n1); e != nil || n1 != 1 {
		t.Fatalf("control: trigger did not fire for satisfiable WHEN (audit=%d err=%v)", n1, e)
	}

	// Probe: d=0 makes the WHEN evaluation error; the statement must fail.
	insertErr := exec("INSERT INTO t VALUES (2, 0)")
	if insertErr == nil {
		t.Fatal("INSERT whose trigger WHEN errors (division by zero) succeeded; trigger was silently skipped")
	}
	if !strings.Contains(insertErr.Error(), "trigger trg") {
		t.Fatalf("INSERT error %q does not identify the failing trigger", insertErr)
	}

	// The audit trail must be unchanged by the failed statement.
	var n2 int
	if e := db.QueryRow(ctx, "SELECT COUNT(*) FROM audit").Scan(&n2); e != nil || n2 != 1 {
		t.Fatalf("audit changed after failed INSERT: audit=%d err=%v", n2, e)
	}
}
