package engine

import (
	"context"
	"strings"
	"testing"
)

// TestInsertDefaultCaseErrorPropagates pins that a column DEFAULT expression
// whose evaluation fails fails the INSERT instead of silently inserting the
// ELSE-branch value.
//
// Regression: EvalExpression's CaseExpr (pkg/catalog/catalog_core.go) swallowed
// WHEN-condition evaluation errors (`continue`) in both the searched and
// simple forms, so an INSERT omitting a column whose DEFAULT contained an
// erroring CASE (e.g. DEFAULT (CASE WHEN 1/0 = 1 THEN 1 ELSE 0 END)) silently
// inserted the ELSE value (0) instead of reporting "division by zero" — while
// the same expression errors in a query context. The fix propagates
// WHEN-condition errors like every other construct in that evaluator.
func TestInsertDefaultCaseErrorPropagates(t *testing.T) {
	db, err := Open(":memory:", nil)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	ctx := context.Background()
	mustExec := func(sql string) {
		t.Helper()
		if _, err := db.Exec(ctx, sql); err != nil {
			t.Fatalf("exec %q: %v", sql, err)
		}
	}

	mustExec("CREATE TABLE t (id INTEGER, v INTEGER DEFAULT (CASE WHEN 1/0 = 1 THEN 1 ELSE 0 END))")

	// Control: the same CASE expression errors in a query context (the
	// constant evaluator propagates).
	if _, err := db.Query(ctx, "SELECT CASE WHEN 1/0 = 1 THEN 1 ELSE 0 END"); err == nil {
		t.Fatal("the same CASE expression in a query context returned no error — control broken")
	}

	// Discriminator: INSERT omitting the defaulted column must fail with the
	// default-evaluation error.
	_, insErr := db.Exec(ctx, "INSERT INTO t (id) VALUES (1)")
	if insErr == nil {
		t.Fatal("INSERT with an erroring CASE DEFAULT succeeded silently — the default-evaluation error was swallowed")
	}
	if !strings.Contains(insErr.Error(), "division by zero") {
		t.Fatalf("INSERT propagated %q, want the division-by-zero error", insErr.Error())
	}
}
