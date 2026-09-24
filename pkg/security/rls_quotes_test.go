package security

import (
	"context"
	"testing"
)

// TestQuotedLiteralsDoNotStructureExpression pins the contract that string
// literals in policy expressions are values: operators inside a quoted span
// must not split the expression.
//
// Regression: findTopLevelOperator scanned for top-level AND/OR/comparison
// operators without tracking quoted spans, so
// `dept = 'sales AND marketing' OR active = 1` split at the literal's AND;
// the right fragment parsed as a comparison against a garbage row key and the
// permissive policy degraded to dept == nil (always false) — deny-all.
func TestQuotedLiteralsDoNotStructureExpression(t *testing.T) {
	m := NewManager()
	m.EnableTable("docs")
	err := m.CreatePolicy(&Policy{
		Name:       "dept_or_active",
		TableName:  "docs",
		Type:       PolicySelect,
		Expression: `dept = 'sales AND marketing' OR active = 1`,
	})
	if err != nil {
		t.Fatalf("CreatePolicy: %v", err)
	}

	ctx := context.Background()
	check := func(dept string, active bool) bool {
		t.Helper()
		allowed, err := m.CheckAccess(ctx, "docs", PolicySelect,
			map[string]interface{}{"dept": dept, "active": active}, "alice", nil)
		if err != nil {
			t.Fatalf("CheckAccess(dept=%q): %v", dept, err)
		}
		return allowed
	}

	// The row the policy's first clause explicitly grants.
	if !check("sales AND marketing", false) {
		t.Fatal(`row with dept = "sales AND marketing" DENIED — literal's AND split the expression`)
	}
	// Granted by the second clause.
	if !check("other", true) {
		t.Fatal(`row with active = 1 DENIED — second clause lost`)
	}
	// Granted by neither clause.
	if check("other", false) {
		t.Fatal(`row with dept="other", active=false unexpectedly ALLOWED`)
	}
}

// TestQuotedComparisonLiteral pins the single-clause variant: a comparison
// whose right side is a quoted literal containing keyword text resolves the
// literal as a value (via the simple-expression fallback), not as structure.
func TestQuotedComparisonLiteral(t *testing.T) {
	m := NewManager()
	m.EnableTable("tasks")
	err := m.CreatePolicy(&Policy{
		Name:       "status_exact",
		TableName:  "tasks",
		Type:       PolicySelect,
		Expression: `status = 'review AND escalate'`,
	})
	if err != nil {
		t.Fatalf("CreatePolicy: %v", err)
	}

	ctx := context.Background()
	allowed, err := m.CheckAccess(ctx, "tasks", PolicySelect,
		map[string]interface{}{"status": "review AND escalate"}, "alice", nil)
	if err != nil {
		t.Fatalf("CheckAccess: %v", err)
	}
	if !allowed {
		t.Fatal(`row with status = "review AND escalate" DENIED — literal mis-parsed`)
	}

	allowed, err = m.CheckAccess(ctx, "tasks", PolicySelect,
		map[string]interface{}{"status": "draft"}, "alice", nil)
	if err != nil {
		t.Fatalf("CheckAccess: %v", err)
	}
	if allowed {
		t.Fatal(`row with status = "draft" unexpectedly ALLOWED`)
	}
}
