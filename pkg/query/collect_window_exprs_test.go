package query

// Regression tests for CollectWindowExprs / ExprContainsWindow traversal
// coverage.
//
// CollectWindowExprs documents that it "appends every WindowExpr nested
// anywhere within expr". The catalog uses ExprContainsWindow to route SELECT
// columns between window-aware evaluation and scalar evaluation
// (catalog_select.go:2203/2479, catalog_select_helpers.go:143); a false
// negative makes the column project as NULL or turns a deferred
// window-computation error into a propagated query error.
//
// The traversal previously descended only through Binary/Unary/FunctionCall/
// Cast/Case/Alias/WindowExpr — missing InExpr, BetweenExpr, IsNullExpr,
// LikeExpr, and subquery projections.

import "testing"

func TestCollectWindowExprsFindsWindowNestedInInList(t *testing.T) {
	stmt, err := ParseStrict("SELECT x IN (1, COUNT(*) OVER ()) FROM c")
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	sel, ok := stmt.(*SelectStmt)
	if !ok {
		t.Fatalf("parsed %T, want *SelectStmt", stmt)
	}
	var ws []*WindowExpr
	CollectWindowExprs(sel.Columns[0], &ws)
	if len(ws) != 1 {
		t.Fatalf("CollectWindowExprs found %d window exprs nested in an IN list, want 1", len(ws))
	}
	if !ExprContainsWindow(sel.Columns[0]) {
		t.Fatal("ExprContainsWindow = false for a column nesting a window function in an IN list")
	}
}

func TestCollectWindowExprsFindsWindowNestedInIsNull(t *testing.T) {
	stmt, err := ParseStrict("SELECT COUNT(*) OVER () IS NULL FROM c")
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	sel, ok := stmt.(*SelectStmt)
	if !ok {
		t.Fatalf("parsed %T, want *SelectStmt", stmt)
	}
	var ws []*WindowExpr
	CollectWindowExprs(sel.Columns[0], &ws)
	if len(ws) != 1 {
		t.Fatalf("CollectWindowExprs found %d window exprs under IS NULL, want 1", len(ws))
	}
}

func TestCollectWindowExprsFindsWindowNestedInBetween(t *testing.T) {
	stmt, err := ParseStrict("SELECT v BETWEEN 1 AND COUNT(*) OVER () FROM c")
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	sel, ok := stmt.(*SelectStmt)
	if !ok {
		t.Fatalf("parsed %T, want *SelectStmt", stmt)
	}
	var ws []*WindowExpr
	CollectWindowExprs(sel.Columns[0], &ws)
	if len(ws) != 1 {
		t.Fatalf("CollectWindowExprs found %d window exprs nested in BETWEEN, want 1", len(ws))
	}
}

func TestCollectWindowExprsFindsWindowNestedInSubqueryProjection(t *testing.T) {
	stmt, err := ParseStrict("SELECT (SELECT COUNT(*) OVER () FROM t2) FROM t1")
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	sel, ok := stmt.(*SelectStmt)
	if !ok {
		t.Fatalf("parsed %T, want *SelectStmt", stmt)
	}
	var ws []*WindowExpr
	CollectWindowExprs(sel.Columns[0], &ws)
	if len(ws) != 1 {
		t.Fatalf("CollectWindowExprs found %d window exprs nested in a scalar subquery projection, want 1", len(ws))
	}
}

func TestCollectWindowExprsCanonicalAliasedFormStillFound(t *testing.T) {
	// Guard: the canonical aliased window column must keep working.
	stmt, err := ParseStrict("SELECT SUM(v) OVER (PARTITION BY p) AS w FROM c")
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	sel, ok := stmt.(*SelectStmt)
	if !ok {
		t.Fatalf("parsed %T, want *SelectStmt", stmt)
	}
	if !ExprContainsWindow(sel.Columns[0]) {
		t.Fatal("ExprContainsWindow = false for the canonical aliased window column")
	}
}
