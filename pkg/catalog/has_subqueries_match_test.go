package catalog

import (
	"testing"

	"github.com/cobaltdb/cobaltdb/pkg/query"
)

// Regression: hasSubqueriesInExpr must descend into MatchExpr.Columns and
// MatchExpr.Pattern. `MATCH(col) AGAINST ((SELECT ...))` parses both as full
// expressions, but the walker had no MatchExpr case, so hasSubqueries(stmt)
// returned false and the parallel scan gate (!hasSubqueries) was bypassed.
// evaluateMatchExprLocked evaluates the pattern per-row on the WHERE path, so
// on ParallelSelectRows worker goroutines (no goroutine-keyed transaction
// state) the pattern subquery read committed data only — read-your-writes
// violated. End-to-end coverage: pkg/engine/match_hidden_subquery_txn_test.go.
func TestHasSubqueriesMatchExpr(t *testing.T) {
	if !hasSubqueriesInExpr(&query.MatchExpr{
		Columns: []query.Expression{&query.Identifier{Name: "content"}},
		Pattern: &query.SubqueryExpr{},
	}) {
		t.Error("MatchExpr wrapping a SubqueryExpr pattern should be detected")
	}
	if hasSubqueriesInExpr(&query.MatchExpr{
		Columns: []query.Expression{&query.Identifier{Name: "content"}},
		Pattern: &query.StringLiteral{Value: "needle"},
	}) {
		t.Error("MatchExpr with a literal pattern should not be detected")
	}
	if !hasSubqueriesInExpr(&query.MatchExpr{
		Columns: []query.Expression{&query.SubqueryExpr{}},
		Pattern: &query.StringLiteral{Value: "needle"},
	}) {
		t.Error("MatchExpr wrapping a SubqueryExpr column should be detected")
	}

	// Parse-level pin of the exact production shape.
	stmt, err := query.Parse("SELECT id FROM t1 WHERE MATCH(content) AGAINST ((SELECT v FROM t2 ORDER BY v ASC LIMIT 1))")
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	sel, ok := stmt.(*query.SelectStmt)
	if !ok {
		t.Fatalf("unexpected statement type %T", stmt)
	}
	if !hasSubqueries(sel) {
		t.Error("SELECT with subquery hidden inside AGAINST should be detected")
	}

	// Literal patterns must stay subquery-free so FTS predicates on large
	// scans remain parallel-eligible (the fix must not over-trigger).
	plain, err := query.Parse("SELECT id FROM t1 WHERE MATCH(content) AGAINST ('needle')")
	if err != nil {
		t.Fatalf("parse plain: %v", err)
	}
	plainSel, ok := plain.(*query.SelectStmt)
	if !ok {
		t.Fatalf("unexpected statement type %T", plain)
	}
	if hasSubqueries(plainSel) {
		t.Error("SELECT with a literal AGAINST pattern should not be flagged as containing subqueries")
	}
}
