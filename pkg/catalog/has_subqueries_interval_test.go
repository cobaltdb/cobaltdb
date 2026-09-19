package catalog

import (
	"testing"

	"github.com/cobaltdb/cobaltdb/pkg/query"
)

// Regression: hasSubqueriesInExpr must descend into IntervalExpr.Value.
// `INTERVAL -(SELECT ...) DAY` parses the subquery into the interval's value
// expression, but the walker had no IntervalExpr case, so hasSubqueries(stmt)
// returned false and the parallel scan gate in scanTableRows (canParallel
// requires !hasSubqueries) was bypassed. ParallelSelectRows worker goroutines
// have no goroutine-keyed transaction state, so the subquery lost
// read-your-writes and evaluated against committed data only.
// End-to-end coverage: pkg/engine/interval_hidden_subquery_txn_test.go.
func TestHasSubqueriesIntervalExpr(t *testing.T) {
	if !hasSubqueriesInExpr(&query.IntervalExpr{Value: &query.SubqueryExpr{}}) {
		t.Error("IntervalExpr wrapping a SubqueryExpr should be detected")
	}
	if hasSubqueriesInExpr(&query.IntervalExpr{Value: &query.NumberLiteral{Value: 5}}) {
		t.Error("IntervalExpr wrapping a literal should not be detected")
	}

	// Parse-level pin of the exact production shape: unary minus reaches
	// parsePrimary, which parses (SELECT ...) as a subquery inside the
	// interval value.
	stmt, err := query.Parse("SELECT id FROM t1 WHERE d > DATE_ADD('2030-01-01', INTERVAL -(SELECT v FROM t2 ORDER BY v DESC LIMIT 1) DAY)")
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	sel, ok := stmt.(*query.SelectStmt)
	if !ok {
		t.Fatalf("unexpected statement type %T", stmt)
	}
	if !hasSubqueries(sel) {
		t.Error("SELECT with subquery hidden inside INTERVAL should be detected")
	}

	// Literal intervals must stay subquery-free so interval arithmetic on
	// large scans remains parallel-eligible (the fix must not over-trigger).
	plain, err := query.Parse("SELECT id FROM t1 WHERE d > DATE_ADD('2030-01-01', INTERVAL 5 DAY)")
	if err != nil {
		t.Fatalf("parse plain: %v", err)
	}
	plainSel, ok := plain.(*query.SelectStmt)
	if !ok {
		t.Fatalf("unexpected statement type %T", plain)
	}
	if hasSubqueries(plainSel) {
		t.Error("SELECT with a literal INTERVAL should not be flagged as containing subqueries")
	}
}
