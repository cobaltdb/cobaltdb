package catalog

import (
	"testing"

	"github.com/cobaltdb/cobaltdb/pkg/query"
)

// parseScalarSelect tokenizes and parses sql into a *SelectStmt.
func parseScalarSelect(t *testing.T, sql string) *query.SelectStmt {
	t.Helper()
	tokens, err := query.Tokenize(sql)
	if err != nil {
		t.Fatalf("Tokenize(%q): %v", sql, err)
	}
	stmt, err := query.NewParser(tokens).Parse()
	if err != nil {
		t.Fatalf("Parse(%q): %v", sql, err)
	}
	sel, ok := stmt.(*query.SelectStmt)
	if !ok {
		t.Fatalf("statement type = %T, want *SelectStmt", stmt)
	}
	return sel
}

// TestScalarAggregateAlias verifies that FROM-less aggregate queries with a
// column alias work. The parser wraps the select item in *AliasExpr;
// executeScalarSelect detects the aggregate through the alias
// (`actual = ae.Expr` before isAggregateCall) and routes to
// executeScalarAggregate — which must unwrap the alias the same way instead
// of asserting the raw column is a *FunctionCall (a bare `col.(*FunctionCall)`
// failed with "aggregate functions required in this context" for every
// aliased FROM-less aggregate).
func TestScalarAggregateAlias(t *testing.T) {
	c := newTestCatalog(t)

	cols, rows, err := c.Select(parseScalarSelect(t, "SELECT COUNT(*) AS total"), nil)
	if err != nil {
		t.Fatalf("SELECT COUNT(*) AS total: %v", err)
	}
	if len(cols) != 1 || cols[0] != "total" {
		t.Errorf("columns = %v, want [total]", cols)
	}
	if len(rows) != 1 || len(rows[0]) != 1 {
		t.Fatalf("rows = %v, want one row with one column", rows)
	}
	if got, ok := rows[0][0].(float64); !ok || got != 1 {
		t.Errorf("COUNT(*) = %v (%T), want 1", rows[0][0], rows[0][0])
	}

	cols, rows, err = c.Select(parseScalarSelect(t, "SELECT SUM(1) AS s"), nil)
	if err != nil {
		t.Fatalf("SELECT SUM(1) AS s: %v", err)
	}
	if len(cols) != 1 || cols[0] != "s" {
		t.Errorf("columns = %v, want [s]", cols)
	}
	if len(rows) != 1 || len(rows[0]) != 1 {
		t.Fatalf("rows = %v, want one row with one column", rows)
	}
	if got, ok := rows[0][0].(float64); !ok || got != 1 {
		t.Errorf("SUM(1) = %v (%T), want 1", rows[0][0], rows[0][0])
	}
}

// TestScalarAggregateNoAlias guards the un-aliased form, which must keep its
// function-name-derived column label.
func TestScalarAggregateNoAlias(t *testing.T) {
	c := newTestCatalog(t)

	cols, rows, err := c.Select(parseScalarSelect(t, "SELECT COUNT(*)"), nil)
	if err != nil {
		t.Fatalf("SELECT COUNT(*): %v", err)
	}
	if len(cols) != 1 || cols[0] != "COUNT(*)" {
		t.Errorf("columns = %v, want [COUNT(*)]", cols)
	}
	if len(rows) != 1 {
		t.Fatalf("rows = %v, want one row", rows)
	}
	if got, ok := rows[0][0].(float64); !ok || got != 1 {
		t.Errorf("COUNT(*) = %v (%T), want 1", rows[0][0], rows[0][0])
	}
}
