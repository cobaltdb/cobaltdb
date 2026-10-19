package catalog

import (
	"testing"

	"github.com/cobaltdb/cobaltdb/pkg/query"
)

// TestExprToSQLIsNullAndLikeRender pins the CheckStr/Default text
// reconstruction for the IS [NOT] NULL and [NOT] LIKE families.
//
// Pre-fix, exprToSQL had no case for *query.IsNullExpr or *query.LikeExpr, so
// both fell through to `default: fmt.Sprintf("%v", expr)` and persisted a Go
// struct dump (e.g. "&{0xc000... true}") as the column's CheckStr. The catalog
// load path (catalog_maintenance.go) re-parses CheckStr on Open, so a table
// with CHECK (x IS NOT NULL) or CHECK (name NOT LIKE 'A%') made the whole
// database unopenable after a restart:
//
//	failed to load catalog: failed to parse check expression for table
//	t_isnull column x: illegal token at line 1, column 9: {
//
// This is a distinct root cause from the subquery-bearing InExpr residual,
// which is deliberately still handled by the %v fallback above: a subquery
// cannot be rendered by this package at all, whereas IS NULL and LIKE are
// self-contained expressions that the renderer simply omitted.
func TestExprToSQLIsNullAndLikeRender(t *testing.T) {
	cases := []struct {
		expr string
		want string
	}{
		{"x IS NULL", "(x IS NULL)"},
		{"x IS NOT NULL", "(x IS NOT NULL)"},
		{"t.c IS NOT NULL", "(t.c IS NOT NULL)"},
		{"name LIKE 'A%'", "(name LIKE 'A%')"},
		{"name NOT LIKE 'A%'", "(name NOT LIKE 'A%')"},
		{"name LIKE 'a_b' ESCAPE '_'", "(name LIKE 'a_b') ESCAPE '_'"},
		{"name NOT LIKE 'a%' ESCAPE '!'", "(name NOT LIKE 'a%') ESCAPE '!'"},
	}
	for _, tt := range cases {
		expr, err := query.ParseExpression(tt.expr)
		if err != nil {
			t.Fatalf("parse %q: %v", tt.expr, err)
		}
		if got := exprToSQL(expr); got != tt.want {
			t.Errorf("exprToSQL(%q) = %q, want %q", tt.expr, got, tt.want)
		}
	}
}

// TestExprToSQLIsNullLikeRoundTrip pins the property the fix exists for: every
// rendered form must re-parse, and rendering must be idempotent — so the
// persisted CheckStr text is stable across a save/load cycle.
func TestExprToSQLIsNullLikeRoundTrip(t *testing.T) {
	cases := []string{
		"x IS NULL",
		"x IS NOT NULL",
		"name LIKE 'A%'",
		"name NOT LIKE 'A%'",
	}
	render := func(checkSQL string) string {
		t.Helper()
		stmt, err := query.Parse("CREATE TABLE t (x INTEGER CHECK (" + checkSQL + "))")
		if err != nil {
			t.Fatalf("parse %q: %v", checkSQL, err)
		}
		ct := stmt.(*query.CreateTableStmt)
		if len(ct.Columns) != 1 || ct.Columns[0].Check == nil {
			t.Fatalf("parse %q: expected one column with a CHECK expression", checkSQL)
		}
		return exprToSQL(ct.Columns[0].Check)
	}

	for _, checkSQL := range cases {
		got := render(checkSQL)
		if _, err := query.Parse("CREATE TABLE t (x INTEGER CHECK (" + got + "))"); err != nil {
			t.Errorf("rendered CHECK for %q does not re-parse: %v (got %q)", checkSQL, err, got)
			continue
		}
		if again := render(got); again != got {
			t.Errorf("render not idempotent for %q: got %q then %q", checkSQL, got, again)
		}
	}
}

// TestExprToSQLAggregateDistinctRender pins COUNT/SUM (DISTINCT x). The
// FunctionCall case renders only Name(Args); Distinct is a separate AST field,
// so the modifier used to be dropped, silently changing the meaning of the
// persisted text (COUNT(DISTINCT x) persisted as COUNT(x)).
func TestExprToSQLAggregateDistinctRender(t *testing.T) {
	cases := []struct {
		expr string
		want string
	}{
		{"COUNT(DISTINCT x)", "COUNT(DISTINCT x)"},
		{"SUM(DISTINCT x)", "SUM(DISTINCT x)"},
		{"COUNT(x)", "COUNT(x)"},
		{"COUNT(DISTINCT x) > 1", "(COUNT(DISTINCT x) > 1)"},
	}
	for _, tt := range cases {
		expr, err := query.ParseExpression(tt.expr)
		if err != nil {
			t.Fatalf("parse %q: %v", tt.expr, err)
		}
		if got := exprToSQL(expr); got != tt.want {
			t.Errorf("exprToSQL(%q) = %q, want %q", tt.expr, got, tt.want)
		}
	}
}
