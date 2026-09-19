package catalog

import (
	"testing"

	"github.com/cobaltdb/cobaltdb/pkg/query"
)

// TestExprToSQLCheckRoundTrip pins the CheckStr/Default text reconstruction:
// whatever the parser accepts inside a CHECK clause must render back to SQL
// text that re-parses to the same rendering (idempotence). Pre-fix,
// exprToSQL rendered BETWEEN as a Go struct dump and bitwise/shift/null-safe
// operators as "?", so the persisted CheckStr failed to re-parse on catalog
// load and a database containing such a CHECK could not be reopened at all
// (load catalog: failed to parse check expression ...: expected ), got ?).
func TestExprToSQLCheckRoundTrip(t *testing.T) {
	cases := []string{
		"x BETWEEN 1 AND 5",
		"x NOT BETWEEN 1 AND 5",
		"x & 1 = 1",
		"x | 2 = 3",
		"x ^ 1 = 0",
		"x << 2 = 4",
		"x >> 1 = 1",
		"x <=> NULL",
		"x IN (1, 2, 3)",
		"x NOT IN (1, 2)",
		"CAST(x AS INTEGER) = 1",
		"CASE WHEN x > 0 THEN 1 ELSE 0 END = 1",
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
		// The rendered text must re-parse.
		if _, err := query.Parse("CREATE TABLE t (x INTEGER CHECK (" + got + "))"); err != nil {
			t.Errorf("rendered CHECK for %q does not re-parse: %v (got %q)", checkSQL, err, got)
			continue
		}
		// And the rendering must be idempotent.
		if again := render(got); again != got {
			t.Errorf("render not idempotent for %q: got %q then %q", checkSQL, got, again)
		}
	}
}
