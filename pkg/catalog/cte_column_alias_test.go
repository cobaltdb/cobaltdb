package catalog

import "testing"

// TestCTEExplicitColumnAliasesAreApplied pins that a non-recursive CTE's
// explicit column-name list renames the produced columns and that the declared
// names are addressable by the main query.
//
// The plain-SELECT CTE path used to materialize results only when more than
// one CTE was present, and the UNION path only substituted the names when the
// query produced no column names at all. A single CTE therefore resolved
// through its registered view and exposed the inner SELECT's own aliases, so
// `WITH cte(a) AS (SELECT n FROM t) SELECT * FROM cte` exposed n instead of a
// and `SELECT a FROM cte` did not resolve the declared name.
//
// applyCTEColumnAliases only renames when the list length matches the produced
// column count, so a mismatched list is deliberately left unchanged.
func TestCTEExplicitColumnAliasesAreApplied(t *testing.T) {
	c := newTestCatalog(t)
	mustExecCTEAlias(t, c, "CREATE TABLE t (n INTEGER)")
	mustExecCTEAlias(t, c, "INSERT INTO t VALUES (1)")

	// Declared alias must be exposed by SELECT *.
	res, err := c.ExecuteQuery("WITH cte(a) AS (SELECT n FROM t) SELECT * FROM cte")
	if err != nil {
		t.Fatalf("SELECT * FROM cte: %v", err)
	}
	if len(res.Columns) != 1 || res.Columns[0] != "a" {
		t.Fatalf("CTE alias not applied: columns = %q, want [\"a\"]", res.Columns)
	}

	// Declared alias must be addressable by name.
	res2, err := c.ExecuteQuery("WITH cte(a) AS (SELECT n FROM t) SELECT a FROM cte")
	if err != nil {
		t.Fatalf("SELECT a FROM cte: %v", err)
	}
	if len(res2.Rows) != 1 || len(res2.Rows[0]) != 1 {
		t.Fatalf("SELECT a FROM cte returned rows %v, want one row with one column", res2.Rows)
	}
	if got := cteAliasValue(res2.Rows[0][0]); got != "1" {
		t.Fatalf("SELECT a FROM cte = %q, want \"1\"", got)
	}

	// Multi-column CTE: every declared name must be addressable.
	res3, err := c.ExecuteQuery("WITH cte(p, q) AS (SELECT 1 AS x, 2 AS y) SELECT p, q FROM cte")
	if err != nil {
		t.Fatalf("multi-column CTE aliases: %v", err)
	}
	if len(res3.Columns) != 2 || res3.Columns[0] != "p" || res3.Columns[1] != "q" {
		t.Fatalf("multi-column aliases = %q, want [\"p\" \"q\"]", res3.Columns)
	}
	if len(res3.Rows) == 1 && len(res3.Rows[0]) == 2 &&
		cteAliasValue(res3.Rows[0][0]) != "1" || cteAliasValue(res3.Rows[0][1]) != "2" {
		t.Fatalf("multi-column rows = %v, want [1 2]", res3.Rows)
	}

	// Mismatched list length is deliberately left unchanged (no partial rename).
	res4, err := c.ExecuteQuery("WITH cte(a) AS (SELECT 1 AS x, 2 AS y) SELECT * FROM cte")
	if err != nil {
		t.Fatalf("mismatched CTE aliases: %v", err)
	}
	if len(res4.Columns) != 2 || res4.Columns[0] != "x" || res4.Columns[1] != "y" {
		t.Fatalf("mismatched aliases = %q, want the inner aliases [\"x\" \"y\"]", res4.Columns)
	}
}

func mustExecCTEAlias(t *testing.T, c *Catalog, sql string) {
	t.Helper()
	if _, err := c.ExecuteQuery(sql); err != nil {
		t.Fatalf("exec %q: %v", sql, err)
	}
}

func cteAliasValue(v interface{}) string {
	switch n := v.(type) {
	case string:
		return n
	case int64:
		return itoa64(n)
	}
	return ""
}

func itoa64(n int64) string {
	if n == 0 {
		return "0"
	}
	neg := n < 0
	if neg {
		n = -n
	}
	var b [24]byte
	i := len(b)
	for n > 0 {
		i--
		b[i] = byte('0' + n%10)
		n /= 10
	}
	if neg {
		i--
		b[i] = '-'
	}
	return string(b[i:])
}
