package query

// Regression tests for:
//  4. parser rejects trailing garbage after a statement
//  6. query-cache utilities (recursive table extraction, non-determinism in
//     all clauses, deterministic cache-key serialization)
//  7. join-reorder safety in the query optimizer

import (
	"strings"
	"testing"
)

// --- Fix 4: trailing garbage ------------------------------------------------

func TestParseRejectsTrailingGarbage(t *testing.T) {
	cases := []string{
		"DELETE FROM g WHERE id = 1 SOME GARBAGE",
		"SELECT id FROM t WHERE id = 1 BOGUS",
		"INSERT INTO t VALUES (1) trailing",
		"UPDATE t SET a = 1 WHERE id = 1 nonsense here",
		"SELECT * FROM schema.table",
	}
	for _, sql := range cases {
		if _, err := Parse(sql); err == nil {
			t.Errorf("Parse(%q): expected trailing-garbage error, got success", sql)
		}
	}
}

func TestParseAllowsTrailingSemicolons(t *testing.T) {
	cases := []string{
		"SELECT 1",
		"SELECT 1;",
		"SELECT 1;;",
		"DELETE FROM g WHERE id = 1;",
		"SELECT a FROM t UNION SELECT b FROM u;",
		"EXPLAIN SELECT 1;",
		"CREATE TABLE t (id INTEGER PRIMARY KEY);",
	}
	for _, sql := range cases {
		if _, err := Parse(sql); err != nil {
			t.Errorf("Parse(%q): unexpected error: %v", sql, err)
		}
	}
}

func TestParseProcedureBodyStillParses(t *testing.T) {
	sql := "CREATE PROCEDURE p() BEGIN SELECT 1; SELECT 2; END"
	if _, err := Parse(sql); err != nil {
		t.Fatalf("procedure body with multiple statements must still parse: %v", err)
	}
}

// --- Fix 6a: recursive table extraction -------------------------------------

func tablesOf(t *testing.T, sql string) map[string]bool {
	t.Helper()
	stmt, err := Parse(sql)
	if err != nil {
		t.Fatalf("parse %q: %v", sql, err)
	}
	sel, ok := stmt.(*SelectStmt)
	if !ok {
		t.Fatalf("parse %q: got %T", sql, stmt)
	}
	set := make(map[string]bool)
	for _, tbl := range ExtractTablesFromQuery(sel) {
		set[strings.ToLower(tbl)] = true
	}
	return set
}

func TestExtractTablesFromQueryRecursesIntoSubqueries(t *testing.T) {
	tables := tablesOf(t, "SELECT id FROM t WHERE id IN (SELECT id FROM u)")
	if !tables["t"] || !tables["u"] {
		t.Fatalf("IN-subquery table missing from deps: %v", tables)
	}

	tables = tablesOf(t, "SELECT x FROM (SELECT a AS x FROM inner_t) AS d")
	if !tables["inner_t"] {
		t.Fatalf("derived-table base table missing from deps: %v", tables)
	}
	if tables[""] {
		t.Fatalf("empty table name recorded for derived table: %v", tables)
	}

	tables = tablesOf(t, "SELECT id FROM t WHERE EXISTS (SELECT 1 FROM v WHERE v.id = t.id)")
	if !tables["t"] || !tables["v"] {
		t.Fatalf("EXISTS-subquery table missing from deps: %v", tables)
	}

	tables = tablesOf(t, "SELECT (SELECT MAX(x) FROM w) FROM t")
	if !tables["t"] || !tables["w"] {
		t.Fatalf("scalar-subquery table missing from deps: %v", tables)
	}

	tables = tablesOf(t, "SELECT id FROM t JOIN (SELECT id FROM j_inner) AS d ON t.id = d.id")
	if !tables["t"] || !tables["j_inner"] {
		t.Fatalf("JOIN derived-table base table missing from deps: %v", tables)
	}
}

// --- Fix 6b: non-determinism in all clauses ---------------------------------

func TestIsCacheableQueryChecksAllClauses(t *testing.T) {
	notCacheable := []string{
		"SELECT a FROM t GROUP BY a HAVING MAX(ts) > NOW()",
		"SELECT a FROM t GROUP BY DATE(NOW())",
		"SELECT a FROM t WHERE id IN (SELECT id FROM u WHERE ts > NOW())",
		"SELECT a FROM (SELECT a FROM u WHERE ts > NOW()) AS d",
		"SELECT a FROM t LIMIT RAND()",
	}
	for _, sql := range notCacheable {
		stmt, err := Parse(sql)
		if err != nil {
			t.Fatalf("parse %q: %v", sql, err)
		}
		if IsCacheableQuery(stmt.(*SelectStmt)) {
			t.Errorf("IsCacheableQuery(%q) = true, want false", sql)
		}
	}

	cacheable := "SELECT a FROM t GROUP BY a HAVING MAX(b) > 5"
	stmt, err := Parse(cacheable)
	if err != nil {
		t.Fatalf("parse %q: %v", cacheable, err)
	}
	if !IsCacheableQuery(stmt.(*SelectStmt)) {
		t.Errorf("IsCacheableQuery(%q) = false, want true", cacheable)
	}
}

// --- Fix 6c: deterministic cache-key serialization --------------------------

func keyOf(t *testing.T, sql string) string {
	t.Helper()
	stmt, err := Parse(sql)
	if err != nil {
		t.Fatalf("parse %q: %v", sql, err)
	}
	return QueryToSQL(stmt.(*SelectStmt))
}

func TestCacheKeyStableAcrossParses(t *testing.T) {
	cases := []string{
		"SELECT id FROM t WHERE id = (SELECT MAX(id) FROM u)",
		"SELECT id FROM t WHERE EXISTS (SELECT 1 FROM u)",
		"SELECT CASE WHEN a > 1 THEN 'x' ELSE 'y' END FROM t",
		"SELECT id, ROW_NUMBER() OVER (PARTITION BY grp ORDER BY id DESC) FROM t",
	}
	for _, sql := range cases {
		k1 := keyOf(t, sql)
		k2 := keyOf(t, sql)
		if k1 != k2 {
			t.Errorf("cache key for %q not stable across parses:\n%q\n%q", sql, k1, k2)
		}
		if ContainsUncacheableExpr(k1) {
			t.Errorf("cache key for %q unexpectedly uncacheable: %q", sql, k1)
		}
	}
}

func TestCacheKeyDistinguishesPredicates(t *testing.T) {
	pairs := [][2]string{
		{
			"SELECT id FROM t WHERE id = (SELECT MAX(id) FROM u)",
			"SELECT id FROM t WHERE id = (SELECT MIN(id) FROM u)",
		},
		{
			"SELECT CASE WHEN a > 1 THEN 'x' ELSE 'y' END FROM t",
			"SELECT CASE WHEN a > 2 THEN 'x' ELSE 'y' END FROM t",
		},
		{
			"SELECT id FROM t WHERE EXISTS (SELECT 1 FROM u WHERE u.a = 1)",
			"SELECT id FROM t WHERE NOT EXISTS (SELECT 1 FROM u WHERE u.a = 1)",
		},
	}
	for _, pair := range pairs {
		if keyOf(t, pair[0]) == keyOf(t, pair[1]) {
			t.Errorf("distinct queries share a cache key:\n%q\n%q", pair[0], pair[1])
		}
	}
}

func TestUnknownExprNodeIsUncacheable(t *testing.T) {
	// An expression node type without explicit serialization must poison the
	// key rather than embed pointer addresses (or collide).
	type fakeExpr struct{ Expression }
	key := ExprToString(&fakeExpr{})
	if !ContainsUncacheableExpr(key) {
		t.Fatalf("unknown expression node must serialize to the uncacheable marker, got %q", key)
	}
}

// --- Fix 7: join reorder safety ---------------------------------------------

func TestOptimizerNeverReordersOuterJoins(t *testing.T) {
	qo := NewQueryOptimizer()
	// Stats that would strongly favor putting "small" first.
	qo.stats.RowCount["big"] = 1000000
	qo.stats.RowCount["small"] = 1

	stmt, err := Parse("SELECT * FROM base LEFT JOIN big ON base.id = big.base_id LEFT JOIN small ON base.id = small.base_id")
	if err != nil {
		t.Fatal(err)
	}
	optimized, err := qo.OptimizeSelect(stmt.(*SelectStmt))
	if err != nil {
		t.Fatal(err)
	}
	if optimized.Joins[0].Table.Name != "big" || optimized.Joins[1].Table.Name != "small" {
		t.Fatalf("LEFT JOIN order changed: %s, %s", optimized.Joins[0].Table.Name, optimized.Joins[1].Table.Name)
	}
}

func TestOptimizerRespectsOnClauseDependencies(t *testing.T) {
	qo := NewQueryOptimizer()
	// Favor "small" (the second join) so a naive reorder would move it before
	// "mid" — but its ON clause references mid, so that order is invalid.
	qo.stats.RowCount["mid"] = 1000000
	qo.stats.RowCount["small"] = 1

	stmt, err := Parse("SELECT * FROM base JOIN mid ON base.id = mid.base_id JOIN small ON mid.id = small.mid_id")
	if err != nil {
		t.Fatal(err)
	}
	optimized, err := qo.OptimizeSelect(stmt.(*SelectStmt))
	if err != nil {
		t.Fatal(err)
	}
	if optimized.Joins[0].Table.Name != "mid" || optimized.Joins[1].Table.Name != "small" {
		t.Fatalf("join order violates ON-clause dependency: %s, %s",
			optimized.Joins[0].Table.Name, optimized.Joins[1].Table.Name)
	}
}

func TestOptimizerReordersSafeInnerJoins(t *testing.T) {
	qo := NewQueryOptimizer()
	qo.stats.RowCount["b_tbl"] = 1000000
	qo.stats.RowCount["c_tbl"] = 1

	// Both joins reference only the FROM table — reordering is safe and the
	// cheaper table should come first.
	stmt, err := Parse("SELECT * FROM a_tbl JOIN b_tbl ON a_tbl.id = b_tbl.a_id JOIN c_tbl ON a_tbl.id = c_tbl.a_id")
	if err != nil {
		t.Fatal(err)
	}
	optimized, err := qo.OptimizeSelect(stmt.(*SelectStmt))
	if err != nil {
		t.Fatal(err)
	}
	if optimized.Joins[0].Table.Name != "c_tbl" {
		t.Fatalf("safe inner-join reorder not applied: got %s first", optimized.Joins[0].Table.Name)
	}
}

func TestEstimateSelectivityVerifiesTableOwnership(t *testing.T) {
	qo := NewQueryOptimizer()
	qo.stats.IndexStats["orders.id"] = &OptimizerIdxStats{
		TableName:   "users", // registered for a DIFFERENT table
		ColumnNames: []string{"id"},
		Selectivity: 0.0001,
	}
	where := &BinaryExpr{
		Left:     &Identifier{Name: "id"},
		Operator: TokenEq,
		Right:    &NumberLiteral{Value: 1},
	}
	sel := qo.estimateSelectivity("orders", where)
	if sel == 0.0001 {
		t.Fatal("selectivity from another table's index stats must not be used")
	}
}
