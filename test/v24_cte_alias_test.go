package test

import (
	"fmt"
	"testing"
)

func TestV24CTEAndAlias(t *testing.T) {
	db, ctx := af(t)
	pass := 0
	total := 0

	check := func(desc string, sql string, expected interface{}) {
		t.Helper()
		total++
		rows := afQuery(t, db, ctx, sql)
		if len(rows) == 0 || len(rows[0]) == 0 {
			t.Errorf("[FAIL] %s: no rows returned", desc)
			return
		}
		got := fmt.Sprintf("%v", rows[0][0])
		exp := fmt.Sprintf("%v", expected)
		if got != exp {
			t.Errorf("[FAIL] %s: got %s, expected %s", desc, got, exp)
			return
		}
		pass++
	}

	checkRowCount := func(desc string, sql string, expected int) {
		t.Helper()
		total++
		rows := afQuery(t, db, ctx, sql)
		if len(rows) != expected {
			t.Errorf("[FAIL] %s: expected %d rows, got %d", desc, expected, len(rows))
			return
		}
		pass++
	}

	// ============================================================
	// === CTE WITH ALIASES ===
	// ============================================================
	check("CTE with alias column",
		"WITH cte AS (SELECT 1 AS a) SELECT a FROM cte", 1)

	check("CTE with multiple alias columns",
		"WITH cte AS (SELECT 1 AS x, 2 AS y) SELECT x FROM cte", 1)

	check("CTE with expression alias",
		"WITH cte AS (SELECT 10 + 20 AS result) SELECT result FROM cte", 30)

	check("CTE with string alias",
		"WITH cte AS (SELECT 'hello' AS greeting) SELECT greeting FROM cte", "hello")

	// ============================================================
	// === CTE WITH REAL TABLE ===
	// ============================================================
	afExec(t, db, ctx, "CREATE TABLE cte_data (id INTEGER PRIMARY KEY, name TEXT, val INTEGER)")
	afExec(t, db, ctx, "INSERT INTO cte_data VALUES (1, 'Alice', 100)")
	afExec(t, db, ctx, "INSERT INTO cte_data VALUES (2, 'Bob', 200)")
	afExec(t, db, ctx, "INSERT INTO cte_data VALUES (3, 'Charlie', 300)")

	check("CTE from table",
		"WITH top AS (SELECT name, val FROM cte_data WHERE val > 150) SELECT name FROM top ORDER BY val DESC LIMIT 1",
		"Charlie")

	checkRowCount("CTE filters correctly",
		"WITH filtered AS (SELECT * FROM cte_data WHERE val >= 200) SELECT * FROM filtered",
		2)

	check("CTE with aggregate",
		"WITH totals AS (SELECT SUM(val) AS total FROM cte_data) SELECT total FROM totals",
		600)

	// ============================================================
	// === RECURSIVE CTE ===
	// ============================================================
	check("Recursive CTE generates sequence",
		"WITH RECURSIVE seq(n) AS (SELECT 1 UNION ALL SELECT n + 1 FROM seq WHERE n < 5) SELECT COUNT(*) FROM seq",
		5)

	check("Recursive CTE max value",
		"WITH RECURSIVE seq(n) AS (SELECT 1 UNION ALL SELECT n + 1 FROM seq WHERE n < 10) SELECT MAX(n) FROM seq",
		10)

	check("Recursive CTE sum",
		"WITH RECURSIVE seq(n) AS (SELECT 1 UNION ALL SELECT n + 1 FROM seq WHERE n < 5) SELECT SUM(n) FROM seq",
		15) // 1+2+3+4+5

	// ============================================================
	// === CTE WITH HIERARCHY ===
	// ============================================================
	afExec(t, db, ctx, "CREATE TABLE org (id INTEGER PRIMARY KEY, name TEXT, parent_id INTEGER)")
	afExec(t, db, ctx, "INSERT INTO org VALUES (1, 'CEO', NULL)")
	afExec(t, db, ctx, "INSERT INTO org VALUES (2, 'CTO', 1)")
	afExec(t, db, ctx, "INSERT INTO org VALUES (3, 'CFO', 1)")
	afExec(t, db, ctx, "INSERT INTO org VALUES (4, 'Dev Lead', 2)")
	afExec(t, db, ctx, "INSERT INTO org VALUES (5, 'Dev', 4)")

	check("Recursive CTE hierarchy count",
		"WITH RECURSIVE tree(id, name, lvl) AS (SELECT id, name, 0 FROM org WHERE parent_id IS NULL UNION ALL SELECT o.id, o.name, t.lvl + 1 FROM org o JOIN tree t ON o.parent_id = t.id) SELECT COUNT(*) FROM tree",
		5)

	check("Recursive CTE max depth",
		"WITH RECURSIVE tree(id, name, lvl) AS (SELECT id, name, 0 FROM org WHERE parent_id IS NULL UNION ALL SELECT o.id, o.name, t.lvl + 1 FROM org o JOIN tree t ON o.parent_id = t.id) SELECT MAX(lvl) FROM tree",
		3) // CEO(0) -> CTO(1) -> Dev Lead(2) -> Dev(3)

	// ============================================================
	// === SCALAR SELECT ALIASES ===
	// ============================================================
	check("Scalar select with alias", "SELECT 42 AS answer", 42)
	check("Scalar expression alias", "SELECT 10 * 5 AS product", 50)
	check("Scalar string alias", "SELECT 'test' AS label", "test")

	// ============================================================
	// === VIEW WITH ALIAS COLUMNS ===
	// ============================================================
	afExec(t, db, ctx, "CREATE VIEW named_view AS SELECT name AS person_name, val AS score FROM cte_data")
	check("View with aliases",
		"SELECT person_name FROM named_view ORDER BY score DESC LIMIT 1", "Charlie")
	checkRowCount("View preserves all rows", "SELECT * FROM named_view", 3)

	t.Logf("\n=== V24 CTE & ALIAS: %d/%d tests passed ===", pass, total)
	if pass < total {
		t.Errorf("Some tests failed: %d/%d", pass, total)
	}
}
