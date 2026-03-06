package test

import (
	"fmt"
	"testing"
)

func TestRecursiveCTE(t *testing.T) {
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

	checkRows := func(desc string, sql string, expectedCount int) {
		t.Helper()
		total++
		rows := afQuery(t, db, ctx, sql)
		if len(rows) != expectedCount {
			t.Errorf("[FAIL] %s: got %d rows, expected %d", desc, len(rows), expectedCount)
			return
		}
		pass++
	}

	// === 1. Simple counting recursive CTE ===
	checkRows("Recursive count 1..10",
		"WITH RECURSIVE cnt(n) AS (SELECT 1 UNION ALL SELECT n + 1 FROM cnt WHERE n < 10) SELECT n FROM cnt",
		10)

	check("Recursive first value",
		"WITH RECURSIVE cnt(n) AS (SELECT 1 UNION ALL SELECT n + 1 FROM cnt WHERE n < 10) SELECT n FROM cnt LIMIT 1",
		1)

	check("Recursive last value",
		"WITH RECURSIVE cnt(n) AS (SELECT 1 UNION ALL SELECT n + 1 FROM cnt WHERE n < 10) SELECT MAX(n) FROM cnt",
		10)

	// === 2. Recursive CTE with SUM ===
	check("Recursive SUM",
		"WITH RECURSIVE cnt(n) AS (SELECT 1 UNION ALL SELECT n + 1 FROM cnt WHERE n < 5) SELECT SUM(n) FROM cnt",
		15) // 1+2+3+4+5 = 15

	// === 3. Factorial-like pattern ===
	checkRows("Recursive 5 rows",
		"WITH RECURSIVE seq(n) AS (SELECT 0 UNION ALL SELECT n + 1 FROM seq WHERE n < 4) SELECT n FROM seq",
		5) // 0,1,2,3,4

	// === 4. Hierarchical data with recursive CTE ===
	afExec(t, db, ctx, "CREATE TABLE categories (id INTEGER PRIMARY KEY, name TEXT, parent_id INTEGER)")
	afExec(t, db, ctx, "INSERT INTO categories VALUES (1, 'Root', NULL)")
	afExec(t, db, ctx, "INSERT INTO categories VALUES (2, 'Electronics', 1)")
	afExec(t, db, ctx, "INSERT INTO categories VALUES (3, 'Books', 1)")
	afExec(t, db, ctx, "INSERT INTO categories VALUES (4, 'Laptops', 2)")
	afExec(t, db, ctx, "INSERT INTO categories VALUES (5, 'Phones', 2)")
	afExec(t, db, ctx, "INSERT INTO categories VALUES (6, 'Fiction', 3)")

	// Get all descendants of 'Root' (id=1) using recursive CTE
	checkRows("Hierarchical descendants",
		"WITH RECURSIVE tree(id, name, parent_id) AS (SELECT id, name, parent_id FROM categories WHERE id = 1 UNION ALL SELECT c.id, c.name, c.parent_id FROM categories c INNER JOIN tree t ON c.parent_id = t.id) SELECT * FROM tree",
		6) // Root + 5 descendants

	// Get all descendants of 'Electronics' (id=2)
	checkRows("Subtree descendants",
		"WITH RECURSIVE tree(id, name, parent_id) AS (SELECT id, name, parent_id FROM categories WHERE id = 2 UNION ALL SELECT c.id, c.name, c.parent_id FROM categories c INNER JOIN tree t ON c.parent_id = t.id) SELECT * FROM tree",
		3) // Electronics, Laptops, Phones

	// === 5. Recursive CTE with non-recursive CTE (RECURSIVE applies to whole WITH) ===
	check("Mixed CTE",
		"WITH RECURSIVE base AS (SELECT 100 AS start_val), cnt(n) AS (SELECT 1 UNION ALL SELECT n + 1 FROM cnt WHERE n < 3) SELECT COUNT(*) FROM cnt",
		3)

	// === 6. CASE expression in INSERT VALUES ===
	afExec(t, db, ctx, "CREATE TABLE case_test (id INTEGER PRIMARY KEY, label TEXT)")
	afExec(t, db, ctx, "INSERT INTO case_test VALUES (1, CASE WHEN 1 > 0 THEN 'positive' ELSE 'negative' END)")
	check("CASE in INSERT value", "SELECT label FROM case_test WHERE id = 1", "positive")

	// === 7. CAST in INSERT VALUES ===
	afExec(t, db, ctx, "CREATE TABLE cast_test (id INTEGER PRIMARY KEY, val INTEGER)")
	afExec(t, db, ctx, "INSERT INTO cast_test VALUES (1, CAST(3.14 AS INTEGER))")
	check("CAST in INSERT value", "SELECT val FROM cast_test WHERE id = 1", 3)

	// === 8. Function in INSERT VALUES ===
	afExec(t, db, ctx, "CREATE TABLE func_test (id INTEGER PRIMARY KEY, val TEXT)")
	afExec(t, db, ctx, "INSERT INTO func_test VALUES (1, UPPER('hello'))")
	check("Function in INSERT value", "SELECT val FROM func_test WHERE id = 1", "HELLO")

	afExec(t, db, ctx, "INSERT INTO func_test VALUES (2, COALESCE(NULL, 'fallback'))")
	check("COALESCE in INSERT value", "SELECT val FROM func_test WHERE id = 2", "fallback")

	t.Logf("\n=== RECURSIVE CTE & EVALEXPRESSION: %d/%d tests passed ===", pass, total)
	if pass < total {
		t.Errorf("Some tests failed: %d/%d", pass, total)
	}
}
