package test

import (
	"fmt"
	"testing"
)

func TestV4Features(t *testing.T) {
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

	checkFail := func(desc string, sql string) {
		t.Helper()
		total++
		_, err := db.Exec(ctx, sql)
		if err != nil {
			pass++
		} else {
			t.Errorf("[FAIL] %s: should have failed", desc)
		}
	}

	// Setup test data
	afExec(t, db, ctx, "CREATE TABLE sales (id INTEGER PRIMARY KEY, product TEXT, region TEXT, amount REAL)")
	afExec(t, db, ctx, "INSERT INTO sales VALUES (1, 'Widget', 'North', 100)")
	afExec(t, db, ctx, "INSERT INTO sales VALUES (2, 'Widget', 'South', 150)")
	afExec(t, db, ctx, "INSERT INTO sales VALUES (3, 'Gadget', 'North', 200)")
	afExec(t, db, ctx, "INSERT INTO sales VALUES (4, 'Gadget', 'South', 250)")
	afExec(t, db, ctx, "INSERT INTO sales VALUES (5, 'Widget', 'North', 120)")

	// === 1. CTE with GROUP BY aggregates ===
	check("CTE SUM by product",
		"WITH totals AS (SELECT product, SUM(amount) as total FROM sales GROUP BY product) SELECT total FROM totals WHERE product = 'Widget'",
		370)

	check("CTE COUNT by product",
		"WITH totals AS (SELECT product, COUNT(*) as cnt FROM sales GROUP BY product) SELECT cnt FROM totals WHERE product = 'Widget'",
		3)

	// CTE with HAVING
	check("CTE HAVING",
		"WITH totals AS (SELECT product, SUM(amount) as total FROM sales GROUP BY product HAVING SUM(amount) > 400) SELECT product FROM totals",
		"Gadget")

	// CTE with ORDER BY on outer query
	rows := afQuery(t, db, ctx, "WITH totals AS (SELECT product, SUM(amount) as total FROM sales GROUP BY product) SELECT product, total FROM totals ORDER BY total DESC")
	total++
	if len(rows) == 2 && fmt.Sprintf("%v", rows[0][0]) == "Gadget" {
		pass++
	} else {
		t.Errorf("[FAIL] CTE ORDER BY: expected Gadget first, got %v", rows)
	}

	// === 2. applyOuterQuery GROUP BY ===
	afExec(t, db, ctx, "CREATE VIEW all_sales AS SELECT * FROM sales")

	checkRows("View GROUP BY",
		"SELECT product, COUNT(*) FROM all_sales GROUP BY product",
		2) // Widget, Gadget

	check("View GROUP BY COUNT value",
		"SELECT COUNT(*) as cnt FROM all_sales WHERE product = 'Widget'",
		3)

	// View with HAVING on outer query
	checkRows("View HAVING",
		"SELECT product, COUNT(*) FROM all_sales GROUP BY product HAVING COUNT(*) >= 3",
		1) // Only Widget has 3

	// View with aggregate and LIMIT
	checkRows("View aggregate LIMIT",
		"SELECT product, SUM(amount) FROM all_sales GROUP BY product ORDER BY SUM(amount) DESC LIMIT 1",
		1)

	// === 3. applyOuterQuery aggregates MIN/MAX/AVG ===
	check("View MAX",
		"SELECT MAX(amount) FROM all_sales",
		250)

	check("View MIN",
		"SELECT MIN(amount) FROM all_sales",
		100)

	check("View AVG",
		"SELECT AVG(amount) FROM all_sales",
		164)

	// === 4. Recursive CTE: counting ===
	checkRows("Recursive count",
		"WITH RECURSIVE cnt(n) AS (SELECT 1 UNION ALL SELECT n + 1 FROM cnt WHERE n < 10) SELECT n FROM cnt",
		10)

	check("Recursive SUM",
		"WITH RECURSIVE cnt(n) AS (SELECT 1 UNION ALL SELECT n + 1 FROM cnt WHERE n < 5) SELECT SUM(n) FROM cnt",
		15)

	// === 5. Recursive CTE: hierarchy ===
	afExec(t, db, ctx, "CREATE TABLE nodes (id INTEGER PRIMARY KEY, parent_id INTEGER, name TEXT)")
	afExec(t, db, ctx, "INSERT INTO nodes VALUES (1, NULL, 'root')")
	afExec(t, db, ctx, "INSERT INTO nodes VALUES (2, 1, 'child1')")
	afExec(t, db, ctx, "INSERT INTO nodes VALUES (3, 1, 'child2')")
	afExec(t, db, ctx, "INSERT INTO nodes VALUES (4, 2, 'grandchild1')")

	checkRows("Recursive tree traversal",
		"WITH RECURSIVE tree(id, name, parent_id) AS (SELECT id, name, parent_id FROM nodes WHERE id = 1 UNION ALL SELECT n.id, n.name, n.parent_id FROM nodes n INNER JOIN tree t ON n.parent_id = t.id) SELECT * FROM tree",
		4)

	// === 6. EvalExpression: CASE, CAST, Functions in INSERT ===
	afExec(t, db, ctx, "CREATE TABLE insert_test (id INTEGER PRIMARY KEY, val TEXT)")

	afExec(t, db, ctx, "INSERT INTO insert_test VALUES (1, CASE WHEN 1 > 0 THEN 'yes' ELSE 'no' END)")
	check("CASE in INSERT", "SELECT val FROM insert_test WHERE id = 1", "yes")

	afExec(t, db, ctx, "INSERT INTO insert_test VALUES (2, CAST(42 AS TEXT))")
	check("CAST in INSERT", "SELECT val FROM insert_test WHERE id = 2", "42")

	afExec(t, db, ctx, "INSERT INTO insert_test VALUES (3, UPPER('hello'))")
	check("UPPER in INSERT", "SELECT val FROM insert_test WHERE id = 3", "HELLO")

	afExec(t, db, ctx, "INSERT INTO insert_test VALUES (4, COALESCE(NULL, 'fallback'))")
	check("COALESCE in INSERT", "SELECT val FROM insert_test WHERE id = 4", "fallback")

	// Simple CASE form in INSERT
	afExec(t, db, ctx, "INSERT INTO insert_test VALUES (5, CASE 'x' WHEN 'a' THEN 'first' WHEN 'x' THEN 'second' ELSE 'other' END)")
	check("Simple CASE in INSERT", "SELECT val FROM insert_test WHERE id = 5", "second")

	// AND/OR in EvalExpression
	afExec(t, db, ctx, "INSERT INTO insert_test VALUES (6, CASE WHEN 1 > 0 AND 2 > 1 THEN 'both' ELSE 'not' END)")
	check("AND in INSERT CASE", "SELECT val FROM insert_test WHERE id = 6", "both")

	// === 7. UNION column count validation ===
	checkFail("UNION column mismatch",
		"SELECT id, product FROM sales UNION SELECT id FROM sales")

	// Valid UNION still works
	checkRows("UNION valid",
		"SELECT product FROM sales WHERE product = 'Widget' UNION SELECT product FROM sales WHERE product = 'Gadget'",
		2)

	// === 8. View with complex aggregates ===
	afExec(t, db, ctx, "CREATE VIEW sales_summary AS SELECT product, SUM(amount) as total, COUNT(*) as cnt, AVG(amount) as avg_amt FROM sales GROUP BY product")

	check("View SUM",
		"SELECT total FROM sales_summary WHERE product = 'Widget'",
		370)

	check("View COUNT via view",
		"SELECT cnt FROM sales_summary WHERE product = 'Gadget'",
		2)

	// View aggregate with ORDER BY
	rows = afQuery(t, db, ctx, "SELECT product, total FROM sales_summary ORDER BY total DESC")
	total++
	if len(rows) == 2 && fmt.Sprintf("%v", rows[0][0]) == "Gadget" {
		pass++
	} else {
		t.Errorf("[FAIL] View ORDER BY: expected Gadget first, got %v", rows)
	}

	// View with LIMIT
	checkRows("View LIMIT", "SELECT * FROM sales_summary LIMIT 1", 1)

	t.Logf("\n=== V4 FEATURES: %d/%d tests passed ===", pass, total)
	if pass < total {
		t.Errorf("Some tests failed: %d/%d", pass, total)
	}
}
