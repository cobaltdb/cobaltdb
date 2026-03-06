package test

import (
	"fmt"
	"testing"
)

// TestV44NewFeatures tests newly added SQL features:
// - GROUP BY positional references (GROUP BY 1, 2)
// - ORDER BY positional references in GROUP BY context
// - Derived tables (subquery in FROM)
// - Multi-CTE cross-join (FROM a, b)
// - Comma-separated FROM tables (implicit CROSS JOIN)
func TestV44NewFeatures(t *testing.T) {
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
	// Setup tables
	// ============================================================
	afExec(t, db, ctx, "CREATE TABLE v44_items (id INTEGER PRIMARY KEY, name TEXT, category TEXT, price INTEGER)")
	afExec(t, db, ctx, "INSERT INTO v44_items VALUES (1, 'Widget', 'Hardware', 10)")
	afExec(t, db, ctx, "INSERT INTO v44_items VALUES (2, 'Gadget', 'Electronics', 25)")
	afExec(t, db, ctx, "INSERT INTO v44_items VALUES (3, 'Phone', 'Electronics', 500)")
	afExec(t, db, ctx, "INSERT INTO v44_items VALUES (4, 'Cable', 'Hardware', 5)")
	afExec(t, db, ctx, "INSERT INTO v44_items VALUES (5, 'Screen', 'Electronics', 300)")
	afExec(t, db, ctx, "INSERT INTO v44_items VALUES (6, 'Bolt', 'Hardware', 2)")

	afExec(t, db, ctx, "CREATE TABLE v44_sales (id INTEGER PRIMARY KEY, item_id INTEGER, qty INTEGER, region TEXT)")
	afExec(t, db, ctx, "INSERT INTO v44_sales VALUES (1, 1, 10, 'East')")
	afExec(t, db, ctx, "INSERT INTO v44_sales VALUES (2, 2, 5, 'West')")
	afExec(t, db, ctx, "INSERT INTO v44_sales VALUES (3, 3, 2, 'East')")
	afExec(t, db, ctx, "INSERT INTO v44_sales VALUES (4, 1, 8, 'West')")
	afExec(t, db, ctx, "INSERT INTO v44_sales VALUES (5, 4, 20, 'East')")
	afExec(t, db, ctx, "INSERT INTO v44_sales VALUES (6, 5, 3, 'West')")
	afExec(t, db, ctx, "INSERT INTO v44_sales VALUES (7, 2, 7, 'East')")
	afExec(t, db, ctx, "INSERT INTO v44_sales VALUES (8, 6, 100, 'East')")

	afExec(t, db, ctx, "CREATE TABLE v44_employees (id INTEGER PRIMARY KEY, name TEXT, dept TEXT, salary INTEGER)")
	afExec(t, db, ctx, "INSERT INTO v44_employees VALUES (1, 'Alice', 'Engineering', 90000)")
	afExec(t, db, ctx, "INSERT INTO v44_employees VALUES (2, 'Bob', 'Engineering', 85000)")
	afExec(t, db, ctx, "INSERT INTO v44_employees VALUES (3, 'Charlie', 'Sales', 70000)")
	afExec(t, db, ctx, "INSERT INTO v44_employees VALUES (4, 'Diana', 'Sales', 75000)")
	afExec(t, db, ctx, "INSERT INTO v44_employees VALUES (5, 'Eve', 'Marketing', 65000)")

	// ============================================================
	// === GROUP BY POSITIONAL REFERENCES ===
	// ============================================================

	// GP1: GROUP BY 1 - group by first column
	checkRowCount("GP1 GROUP BY 1 groups by first column",
		"SELECT category, COUNT(*) FROM v44_items GROUP BY 1", 2) // Hardware, Electronics

	// GP2: GROUP BY 1 with ORDER BY
	check("GP2 GROUP BY 1 ORDER BY 2 DESC",
		"SELECT category, SUM(price) AS total FROM v44_items GROUP BY 1 ORDER BY 2 DESC LIMIT 1",
		"Electronics") // 825 > 17

	// GP3: GROUP BY 2 - group by second column
	check("GP3 GROUP BY 2 groups by second column",
		"SELECT COUNT(*), category FROM v44_items GROUP BY 2 ORDER BY COUNT(*) DESC LIMIT 1",
		3) // Both groups have 3

	// GP4: GROUP BY 1 with aggregate
	check("GP4 GROUP BY 1 SUM",
		"SELECT category, SUM(price) FROM v44_items GROUP BY 1 ORDER BY SUM(price) DESC LIMIT 1",
		"Electronics") // 25+500+300 = 825 vs 10+5+2 = 17

	// GP5: GROUP BY 1 with HAVING
	checkRowCount("GP5 GROUP BY 1 HAVING",
		"SELECT category, SUM(price) AS total FROM v44_items GROUP BY 1 HAVING SUM(price) > 100", 1) // Only Electronics

	// GP6: GROUP BY with JOIN and positional ref
	check("GP6 GROUP BY 1 with JOIN",
		"SELECT v44_items.category, SUM(v44_sales.qty) FROM v44_items JOIN v44_sales ON v44_items.id = v44_sales.item_id GROUP BY 1 ORDER BY SUM(v44_sales.qty) DESC LIMIT 1",
		"Hardware") // Widget:18 + Cable:20 + Bolt:100 = 138 vs Gadget:12 + Phone:2 + Screen:3 = 17

	// GP7: GROUP BY 1, 2 - group by multiple positional refs
	// Sales: (E,1),(W,2),(E,3),(W,1),(E,4),(W,5),(E,2),(E,6) = 8 distinct combos
	checkRowCount("GP7 GROUP BY 1 with two columns",
		"SELECT region, item_id, SUM(qty) FROM v44_sales GROUP BY 1, 2", 8)

	// GP8: GROUP BY mixing positional and named
	checkRowCount("GP8 GROUP BY 1, named column",
		"SELECT region, item_id, COUNT(*) FROM v44_sales GROUP BY 1, item_id", 8)

	// ============================================================
	// === ORDER BY POSITIONAL REFERENCES (in GROUP BY context) ===
	// ============================================================

	// OP1: ORDER BY 1 in GROUP BY result
	check("OP1 ORDER BY 1 ASC",
		"SELECT category, COUNT(*) FROM v44_items GROUP BY category ORDER BY 1 LIMIT 1",
		"Electronics") // 'E' < 'H' alphabetically

	// OP2: ORDER BY 1 DESC
	check("OP2 ORDER BY 1 DESC",
		"SELECT category, COUNT(*) FROM v44_items GROUP BY category ORDER BY 1 DESC LIMIT 1",
		"Hardware")

	// OP3: ORDER BY 2 (aggregate column)
	check("OP3 ORDER BY 2 aggregate",
		"SELECT category, SUM(price) FROM v44_items GROUP BY category ORDER BY 2 DESC LIMIT 1",
		"Electronics") // 825 > 17

	// OP4: ORDER BY positional in non-GROUP BY context
	check("OP4 ORDER BY 1 no GROUP BY",
		"SELECT name, price FROM v44_items ORDER BY 2 DESC LIMIT 1",
		"Phone") // 500

	// OP5: ORDER BY 2 ASC
	check("OP5 ORDER BY 2 ASC no GROUP BY",
		"SELECT name, price FROM v44_items ORDER BY 2 ASC LIMIT 1",
		"Bolt") // 2

	// ============================================================
	// === DERIVED TABLES (Subquery in FROM) ===
	// ============================================================

	// DT1: Simple derived table
	check("DT1 simple derived table",
		"SELECT name FROM (SELECT name, price FROM v44_items WHERE price > 100) AS expensive ORDER BY name LIMIT 1",
		"Phone") // Phone(500), Screen(300)

	// DT2: Derived table with COUNT
	checkRowCount("DT2 derived table with filter",
		"SELECT * FROM (SELECT name, price FROM v44_items WHERE category = 'Electronics') AS elec", 3)

	// DT3: Derived table with aggregate
	check("DT3 derived table aggregate",
		"SELECT total FROM (SELECT SUM(price) AS total FROM v44_items) AS summary",
		842) // 10+25+500+5+300+2 = 842

	// DT4: Nested derived tables
	check("DT4 nested derived tables",
		"SELECT cnt FROM (SELECT COUNT(*) AS cnt FROM (SELECT * FROM v44_items WHERE price > 10) AS mid) AS outer_t",
		3) // Gadget(25), Phone(500), Screen(300)

	// DT5: Derived table with GROUP BY - both 3, check count instead
	check("DT5 derived table with GROUP BY",
		"SELECT cnt FROM (SELECT category AS cat, COUNT(*) AS cnt FROM v44_items GROUP BY category) AS grouped ORDER BY cnt DESC LIMIT 1",
		3)

	// DT6: Derived table with WHERE on outer query
	checkRowCount("DT6 derived table with outer WHERE",
		"SELECT * FROM (SELECT id, name, price FROM v44_items) AS all_items WHERE price > 50", 2) // Phone, Screen

	// DT7: Derived table with ORDER BY
	check("DT7 derived table with outer ORDER BY",
		"SELECT name FROM (SELECT name, price FROM v44_items) AS all_items ORDER BY price DESC LIMIT 1",
		"Phone")

	// DT8: Derived table with LIMIT
	checkRowCount("DT8 derived table with LIMIT",
		"SELECT * FROM (SELECT * FROM v44_items ORDER BY price DESC LIMIT 3) AS top3", 3)

	// DT9: Derived table used in aggregate
	check("DT9 aggregate on derived table",
		"SELECT MAX(price) FROM (SELECT price FROM v44_items WHERE category = 'Hardware') AS hw",
		10) // max of 10, 5, 2

	// DT10: Derived table with aliased columns
	check("DT10 derived table aliased columns",
		"SELECT item_name FROM (SELECT name AS item_name, price AS item_price FROM v44_items) AS renamed ORDER BY item_price DESC LIMIT 1",
		"Phone")

	// ============================================================
	// === MULTI-CTE AND CROSS JOIN ===
	// ============================================================

	// MC1: Simple multi-CTE
	check("MC1 multi-CTE sequential access",
		`WITH hw AS (SELECT * FROM v44_items WHERE category = 'Hardware'),
		      elec AS (SELECT * FROM v44_items WHERE category = 'Electronics')
		 SELECT COUNT(*) FROM hw`,
		3) // Widget, Cable, Bolt

	// MC2: Multi-CTE accessing second CTE
	check("MC2 multi-CTE second CTE",
		`WITH hw AS (SELECT * FROM v44_items WHERE category = 'Hardware'),
		      elec AS (SELECT * FROM v44_items WHERE category = 'Electronics')
		 SELECT COUNT(*) FROM elec`,
		3) // Gadget, Phone, Screen

	// MC3: Multi-CTE cross join
	checkRowCount("MC3 multi-CTE cross join",
		`WITH colors AS (SELECT 'Red' AS color UNION ALL SELECT 'Blue'),
		      sizes AS (SELECT 'S' AS size UNION ALL SELECT 'L')
		 SELECT * FROM colors, sizes`, 4) // 2*2 = 4 combinations

	// MC4: Comma-separated FROM with real tables
	checkRowCount("MC4 FROM a, b cartesian",
		"SELECT * FROM v44_employees, v44_items WHERE v44_employees.dept = 'Marketing'", 6) // 1 employee * 6 items

	// MC5: Comma-separated FROM with WHERE filter
	check("MC5 FROM a, b with filter",
		"SELECT v44_employees.name FROM v44_employees, v44_items WHERE v44_items.name = 'Phone' AND v44_employees.dept = 'Engineering' ORDER BY v44_employees.name LIMIT 1",
		"Alice")

	// MC6: Three comma-separated tables
	afExec(t, db, ctx, "CREATE TABLE v44_colors (id INTEGER PRIMARY KEY, color TEXT)")
	afExec(t, db, ctx, "INSERT INTO v44_colors VALUES (1, 'Red')")
	afExec(t, db, ctx, "INSERT INTO v44_colors VALUES (2, 'Blue')")

	afExec(t, db, ctx, "CREATE TABLE v44_sizes (id INTEGER PRIMARY KEY, size TEXT)")
	afExec(t, db, ctx, "INSERT INTO v44_sizes VALUES (1, 'S')")
	afExec(t, db, ctx, "INSERT INTO v44_sizes VALUES (2, 'M')")
	afExec(t, db, ctx, "INSERT INTO v44_sizes VALUES (3, 'L')")

	checkRowCount("MC6 three-way cartesian product",
		"SELECT v44_colors.color, v44_sizes.size FROM v44_colors, v44_sizes", 6) // 2*3 = 6

	// MC7: CTE with derived table
	check("MC7 CTE with derived table",
		`WITH summary AS (SELECT category, SUM(price) AS total FROM v44_items GROUP BY category)
		 SELECT category FROM (SELECT * FROM summary WHERE total > 100) AS big_cats`,
		"Electronics") // 825 > 100

	// MC8: Multi-CTE with aggregates
	check("MC8 multi-CTE aggregates",
		`WITH hw_total AS (SELECT SUM(price) AS total FROM v44_items WHERE category = 'Hardware'),
		      elec_total AS (SELECT SUM(price) AS total FROM v44_items WHERE category = 'Electronics')
		 SELECT total FROM elec_total`,
		825) // 25+500+300

	// ============================================================
	// === COMBINED FEATURE TESTS ===
	// ============================================================

	// CF1: GROUP BY positional with derived table
	checkRowCount("CF1 GROUP BY 1 on derived table",
		"SELECT category, COUNT(*) FROM (SELECT * FROM v44_items) AS all_items GROUP BY 1", 2) // 2 groups

	// CF2: Derived table with GROUP BY positional and ORDER BY positional
	check("CF2 derived table + GROUP BY 1 + ORDER BY 2",
		"SELECT cat, total FROM (SELECT category AS cat, SUM(price) AS total FROM v44_items GROUP BY 1) AS summary ORDER BY 2 DESC LIMIT 1",
		"Electronics")

	// CF3: CTE + derived table + positional
	check("CF3 CTE + derived + positional",
		`WITH data AS (SELECT * FROM v44_items)
		 SELECT cnt FROM (SELECT category, COUNT(*) AS cnt FROM data GROUP BY 1) AS grouped ORDER BY 2 DESC LIMIT 1`,
		3)

	// CF4: Derived table in subquery context
	check("CF4 derived table EXISTS subquery",
		"SELECT COUNT(*) FROM v44_items WHERE EXISTS (SELECT 1 FROM (SELECT * FROM v44_sales WHERE v44_sales.item_id = v44_items.id) AS item_sales)",
		6) // All items have at least one sale

	// CF5: ORDER BY positional with LIMIT
	check("CF5 ORDER BY 1 with LIMIT",
		"SELECT dept, AVG(salary) FROM v44_employees GROUP BY 1 ORDER BY 2 DESC LIMIT 1",
		"Engineering") // avg 87500

	// CF6: GROUP BY 1 with MIN/MAX
	check("CF6 GROUP BY 1 MIN",
		"SELECT category, MIN(price) FROM v44_items GROUP BY 1 ORDER BY MIN(price) LIMIT 1",
		"Hardware") // min=2

	// CF7: Multiple positional GROUP BY + ORDER BY
	check("CF7 GROUP BY 1 ORDER BY 1",
		"SELECT region, COUNT(*) FROM v44_sales GROUP BY 1 ORDER BY 1 LIMIT 1",
		"East") // 'E' < 'W'

	// ============================================================
	// === EDGE CASES ===
	// ============================================================

	// EC1: Derived table with empty result
	checkRowCount("EC1 derived table empty",
		"SELECT * FROM (SELECT * FROM v44_items WHERE price > 10000) AS empty", 0)

	// EC2: Derived table with single row
	checkRowCount("EC2 derived table single row",
		"SELECT * FROM (SELECT 42 AS val) AS single", 1)
	check("EC2b derived table single value",
		"SELECT val FROM (SELECT 42 AS val) AS single", 42)

	// EC3: GROUP BY positional on expression
	check("EC3 GROUP BY 1 on expression column",
		"SELECT CASE WHEN price > 100 THEN 'expensive' ELSE 'cheap' END AS tier, COUNT(*) FROM v44_items GROUP BY 1 ORDER BY COUNT(*) DESC LIMIT 1",
		"cheap") // 4 cheap (Widget, Gadget, Cable, Bolt) vs 2 expensive (Phone, Screen)

	// EC4: ORDER BY 1 DESC with ties
	checkRowCount("EC4 ORDER BY 1 DESC works",
		"SELECT category, COUNT(*) FROM v44_items GROUP BY 1 ORDER BY 1 DESC", 2)

	// EC5: Derived table alias used in expressions
	check("EC5 derived table column expression",
		"SELECT p * 2 FROM (SELECT price AS p FROM v44_items WHERE name = 'Phone') AS t",
		1000) // 500*2

	// EC6: Multi-level nested derived tables
	check("EC6 three-level derived table",
		"SELECT result FROM (SELECT inner_val AS result FROM (SELECT price AS inner_val FROM (SELECT * FROM v44_items WHERE name = 'Widget') AS t1) AS t2) AS t3",
		10)

	// EC7: Derived table with multiple columns selected
	check("EC7 derived table multi-col",
		"SELECT name FROM (SELECT name, price FROM v44_items ORDER BY price DESC) AS ordered LIMIT 1",
		"Phone")

	// EC8: Comma-separated FROM with aggregate
	check("EC8 FROM a, b aggregate",
		"SELECT COUNT(*) FROM v44_colors, v44_sizes",
		6) // 2*3

	// EC9: GROUP BY positional out of range treated as constant
	// GROUP BY 100 when only 2 columns - should still work (groups all as one)
	// Actually in SQL this would be an error, but let's verify behavior
	// Skipped - depends on implementation choice

	// EC10: Derived table with DISTINCT
	checkRowCount("EC10 derived table DISTINCT",
		"SELECT * FROM (SELECT DISTINCT category FROM v44_items) AS cats", 2)

	t.Logf("\n=== V44 NEW FEATURES: %d/%d tests passed ===", pass, total)
	if pass < total {
		t.Errorf("Some tests failed: %d/%d", pass, total)
	}
}
