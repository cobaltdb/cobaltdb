package test

import (
	"fmt"
	"testing"
)

func TestV13EdgeCases(t *testing.T) {
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

	checkNull := func(desc string, sql string) {
		t.Helper()
		total++
		rows := afQuery(t, db, ctx, sql)
		if len(rows) == 0 || len(rows[0]) == 0 {
			t.Errorf("[FAIL] %s: no rows returned", desc)
			return
		}
		if rows[0][0] != nil {
			t.Errorf("[FAIL] %s: expected NULL, got %v", desc, rows[0][0])
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

	checkNoError := func(desc string, sql string) {
		t.Helper()
		total++
		_, err := db.Exec(ctx, sql)
		if err != nil {
			t.Errorf("[FAIL] %s: %v", desc, err)
			return
		}
		pass++
	}

	_ = checkNoError

	// Setup
	afExec(t, db, ctx, "CREATE TABLE edge_data (id INTEGER PRIMARY KEY, name TEXT, val INTEGER)")
	afExec(t, db, ctx, "INSERT INTO edge_data VALUES (1, 'Alice', 100)")
	afExec(t, db, ctx, "INSERT INTO edge_data VALUES (2, 'Bob', 200)")
	afExec(t, db, ctx, "INSERT INTO edge_data VALUES (3, 'Charlie', 300)")
	afExec(t, db, ctx, "INSERT INTO edge_data VALUES (4, 'Dave', NULL)")

	// ============================================================
	// === LIMIT EDGE CASES ===
	// ============================================================

	// LIMIT larger than result set should return all rows
	checkRowCount("LIMIT > rows", "SELECT * FROM edge_data LIMIT 100", 4)

	// LIMIT equal to row count should return all rows
	checkRowCount("LIMIT == rows", "SELECT * FROM edge_data LIMIT 4", 4)

	// LIMIT 0 should return empty
	checkRowCount("LIMIT 0", "SELECT * FROM edge_data LIMIT 0", 0)

	// LIMIT 1 returns one row
	checkRowCount("LIMIT 1", "SELECT * FROM edge_data LIMIT 1", 1)

	// LIMIT 2 returns two rows
	checkRowCount("LIMIT 2", "SELECT * FROM edge_data LIMIT 2", 2)

	// LIMIT with OFFSET
	checkRowCount("LIMIT 2 OFFSET 1", "SELECT * FROM edge_data LIMIT 2 OFFSET 1", 2)

	// OFFSET larger than result set
	checkRowCount("OFFSET > rows", "SELECT * FROM edge_data LIMIT 10 OFFSET 100", 0)

	// ============================================================
	// === SQL COMMENTS ===
	// ============================================================

	// Line comment
	check("Line comment at end", "SELECT val FROM edge_data WHERE id = 1 -- this is a comment", 100)

	// Block comment
	check("Block comment inline", "SELECT /* the value */ val FROM edge_data WHERE id = 1", 100)

	// Block comment between clauses
	check("Block comment between clauses", "SELECT val /* comment */ FROM edge_data WHERE id = 2", 200)

	// ============================================================
	// === NULL PROPAGATION IN EXPRESSIONS ===
	// ============================================================

	// NULL IN (list) should return NULL
	checkNull("NULL IN list", "SELECT CASE WHEN NULL IN (1, 2, 3) THEN 'yes' WHEN NOT (NULL IN (1, 2, 3)) THEN 'no' ELSE NULL END FROM edge_data WHERE id = 1")

	// val IN (list with NULL) where val not found - should return NULL
	// When id=4, val is NULL → NULL IN (...) → NULL
	checkNull("NULL col IN list", "SELECT CASE WHEN val IN (999, 888) THEN 'found' WHEN NOT (val IN (999, 888)) THEN 'not found' ELSE NULL END FROM edge_data WHERE id = 4")

	// BETWEEN with NULL should return NULL
	checkNull("BETWEEN with NULL bound", "SELECT CASE WHEN 5 BETWEEN 1 AND NULL THEN 'yes' WHEN NOT (5 BETWEEN 1 AND NULL) THEN 'no' ELSE NULL END FROM edge_data WHERE id = 1")

	// LIKE with NULL pattern should return NULL
	checkNull("LIKE NULL pattern", "SELECT CASE WHEN 'test' LIKE NULL THEN 'yes' WHEN NOT ('test' LIKE NULL) THEN 'no' ELSE NULL END FROM edge_data WHERE id = 1")

	// ============================================================
	// === DOUBLE-QUOTED IDENTIFIERS ===
	// ============================================================

	// Create table with column that needs quoting
	afExec(t, db, ctx, "CREATE TABLE quoted_test (id INTEGER PRIMARY KEY, \"order\" INTEGER, \"select\" TEXT)")
	afExec(t, db, ctx, "INSERT INTO quoted_test VALUES (1, 42, 'hello')")

	// Query with double-quoted identifiers
	check("Double-quoted column", "SELECT \"order\" FROM quoted_test WHERE id = 1", 42)
	check("Double-quoted text column", "SELECT \"select\" FROM quoted_test WHERE id = 1", "hello")

	// ============================================================
	// === AGGREGATE WITH NULL ===
	// ============================================================
	check("COUNT(*) includes NULL rows", "SELECT COUNT(*) FROM edge_data", 4)
	check("COUNT(val) excludes NULL", "SELECT COUNT(val) FROM edge_data", 3)
	check("SUM ignores NULL", "SELECT SUM(val) FROM edge_data", 600)
	check("AVG ignores NULL", "SELECT AVG(val) FROM edge_data", 200)
	check("MIN ignores NULL", "SELECT MIN(val) FROM edge_data", 100)
	check("MAX ignores NULL", "SELECT MAX(val) FROM edge_data", 300)

	// ============================================================
	// === COALESCE IN WHERE ===
	// ============================================================
	checkRowCount("COALESCE in WHERE", "SELECT * FROM edge_data WHERE COALESCE(val, 0) > 0", 3)
	checkRowCount("COALESCE catches NULL", "SELECT * FROM edge_data WHERE COALESCE(val, 0) = 0", 1)

	// ============================================================
	// === EXPRESSION IN ORDER BY ===
	// ============================================================
	check("ORDER BY expression ASC",
		"SELECT name FROM edge_data WHERE val IS NOT NULL ORDER BY val ASC LIMIT 1", "Alice")
	check("ORDER BY expression DESC",
		"SELECT name FROM edge_data WHERE val IS NOT NULL ORDER BY val DESC LIMIT 1", "Charlie")

	// ============================================================
	// === MULTIPLE COLUMN ORDER BY ===
	// ============================================================
	afExec(t, db, ctx, "CREATE TABLE order_test (id INTEGER PRIMARY KEY, group_id INTEGER, val INTEGER)")
	afExec(t, db, ctx, "INSERT INTO order_test VALUES (1, 1, 30)")
	afExec(t, db, ctx, "INSERT INTO order_test VALUES (2, 1, 10)")
	afExec(t, db, ctx, "INSERT INTO order_test VALUES (3, 2, 20)")
	afExec(t, db, ctx, "INSERT INTO order_test VALUES (4, 2, 10)")

	check("Multi-col ORDER BY",
		"SELECT id FROM order_test ORDER BY group_id ASC, val ASC LIMIT 1", 2)

	// ============================================================
	// === STRING CONCATENATION ===
	// ============================================================
	check("String concat ||", "SELECT 'hello' || ' ' || 'world' FROM edge_data WHERE id = 1", "hello world")
	check("Concat with column", "SELECT name || '!' FROM edge_data WHERE id = 1", "Alice!")

	// ============================================================
	// === IS NULL / IS NOT NULL ===
	// ============================================================
	checkRowCount("IS NULL", "SELECT * FROM edge_data WHERE val IS NULL", 1)
	checkRowCount("IS NOT NULL", "SELECT * FROM edge_data WHERE val IS NOT NULL", 3)

	// ============================================================
	// === DISTINCT ===
	// ============================================================
	afExec(t, db, ctx, "CREATE TABLE distinct_test (id INTEGER PRIMARY KEY, color TEXT)")
	afExec(t, db, ctx, "INSERT INTO distinct_test VALUES (1, 'red')")
	afExec(t, db, ctx, "INSERT INTO distinct_test VALUES (2, 'blue')")
	afExec(t, db, ctx, "INSERT INTO distinct_test VALUES (3, 'red')")
	afExec(t, db, ctx, "INSERT INTO distinct_test VALUES (4, NULL)")

	checkRowCount("DISTINCT values", "SELECT DISTINCT color FROM distinct_test", 3) // red, blue, NULL

	// ============================================================
	// === SUBQUERY IN WHERE ===
	// ============================================================
	check("Scalar subquery", "SELECT name FROM edge_data WHERE val = (SELECT MAX(val) FROM edge_data)", "Charlie")
	checkRowCount("IN subquery", "SELECT * FROM edge_data WHERE val IN (SELECT val FROM edge_data WHERE val > 150)", 2)

	t.Logf("\n=== V13 EDGE CASES: %d/%d tests passed ===", pass, total)
	if pass < total {
		t.Errorf("Some tests failed: %d/%d", pass, total)
	}
}
