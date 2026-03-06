package test

import (
	"fmt"
	"testing"
)

func TestEdgeCases(t *testing.T) {
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

	// Setup
	afExec(t, db, ctx, "CREATE TABLE items (id INTEGER PRIMARY KEY, name TEXT, val REAL, cat TEXT)")
	afExec(t, db, ctx, "INSERT INTO items VALUES (1, 'alpha', 10, 'A')")
	afExec(t, db, ctx, "INSERT INTO items VALUES (2, 'beta', 20, 'A')")
	afExec(t, db, ctx, "INSERT INTO items VALUES (3, 'gamma', 30, 'B')")
	afExec(t, db, ctx, "INSERT INTO items VALUES (4, 'delta', NULL, 'B')")
	afExec(t, db, ctx, "INSERT INTO items VALUES (5, 'epsilon', 50, NULL)")

	// === LIMIT EDGE CASES ===
	checkRows("LIMIT exact count", "SELECT * FROM items LIMIT 5", 5)         // Bug: LIMIT == row count
	checkRows("LIMIT more than rows", "SELECT * FROM items LIMIT 100", 5)    // LIMIT > row count
	checkRows("LIMIT 1", "SELECT * FROM items LIMIT 1", 1)
	checkRows("LIMIT 0", "SELECT * FROM items LIMIT 0", 0)
	checkRows("LIMIT with ORDER", "SELECT * FROM items ORDER BY val DESC LIMIT 3", 3)
	checkRows("LIMIT exact with ORDER", "SELECT * FROM items ORDER BY id LIMIT 5", 5)

	// === OFFSET EDGE CASES ===
	checkRows("OFFSET 0", "SELECT * FROM items LIMIT 5 OFFSET 0", 5)
	checkRows("OFFSET 2", "SELECT * FROM items ORDER BY id LIMIT 5 OFFSET 2", 3)
	checkRows("OFFSET all", "SELECT * FROM items LIMIT 5 OFFSET 5", 0)
	checkRows("OFFSET past end", "SELECT * FROM items LIMIT 5 OFFSET 100", 0)

	// === NULL HANDLING ===
	// NULL in ORDER BY - NULLs should appear (either first or last, but not crash)
	rows := afQuery(t, db, ctx, "SELECT name, val FROM items ORDER BY val ASC")
	t.Logf("ORDER BY with NULL: %v", rows)
	total++
	if len(rows) == 5 {
		pass++
	} else {
		t.Errorf("[FAIL] ORDER BY NULL: expected 5 rows, got %d", len(rows))
	}

	// NULL in GROUP BY
	rows = afQuery(t, db, ctx, "SELECT cat, COUNT(*) FROM items GROUP BY cat")
	t.Logf("GROUP BY with NULL: %v", rows)
	total++
	if len(rows) >= 2 { // At least A, B groups (NULL group may or may not appear)
		pass++
	} else {
		t.Errorf("[FAIL] GROUP BY NULL: expected >= 2 groups, got %d", len(rows))
	}

	// NULL arithmetic
	check("NULL + number", "SELECT NULL + 5", "<nil>")
	check("NULL comparison", "SELECT CASE WHEN NULL = NULL THEN 'equal' ELSE 'not-equal' END", "not-equal")

	// Aggregate on empty set
	check("COUNT empty", "SELECT COUNT(*) FROM items WHERE id > 999", 0)
	check("SUM empty", "SELECT SUM(val) FROM items WHERE id > 999", "<nil>")
	check("AVG empty", "SELECT AVG(val) FROM items WHERE id > 999", "<nil>")
	check("MIN empty", "SELECT MIN(val) FROM items WHERE id > 999", "<nil>")
	check("MAX empty", "SELECT MAX(val) FROM items WHERE id > 999", "<nil>")

	// COUNT with NULL
	check("COUNT col with NULLs", "SELECT COUNT(val) FROM items", 4) // delta has NULL val
	check("COUNT * with NULLs", "SELECT COUNT(*) FROM items", 5)

	// HAVING without GROUP BY (aggregate on whole table)
	check("HAVING no GROUP BY", "SELECT COUNT(*) FROM items HAVING COUNT(*) > 3", 5)

	// CASE without ELSE
	check("CASE no ELSE match", "SELECT CASE WHEN 1=1 THEN 'yes' END", "yes")
	check("CASE no ELSE nomatch", "SELECT CASE WHEN 1=2 THEN 'yes' END", "<nil>")

	// IN with single value
	checkRows("IN single", "SELECT * FROM items WHERE id IN (3)", 1)

	// Nested functions
	check("Nested func", "SELECT UPPER(SUBSTR('hello world', 1, 5))", "HELLO")
	check("Nested math", "SELECT ABS(ROUND(-3.7))", 4)

	// Expression in ORDER BY
	rows = afQuery(t, db, ctx, "SELECT name, val FROM items WHERE val IS NOT NULL ORDER BY val * -1 LIMIT 3")
	t.Logf("ORDER BY expr: %v", rows)
	total++
	if len(rows) == 3 && fmt.Sprintf("%v", rows[0][0]) == "epsilon" {
		pass++
	} else {
		t.Errorf("[FAIL] ORDER BY expr: expected epsilon first, got %v", rows)
	}

	// Multiple subqueries in same query
	check("Multi subquery",
		"SELECT (SELECT MAX(val) FROM items) - (SELECT MIN(val) FROM items)", 40)

	// DELETE with subquery WHERE
	afExec(t, db, ctx, "CREATE TABLE temp (id INTEGER PRIMARY KEY, val INTEGER)")
	afExec(t, db, ctx, "INSERT INTO temp VALUES (1, 100)")
	afExec(t, db, ctx, "INSERT INTO temp VALUES (2, 200)")
	afExec(t, db, ctx, "INSERT INTO temp VALUES (3, 300)")
	afExec(t, db, ctx, "DELETE FROM temp WHERE val > (SELECT AVG(val) FROM temp)")
	checkRows("DELETE subquery", "SELECT * FROM temp", 2)

	// Correlated subquery in SELECT
	rows = afQuery(t, db, ctx, "SELECT name, (SELECT COUNT(*) FROM items i2 WHERE i2.cat = items.cat) as cat_count FROM items WHERE id = 1")
	t.Logf("Correlated subquery: %v", rows)
	total++
	if len(rows) > 0 && fmt.Sprintf("%v", rows[0][1]) == "2" {
		pass++ // id=1 has cat='A', and there are 2 items with cat='A'
	} else {
		t.Errorf("[FAIL] Correlated subquery: expected cat_count=2, got %v", rows)
	}

	// ALTER TABLE ADD COLUMN
	afExec(t, db, ctx, "CREATE TABLE grow (id INTEGER PRIMARY KEY, name TEXT)")
	afExec(t, db, ctx, "INSERT INTO grow VALUES (1, 'first')")
	afExec(t, db, ctx, "INSERT INTO grow VALUES (2, 'second')")
	afExec(t, db, ctx, "ALTER TABLE grow ADD COLUMN extra TEXT")
	// Existing rows should still be readable (with NULL for new column)
	rows = afQuery(t, db, ctx, "SELECT id, name, extra FROM grow")
	t.Logf("ALTER TABLE result: %v", rows)
	total++
	if len(rows) == 2 {
		pass++
	} else {
		t.Errorf("[FAIL] ALTER TABLE read: expected 2 rows, got %d", len(rows))
	}

	// Insert into altered table
	afExec(t, db, ctx, "INSERT INTO grow VALUES (3, 'third', 'bonus')")
	check("ALTER TABLE new row", "SELECT extra FROM grow WHERE id = 3", "bonus")

	t.Logf("\n=== EDGE CASES: %d/%d tests passed ===", pass, total)
	if pass < total {
		t.Errorf("Some tests failed: %d/%d", pass, total)
	}
}
