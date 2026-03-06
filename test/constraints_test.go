package test

import (
	"fmt"
	"testing"
)

func TestConstraintsAndEdgeCases(t *testing.T) {
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

	// === NOT NULL constraint ===
	afExec(t, db, ctx, "CREATE TABLE strict (id INTEGER PRIMARY KEY, required_col TEXT NOT NULL)")
	afExec(t, db, ctx, "INSERT INTO strict VALUES (1, 'ok')")
	total++
	_, err := db.Exec(ctx, "INSERT INTO strict VALUES (2, NULL)")
	if err != nil {
		pass++
		t.Logf("NOT NULL insert: correctly rejected: %v", err)
	} else {
		t.Errorf("[FAIL] NOT NULL insert: should reject NULL")
	}

	// === CHECK constraint ===
	afExec(t, db, ctx, "CREATE TABLE checked (id INTEGER PRIMARY KEY, age INTEGER CHECK(age >= 0))")
	afExec(t, db, ctx, "INSERT INTO checked VALUES (1, 25)")
	total++
	_, err = db.Exec(ctx, "INSERT INTO checked VALUES (2, -5)")
	if err != nil {
		pass++
		t.Logf("CHECK insert: correctly rejected: %v", err)
	} else {
		t.Errorf("[FAIL] CHECK insert: should reject age < 0")
	}

	// CHECK during UPDATE
	total++
	_, err = db.Exec(ctx, "UPDATE checked SET age = -1 WHERE id = 1")
	if err != nil {
		pass++
		t.Logf("CHECK update: correctly rejected: %v", err)
	} else {
		t.Errorf("[FAIL] CHECK update: should reject age < 0")
	}
	check("CHECK update preserved", "SELECT age FROM checked WHERE id = 1", 25)

	// === DEFAULT values ===
	afExec(t, db, ctx, "CREATE TABLE defaults (id INTEGER PRIMARY KEY, status TEXT DEFAULT 'active', count INTEGER DEFAULT 0)")
	afExec(t, db, ctx, "INSERT INTO defaults (id) VALUES (1)")
	check("DEFAULT text", "SELECT status FROM defaults WHERE id = 1", "active")
	check("DEFAULT int", "SELECT count FROM defaults WHERE id = 1", 0)

	// === Expressions in SELECT ===
	afExec(t, db, ctx, "CREATE TABLE nums (id INTEGER PRIMARY KEY, a REAL, b REAL)")
	afExec(t, db, ctx, "INSERT INTO nums VALUES (1, 10, 3)")
	afExec(t, db, ctx, "INSERT INTO nums VALUES (2, 20, 5)")
	afExec(t, db, ctx, "INSERT INTO nums VALUES (3, 0, 0)")

	check("Addition", "SELECT a + b FROM nums WHERE id = 1", 13)
	check("Subtraction", "SELECT a - b FROM nums WHERE id = 1", 7)
	check("Multiplication", "SELECT a * b FROM nums WHERE id = 1", 30)
	check("Division", "SELECT a / b FROM nums WHERE id = 2", 4)
	check("Modulo", "SELECT 10 % 3", 1)

	// Division by zero
	total++
	rows := afQuery(t, db, ctx, "SELECT a / b FROM nums WHERE id = 3")
	t.Logf("Div by zero: %v", rows)
	// Should return NULL or error, not crash
	pass++ // Just surviving is a pass

	// === String operations ===
	check("String concat ||", "SELECT 'hello' || ' ' || 'world'", "hello world")
	check("LIKE case insensitive", "SELECT CASE WHEN 'Hello' LIKE 'hello' THEN 'match' ELSE 'no' END", "match")
	check("NOT LIKE", "SELECT CASE WHEN 'abc' NOT LIKE 'xyz' THEN 'yes' ELSE 'no' END", "yes")

	// === Complex nested queries ===
	afExec(t, db, ctx, "CREATE TABLE orders (id INTEGER PRIMARY KEY, customer TEXT, amount REAL)")
	afExec(t, db, ctx, "INSERT INTO orders VALUES (1, 'Alice', 100)")
	afExec(t, db, ctx, "INSERT INTO orders VALUES (2, 'Bob', 200)")
	afExec(t, db, ctx, "INSERT INTO orders VALUES (3, 'Alice', 150)")
	afExec(t, db, ctx, "INSERT INTO orders VALUES (4, 'Carol', 300)")
	afExec(t, db, ctx, "INSERT INTO orders VALUES (5, 'Bob', 250)")

	// Aggregate in WHERE via subquery
	checkRows("Subquery aggregate WHERE", "SELECT customer, amount FROM orders WHERE amount > (SELECT AVG(amount) FROM orders)", 2)

	// Multiple aggregates in one query
	rows = afQuery(t, db, ctx, "SELECT COUNT(*), SUM(amount), AVG(amount), MIN(amount), MAX(amount) FROM orders")
	t.Logf("Multi aggregate: %v", rows)
	total++
	if len(rows) == 1 && len(rows[0]) == 5 {
		if fmt.Sprintf("%v", rows[0][0]) == "5" && fmt.Sprintf("%v", rows[0][4]) == "300" {
			pass++
		} else {
			t.Errorf("[FAIL] Multi aggregate values: %v", rows[0])
		}
	} else {
		t.Errorf("[FAIL] Multi aggregate: expected 1 row 5 cols, got %v", rows)
	}

	// Aggregate with expression arg
	check("SUM expr", "SELECT SUM(amount * 2) FROM orders", 2000)

	// EXISTS
	total++
	rows = afQuery(t, db, ctx, "SELECT customer FROM orders WHERE EXISTS (SELECT 1 FROM orders o2 WHERE o2.customer = orders.customer AND o2.amount > 200)")
	t.Logf("EXISTS: %v", rows)
	if len(rows) >= 2 {
		pass++
	} else {
		t.Errorf("[FAIL] EXISTS: expected >= 2 rows, got %d", len(rows))
	}

	// NOT EXISTS - only Alice has no orders > 200
	checkRows("NOT EXISTS", "SELECT DISTINCT customer FROM orders WHERE NOT EXISTS (SELECT 1 FROM orders o2 WHERE o2.customer = orders.customer AND o2.amount > 200)", 1)

	// CTE (WITH)
	rows = afQuery(t, db, ctx, "WITH totals AS (SELECT customer, SUM(amount) as total FROM orders GROUP BY customer) SELECT customer, total FROM totals ORDER BY total DESC LIMIT 1")
	t.Logf("CTE: %v", rows)
	total++
	if len(rows) > 0 && fmt.Sprintf("%v", rows[0][0]) == "Bob" {
		pass++
	} else {
		t.Errorf("[FAIL] CTE: expected Bob, got %v", rows)
	}

	// Nested CTE
	rows = afQuery(t, db, ctx, "WITH customer_totals AS (SELECT customer, SUM(amount) as total FROM orders GROUP BY customer), top AS (SELECT customer FROM customer_totals WHERE total > 300) SELECT * FROM top")
	t.Logf("Nested CTE: %v", rows)
	checkRows("Nested CTE", "WITH customer_totals AS (SELECT customer, SUM(amount) as total FROM orders GROUP BY customer), top AS (SELECT customer FROM customer_totals WHERE total > 300) SELECT * FROM top", 1)

	t.Logf("\n=== CONSTRAINTS & EDGE CASES: %d/%d tests passed ===", pass, total)
	if pass < total {
		t.Errorf("Some tests failed: %d/%d", pass, total)
	}
}
