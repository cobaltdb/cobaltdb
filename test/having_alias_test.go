package test

import (
	"fmt"
	"testing"
)

func TestHavingAlias(t *testing.T) {
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
	afExec(t, db, ctx, "CREATE TABLE orders (id INTEGER PRIMARY KEY, customer TEXT, amount REAL)")
	afExec(t, db, ctx, "INSERT INTO orders VALUES (1, 'Alice', 100)")
	afExec(t, db, ctx, "INSERT INTO orders VALUES (2, 'Alice', 200)")
	afExec(t, db, ctx, "INSERT INTO orders VALUES (3, 'Alice', 150)")
	afExec(t, db, ctx, "INSERT INTO orders VALUES (4, 'Bob', 50)")
	afExec(t, db, ctx, "INSERT INTO orders VALUES (5, 'Carol', 300)")
	afExec(t, db, ctx, "INSERT INTO orders VALUES (6, 'Carol', 250)")

	// HAVING with direct aggregate
	checkRows("HAVING COUNT(*) > 2",
		"SELECT customer, COUNT(*) FROM orders GROUP BY customer HAVING COUNT(*) > 2",
		1) // Alice has 3

	// HAVING with aggregate alias
	checkRows("HAVING alias cnt",
		"SELECT customer, COUNT(*) as cnt FROM orders GROUP BY customer HAVING cnt > 1",
		2) // Alice(3) and Carol(2)

	// HAVING with SUM alias
	check("HAVING SUM alias",
		"SELECT customer, SUM(amount) as total FROM orders GROUP BY customer HAVING total > 400",
		"Alice") // Alice has 450

	// HAVING with multiple conditions
	checkRows("HAVING alias AND",
		"SELECT customer, COUNT(*) as cnt, SUM(amount) as total FROM orders GROUP BY customer HAVING cnt >= 2 AND total > 400",
		2) // Alice (cnt=3, total=450) and Carol (cnt=2, total=550)

	// DROP TABLE test
	afExec(t, db, ctx, "CREATE TABLE drop_test (id INTEGER PRIMARY KEY, val TEXT)")
	afExec(t, db, ctx, "CREATE INDEX idx_drop ON drop_test (val)")
	afExec(t, db, ctx, "INSERT INTO drop_test VALUES (1, 'hello')")
	afExec(t, db, ctx, "DROP TABLE drop_test")
	total++
	_, err := db.Exec(ctx, "SELECT * FROM drop_test")
	if err != nil {
		pass++
	} else {
		t.Errorf("[FAIL] DROP TABLE: should fail after drop")
	}

	// Recreate same table name after drop
	afExec(t, db, ctx, "CREATE TABLE drop_test (id INTEGER PRIMARY KEY, name TEXT)")
	afExec(t, db, ctx, "INSERT INTO drop_test VALUES (1, 'new')")
	check("Recreated table", "SELECT name FROM drop_test WHERE id = 1", "new")

	// DROP TABLE IF EXISTS
	afExec(t, db, ctx, "DROP TABLE IF EXISTS nonexistent")
	total++
	pass++ // Didn't crash

	t.Logf("\n=== HAVING ALIAS & DROP TABLE: %d/%d tests passed ===", pass, total)
	if pass < total {
		t.Errorf("Some tests failed: %d/%d", pass, total)
	}
}
