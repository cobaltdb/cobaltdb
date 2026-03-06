package test

import (
	"fmt"
	"testing"
)

func TestV17AggregateAndOrderBy(t *testing.T) {
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

	checkError := func(desc string, sql string) {
		t.Helper()
		total++
		_, err := db.Exec(ctx, sql)
		if err == nil {
			t.Errorf("[FAIL] %s: expected error but got none", desc)
			return
		}
		pass++
	}

	// ============================================================
	// === SETUP ===
	// ============================================================
	afExec(t, db, ctx, "CREATE TABLE products (id INTEGER PRIMARY KEY, name TEXT, category TEXT, price INTEGER, stock INTEGER)")
	afExec(t, db, ctx, "INSERT INTO products VALUES (1, 'Widget', 'Hardware', 25, 100)")
	afExec(t, db, ctx, "INSERT INTO products VALUES (2, 'Gadget', 'Electronics', 50, 50)")
	afExec(t, db, ctx, "INSERT INTO products VALUES (3, 'Bolt', 'Hardware', 5, 500)")
	afExec(t, db, ctx, "INSERT INTO products VALUES (4, 'Phone', 'Electronics', 800, 20)")
	afExec(t, db, ctx, "INSERT INTO products VALUES (5, 'Screw', 'Hardware', 2, 1000)")
	afExec(t, db, ctx, "INSERT INTO products VALUES (6, 'Tablet', 'Electronics', 400, 30)")
	afExec(t, db, ctx, "INSERT INTO products VALUES (7, 'TV', 'Electronics', 600, 15)")
	// Hardware: 3 items (Widget 25, Bolt 5, Screw 2), SUM=32, AVG≈10.67, MAX=25, MIN=2
	// Electronics: 4 items (Gadget 50, Phone 800, Tablet 400, TV 600), SUM=1850, AVG=462.5, MAX=800, MIN=50

	// ============================================================
	// === ORDER BY COUNT(*) WITH GROUP BY ===
	// ============================================================
	check("ORDER BY COUNT(*) ASC",
		"SELECT category, COUNT(*) FROM products GROUP BY category ORDER BY COUNT(*) ASC LIMIT 1",
		"Hardware") // Hardware has 3, Electronics has 4

	check("ORDER BY COUNT(*) DESC",
		"SELECT category, COUNT(*) FROM products GROUP BY category ORDER BY COUNT(*) DESC LIMIT 1",
		"Electronics") // Electronics has 4, Hardware has 3

	check("ORDER BY SUM(price) ASC",
		"SELECT category FROM products GROUP BY category ORDER BY SUM(price) ASC LIMIT 1",
		"Hardware") // Hardware: 25+5+2=32, Electronics: 50+800+400=1250

	check("ORDER BY SUM(price) DESC",
		"SELECT category FROM products GROUP BY category ORDER BY SUM(price) DESC LIMIT 1",
		"Electronics")

	check("ORDER BY AVG(price)",
		"SELECT category FROM products GROUP BY category ORDER BY AVG(price) DESC LIMIT 1",
		"Electronics")

	check("ORDER BY MAX(price)",
		"SELECT category FROM products GROUP BY category ORDER BY MAX(price) DESC LIMIT 1",
		"Electronics")

	check("ORDER BY MIN(price)",
		"SELECT category FROM products GROUP BY category ORDER BY MIN(price) ASC LIMIT 1",
		"Hardware") // min price 2

	// ============================================================
	// === HAVING WITH NOT ===
	// ============================================================
	checkRowCount("HAVING NOT simple",
		"SELECT category FROM products GROUP BY category HAVING NOT COUNT(*) > 3",
		1) // Hardware: NOT(3>3)=true, Electronics: NOT(4>3)=false → 1 row

	checkRowCount("HAVING NOT with aggregate comparison",
		"SELECT category FROM products GROUP BY category HAVING NOT SUM(price) > 100",
		1) // Hardware sum=32, NOT > 100 → true

	// ============================================================
	// === HAVING WITH BETWEEN ===
	// ============================================================
	checkRowCount("HAVING COUNT BETWEEN",
		"SELECT category FROM products GROUP BY category HAVING COUNT(*) BETWEEN 2 AND 4",
		2) // Hardware 3, Electronics 4 - both in [2,4]

	checkRowCount("HAVING SUM BETWEEN",
		"SELECT category FROM products GROUP BY category HAVING SUM(price) BETWEEN 10 AND 100",
		1) // Only Hardware (32)

	// ============================================================
	// === HAVING WITH IN ===
	// ============================================================
	checkRowCount("HAVING COUNT IN list",
		"SELECT category FROM products GROUP BY category HAVING COUNT(*) IN (3, 5, 7)",
		1) // Only Hardware has count 3

	checkRowCount("HAVING COUNT NOT IN list",
		"SELECT category FROM products GROUP BY category HAVING COUNT(*) NOT IN (1, 2)",
		2) // Hardware=3, Electronics=4 - neither in (1,2)

	// ============================================================
	// === MULTIPLE AGGREGATES + ORDER BY ===
	// ============================================================
	check("Multiple aggs with ORDER BY",
		"SELECT category, COUNT(*), SUM(price), AVG(price) FROM products GROUP BY category ORDER BY SUM(price) DESC LIMIT 1",
		"Electronics")

	// ============================================================
	// === WHERE ERROR PROPAGATION ===
	// ============================================================
	// UPDATE with bad WHERE should error, not silently skip
	checkError("UPDATE with WHERE error",
		"UPDATE products SET price = 0 WHERE nonexistent_column = 1")

	// DELETE with bad WHERE should error, not silently skip
	checkError("DELETE with WHERE error",
		"DELETE FROM products WHERE nonexistent_column = 1")

	// Verify data not changed by errored operations
	check("Data unchanged after error", "SELECT price FROM products WHERE id = 1", 25)

	// ============================================================
	// === GROUP BY + ORDER BY + LIMIT COMBINATIONS ===
	// ============================================================
	check("GROUP BY ORDER BY LIMIT 1",
		"SELECT category FROM products GROUP BY category ORDER BY category LIMIT 1",
		"Electronics")

	check("GROUP BY ORDER BY LIMIT 1 OFFSET 1",
		"SELECT category FROM products GROUP BY category ORDER BY category LIMIT 1 OFFSET 1",
		"Hardware")

	// ============================================================
	// === HAVING WITH CASE ===
	// ============================================================
	check("HAVING with CASE",
		"SELECT category FROM products GROUP BY category HAVING CASE WHEN COUNT(*) >= 3 THEN 1 ELSE 0 END = 1 ORDER BY category LIMIT 1",
		"Electronics")

	// ============================================================
	// === COUNT(DISTINCT) ===
	// ============================================================
	check("COUNT DISTINCT",
		"SELECT COUNT(DISTINCT category) FROM products", 2)

	check("COUNT DISTINCT with GROUP BY",
		"SELECT category, COUNT(DISTINCT price) FROM products GROUP BY category HAVING COUNT(DISTINCT price) = 3 LIMIT 1",
		"Hardware") // Hardware: 25, 5, 2 = 3 distinct; Electronics has 4 distinct

	// ============================================================
	// === AGGREGATE ON EMPTY RESULT ===
	// ============================================================
	afExec(t, db, ctx, "CREATE TABLE empty_t (id INTEGER PRIMARY KEY, val INTEGER)")

	check("COUNT on empty", "SELECT COUNT(*) FROM empty_t", 0)
	checkRowCount("GROUP BY on empty", "SELECT val, COUNT(*) FROM empty_t GROUP BY val", 0)

	// ============================================================
	// === SUBQUERY IN WHERE ===
	// ============================================================
	check("EXISTS subquery",
		"SELECT name FROM products WHERE EXISTS (SELECT 1 FROM products p2 WHERE p2.category = 'Electronics' AND p2.price > 700) ORDER BY id LIMIT 1",
		"Widget")

	checkRowCount("IN subquery",
		"SELECT * FROM products WHERE category IN (SELECT DISTINCT category FROM products WHERE price > 100)",
		4) // All 4 Electronics products

	// ============================================================
	// === UPDATE WITH SUBQUERY IN SET ===
	// ============================================================
	afExec(t, db, ctx, "CREATE TABLE prices (id INTEGER PRIMARY KEY, base_price INTEGER, adjusted_price INTEGER)")
	afExec(t, db, ctx, "INSERT INTO prices VALUES (1, 100, 0)")
	afExec(t, db, ctx, "INSERT INTO prices VALUES (2, 200, 0)")

	afExec(t, db, ctx, "UPDATE prices SET adjusted_price = (SELECT MAX(price) FROM products) WHERE id = 1")
	check("UPDATE with subquery SET", "SELECT adjusted_price FROM prices WHERE id = 1", 800)

	t.Logf("\n=== V17 AGGREGATE & ORDER BY: %d/%d tests passed ===", pass, total)
	if pass < total {
		t.Errorf("Some tests failed: %d/%d", pass, total)
	}
}
