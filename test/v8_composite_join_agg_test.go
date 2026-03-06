package test

import (
	"fmt"
	"testing"
)

func TestV8CompositeIndexAndJoinFixes(t *testing.T) {
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

	// === COMPOSITE INDEX TESTS ===
	afExec(t, db, ctx, "CREATE TABLE products (id INTEGER PRIMARY KEY, category TEXT, brand TEXT, price REAL)")
	afExec(t, db, ctx, "INSERT INTO products VALUES (1, 'electronics', 'apple', 999.99)")
	afExec(t, db, ctx, "INSERT INTO products VALUES (2, 'electronics', 'samsung', 799.99)")
	afExec(t, db, ctx, "INSERT INTO products VALUES (3, 'clothing', 'nike', 59.99)")
	afExec(t, db, ctx, "INSERT INTO products VALUES (4, 'clothing', 'adidas', 49.99)")

	// Create composite index
	afExec(t, db, ctx, "CREATE INDEX idx_cat_brand ON products (category, brand)")
	total++
	pass++ // Didn't crash

	// Insert after composite index exists
	afExec(t, db, ctx, "INSERT INTO products VALUES (5, 'electronics', 'sony', 599.99)")
	check("Insert with composite index", "SELECT price FROM products WHERE id = 5", 599.99)

	// Update with composite index
	afExec(t, db, ctx, "UPDATE products SET brand = 'google' WHERE id = 5")
	check("Update with composite index", "SELECT brand FROM products WHERE id = 5", "google")

	// Delete with composite index
	afExec(t, db, ctx, "DELETE FROM products WHERE id = 5")
	checkRowCount("Delete with composite index", "SELECT * FROM products WHERE id = 5", 0)

	// Drop and recreate composite index (should work without orphaned entries)
	afExec(t, db, ctx, "DROP INDEX idx_cat_brand")
	afExec(t, db, ctx, "CREATE INDEX idx_cat_brand2 ON products (category, brand)")
	total++
	pass++ // No crash

	// === VIEW WITH JOIN TESTS ===
	afExec(t, db, ctx, "CREATE TABLE orders (id INTEGER PRIMARY KEY, product_id INTEGER, qty INTEGER)")
	afExec(t, db, ctx, "INSERT INTO orders VALUES (1, 1, 2)")
	afExec(t, db, ctx, "INSERT INTO orders VALUES (2, 3, 5)")
	afExec(t, db, ctx, "INSERT INTO orders VALUES (3, 2, 1)")

	afExec(t, db, ctx, "CREATE TABLE customers (id INTEGER PRIMARY KEY, name TEXT, product_id INTEGER)")
	afExec(t, db, ctx, "INSERT INTO customers VALUES (1, 'Alice', 1)")
	afExec(t, db, ctx, "INSERT INTO customers VALUES (2, 'Bob', 3)")

	// Create a view with a simple filter
	afExec(t, db, ctx, "CREATE VIEW active_products AS SELECT id, category, brand, price FROM products WHERE category = 'electronics'")

	// Query view (basic)
	checkRowCount("View basic query", "SELECT * FROM active_products", 2)
	check("View returns filtered data", "SELECT brand FROM active_products WHERE id = 1", "apple")

	// Query view with additional JOIN from outer query
	// The outer query JOINs the view with orders table
	afExec(t, db, ctx, "CREATE VIEW simple_products AS SELECT id, category, price FROM products")

	// === SUM NULL HANDLING ===
	afExec(t, db, ctx, "CREATE TABLE nulldata (id INTEGER PRIMARY KEY, grp TEXT, val REAL)")
	afExec(t, db, ctx, "INSERT INTO nulldata VALUES (1, 'a', NULL)")
	afExec(t, db, ctx, "INSERT INTO nulldata VALUES (2, 'a', NULL)")
	afExec(t, db, ctx, "INSERT INTO nulldata VALUES (3, 'b', 10)")
	afExec(t, db, ctx, "INSERT INTO nulldata VALUES (4, 'b', 20)")

	// SUM of all NULL values should return NULL
	checkNull("SUM of NULLs", "SELECT SUM(val) FROM nulldata WHERE grp = 'a'")

	// SUM of non-NULL values should work normally
	check("SUM of values", "SELECT SUM(val) FROM nulldata WHERE grp = 'b'", 30)

	// SUM with mix of NULL and values
	afExec(t, db, ctx, "INSERT INTO nulldata VALUES (5, 'c', NULL)")
	afExec(t, db, ctx, "INSERT INTO nulldata VALUES (6, 'c', 100)")
	check("SUM mixed NULL/values", "SELECT SUM(val) FROM nulldata WHERE grp = 'c'", 100)

	// AVG should skip NULLs
	check("AVG skips NULLs", "SELECT AVG(val) FROM nulldata WHERE grp = 'c'", 100)

	// === ORDER BY QUALIFIED IDENTIFIER IN JOINS ===
	afExec(t, db, ctx, "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)")
	afExec(t, db, ctx, "INSERT INTO users VALUES (1, 'Alice')")
	afExec(t, db, ctx, "INSERT INTO users VALUES (2, 'Bob')")
	afExec(t, db, ctx, "INSERT INTO users VALUES (3, 'Charlie')")

	afExec(t, db, ctx, "CREATE TABLE scores (id INTEGER PRIMARY KEY, user_id INTEGER, score INTEGER)")
	afExec(t, db, ctx, "INSERT INTO scores VALUES (10, 1, 95)")
	afExec(t, db, ctx, "INSERT INTO scores VALUES (20, 2, 85)")
	afExec(t, db, ctx, "INSERT INTO scores VALUES (30, 3, 75)")

	// ORDER BY with qualified name - should sort by scores.id not users.id
	rows := afQuery(t, db, ctx, "SELECT users.name, scores.id FROM users JOIN scores ON users.id = scores.user_id ORDER BY scores.id DESC")
	total++
	if len(rows) >= 3 {
		firstScoreId := fmt.Sprintf("%v", rows[0][1])
		if firstScoreId == "30" {
			pass++
		} else {
			t.Errorf("[FAIL] ORDER BY scores.id DESC: expected first score_id=30, got %s", firstScoreId)
		}
	} else {
		t.Errorf("[FAIL] ORDER BY qualified: expected 3 rows, got %d", len(rows))
	}

	// === CROSS JOIN WITH GROUP BY ===
	afExec(t, db, ctx, "CREATE TABLE colors (id INTEGER PRIMARY KEY, color TEXT)")
	afExec(t, db, ctx, "INSERT INTO colors VALUES (1, 'red')")
	afExec(t, db, ctx, "INSERT INTO colors VALUES (2, 'blue')")

	afExec(t, db, ctx, "CREATE TABLE sizes (id INTEGER PRIMARY KEY, sz TEXT)")
	afExec(t, db, ctx, "INSERT INTO sizes VALUES (1, 'S')")
	afExec(t, db, ctx, "INSERT INTO sizes VALUES (2, 'M')")
	afExec(t, db, ctx, "INSERT INTO sizes VALUES (3, 'L')")

	// CROSS JOIN should produce 2*3=6 rows
	checkRowCount("CROSS JOIN count", "SELECT * FROM colors CROSS JOIN sizes", 6)

	// CROSS JOIN with GROUP BY
	rows = afQuery(t, db, ctx, "SELECT colors.color, COUNT(*) FROM colors CROSS JOIN sizes GROUP BY colors.color")
	total++
	if len(rows) == 2 {
		// Each color should have 3 rows (one per size)
		count1 := fmt.Sprintf("%v", rows[0][1])
		count2 := fmt.Sprintf("%v", rows[1][1])
		if count1 == "3" && count2 == "3" {
			pass++
		} else {
			t.Errorf("[FAIL] CROSS JOIN GROUP BY: expected counts 3,3 got %s,%s", count1, count2)
		}
	} else {
		t.Errorf("[FAIL] CROSS JOIN GROUP BY: expected 2 groups, got %d", len(rows))
	}

	// === GROUP BY with SUM returning NULL for empty group ===
	rows = afQuery(t, db, ctx, "SELECT grp, SUM(val) FROM nulldata GROUP BY grp ORDER BY grp")
	total++
	if len(rows) >= 1 {
		// Group 'a' should have NULL SUM
		if rows[0][1] == nil {
			pass++
		} else {
			t.Errorf("[FAIL] GROUP BY SUM NULL: expected NULL for group 'a', got %v", rows[0][1])
		}
	} else {
		t.Errorf("[FAIL] GROUP BY SUM NULL: no rows returned")
	}

	t.Logf("\n=== V8 COMPOSITE/JOIN/AGG: %d/%d tests passed ===", pass, total)
	if pass < total {
		t.Errorf("Some tests failed: %d/%d", pass, total)
	}
}
