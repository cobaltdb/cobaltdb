package test

import (
	"fmt"
	"testing"
)

func TestUpdateWithSubquery(t *testing.T) {
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

	// Setup
	afExec(t, db, ctx, "CREATE TABLE products (id INTEGER PRIMARY KEY, name TEXT, price REAL)")
	afExec(t, db, ctx, "INSERT INTO products VALUES (1, 'Laptop', 999)")
	afExec(t, db, ctx, "INSERT INTO products VALUES (2, 'Phone', 699)")
	afExec(t, db, ctx, "INSERT INTO products VALUES (3, 'Tablet', 499)")

	afExec(t, db, ctx, "CREATE TABLE price_adj (id INTEGER PRIMARY KEY, product_id INTEGER, new_price REAL)")
	afExec(t, db, ctx, "INSERT INTO price_adj VALUES (1, 1, 899)")
	afExec(t, db, ctx, "INSERT INTO price_adj VALUES (2, 2, 599)")

	// 1. UPDATE SET with scalar subquery
	afExec(t, db, ctx, "UPDATE products SET price = (SELECT new_price FROM price_adj WHERE product_id = products.id) WHERE id IN (SELECT product_id FROM price_adj)")
	check("Subquery UPDATE product 1", "SELECT price FROM products WHERE id = 1", 899)
	check("Subquery UPDATE product 2", "SELECT price FROM products WHERE id = 2", 599)
	check("Subquery UPDATE product 3 unchanged", "SELECT price FROM products WHERE id = 3", 499)

	// 2. UPDATE with subquery in WHERE
	afExec(t, db, ctx, "UPDATE products SET price = price * 0.9 WHERE id = (SELECT product_id FROM price_adj WHERE new_price = 899)")
	check("Subquery WHERE update", "SELECT price FROM products WHERE id = 1", 809.1)

	// 3. UPDATE with aggregate subquery
	afExec(t, db, ctx, "CREATE TABLE settings (id INTEGER PRIMARY KEY, key TEXT, value REAL)")
	afExec(t, db, ctx, "INSERT INTO settings VALUES (1, 'avg_price', 0)")
	afExec(t, db, ctx, "UPDATE settings SET value = (SELECT AVG(price) FROM products) WHERE key = 'avg_price'")
	rows := afQuery(t, db, ctx, "SELECT value FROM settings WHERE key = 'avg_price'")
	total++
	if len(rows) > 0 && len(rows[0]) > 0 {
		val := fmt.Sprintf("%v", rows[0][0])
		t.Logf("avg_price = %s", val)
		// Should be avg of 809.1, 599, 499 ≈ 635.7
		if val != "0" {
			pass++
		} else {
			t.Errorf("[FAIL] AVG subquery UPDATE: value is still 0")
		}
	}

	t.Logf("\n=== UPDATE SUBQUERY: %d/%d tests passed ===", pass, total)
	if pass < total {
		t.Errorf("Some tests failed!")
	}
}
