package test

import (
	"fmt"
	"testing"
)

func TestComprehensiveV2(t *testing.T) {
	db, ctx := af(t)
	pass := 0

	check := func(desc string, sql string, expected interface{}) {
		t.Helper()
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
		rows := afQuery(t, db, ctx, sql)
		if len(rows) != expectedCount {
			t.Errorf("[FAIL] %s: got %d rows, expected %d", desc, len(rows), expectedCount)
			return
		}
		pass++
	}

	// Setup
	afExec(t, db, ctx, "CREATE TABLE products (id INTEGER PRIMARY KEY, name TEXT, category TEXT, price REAL, stock INTEGER)")
	afExec(t, db, ctx, "INSERT INTO products VALUES (1, 'Laptop', 'Electronics', 999.99, 50)")
	afExec(t, db, ctx, "INSERT INTO products VALUES (2, 'Phone', 'Electronics', 699.99, 100)")
	afExec(t, db, ctx, "INSERT INTO products VALUES (3, 'Tablet', 'Electronics', 499.99, 75)")
	afExec(t, db, ctx, "INSERT INTO products VALUES (4, 'Chair', 'Furniture', 199.99, 200)")
	afExec(t, db, ctx, "INSERT INTO products VALUES (5, 'Desk', 'Furniture', 349.99, 150)")
	afExec(t, db, ctx, "INSERT INTO products VALUES (6, 'Pen', 'Office', 4.99, 1000)")
	afExec(t, db, ctx, "INSERT INTO products VALUES (7, 'Notebook', 'Office', 9.99, 500)")

	afExec(t, db, ctx, "CREATE TABLE orders (id INTEGER PRIMARY KEY, product_id INTEGER, quantity INTEGER, total REAL)")
	afExec(t, db, ctx, "INSERT INTO orders VALUES (1, 1, 2, 1999.98)")
	afExec(t, db, ctx, "INSERT INTO orders VALUES (2, 2, 1, 699.99)")
	afExec(t, db, ctx, "INSERT INTO orders VALUES (3, 1, 1, 999.99)")
	afExec(t, db, ctx, "INSERT INTO orders VALUES (4, 4, 5, 999.95)")
	afExec(t, db, ctx, "INSERT INTO orders VALUES (5, 6, 100, 499.00)")

	// 1. INTERSECT
	checkRows("INTERSECT", "SELECT category FROM products WHERE price > 100 INTERSECT SELECT category FROM products WHERE stock > 100", 1)
	check("INTERSECT result", "SELECT category FROM products WHERE price > 100 INTERSECT SELECT category FROM products WHERE stock > 100", "Furniture")

	// 2. EXCEPT
	checkRows("EXCEPT", "SELECT category FROM products GROUP BY category EXCEPT SELECT category FROM products WHERE price < 10 GROUP BY category", 2)

	// 3. Window: ROW_NUMBER
	rows := afQuery(t, db, ctx, "SELECT name, price, ROW_NUMBER() OVER (ORDER BY price DESC) as rn FROM products")
	if len(rows) != 7 {
		t.Errorf("Window ROW_NUMBER: expected 7 rows, got %d", len(rows))
	} else {
		// Laptop should be rn=1
		for _, row := range rows {
			if fmt.Sprintf("%v", row[0]) == "Laptop" {
				if fmt.Sprintf("%v", row[2]) != "1" {
					t.Errorf("Window ROW_NUMBER: Laptop should be 1, got %v", row[2])
				} else {
					pass++
				}
				break
			}
		}
	}

	// 4. Window: RANK with PARTITION BY
	rows = afQuery(t, db, ctx, "SELECT name, category, price, RANK() OVER (PARTITION BY category ORDER BY price DESC) as rnk FROM products")
	t.Logf("RANK PARTITION: %v", rows)
	if len(rows) != 7 {
		t.Errorf("Window RANK PARTITION: expected 7 rows, got %d", len(rows))
	} else {
		for _, row := range rows {
			if fmt.Sprintf("%v", row[0]) == "Laptop" {
				if fmt.Sprintf("%v", row[3]) != "1" {
					t.Errorf("Window RANK PARTITION: Laptop should be 1 in Electronics, got %v", row[3])
				} else {
					pass++
				}
				break
			}
		}
	}

	// 5. Window: DENSE_RANK
	afExec(t, db, ctx, "CREATE TABLE scores (id INTEGER PRIMARY KEY, student TEXT, score INTEGER)")
	afExec(t, db, ctx, "INSERT INTO scores VALUES (1, 'A', 95)")
	afExec(t, db, ctx, "INSERT INTO scores VALUES (2, 'B', 90)")
	afExec(t, db, ctx, "INSERT INTO scores VALUES (3, 'C', 90)")
	afExec(t, db, ctx, "INSERT INTO scores VALUES (4, 'D', 85)")

	rows = afQuery(t, db, ctx, "SELECT student, score, DENSE_RANK() OVER (ORDER BY score DESC) as drnk FROM scores")
	t.Logf("DENSE_RANK: %v", rows)
	// D should have dense_rank=3
	for _, row := range rows {
		if fmt.Sprintf("%v", row[0]) == "D" {
			if fmt.Sprintf("%v", row[2]) != "3" {
				t.Errorf("DENSE_RANK: D should be 3, got %v", row[2])
			} else {
				pass++
			}
			break
		}
	}

	// 6. Window: LAG/LEAD
	rows = afQuery(t, db, ctx, "SELECT student, score, LAG(score) OVER (ORDER BY score DESC) as prev FROM scores")
	t.Logf("LAG: %v", rows)
	for _, row := range rows {
		if fmt.Sprintf("%v", row[0]) == "A" {
			if fmt.Sprintf("%v", row[2]) != "<nil>" {
				t.Errorf("LAG: A should have nil prev, got %v", row[2])
			} else {
				pass++
			}
			break
		}
	}

	// 7. FULL OUTER JOIN
	afExec(t, db, ctx, "CREATE TABLE t1 (id INTEGER PRIMARY KEY, val TEXT)")
	afExec(t, db, ctx, "INSERT INTO t1 VALUES (1, 'a')")
	afExec(t, db, ctx, "INSERT INTO t1 VALUES (2, 'b')")

	afExec(t, db, ctx, "CREATE TABLE t2 (id INTEGER PRIMARY KEY, val TEXT)")
	afExec(t, db, ctx, "INSERT INTO t2 VALUES (2, 'x')")
	afExec(t, db, ctx, "INSERT INTO t2 VALUES (3, 'y')")

	checkRows("FULL OUTER JOIN", "SELECT t1.id, t2.id FROM t1 FULL OUTER JOIN t2 ON t1.id = t2.id", 3)

	// 8. CROSS JOIN
	afExec(t, db, ctx, "CREATE TABLE a (id INTEGER PRIMARY KEY, v TEXT)")
	afExec(t, db, ctx, "INSERT INTO a VALUES (1, 'x')")
	afExec(t, db, ctx, "INSERT INTO a VALUES (2, 'y')")
	afExec(t, db, ctx, "CREATE TABLE b (id INTEGER PRIMARY KEY, v TEXT)")
	afExec(t, db, ctx, "INSERT INTO b VALUES (1, 'p')")
	afExec(t, db, ctx, "INSERT INTO b VALUES (2, 'q')")
	afExec(t, db, ctx, "INSERT INTO b VALUES (3, 'r')")

	checkRows("CROSS JOIN", "SELECT a.v, b.v FROM a CROSS JOIN b", 6)

	// 9. HAVING with multiple conditions
	check("HAVING multi", "SELECT category FROM products GROUP BY category HAVING COUNT(*) >= 2 AND AVG(price) > 100", "Electronics")

	// 10. Complex JOIN + aggregates
	check("JOIN SUM", "SELECT SUM(o.total) FROM orders o INNER JOIN products p ON o.product_id = p.id WHERE p.category = 'Electronics'", 3699.96)

	// 11. Subquery in SELECT
	rows = afQuery(t, db, ctx, "SELECT name, (SELECT COUNT(*) FROM orders WHERE orders.product_id = products.id) as order_count FROM products WHERE id = 1")
	t.Logf("Subquery in SELECT: %v", rows)
	if len(rows) > 0 && len(rows[0]) >= 2 {
		if fmt.Sprintf("%v", rows[0][1]) == "2" {
			pass++
		} else {
			t.Errorf("Subquery in SELECT: expected 2 orders for Laptop, got %v", rows[0][1])
		}
	}

	// 12. UNION + ORDER BY + LIMIT
	rows = afQuery(t, db, ctx, "SELECT name FROM products WHERE category = 'Electronics' UNION SELECT name FROM products WHERE category = 'Office' ORDER BY name LIMIT 3")
	t.Logf("UNION ORDER LIMIT: %v", rows)
	checkRows("UNION ORDER LIMIT", "SELECT name FROM products WHERE category = 'Electronics' UNION SELECT name FROM products WHERE category = 'Office' ORDER BY name LIMIT 3", 3)

	// 13. INTERSECT ALL
	checkRows("INTERSECT ALL", "SELECT category FROM products INTERSECT ALL SELECT category FROM products WHERE price > 100", 5)

	// 14. EXCEPT ALL
	rows = afQuery(t, db, ctx, "SELECT category FROM products EXCEPT ALL SELECT category FROM products WHERE price > 100")
	t.Logf("EXCEPT ALL: %v", rows)
	checkRows("EXCEPT ALL", "SELECT category FROM products EXCEPT ALL SELECT category FROM products WHERE price > 100", 2)

	// 15. Multi-level subquery
	check("Multi-level subquery",
		"SELECT name FROM products WHERE id = (SELECT product_id FROM orders WHERE total = (SELECT MAX(total) FROM orders))",
		"Laptop")

	// 16. GROUP BY with expression
	checkRows("GROUP BY expr",
		"SELECT CASE WHEN price > 100 THEN 'expensive' ELSE 'cheap' END as tier, COUNT(*) FROM products GROUP BY CASE WHEN price > 100 THEN 'expensive' ELSE 'cheap' END",
		2)

	// 17. ORDER BY with expression
	rows = afQuery(t, db, ctx, "SELECT name, price * stock as value FROM products ORDER BY price * stock DESC LIMIT 3")
	t.Logf("ORDER BY expr: %v", rows)
	if len(rows) >= 1 {
		if fmt.Sprintf("%v", rows[0][0]) == "Desk" || fmt.Sprintf("%v", rows[0][0]) == "Phone" || fmt.Sprintf("%v", rows[0][0]) == "Laptop" {
			pass++
		} else {
			t.Errorf("ORDER BY expr: unexpected first result %v", rows[0][0])
		}
	}

	// 18. COALESCE in complex context
	check("COALESCE complex", "SELECT COALESCE(NULL, NULL, 'found')", "found")

	// 19. CAST in expression
	check("CAST expr", "SELECT CAST(price AS INTEGER) FROM products WHERE id = 1", 999)

	// 20. Nested aggregate
	check("Nested agg", "SELECT COUNT(*) FROM products WHERE price > (SELECT AVG(price) FROM products)", 3)

	t.Logf("\n=== COMPREHENSIVE V2: %d/20 tests passed ===", pass)
}
