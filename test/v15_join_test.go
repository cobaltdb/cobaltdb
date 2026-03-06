package test

import (
	"fmt"
	"testing"
)

func TestV15JoinComprehensive(t *testing.T) {
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

	checkColCount := func(desc string, sql string, expectedCols int) {
		t.Helper()
		total++
		rows := afQuery(t, db, ctx, sql)
		if len(rows) == 0 {
			t.Errorf("[FAIL] %s: no rows returned", desc)
			return
		}
		if len(rows[0]) != expectedCols {
			t.Errorf("[FAIL] %s: expected %d columns, got %d", desc, expectedCols, len(rows[0]))
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

	_ = checkNull

	// ============================================================
	// === SETUP ===
	// ============================================================
	afExec(t, db, ctx, "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, age INTEGER)")
	afExec(t, db, ctx, "INSERT INTO users VALUES (1, 'Alice', 30)")
	afExec(t, db, ctx, "INSERT INTO users VALUES (2, 'Bob', 25)")
	afExec(t, db, ctx, "INSERT INTO users VALUES (3, 'Charlie', 35)")
	afExec(t, db, ctx, "INSERT INTO users VALUES (4, 'Dave', 28)")

	afExec(t, db, ctx, "CREATE TABLE orders (id INTEGER PRIMARY KEY, user_id INTEGER, amount INTEGER, product TEXT)")
	afExec(t, db, ctx, "INSERT INTO orders VALUES (1, 1, 100, 'Widget')")
	afExec(t, db, ctx, "INSERT INTO orders VALUES (2, 1, 200, 'Gadget')")
	afExec(t, db, ctx, "INSERT INTO orders VALUES (3, 2, 150, 'Widget')")
	afExec(t, db, ctx, "INSERT INTO orders VALUES (4, 5, 300, 'Gizmo')") // user_id=5 doesn't exist

	afExec(t, db, ctx, "CREATE TABLE products (id INTEGER PRIMARY KEY, name TEXT, category TEXT)")
	afExec(t, db, ctx, "INSERT INTO products VALUES (1, 'Widget', 'Hardware')")
	afExec(t, db, ctx, "INSERT INTO products VALUES (2, 'Gadget', 'Electronics')")
	afExec(t, db, ctx, "INSERT INTO products VALUES (3, 'Gizmo', 'Hardware')")

	// ============================================================
	// === INNER JOIN ===
	// ============================================================
	checkRowCount("INNER JOIN basic",
		"SELECT users.name, orders.amount FROM users INNER JOIN orders ON users.id = orders.user_id", 3)

	check("INNER JOIN first row name",
		"SELECT users.name FROM users INNER JOIN orders ON users.id = orders.user_id ORDER BY orders.id LIMIT 1", "Alice")

	check("INNER JOIN with WHERE",
		"SELECT users.name FROM users INNER JOIN orders ON users.id = orders.user_id WHERE orders.amount > 100 ORDER BY orders.amount ASC LIMIT 1", "Bob")

	checkRowCount("INNER JOIN with WHERE filter",
		"SELECT users.name, orders.amount FROM users INNER JOIN orders ON users.id = orders.user_id WHERE orders.amount > 100", 2)

	// Bare JOIN = INNER JOIN
	checkRowCount("Bare JOIN",
		"SELECT users.name FROM users JOIN orders ON users.id = orders.user_id", 3)

	// ============================================================
	// === LEFT JOIN ===
	// ============================================================
	checkRowCount("LEFT JOIN returns all left rows",
		"SELECT users.name, orders.amount FROM users LEFT JOIN orders ON users.id = orders.user_id", 5)

	// Users without orders should have NULL
	checkNull("LEFT JOIN NULL for unmatched",
		"SELECT orders.amount FROM users LEFT JOIN orders ON users.id = orders.user_id WHERE users.name = 'Charlie'")

	checkNull("LEFT JOIN NULL for Dave",
		"SELECT orders.amount FROM users LEFT JOIN orders ON users.id = orders.user_id WHERE users.name = 'Dave'")

	// ============================================================
	// === RIGHT JOIN ===
	// ============================================================
	checkRowCount("RIGHT JOIN returns all right rows",
		"SELECT users.name, orders.amount FROM users RIGHT JOIN orders ON users.id = orders.user_id", 4)

	// Order with user_id=5 should have NULL user name
	checkNull("RIGHT JOIN NULL for unmatched",
		"SELECT users.name FROM users RIGHT JOIN orders ON users.id = orders.user_id WHERE orders.user_id = 5")

	// ============================================================
	// === CROSS JOIN ===
	// ============================================================
	checkRowCount("CROSS JOIN",
		"SELECT users.name, products.name FROM users CROSS JOIN products", 12) // 4 users * 3 products

	// ============================================================
	// === SELECT * WITH JOIN ===
	// ============================================================
	checkColCount("SELECT * INNER JOIN includes all columns",
		"SELECT * FROM users INNER JOIN orders ON users.id = orders.user_id", 7) // 3 user cols + 4 order cols

	checkRowCount("SELECT * INNER JOIN row count",
		"SELECT * FROM users INNER JOIN orders ON users.id = orders.user_id", 3)

	checkColCount("SELECT * LEFT JOIN includes all columns",
		"SELECT * FROM users LEFT JOIN orders ON users.id = orders.user_id", 7)

	// ============================================================
	// === QUALIFIED COLUMN NAMES ===
	// ============================================================
	check("Qualified column in SELECT",
		"SELECT users.name FROM users INNER JOIN orders ON users.id = orders.user_id ORDER BY orders.id LIMIT 1", "Alice")

	check("Qualified column in WHERE",
		"SELECT users.name FROM users INNER JOIN orders ON users.id = orders.user_id WHERE orders.amount = 200", "Alice")

	check("Qualified column in ORDER BY",
		"SELECT users.name FROM users INNER JOIN orders ON users.id = orders.user_id ORDER BY orders.amount DESC LIMIT 1", "Alice")

	// ============================================================
	// === JOIN WITH ALIASES ===
	// ============================================================
	checkRowCount("JOIN with aliases",
		"SELECT u.name, o.amount FROM users u INNER JOIN orders o ON u.id = o.user_id", 3)

	check("Aliased JOIN first row",
		"SELECT u.name FROM users u INNER JOIN orders o ON u.id = o.user_id ORDER BY o.id LIMIT 1", "Alice")

	check("Aliased JOIN with WHERE",
		"SELECT u.name FROM users u JOIN orders o ON u.id = o.user_id WHERE o.amount > 100 ORDER BY o.amount ASC LIMIT 1", "Bob")

	// ============================================================
	// === AGGREGATE WITH JOIN ===
	// ============================================================
	check("COUNT with JOIN",
		"SELECT COUNT(*) FROM users INNER JOIN orders ON users.id = orders.user_id", 3)

	check("SUM with JOIN",
		"SELECT SUM(orders.amount) FROM users INNER JOIN orders ON users.id = orders.user_id", 450)

	check("AVG with JOIN",
		"SELECT AVG(orders.amount) FROM users INNER JOIN orders ON users.id = orders.user_id", 150)

	check("MAX with JOIN",
		"SELECT MAX(orders.amount) FROM users INNER JOIN orders ON users.id = orders.user_id", 200)

	check("MIN with JOIN",
		"SELECT MIN(orders.amount) FROM users INNER JOIN orders ON users.id = orders.user_id", 100)

	// ============================================================
	// === GROUP BY WITH JOIN ===
	// ============================================================
	check("GROUP BY with JOIN - count orders per user",
		"SELECT COUNT(*) FROM users INNER JOIN orders ON users.id = orders.user_id GROUP BY users.name ORDER BY COUNT(*) DESC LIMIT 1", 2)

	check("GROUP BY with JOIN - sum per user",
		"SELECT users.name FROM users INNER JOIN orders ON users.id = orders.user_id GROUP BY users.name HAVING SUM(orders.amount) > 200 LIMIT 1", "Alice")

	// ============================================================
	// === MULTI-TABLE JOIN ===
	// ============================================================
	afExec(t, db, ctx, "CREATE TABLE order_items (id INTEGER PRIMARY KEY, order_id INTEGER, product_id INTEGER, qty INTEGER)")
	afExec(t, db, ctx, "INSERT INTO order_items VALUES (1, 1, 1, 2)")
	afExec(t, db, ctx, "INSERT INTO order_items VALUES (2, 2, 2, 1)")
	afExec(t, db, ctx, "INSERT INTO order_items VALUES (3, 3, 1, 3)")

	checkRowCount("Three-table JOIN",
		"SELECT users.name, orders.amount, order_items.qty FROM users JOIN orders ON users.id = orders.user_id JOIN order_items ON orders.id = order_items.order_id", 3)

	check("Three-table JOIN value",
		"SELECT users.name FROM users JOIN orders ON users.id = orders.user_id JOIN order_items ON orders.id = order_items.order_id ORDER BY order_items.qty DESC LIMIT 1", "Bob")

	// ============================================================
	// === SELF-JOIN ===
	// ============================================================
	afExec(t, db, ctx, "CREATE TABLE employees (id INTEGER PRIMARY KEY, name TEXT, manager_id INTEGER)")
	afExec(t, db, ctx, "INSERT INTO employees VALUES (1, 'CEO', NULL)")
	afExec(t, db, ctx, "INSERT INTO employees VALUES (2, 'VP', 1)")
	afExec(t, db, ctx, "INSERT INTO employees VALUES (3, 'Dev', 2)")

	checkRowCount("Self-JOIN",
		"SELECT e.name, m.name FROM employees e JOIN employees m ON e.manager_id = m.id", 2)

	check("Self-JOIN manager name",
		"SELECT m.name FROM employees e JOIN employees m ON e.manager_id = m.id WHERE e.name = 'Dev'", "VP")

	// ============================================================
	// === JOIN WITH EXPRESSIONS ===
	// ============================================================
	check("JOIN with expression in SELECT",
		"SELECT orders.amount * 2 FROM users JOIN orders ON users.id = orders.user_id WHERE users.name = 'Alice' ORDER BY orders.id LIMIT 1", 200)

	check("JOIN with string concat",
		"SELECT users.name || '-' || orders.product FROM users JOIN orders ON users.id = orders.user_id ORDER BY orders.id LIMIT 1", "Alice-Widget")

	// ============================================================
	// === JOIN WITH SUBQUERY IN WHERE ===
	// ============================================================
	checkRowCount("JOIN with subquery WHERE",
		"SELECT users.name FROM users JOIN orders ON users.id = orders.user_id WHERE orders.amount > (SELECT AVG(amount) FROM orders)", 1)

	// ============================================================
	// === DISTINCT WITH JOIN ===
	// ============================================================
	check("DISTINCT with JOIN",
		"SELECT COUNT(DISTINCT users.name) FROM users INNER JOIN orders ON users.id = orders.user_id", 2)

	// ============================================================
	// === JOIN ORDER BY + LIMIT ===
	// ============================================================
	checkRowCount("JOIN with LIMIT",
		"SELECT users.name FROM users JOIN orders ON users.id = orders.user_id LIMIT 2", 2)

	check("JOIN ORDER BY DESC LIMIT 1",
		"SELECT orders.amount FROM users JOIN orders ON users.id = orders.user_id ORDER BY orders.amount DESC LIMIT 1", 200)

	check("JOIN ORDER BY ASC LIMIT 1",
		"SELECT orders.amount FROM users JOIN orders ON users.id = orders.user_id ORDER BY orders.amount ASC LIMIT 1", 100)

	t.Logf("\n=== V15 JOIN COMPREHENSIVE: %d/%d tests passed ===", pass, total)
	if pass < total {
		t.Errorf("Some tests failed: %d/%d", pass, total)
	}
}
