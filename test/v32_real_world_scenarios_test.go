package test

import (
	"fmt"
	"testing"
)

func TestV32RealWorldScenarios(t *testing.T) {
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

	// ============================================================
	// === E-COMMERCE SCENARIO ===
	// ============================================================
	afExec(t, db, ctx, "CREATE TABLE customers (id INTEGER PRIMARY KEY AUTO_INCREMENT, name TEXT, email TEXT UNIQUE, city TEXT)")
	afExec(t, db, ctx, "CREATE TABLE products (id INTEGER PRIMARY KEY AUTO_INCREMENT, name TEXT, price INTEGER, category TEXT)")
	afExec(t, db, ctx, "CREATE TABLE orders (id INTEGER PRIMARY KEY AUTO_INCREMENT, customer_id INTEGER, order_date TEXT, status TEXT, FOREIGN KEY (customer_id) REFERENCES customers(id))")
	afExec(t, db, ctx, "CREATE TABLE order_items (id INTEGER PRIMARY KEY AUTO_INCREMENT, order_id INTEGER, product_id INTEGER, quantity INTEGER, FOREIGN KEY (order_id) REFERENCES orders(id) ON DELETE CASCADE)")

	// Create indexes
	checkNoError("Index on city", "CREATE INDEX idx_cust_city ON customers(city)")
	checkNoError("Index on category", "CREATE INDEX idx_prod_cat ON products(category)")
	checkNoError("Index on customer_id", "CREATE INDEX idx_ord_cust ON orders(customer_id)")

	// Insert customers
	afExec(t, db, ctx, "INSERT INTO customers (name, email, city) VALUES ('Alice Johnson', 'alice@example.com', 'NYC')")
	afExec(t, db, ctx, "INSERT INTO customers (name, email, city) VALUES ('Bob Smith', 'bob@example.com', 'LA')")
	afExec(t, db, ctx, "INSERT INTO customers (name, email, city) VALUES ('Charlie Brown', 'charlie@example.com', 'NYC')")
	afExec(t, db, ctx, "INSERT INTO customers (name, email, city) VALUES ('Diana Ross', 'diana@example.com', 'Chicago')")
	afExec(t, db, ctx, "INSERT INTO customers (name, email, city) VALUES ('Eve Wilson', 'eve@example.com', 'LA')")

	// Insert products
	afExec(t, db, ctx, "INSERT INTO products (name, price, category) VALUES ('Widget', 10, 'Hardware')")
	afExec(t, db, ctx, "INSERT INTO products (name, price, category) VALUES ('Gadget', 25, 'Electronics')")
	afExec(t, db, ctx, "INSERT INTO products (name, price, category) VALUES ('Phone', 500, 'Electronics')")
	afExec(t, db, ctx, "INSERT INTO products (name, price, category) VALUES ('Cable', 5, 'Hardware')")
	afExec(t, db, ctx, "INSERT INTO products (name, price, category) VALUES ('Screen', 300, 'Electronics')")

	// Insert orders
	afExec(t, db, ctx, "INSERT INTO orders (customer_id, order_date, status) VALUES (1, '2024-01-15', 'completed')")
	afExec(t, db, ctx, "INSERT INTO orders (customer_id, order_date, status) VALUES (1, '2024-02-20', 'completed')")
	afExec(t, db, ctx, "INSERT INTO orders (customer_id, order_date, status) VALUES (2, '2024-01-20', 'completed')")
	afExec(t, db, ctx, "INSERT INTO orders (customer_id, order_date, status) VALUES (3, '2024-03-10', 'pending')")
	afExec(t, db, ctx, "INSERT INTO orders (customer_id, order_date, status) VALUES (4, '2024-03-15', 'completed')")
	afExec(t, db, ctx, "INSERT INTO orders (customer_id, order_date, status) VALUES (5, '2024-02-28', 'cancelled')")

	// Insert order items
	afExec(t, db, ctx, "INSERT INTO order_items (order_id, product_id, quantity) VALUES (1, 1, 3)") // Alice: 3 Widgets
	afExec(t, db, ctx, "INSERT INTO order_items (order_id, product_id, quantity) VALUES (1, 2, 1)") // Alice: 1 Gadget
	afExec(t, db, ctx, "INSERT INTO order_items (order_id, product_id, quantity) VALUES (2, 3, 1)") // Alice: 1 Phone
	afExec(t, db, ctx, "INSERT INTO order_items (order_id, product_id, quantity) VALUES (3, 1, 5)") // Bob: 5 Widgets
	afExec(t, db, ctx, "INSERT INTO order_items (order_id, product_id, quantity) VALUES (3, 4, 2)") // Bob: 2 Cables
	afExec(t, db, ctx, "INSERT INTO order_items (order_id, product_id, quantity) VALUES (4, 5, 1)") // Charlie: 1 Screen
	afExec(t, db, ctx, "INSERT INTO order_items (order_id, product_id, quantity) VALUES (5, 2, 2)") // Diana: 2 Gadgets
	afExec(t, db, ctx, "INSERT INTO order_items (order_id, product_id, quantity) VALUES (6, 3, 1)") // Eve: 1 Phone (cancelled)

	// === Queries ===

	// 1. Customer with most orders
	check("Customer most orders",
		"SELECT customers.name FROM customers JOIN orders ON customers.id = orders.customer_id GROUP BY customers.name ORDER BY COUNT(*) DESC LIMIT 1",
		"Alice Johnson") // 2 orders

	// 2. Total revenue by category (using SUM expression across JOIN)
	check("Top revenue category",
		"SELECT products.category FROM products JOIN order_items ON products.id = order_items.product_id GROUP BY products.category ORDER BY SUM(products.price * order_items.quantity) DESC LIMIT 1",
		"Electronics") // Electronics: 1375, Hardware: 90

	// 3. Customers in NYC
	checkRowCount("NYC customers", "SELECT * FROM customers WHERE city = 'NYC'", 2)

	// 4. Average order value for completed orders
	// Alice order 1: 3*10 + 1*25 = 55
	// Alice order 2: 1*500 = 500
	// Bob order 3: 5*10 + 2*5 = 60
	// Diana order 5: 2*25 = 50
	check("Completed order count", "SELECT COUNT(DISTINCT orders.id) FROM orders WHERE status = 'completed'", 4)

	// 5. Products never ordered
	// All products have been ordered, so this should return 0
	checkRowCount("Products not ordered",
		"SELECT * FROM products WHERE NOT EXISTS (SELECT 1 FROM order_items WHERE order_items.product_id = products.id)", 0)

	// 6. Customers who never placed an order
	checkRowCount("Customers without orders",
		"SELECT * FROM customers WHERE NOT EXISTS (SELECT 1 FROM orders WHERE orders.customer_id = customers.id)", 0)

	// 7. Order cancellation - should CASCADE delete order items
	checkNoError("Cancel order (delete)", "DELETE FROM orders WHERE id = 6")
	checkRowCount("Cascaded items deleted", "SELECT * FROM order_items WHERE order_id = 6", 0)

	// 8. Total quantity sold per product
	check("Most sold product",
		"SELECT products.name FROM products JOIN order_items ON products.id = order_items.product_id GROUP BY products.name ORDER BY SUM(order_items.quantity) DESC LIMIT 1",
		"Widget") // 3+5=8

	// ============================================================
	// === BLOG/CMS SCENARIO ===
	// ============================================================
	afExec(t, db, ctx, "CREATE TABLE users (id INTEGER PRIMARY KEY AUTO_INCREMENT, username TEXT UNIQUE, role TEXT DEFAULT 'user')")
	afExec(t, db, ctx, "CREATE TABLE posts (id INTEGER PRIMARY KEY AUTO_INCREMENT, author_id INTEGER, title TEXT, content TEXT, status TEXT DEFAULT 'draft', FOREIGN KEY (author_id) REFERENCES users(id))")
	afExec(t, db, ctx, "CREATE TABLE comments (id INTEGER PRIMARY KEY AUTO_INCREMENT, post_id INTEGER, user_id INTEGER, body TEXT, FOREIGN KEY (post_id) REFERENCES posts(id) ON DELETE CASCADE)")
	afExec(t, db, ctx, "CREATE TABLE tags (id INTEGER PRIMARY KEY AUTO_INCREMENT, name TEXT UNIQUE)")
	afExec(t, db, ctx, "CREATE TABLE post_tags (id INTEGER PRIMARY KEY AUTO_INCREMENT, post_id INTEGER, tag_id INTEGER)")

	// Users
	afExec(t, db, ctx, "INSERT INTO users (username, role) VALUES ('admin', 'admin')")
	afExec(t, db, ctx, "INSERT INTO users (username, role) VALUES ('editor1', 'editor')")
	afExec(t, db, ctx, "INSERT INTO users (username) VALUES ('reader1')")
	afExec(t, db, ctx, "INSERT INTO users (username) VALUES ('reader2')")

	// Posts
	afExec(t, db, ctx, "INSERT INTO posts (author_id, title, content, status) VALUES (1, 'Getting Started', 'Welcome!', 'published')")
	afExec(t, db, ctx, "INSERT INTO posts (author_id, title, content, status) VALUES (2, 'Advanced Tips', 'Deep dive', 'published')")
	afExec(t, db, ctx, "INSERT INTO posts (author_id, title, content, status) VALUES (1, 'Draft Post', 'WIP', 'draft')")
	afExec(t, db, ctx, "INSERT INTO posts (author_id, title, content) VALUES (2, 'Another Draft', 'WIP2')")

	// Comments
	afExec(t, db, ctx, "INSERT INTO comments (post_id, user_id, body) VALUES (1, 3, 'Great post!')")
	afExec(t, db, ctx, "INSERT INTO comments (post_id, user_id, body) VALUES (1, 4, 'Very helpful')")
	afExec(t, db, ctx, "INSERT INTO comments (post_id, user_id, body) VALUES (2, 3, 'Nice tips')")

	// Tags
	afExec(t, db, ctx, "INSERT INTO tags (name) VALUES ('tutorial')")
	afExec(t, db, ctx, "INSERT INTO tags (name) VALUES ('advanced')")
	afExec(t, db, ctx, "INSERT INTO tags (name) VALUES ('tips')")

	// Post tags
	afExec(t, db, ctx, "INSERT INTO post_tags (post_id, tag_id) VALUES (1, 1)") // Getting Started: tutorial
	afExec(t, db, ctx, "INSERT INTO post_tags (post_id, tag_id) VALUES (2, 2)") // Advanced Tips: advanced
	afExec(t, db, ctx, "INSERT INTO post_tags (post_id, tag_id) VALUES (2, 3)") // Advanced Tips: tips

	// Queries
	check("Published post count", "SELECT COUNT(*) FROM posts WHERE status = 'published'", 2)
	check("Draft post count", "SELECT COUNT(*) FROM posts WHERE status = 'draft'", 2) // 'draft' + NULL default = 'draft'

	check("Most commented post",
		"SELECT posts.title FROM posts JOIN comments ON posts.id = comments.post_id GROUP BY posts.title ORDER BY COUNT(*) DESC LIMIT 1",
		"Getting Started") // 2 comments

	check("User with most posts",
		"SELECT users.username FROM users JOIN posts ON users.id = posts.author_id GROUP BY users.username ORDER BY COUNT(*) DESC LIMIT 1",
		"admin") // admin:2, editor1:2 - tied, admin first alphabetically

	// Delete a post - comments should cascade
	checkNoError("Delete published post", "DELETE FROM posts WHERE id = 1")
	checkRowCount("Cascaded comments deleted", "SELECT * FROM comments WHERE post_id = 1", 0)
	checkRowCount("Other comments intact", "SELECT * FROM comments", 1)

	// ============================================================
	// === INVENTORY MANAGEMENT SCENARIO ===
	// ============================================================
	afExec(t, db, ctx, "CREATE TABLE warehouses (id INTEGER PRIMARY KEY, name TEXT, location TEXT)")
	afExec(t, db, ctx, "CREATE TABLE inventory (id INTEGER PRIMARY KEY AUTO_INCREMENT, warehouse_id INTEGER, product TEXT, quantity INTEGER, min_stock INTEGER)")

	afExec(t, db, ctx, "INSERT INTO warehouses VALUES (1, 'Main', 'NYC')")
	afExec(t, db, ctx, "INSERT INTO warehouses VALUES (2, 'West', 'LA')")

	afExec(t, db, ctx, "INSERT INTO inventory (warehouse_id, product, quantity, min_stock) VALUES (1, 'Widget', 100, 20)")
	afExec(t, db, ctx, "INSERT INTO inventory (warehouse_id, product, quantity, min_stock) VALUES (1, 'Gadget', 50, 10)")
	afExec(t, db, ctx, "INSERT INTO inventory (warehouse_id, product, quantity, min_stock) VALUES (2, 'Widget', 30, 20)")
	afExec(t, db, ctx, "INSERT INTO inventory (warehouse_id, product, quantity, min_stock) VALUES (2, 'Phone', 5, 10)")

	// Stock alert: items below minimum
	checkRowCount("Low stock items",
		"SELECT * FROM inventory WHERE quantity < min_stock", 1) // Phone: 5 < 10

	// Total stock per product across warehouses
	check("Total Widget stock",
		"SELECT SUM(quantity) FROM inventory WHERE product = 'Widget'", 130)

	// Warehouse with most total stock
	check("Warehouse most stock",
		"SELECT warehouses.name FROM warehouses JOIN inventory ON warehouses.id = inventory.warehouse_id GROUP BY warehouses.name ORDER BY SUM(inventory.quantity) DESC LIMIT 1",
		"Main") // Main: 150, West: 35

	// Update stock (simulate sale)
	checkNoError("Process sale",
		"UPDATE inventory SET quantity = quantity - 10 WHERE warehouse_id = 1 AND product = 'Widget'")
	check("Stock after sale", "SELECT quantity FROM inventory WHERE warehouse_id = 1 AND product = 'Widget'", 90)

	// Transaction: restock with rollback
	checkNoError("BEGIN restock", "BEGIN")
	checkNoError("Restock widget", "UPDATE inventory SET quantity = quantity + 50 WHERE product = 'Widget'")
	check("Stock during restock (main)",
		"SELECT quantity FROM inventory WHERE warehouse_id = 1 AND product = 'Widget'", 140)
	checkNoError("ROLLBACK restock", "ROLLBACK")
	check("Stock after rollback",
		"SELECT quantity FROM inventory WHERE warehouse_id = 1 AND product = 'Widget'", 90)

	// ============================================================
	// === REPORTING QUERIES ===
	// ============================================================

	// CTE-based report
	check("CTE report",
		"WITH stock_report AS (SELECT product, SUM(quantity) AS total_stock FROM inventory GROUP BY product) SELECT product FROM stock_report ORDER BY total_stock DESC LIMIT 1",
		"Widget") // Widget: 120, Gadget: 50, Phone: 5

	// Correlated subquery report
	checkRowCount("Warehouses with low stock items",
		"SELECT DISTINCT warehouses.name FROM warehouses WHERE EXISTS (SELECT 1 FROM inventory WHERE inventory.warehouse_id = warehouses.id AND inventory.quantity < inventory.min_stock)", 1) // Only West

	// CASE-based categorization
	check("Stock category",
		"SELECT CASE WHEN quantity > 50 THEN 'high' WHEN quantity > 10 THEN 'medium' ELSE 'low' END FROM inventory WHERE warehouse_id = 1 AND product = 'Widget'",
		"high") // 90 > 50

	t.Logf("\n=== V32 REAL WORLD SCENARIOS: %d/%d tests passed ===", pass, total)
	if pass < total {
		t.Errorf("Some tests failed: %d/%d", pass, total)
	}
}
