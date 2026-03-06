package test

import (
	"context"
	"fmt"
	"path/filepath"
	"strings"
	"testing"

	"github.com/cobaltdb/cobaltdb/pkg/engine"
)

// TestRealWorldScenario - Gerçek hayat senaryosu
// Bir e-ticaret sistemi: kullanıcılar, ürünler, siparişler, yorumlar
// Peş peşe SQL sorguları, her özellik test ediliyor
func TestRealWorldScenario(t *testing.T) {
	db, err := engine.Open(":memory:", &engine.Options{InMemory: true})
	if err != nil {
		t.Fatalf("DB açılamadı: %v", err)
	}
	defer db.Close()
	ctx := context.Background()

	// Helper: exec ve hata kontrolü
	exec := func(sql string) {
		t.Helper()
		_, err := db.Exec(ctx, sql)
		if err != nil {
			t.Fatalf("EXEC FAILED:\n  SQL: %s\n  ERR: %v", sql, err)
		}
	}

	// Helper: query ve satır sayısı kontrolü
	queryExpectRows := func(sql string, expectedRows int) [][]interface{} {
		t.Helper()
		rows, err := db.Query(ctx, sql)
		if err != nil {
			t.Fatalf("QUERY FAILED:\n  SQL: %s\n  ERR: %v", sql, err)
		}
		defer rows.Close()

		cols := rows.Columns()
		var result [][]interface{}
		for rows.Next() {
			vals := make([]interface{}, len(cols))
			dest := make([]interface{}, len(cols))
			for i := range dest {
				dest[i] = &vals[i]
			}
			if err := rows.Scan(dest...); err != nil {
				t.Fatalf("SCAN FAILED:\n  SQL: %s\n  ERR: %v", sql, err)
			}
			result = append(result, vals)
		}
		if expectedRows >= 0 && len(result) != expectedRows {
			t.Fatalf("ROW COUNT MISMATCH:\n  SQL: %s\n  Expected: %d rows\n  Got: %d rows", sql, expectedRows, len(result))
		}
		return result
	}

	// Helper: tek değer sorgusu
	queryOne := func(sql string) interface{} {
		t.Helper()
		rows := queryExpectRows(sql, 1)
		return rows[0][0]
	}

	// Helper: exec'in hata vermesini bekle
	expectError := func(sql string, containsMsg string) {
		t.Helper()
		_, err := db.Exec(ctx, sql)
		if err == nil {
			t.Fatalf("EXPECTED ERROR but got nil:\n  SQL: %s\n  Expected to contain: %s", sql, containsMsg)
		}
		if containsMsg != "" && !strings.Contains(strings.ToLower(err.Error()), strings.ToLower(containsMsg)) {
			t.Fatalf("WRONG ERROR:\n  SQL: %s\n  Expected containing: %s\n  Got: %v", sql, containsMsg, err)
		}
	}

	t.Log("========================================")
	t.Log("  COBALTDB REAL-WORLD SCENARIO TEST")
	t.Log("  E-Ticaret Sistemi Simülasyonu")
	t.Log("========================================")

	// ============================================================
	// PHASE 1: Schema oluşturma
	// ============================================================
	t.Log("\n--- PHASE 1: Schema Creation ---")

	exec("CREATE TABLE categories (id INTEGER PRIMARY KEY, name TEXT NOT NULL, description TEXT)")
	t.Log("  [OK] CREATE TABLE categories")

	exec("CREATE TABLE users (id INTEGER PRIMARY KEY AUTOINCREMENT, username TEXT UNIQUE NOT NULL, email TEXT UNIQUE NOT NULL, full_name TEXT, balance REAL DEFAULT 0)")
	t.Log("  [OK] CREATE TABLE users (AUTOINCREMENT, UNIQUE, NOT NULL, DEFAULT)")

	exec("CREATE TABLE products (id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT NOT NULL, price REAL NOT NULL CHECK (price > 0), stock INTEGER NOT NULL DEFAULT 0, category_id INTEGER, metadata TEXT)")
	t.Log("  [OK] CREATE TABLE products (CHECK constraint, DEFAULT)")

	exec("CREATE TABLE orders (id INTEGER PRIMARY KEY AUTOINCREMENT, user_id INTEGER NOT NULL, total REAL NOT NULL, status TEXT DEFAULT 'pending', created_at TEXT)")
	t.Log("  [OK] CREATE TABLE orders")

	exec("CREATE TABLE order_items (id INTEGER PRIMARY KEY AUTOINCREMENT, order_id INTEGER NOT NULL, product_id INTEGER NOT NULL, quantity INTEGER NOT NULL CHECK (quantity > 0), unit_price REAL NOT NULL)")
	t.Log("  [OK] CREATE TABLE order_items (CHECK constraint)")

	exec("CREATE TABLE reviews (id INTEGER PRIMARY KEY AUTOINCREMENT, product_id INTEGER NOT NULL, user_id INTEGER NOT NULL, rating INTEGER NOT NULL CHECK (rating >= 1), comment TEXT)")
	t.Log("  [OK] CREATE TABLE reviews (CHECK rating >= 1)")

	// Index oluşturma
	exec("CREATE INDEX idx_products_category ON products(category_id)")
	exec("CREATE INDEX idx_orders_user ON orders(user_id)")
	exec("CREATE INDEX idx_order_items_order ON order_items(order_id)")
	t.Log("  [OK] CREATE INDEX x3")

	// ============================================================
	// PHASE 2: Veri ekleme
	// ============================================================
	t.Log("\n--- PHASE 2: Data Population ---")

	// Kategoriler
	exec("INSERT INTO categories (id, name, description) VALUES (1, 'Electronics', 'Phones, laptops, gadgets')")
	exec("INSERT INTO categories (id, name, description) VALUES (2, 'Books', 'Fiction, non-fiction, textbooks')")
	exec("INSERT INTO categories (id, name, description) VALUES (3, 'Clothing', 'Shirts, pants, accessories')")
	exec("INSERT INTO categories (id, name, description) VALUES (4, 'Home & Garden', 'Furniture, decor, tools')")
	t.Log("  [OK] 4 categories inserted")

	// Kullanıcılar (AUTOINCREMENT test)
	exec("INSERT INTO users (username, email, full_name, balance) VALUES ('alice', 'alice@example.com', 'Alice Johnson', 5000.00)")
	exec("INSERT INTO users (username, email, full_name, balance) VALUES ('bob', 'bob@example.com', 'Bob Smith', 2500.50)")
	exec("INSERT INTO users (username, email, full_name, balance) VALUES ('carol', 'carol@example.com', 'Carol Williams', 10000.00)")
	exec("INSERT INTO users (username, email, full_name, balance) VALUES ('dave', 'dave@example.com', 'Dave Brown', 750.25)")
	exec("INSERT INTO users (username, email, full_name, balance) VALUES ('eve', 'eve@example.com', 'Eve Davis', 3200.00)")
	t.Log("  [OK] 5 users inserted (AUTOINCREMENT)")

	// Ürünler (JSON metadata dahil)
	exec(`INSERT INTO products (name, price, stock, category_id, metadata) VALUES ('iPhone 15', 999.99, 50, 1, '{"brand":"Apple","color":"black","storage":"128GB"}')`)
	exec(`INSERT INTO products (name, price, stock, category_id, metadata) VALUES ('MacBook Pro', 2499.99, 25, 1, '{"brand":"Apple","ram":"16GB","ssd":"512GB"}')`)
	exec(`INSERT INTO products (name, price, stock, category_id, metadata) VALUES ('Samsung Galaxy S24', 849.99, 75, 1, '{"brand":"Samsung","color":"blue"}')`)
	exec("INSERT INTO products (name, price, stock, category_id) VALUES ('Python Crash Course', 39.99, 200, 2)")
	exec("INSERT INTO products (name, price, stock, category_id) VALUES ('Clean Code', 44.99, 150, 2)")
	exec("INSERT INTO products (name, price, stock, category_id) VALUES ('Cotton T-Shirt', 19.99, 500, 3)")
	exec("INSERT INTO products (name, price, stock, category_id) VALUES ('Denim Jeans', 59.99, 300, 3)")
	exec("INSERT INTO products (name, price, stock, category_id) VALUES ('Standing Desk', 399.99, 40, 4)")
	exec("INSERT INTO products (name, price, stock, category_id) VALUES ('LED Desk Lamp', 29.99, 120, 4)")
	exec("INSERT INTO products (name, price, stock, category_id) VALUES ('Wireless Mouse', 24.99, 350, 1)")
	t.Log("  [OK] 10 products inserted (with JSON metadata)")

	// Siparişler
	exec("INSERT INTO orders (user_id, total, status, created_at) VALUES (1, 1049.98, 'completed', '2024-01-15')")
	exec("INSERT INTO orders (user_id, total, status, created_at) VALUES (1, 39.99, 'completed', '2024-02-20')")
	exec("INSERT INTO orders (user_id, total, status, created_at) VALUES (2, 2499.99, 'shipped', '2024-03-01')")
	exec("INSERT INTO orders (user_id, total, status, created_at) VALUES (3, 79.98, 'pending', '2024-03-10')")
	exec("INSERT INTO orders (user_id, total, status, created_at) VALUES (3, 849.99, 'completed', '2024-03-15')")
	exec("INSERT INTO orders (user_id, total, status, created_at) VALUES (4, 429.98, 'cancelled', '2024-03-20')")
	exec("INSERT INTO orders (user_id, total, status, created_at) VALUES (5, 24.99, 'completed', '2024-03-25')")
	t.Log("  [OK] 7 orders inserted")

	// Sipariş kalemleri
	exec("INSERT INTO order_items (order_id, product_id, quantity, unit_price) VALUES (1, 1, 1, 999.99)")
	exec("INSERT INTO order_items (order_id, product_id, quantity, unit_price) VALUES (1, 6, 1, 19.99)")
	exec("INSERT INTO order_items (order_id, product_id, quantity, unit_price) VALUES (1, 9, 1, 29.99)")
	exec("INSERT INTO order_items (order_id, product_id, quantity, unit_price) VALUES (2, 4, 1, 39.99)")
	exec("INSERT INTO order_items (order_id, product_id, quantity, unit_price) VALUES (3, 2, 1, 2499.99)")
	exec("INSERT INTO order_items (order_id, product_id, quantity, unit_price) VALUES (4, 6, 2, 19.99)")
	exec("INSERT INTO order_items (order_id, product_id, quantity, unit_price) VALUES (4, 7, 1, 39.99)")
	exec("INSERT INTO order_items (order_id, product_id, quantity, unit_price) VALUES (5, 3, 1, 849.99)")
	exec("INSERT INTO order_items (order_id, product_id, quantity, unit_price) VALUES (6, 8, 1, 399.99)")
	exec("INSERT INTO order_items (order_id, product_id, quantity, unit_price) VALUES (6, 9, 1, 29.99)")
	exec("INSERT INTO order_items (order_id, product_id, quantity, unit_price) VALUES (7, 10, 1, 24.99)")
	t.Log("  [OK] 11 order_items inserted")

	// Yorumlar
	exec("INSERT INTO reviews (product_id, user_id, rating, comment) VALUES (1, 1, 5, 'Amazing phone!')")
	exec("INSERT INTO reviews (product_id, user_id, rating, comment) VALUES (1, 2, 4, 'Good but expensive')")
	exec("INSERT INTO reviews (product_id, user_id, rating, comment) VALUES (2, 3, 5, 'Best laptop ever')")
	exec("INSERT INTO reviews (product_id, user_id, rating, comment) VALUES (4, 1, 4, 'Great book for beginners')")
	exec("INSERT INTO reviews (product_id, user_id, rating, comment) VALUES (3, 5, 3, 'Decent phone')")
	t.Log("  [OK] 5 reviews inserted")

	// ============================================================
	// PHASE 3: Constraint Enforcement
	// ============================================================
	t.Log("\n--- PHASE 3: Constraint Enforcement ---")

	// PRIMARY KEY duplicate
	expectError("INSERT INTO categories (id, name) VALUES (1, 'Duplicate')", "duplicate")
	t.Log("  [OK] PRIMARY KEY duplicate rejected")

	// UNIQUE constraint
	expectError("INSERT INTO users (username, email, full_name) VALUES ('alice', 'new@email.com', 'Test')", "UNIQUE")
	t.Log("  [OK] UNIQUE username rejected")

	expectError("INSERT INTO users (username, email, full_name) VALUES ('newuser', 'alice@example.com', 'Test')", "UNIQUE")
	t.Log("  [OK] UNIQUE email rejected")

	// NOT NULL constraint
	expectError("INSERT INTO users (username, email, full_name) VALUES (NULL, 'test@test.com', 'Test')", "NOT NULL")
	t.Log("  [OK] NOT NULL username rejected")

	// CHECK constraint
	expectError("INSERT INTO products (name, price, stock, category_id) VALUES ('Bad', -10, 5, 1)", "CHECK")
	t.Log("  [OK] CHECK (price > 0) rejected negative price")

	expectError("INSERT INTO order_items (order_id, product_id, quantity, unit_price) VALUES (1, 1, 0, 10)", "CHECK")
	t.Log("  [OK] CHECK (quantity > 0) rejected zero quantity")

	expectError("INSERT INTO reviews (product_id, user_id, rating, comment) VALUES (1, 1, 0, 'bad')", "CHECK")
	t.Log("  [OK] CHECK (rating >= 1) rejected zero rating")

	// ============================================================
	// PHASE 4: Temel SELECT sorguları
	// ============================================================
	t.Log("\n--- PHASE 4: Basic Queries ---")

	// COUNT
	count := queryOne("SELECT COUNT(*) FROM products")
	if fmt.Sprintf("%v", count) != "10" {
		t.Fatalf("Expected 10 products, got %v", count)
	}
	t.Log("  [OK] SELECT COUNT(*) FROM products = 10")

	// WHERE (iPhone $999.99, MacBook $2499.99, Samsung $849.99, Standing Desk $399.99)
	rows := queryExpectRows("SELECT name, price FROM products WHERE price > 100 ORDER BY price DESC", 4)
	t.Logf("  [OK] WHERE price > 100: %d products found", len(rows))
	for _, row := range rows {
		t.Logf("       %v: $%v", row[0], row[1])
	}

	// BETWEEN
	rows = queryExpectRows("SELECT name FROM products WHERE price BETWEEN 20 AND 50", 4)
	t.Logf("  [OK] BETWEEN 20 AND 50: %d products found", len(rows))

	// LIKE
	rows = queryExpectRows("SELECT name FROM products WHERE name LIKE '%Phone%'", -1)
	t.Logf("  [OK] LIKE '%%Phone%%': %d products found", len(rows))

	// IN
	rows = queryExpectRows("SELECT name FROM categories WHERE id IN (1, 3)", 2)
	t.Logf("  [OK] IN (1, 3): %v, %v", rows[0][0], rows[1][0])

	// IS NULL
	rows = queryExpectRows("SELECT name FROM products WHERE metadata IS NULL", -1)
	t.Logf("  [OK] IS NULL: %d products without metadata", len(rows))

	// IS NOT NULL
	rows = queryExpectRows("SELECT name FROM products WHERE metadata IS NOT NULL", -1)
	t.Logf("  [OK] IS NOT NULL: %d products with metadata", len(rows))

	// DISTINCT
	rows = queryExpectRows("SELECT DISTINCT status FROM orders", -1)
	t.Logf("  [OK] DISTINCT status: %d unique statuses", len(rows))

	// ORDER BY + LIMIT + OFFSET
	rows = queryExpectRows("SELECT name, price FROM products ORDER BY price DESC LIMIT 3", 3)
	t.Logf("  [OK] Top 3 expensive: %v ($%v)", rows[0][0], rows[0][1])

	rows = queryExpectRows("SELECT name FROM products ORDER BY price DESC LIMIT 3 OFFSET 3", 3)
	t.Logf("  [OK] LIMIT 3 OFFSET 3: %v", rows[0][0])

	// ============================================================
	// PHASE 5: Aggregation
	// ============================================================
	t.Log("\n--- PHASE 5: Aggregation ---")

	rows = queryExpectRows("SELECT COUNT(*), SUM(price), AVG(price), MIN(price), MAX(price) FROM products", 1)
	t.Logf("  [OK] Aggregates: COUNT=%v, SUM=%v, AVG=%v, MIN=%v, MAX=%v",
		rows[0][0], rows[0][1], rows[0][2], rows[0][3], rows[0][4])

	// GROUP BY
	rows = queryExpectRows("SELECT category_id, COUNT(*), AVG(price) FROM products GROUP BY category_id ORDER BY category_id", 4)
	t.Log("  [OK] GROUP BY category_id:")
	for _, row := range rows {
		t.Logf("       Category %v: %v products, avg $%v", row[0], row[1], row[2])
	}

	// HAVING
	rows = queryExpectRows("SELECT category_id, COUNT(*) FROM products GROUP BY category_id HAVING COUNT(*) >= 2 ORDER BY category_id", -1)
	t.Logf("  [OK] HAVING COUNT(*) >= 2: %d categories", len(rows))

	// ============================================================
	// PHASE 6: JOINs
	// ============================================================
	t.Log("\n--- PHASE 6: JOINs ---")

	// INNER JOIN
	rows = queryExpectRows("SELECT p.name, c.name FROM products p JOIN categories c ON p.category_id = c.id ORDER BY p.name", 10)
	t.Logf("  [OK] INNER JOIN products-categories: %d rows", len(rows))
	t.Logf("       First: %v -> %v", rows[0][0], rows[0][1])

	// LEFT JOIN - tüm kategoriler (boş olanlar dahil)
	// Önce boş kategori ekle
	exec("INSERT INTO categories (id, name, description) VALUES (5, 'Sports', 'No products yet')")
	rows = queryExpectRows(`
		SELECT c.name, COUNT(p.id)
		FROM categories c
		LEFT JOIN products p ON c.id = p.category_id
		GROUP BY c.name
		ORDER BY c.name`, 5)
	t.Log("  [OK] LEFT JOIN with GROUP BY:")
	foundZero := false
	for _, row := range rows {
		t.Logf("       %v: %v products", row[0], row[1])
		if fmt.Sprintf("%v", row[1]) == "0" {
			foundZero = true
		}
	}
	if !foundZero {
		t.Fatal("  [FAIL] LEFT JOIN should show category with 0 products!")
	}
	t.Log("  [OK] LEFT JOIN correctly shows category with 0 products")

	// Multi-table JOIN: users -> orders -> order_items -> products
	rows = queryExpectRows(`
		SELECT u.username, o.id, p.name, oi.quantity, oi.unit_price
		FROM users u
		JOIN orders o ON u.id = o.user_id
		JOIN order_items oi ON o.id = oi.order_id
		JOIN products p ON oi.product_id = p.id
		WHERE o.status = 'completed'
		ORDER BY u.username, o.id`, -1)
	t.Logf("  [OK] 4-table JOIN (completed orders): %d rows", len(rows))
	for _, row := range rows {
		t.Logf("       %v | Order#%v | %v | qty:%v | $%v", row[0], row[1], row[2], row[3], row[4])
	}

	// ============================================================
	// PHASE 7: JOIN + GROUP BY (eski bug)
	// ============================================================
	t.Log("\n--- PHASE 7: JOIN + GROUP BY + WHERE ---")

	rows = queryExpectRows(`
		SELECT u.username, COUNT(o.id), SUM(o.total)
		FROM users u
		JOIN orders o ON u.id = o.user_id
		WHERE o.status = 'completed'
		GROUP BY u.username
		ORDER BY SUM(o.total) DESC`, -1)
	t.Log("  [OK] JOIN + GROUP BY + WHERE (completed orders by user):")
	for _, row := range rows {
		t.Logf("       %v: %v orders, total $%v", row[0], row[1], row[2])
	}

	// ============================================================
	// PHASE 8: Subqueries
	// ============================================================
	t.Log("\n--- PHASE 8: Subqueries ---")

	// IN subquery
	rows = queryExpectRows(`
		SELECT name, price FROM products
		WHERE category_id IN (SELECT id FROM categories WHERE name = 'Electronics')
		ORDER BY price DESC`, -1)
	t.Logf("  [OK] IN subquery (Electronics): %d products", len(rows))
	for _, row := range rows {
		t.Logf("       %v: $%v", row[0], row[1])
	}

	// Scalar subquery in SELECT
	rows = queryExpectRows(`
		SELECT name, price,
			(SELECT AVG(price) FROM products)
		FROM products
		WHERE price > (SELECT AVG(price) FROM products)
		ORDER BY price DESC`, -1)
	t.Logf("  [OK] Scalar subquery (above avg price): %d products", len(rows))
	for _, row := range rows {
		t.Logf("       %v: $%v (avg: $%v)", row[0], row[1], row[2])
	}

	// ============================================================
	// PHASE 9: CTEs (Common Table Expressions)
	// ============================================================
	t.Log("\n--- PHASE 9: CTEs ---")

	rows = queryExpectRows(`
		WITH expensive AS (
			SELECT id, name, price FROM products WHERE price > 100
		)
		SELECT name, price FROM expensive ORDER BY price DESC`, -1)
	t.Logf("  [OK] CTE (expensive products): %d results", len(rows))
	for _, row := range rows {
		t.Logf("       %v: $%v", row[0], row[1])
	}

	// ============================================================
	// PHASE 10: String Functions
	// ============================================================
	t.Log("\n--- PHASE 10: Functions ---")

	rows = queryExpectRows("SELECT UPPER(username), LENGTH(email) FROM users ORDER BY username LIMIT 3", 3)
	t.Logf("  [OK] UPPER + LENGTH: %v (email len: %v)", rows[0][0], rows[0][1])

	rows = queryExpectRows("SELECT LOWER(name), SUBSTR(name, 1, 5) FROM categories LIMIT 2", 2)
	t.Logf("  [OK] LOWER + SUBSTR: %v -> %v", rows[0][0], rows[0][1])

	rows = queryExpectRows("SELECT COALESCE(metadata, 'no metadata') FROM products WHERE id = 4", 1)
	t.Logf("  [OK] COALESCE: %v", rows[0][0])

	rows = queryExpectRows("SELECT ABS(-42)", 1)
	t.Logf("  [OK] ABS(-42) = %v", rows[0][0])

	// ============================================================
	// PHASE 11: CASE WHEN
	// ============================================================
	t.Log("\n--- PHASE 11: CASE WHEN ---")

	rows = queryExpectRows(`
		SELECT name, price,
			CASE
				WHEN price > 1000 THEN 'premium'
				WHEN price > 100 THEN 'mid-range'
				ELSE 'budget'
			END
		FROM products ORDER BY price DESC`, 10)
	t.Log("  [OK] CASE WHEN price tiers:")
	for _, row := range rows {
		t.Logf("       %v ($%v) -> %v", row[0], row[1], row[2])
	}

	// ============================================================
	// PHASE 12: Arithmetic & Expressions
	// ============================================================
	t.Log("\n--- PHASE 12: Arithmetic ---")

	rows = queryExpectRows("SELECT name, price, stock, price * stock FROM products WHERE stock > 0 ORDER BY price * stock DESC LIMIT 5", 5)
	t.Log("  [OK] Price * Stock (inventory value):")
	for _, row := range rows {
		t.Logf("       %v: $%v x %v = $%v", row[0], row[1], row[2], row[3])
	}

	// String concatenation
	rows = queryExpectRows("SELECT username || ' <' || email || '>' FROM users LIMIT 3", 3)
	t.Logf("  [OK] String concat: %v", rows[0][0])

	// ============================================================
	// PHASE 13: UPDATE
	// ============================================================
	t.Log("\n--- PHASE 13: UPDATE ---")

	exec("UPDATE products SET stock = stock - 1 WHERE id = 1")
	val := queryOne("SELECT stock FROM products WHERE id = 1")
	t.Logf("  [OK] UPDATE stock - 1: iPhone stock = %v (was 50)", val)

	exec("UPDATE products SET price = price * 0.9 WHERE category_id = 3")
	rows = queryExpectRows("SELECT name, price FROM products WHERE category_id = 3 ORDER BY name", 2)
	t.Log("  [OK] UPDATE 10%% discount on Clothing:")
	for _, row := range rows {
		t.Logf("       %v: $%v", row[0], row[1])
	}

	// Multi-column update
	exec("UPDATE users SET balance = balance + 100, full_name = 'Alice J.' WHERE username = 'alice'")
	rows = queryExpectRows("SELECT full_name, balance FROM users WHERE username = 'alice'", 1)
	t.Logf("  [OK] Multi-column update: %v, balance=$%v", rows[0][0], rows[0][1])

	// ============================================================
	// PHASE 14: DELETE
	// ============================================================
	t.Log("\n--- PHASE 14: DELETE ---")

	countBefore := queryOne("SELECT COUNT(*) FROM reviews")
	exec("DELETE FROM reviews WHERE rating < 4")
	countAfter := queryOne("SELECT COUNT(*) FROM reviews")
	t.Logf("  [OK] DELETE WHERE rating < 4: %v -> %v reviews", countBefore, countAfter)

	// ============================================================
	// PHASE 15: Transactions
	// ============================================================
	t.Log("\n--- PHASE 15: Transactions ---")

	// Transaction commit
	exec("BEGIN")
	exec("INSERT INTO categories (id, name) VALUES (10, 'Test Category')")
	exec("COMMIT")
	val = queryOne("SELECT name FROM categories WHERE id = 10")
	if fmt.Sprintf("%v", val) != "Test Category" {
		t.Fatalf("Transaction COMMIT failed: expected 'Test Category', got %v", val)
	}
	t.Log("  [OK] BEGIN + INSERT + COMMIT persisted")

	// Transaction rollback
	exec("BEGIN")
	exec("INSERT INTO categories (id, name) VALUES (11, 'Rollback Me')")
	exec("ROLLBACK")
	rows = queryExpectRows("SELECT * FROM categories WHERE id = 11", 0)
	t.Log("  [OK] BEGIN + INSERT + ROLLBACK correctly discarded")

	// ============================================================
	// PHASE 16: Views
	// ============================================================
	t.Log("\n--- PHASE 16: Views ---")

	exec("CREATE VIEW expensive_products AS SELECT name, price FROM products WHERE price > 100")
	rows = queryExpectRows("SELECT name, price FROM expensive_products ORDER BY price DESC", -1)
	t.Logf("  [OK] VIEW expensive_products: %d products", len(rows))
	for _, row := range rows {
		t.Logf("       %v: $%v", row[0], row[1])
	}

	// ============================================================
	// PHASE 17: SHOW / DESCRIBE / SET / USE (MySQL compat)
	// ============================================================
	t.Log("\n--- PHASE 17: MySQL Compatibility ---")

	rows = queryExpectRows("SHOW TABLES", -1)
	t.Logf("  [OK] SHOW TABLES: %d tables", len(rows))
	for _, row := range rows {
		t.Logf("       %v", row[0])
	}

	rows = queryExpectRows("SHOW CREATE TABLE users", 1)
	t.Logf("  [OK] SHOW CREATE TABLE users: returned DDL")

	rows = queryExpectRows("DESCRIBE products", -1)
	t.Logf("  [OK] DESCRIBE products: %d columns", len(rows))
	for _, row := range rows {
		t.Logf("       %v %v (null=%v, key=%v, extra=%v)", row[0], row[1], row[2], row[3], row[5])
	}

	rows = queryExpectRows("SHOW DATABASES", 1)
	t.Logf("  [OK] SHOW DATABASES: %v", rows[0][0])

	exec("SET NAMES utf8")
	t.Log("  [OK] SET NAMES utf8")

	exec("SET character_set_client = utf8mb4")
	t.Log("  [OK] SET character_set_client = utf8mb4")

	exec("USE cobaltdb")
	t.Log("  [OK] USE cobaltdb")

	// ============================================================
	// PHASE 18: Complex real-world queries
	// ============================================================
	t.Log("\n--- PHASE 18: Complex Real-World Queries ---")

	// En çok harcayan müşteriler
	rows = queryExpectRows(`
		SELECT u.username, u.full_name, COUNT(o.id), SUM(o.total)
		FROM users u
		JOIN orders o ON u.id = o.user_id
		GROUP BY u.username, u.full_name
		ORDER BY SUM(o.total) DESC`, -1)
	t.Log("  [OK] Top customers by spending:")
	for _, row := range rows {
		t.Logf("       %v (%v): %v orders, $%v total", row[0], row[1], row[2], row[3])
	}

	// Kategori bazlı gelir
	rows = queryExpectRows(`
		SELECT c.name, COUNT(DISTINCT p.id), SUM(oi.quantity * oi.unit_price)
		FROM categories c
		JOIN products p ON c.id = p.category_id
		JOIN order_items oi ON p.id = oi.product_id
		GROUP BY c.name
		ORDER BY SUM(oi.quantity * oi.unit_price) DESC`, -1)
	t.Log("  [OK] Revenue by category:")
	for _, row := range rows {
		t.Logf("       %v: %v products, $%v revenue", row[0], row[1], row[2])
	}

	// Ortalama üzeri fiyatlı ürünler (subquery)
	avgPrice := queryOne("SELECT AVG(price) FROM products")
	rows = queryExpectRows(fmt.Sprintf(`
		SELECT name, price FROM products
		WHERE price > %v
		ORDER BY price DESC`, avgPrice), -1)
	t.Logf("  [OK] Products above avg ($%v): %d products", avgPrice, len(rows))

	// ============================================================
	// PHASE 19: Edge Cases
	// ============================================================
	t.Log("\n--- PHASE 19: Edge Cases ---")

	// Empty table query
	exec("CREATE TABLE empty_table (id INTEGER PRIMARY KEY, val TEXT)")
	rows = queryExpectRows("SELECT COUNT(*) FROM empty_table", 1)
	t.Logf("  [OK] COUNT on empty table: %v", rows[0][0])

	rows = queryExpectRows("SELECT * FROM empty_table", 0)
	t.Log("  [OK] SELECT * on empty table: 0 rows")

	// NULL handling
	exec("CREATE TABLE null_test (id INTEGER PRIMARY KEY, a TEXT, b TEXT)")
	exec("INSERT INTO null_test (id, a, b) VALUES (1, 'hello', NULL)")
	exec("INSERT INTO null_test (id, a, b) VALUES (2, NULL, 'world')")
	exec("INSERT INTO null_test (id, a, b) VALUES (3, NULL, NULL)")

	rows = queryExpectRows("SELECT COALESCE(a, b, 'nothing') FROM null_test ORDER BY id", 3)
	t.Logf("  [OK] COALESCE with NULLs: %v, %v, %v", rows[0][0], rows[1][0], rows[2][0])

	rows = queryExpectRows("SELECT IFNULL(a, 'N/A') FROM null_test ORDER BY id", 3)
	t.Logf("  [OK] IFNULL: %v, %v, %v", rows[0][0], rows[1][0], rows[2][0])

	// Mixed case SQL
	rows = queryExpectRows("select NAME from categories where ID = 1", 1)
	t.Logf("  [OK] Mixed case SQL: %v", rows[0][0])

	// ============================================================
	// FINAL SUMMARY
	// ============================================================
	t.Log("\n========================================")
	t.Log("  ALL PHASES PASSED!")
	t.Log("========================================")

	// Final data integrity check
	finalCounts := map[string]string{
		"categories":  "SELECT COUNT(*) FROM categories",
		"users":       "SELECT COUNT(*) FROM users",
		"products":    "SELECT COUNT(*) FROM products",
		"orders":      "SELECT COUNT(*) FROM orders",
		"order_items": "SELECT COUNT(*) FROM order_items",
		"reviews":     "SELECT COUNT(*) FROM reviews",
	}
	t.Log("\nFinal row counts:")
	for table, sql := range finalCounts {
		val := queryOne(sql)
		t.Logf("  %s: %v rows", table, val)
	}
}

// TestDiskPersistenceRealWorld - Disk'e yazıp tekrar okuma gerçek senaryosu
func TestDiskPersistenceRealWorld(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "ecommerce.db")
	ctx := context.Background()

	t.Log("--- Phase 1: Create and populate ---")

	db, err := engine.Open(dbPath, &engine.Options{CacheSize: 128})
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}

	stmts := []string{
		"CREATE TABLE users (id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT NOT NULL, email TEXT UNIQUE)",
		"CREATE TABLE products (id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT, price REAL)",
		"CREATE TABLE orders (id INTEGER PRIMARY KEY AUTOINCREMENT, user_id INTEGER, product_id INTEGER, quantity INTEGER)",
	}
	for _, sql := range stmts {
		if _, err := db.Exec(ctx, sql); err != nil {
			t.Fatalf("DDL failed: %v\nSQL: %s", err, sql)
		}
	}

	// 50 users
	for i := 1; i <= 50; i++ {
		sql := fmt.Sprintf("INSERT INTO users (name, email) VALUES ('User_%d', 'user%d@test.com')", i, i)
		if _, err := db.Exec(ctx, sql); err != nil {
			t.Fatalf("Insert user %d failed: %v", i, err)
		}
	}

	// 100 products
	for i := 1; i <= 100; i++ {
		sql := fmt.Sprintf("INSERT INTO products (name, price) VALUES ('Product_%d', %.2f)", i, float64(i)*9.99)
		if _, err := db.Exec(ctx, sql); err != nil {
			t.Fatalf("Insert product %d failed: %v", i, err)
		}
	}

	// 200 orders
	for i := 1; i <= 200; i++ {
		sql := fmt.Sprintf("INSERT INTO orders (user_id, product_id, quantity) VALUES (%d, %d, %d)",
			(i%50)+1, (i%100)+1, (i%5)+1)
		if _, err := db.Exec(ctx, sql); err != nil {
			t.Fatalf("Insert order %d failed: %v", i, err)
		}
	}

	// Update some data
	db.Exec(ctx, "UPDATE products SET price = price * 1.1 WHERE id <= 10")
	db.Exec(ctx, "DELETE FROM orders WHERE quantity = 1")

	// Capture expected values
	rows, _ := db.Query(ctx, "SELECT COUNT(*) FROM users")
	rows.Next()
	var userCount int
	rows.Scan(&userCount)
	rows.Close()

	rows, _ = db.Query(ctx, "SELECT COUNT(*) FROM orders")
	rows.Next()
	var orderCount int
	rows.Scan(&orderCount)
	rows.Close()

	rows, _ = db.Query(ctx, "SELECT price FROM products WHERE id = 5")
	rows.Next()
	var price5 string
	rows.Scan(&price5)
	rows.Close()

	t.Logf("  Before close: users=%d, orders=%d, product5_price=%s", userCount, orderCount, price5)

	db.Close()
	t.Log("  Database closed.")

	// Phase 2: Reopen and verify
	t.Log("--- Phase 2: Reopen and verify ---")

	db2, err := engine.Open(dbPath, &engine.Options{CacheSize: 128})
	if err != nil {
		t.Fatalf("Reopen failed: %v", err)
	}
	defer db2.Close()

	// Verify counts
	rows, _ = db2.Query(ctx, "SELECT COUNT(*) FROM users")
	rows.Next()
	var userCount2 int
	rows.Scan(&userCount2)
	rows.Close()

	rows, _ = db2.Query(ctx, "SELECT COUNT(*) FROM orders")
	rows.Next()
	var orderCount2 int
	rows.Scan(&orderCount2)
	rows.Close()

	rows, _ = db2.Query(ctx, "SELECT price FROM products WHERE id = 5")
	rows.Next()
	var price5_2 string
	rows.Scan(&price5_2)
	rows.Close()

	t.Logf("  After reopen: users=%d, orders=%d, product5_price=%s", userCount2, orderCount2, price5_2)

	if userCount2 != userCount {
		t.Fatalf("User count mismatch: before=%d, after=%d", userCount, userCount2)
	}
	if orderCount2 != orderCount {
		t.Fatalf("Order count mismatch: before=%d, after=%d", orderCount, orderCount2)
	}
	if price5_2 != price5 {
		t.Fatalf("Price mismatch: before=%s, after=%s", price5, price5_2)
	}

	t.Log("  [OK] All data persisted correctly!")

	// Phase 3: Reopen'da da JOIN çalışıyor mu
	t.Log("--- Phase 3: Complex query after reopen ---")

	rows, err = db2.Query(ctx, `
		SELECT u.name, COUNT(o.id), SUM(o.quantity)
		FROM users u
		JOIN orders o ON u.id = o.user_id
		GROUP BY u.name
		ORDER BY SUM(o.quantity) DESC
		LIMIT 5`)
	if err != nil {
		t.Fatalf("JOIN after reopen failed: %v", err)
	}
	defer rows.Close()

	count := 0
	for rows.Next() {
		var name string
		var orderCnt, totalQty int
		rows.Scan(&name, &orderCnt, &totalQty)
		t.Logf("  %s: %d orders, %d total qty", name, orderCnt, totalQty)
		count++
	}
	if count == 0 {
		t.Fatal("No results from JOIN after reopen!")
	}
	t.Logf("  [OK] JOIN query works after reopen (%d rows)", count)

	// AUTOINCREMENT devam ediyor mu
	t.Log("--- Phase 4: AutoIncrement continues ---")
	db2.Exec(ctx, "INSERT INTO users (name, email) VALUES ('NewUser', 'new@test.com')")
	rows, _ = db2.Query(ctx, "SELECT id FROM users WHERE name = 'NewUser'")
	rows.Next()
	var newID int
	rows.Scan(&newID)
	rows.Close()

	if newID <= 50 {
		t.Fatalf("AutoIncrement not continuing: new ID=%d, expected > 50", newID)
	}
	t.Logf("  [OK] AutoIncrement continues: new user ID = %d", newID)
}
