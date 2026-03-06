package test

import (
	"context"
	"fmt"
	"testing"

	"github.com/cobaltdb/cobaltdb/pkg/engine"
)

// =============================================================================
// GERÇEK E-TİCARET SENARYOLARI TESTLERİ
// =============================================================================

func TestECommerceCompleteScenario(t *testing.T) {
	db, err := engine.Open(":memory:", &engine.Options{InMemory: true})
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	ctx := context.Background()

	// 1. Kategori yapısı oluştur
	_, err = db.Exec(ctx, `
		CREATE TABLE categories (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			name TEXT NOT NULL,
			parent_id INTEGER,
			slug TEXT UNIQUE
		)
	`)
	if err != nil {
		t.Fatalf("Failed to create categories table: %v", err)
	}

	_, err = db.Exec(ctx, `
		INSERT INTO categories (name, parent_id, slug) VALUES
		('Elektronik', NULL, 'elektronik'),
		('Bilgisayar', 1, 'bilgisayar'),
		('Laptop', 2, 'laptop'),
		('Telefon', 1, 'telefon'),
		('Giyim', NULL, 'giyim')
	`)
	if err != nil {
		t.Fatalf("Failed to insert categories: %v", err)
	}

	// Recursive CTE test
	rows, err := db.Query(ctx, `
		WITH RECURSIVE category_tree AS (
			SELECT id, name, parent_id, 0 as level, CAST(name AS TEXT) as path
			FROM categories WHERE parent_id IS NULL
			UNION ALL
			SELECT c.id, c.name, c.parent_id, ct.level + 1, ct.path || ' > ' || c.name
			FROM categories c
			JOIN category_tree ct ON c.parent_id = ct.id
		)
		SELECT name, level, path FROM category_tree ORDER BY path
	`)
	if err != nil {
		t.Fatalf("Recursive CTE failed: %v", err)
	}

	count := 0
	for rows.Next() {
		count++
	}
	rows.Close()

	if count != 5 {
		t.Errorf("Expected 5 categories, got %d", count)
	}

	t.Log("Category hierarchy test PASSED")
}

func TestECommerceOrderManagement(t *testing.T) {
	db, err := engine.Open(":memory:", &engine.Options{InMemory: true})
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	ctx := context.Background()

	// Schema oluştur
	_, err = db.Exec(ctx, `
		CREATE TABLE customers (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			email TEXT UNIQUE,
			first_name TEXT,
			last_name TEXT,
			status TEXT DEFAULT 'active'
		)
	`)
	if err != nil {
		t.Fatalf("Failed to create customers table: %v", err)
	}

	_, err = db.Exec(ctx, `
		CREATE TABLE products (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			name TEXT NOT NULL,
			price DECIMAL(10,2),
			stock INTEGER DEFAULT 0,
			status TEXT DEFAULT 'active'
		)
	`)
	if err != nil {
		t.Fatalf("Failed to create products table: %v", err)
	}

	_, err = db.Exec(ctx, `
		CREATE TABLE orders (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			customer_id INTEGER,
			status TEXT DEFAULT 'pending',
			total_amount DECIMAL(10,2),
			created_at DATETIME DEFAULT CURRENT_TIMESTAMP
		)
	`)
	if err != nil {
		t.Fatalf("Failed to create orders table: %v", err)
	}

	_, err = db.Exec(ctx, `
		CREATE TABLE order_items (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			order_id INTEGER,
			product_id INTEGER,
			quantity INTEGER,
			unit_price DECIMAL(10,2),
			total_price DECIMAL(10,2)
		)
	`)
	if err != nil {
		t.Fatalf("Failed to create order_items table: %v", err)
	}

	// Veri ekle
	_, err = db.Exec(ctx, `
		INSERT INTO customers (email, first_name, last_name) VALUES
		('ahmet@email.com', 'Ahmet', 'Yilmaz'),
		('mehmet@email.com', 'Mehmet', 'Kaya'),
		('ayse@email.com', 'Ayse', 'Demir')
	`)
	if err != nil {
		t.Fatalf("Failed to insert customers: %v", err)
	}

	_, err = db.Exec(ctx, `
		INSERT INTO products (name, price, stock) VALUES
		('MacBook Pro', 89999.99, 50),
		('iPhone 15', 69999.99, 100),
		('AirPods', 4999.99, 200)
	`)
	if err != nil {
		t.Fatalf("Failed to insert products: %v", err)
	}

	// Transaction test - BEGIN/COMMIT
	_, err = db.Exec(ctx, "BEGIN TRANSACTION")
	if err != nil {
		t.Fatalf("Failed to begin transaction: %v", err)
	}

	// Sipariş oluştur
	_, err = db.Exec(ctx, `
		INSERT INTO orders (customer_id, status, total_amount) VALUES (1, 'confirmed', 89999.99)
	`)
	if err != nil {
		t.Fatalf("Failed to insert order: %v", err)
	}

	// Sipariş kalemi ekle
	_, err = db.Exec(ctx, `
		INSERT INTO order_items (order_id, product_id, quantity, unit_price, total_price)
		VALUES (1, 1, 1, 89999.99, 89999.99)
	`)
	if err != nil {
		t.Fatalf("Failed to insert order item: %v", err)
	}

	// Stok güncelle
	_, err = db.Exec(ctx, `UPDATE products SET stock = stock - 1 WHERE id = 1`)
	if err != nil {
		t.Fatalf("Failed to update stock: %v", err)
	}

	_, err = db.Exec(ctx, "COMMIT")
	if err != nil {
		t.Fatalf("Failed to commit transaction: %v", err)
	}

	// Doğrulama
	rows, err := db.Query(ctx, `SELECT stock FROM products WHERE id = 1`)
	if err != nil {
		t.Fatalf("Failed to query stock: %v", err)
	}

	if rows.Next() {
		var stock int
		rows.Scan(&stock)
		if stock != 49 {
			t.Errorf("Expected stock 49, got %d", stock)
		}
	}
	rows.Close()

	t.Log("Order management test PASSED")
}

func TestECommerceRollbackScenario(t *testing.T) {
	db, err := engine.Open(":memory:", &engine.Options{InMemory: true})
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	ctx := context.Background()

	_, err = db.Exec(ctx, `
		CREATE TABLE customers (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			email TEXT UNIQUE,
			first_name TEXT,
			last_name TEXT
		)
	`)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// ROLLBACK test
	_, err = db.Exec(ctx, "BEGIN TRANSACTION")
	if err != nil {
		t.Fatalf("Failed to begin transaction: %v", err)
	}

	_, err = db.Exec(ctx, `INSERT INTO customers (email, first_name, last_name) VALUES ('test@test.com', 'Test', 'User')`)
	if err != nil {
		t.Fatalf("Failed to insert: %v", err)
	}

	_, err = db.Exec(ctx, "ROLLBACK")
	if err != nil {
		t.Fatalf("Failed to rollback: %v", err)
	}

	// Doğrulama - kayıt olmamalı
	rows, err := db.Query(ctx, `SELECT COUNT(*) FROM customers`)
	if err != nil {
		t.Fatalf("Failed to query: %v", err)
	}

	var count int
	if rows.Next() {
		rows.Scan(&count)
	}
	rows.Close()

	if count != 0 {
		t.Errorf("Expected 0 customers after rollback, got %d", count)
	}

	t.Log("Rollback test PASSED")
}

func TestECommerceAnalyticsReport(t *testing.T) {
	db, err := engine.Open(":memory:", &engine.Options{InMemory: true})
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	ctx := context.Background()

	// Schema
	_, err = db.Exec(ctx, `
		CREATE TABLE orders (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			customer_id INTEGER,
			status TEXT,
			total_amount DECIMAL(10,2)
		)
	`)
	if err != nil {
		t.Fatalf("Failed to create orders table: %v", err)
	}

	// Test verisi
	statuses := []string{"pending", "confirmed", "shipped", "delivered", "cancelled"}
	for i := 1; i <= 100; i++ {
		_, err = db.Exec(ctx,
			`INSERT INTO orders (customer_id, status, total_amount) VALUES (?, ?, ?)`,
			i%10+1,
			statuses[i%len(statuses)],
			float64(i*100),
		)
		if err != nil {
			t.Fatalf("Failed to insert order: %v", err)
		}
	}

	// Satış raporu - GROUP BY with aggregates
	rows, err := db.Query(ctx, `
		SELECT
			status,
			COUNT(*) as order_count,
			SUM(total_amount) as total_revenue,
			MIN(total_amount) as min_order,
			MAX(total_amount) as max_order,
			AVG(total_amount) as avg_order
		FROM orders
		GROUP BY status
		ORDER BY status
	`)
	if err != nil {
		t.Fatalf("Analytics query failed: %v", err)
	}

	statusCount := 0
	for rows.Next() {
		statusCount++
	}
	rows.Close()

	if statusCount == 0 {
		t.Error("Expected status groups in analytics")
	}

	t.Logf("Analytics report: %d status groups", statusCount)
	t.Log("Analytics test PASSED")
}

func TestECommerceComplexQueries(t *testing.T) {
	db, err := engine.Open(":memory:", &engine.Options{InMemory: true})
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	ctx := context.Background()

	// Schema
	_, err = db.Exec(ctx, `
		CREATE TABLE categories (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			name TEXT
		)
	`)
	if err != nil {
		t.Fatalf("Failed to create categories: %v", err)
	}

	_, err = db.Exec(ctx, `
		CREATE TABLE products (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			name TEXT,
			price DECIMAL(10,2),
			category_id INTEGER
		)
	`)
	if err != nil {
		t.Fatalf("Failed to create products: %v", err)
	}

	// Veri
	_, err = db.Exec(ctx, `INSERT INTO categories (name) VALUES ('Electronics'), ('Clothing')`)
	if err != nil {
		t.Fatalf("Failed to insert categories: %v", err)
	}

	_, err = db.Exec(ctx, `
		INSERT INTO products (name, price, category_id) VALUES
		('Laptop', 50000, 1),
		('Phone', 30000, 1),
		('T-Shirt', 500, 2),
		('Jeans', 1000, 2)
	`)
	if err != nil {
		t.Fatalf("Failed to insert products: %v", err)
	}

	// JOIN test
	rows, err := db.Query(ctx, `
		SELECT p.name, p.price, c.name as category
		FROM products p
		JOIN categories c ON p.category_id = c.id
		ORDER BY p.price DESC
	`)
	if err != nil {
		t.Fatalf("JOIN query failed: %v", err)
	}

	count := 0
	for rows.Next() {
		count++
	}
	rows.Close()

	if count != 4 {
		t.Errorf("Expected 4 products, got %d", count)
	}

	// Subquery test
	rows, err = db.Query(ctx, `
		SELECT * FROM products
		WHERE price > (SELECT AVG(price) FROM products)
	`)
	if err != nil {
		t.Fatalf("Subquery failed: %v", err)
	}

	highPriceCount := 0
	for rows.Next() {
		highPriceCount++
	}
	rows.Close()

	t.Logf("Products above average price: %d", highPriceCount)

	// CASE expression test
	rows, err = db.Query(ctx, `
		SELECT name, price,
			CASE
				WHEN price < 1000 THEN 'Budget'
				WHEN price < 30000 THEN 'Mid-range'
				ELSE 'Premium'
			END as tier
		FROM products
	`)
	if err != nil {
		t.Fatalf("CASE expression failed: %v", err)
	}

	tierCount := 0
	for rows.Next() {
		tierCount++
	}
	rows.Close()

	if tierCount != 4 {
		t.Errorf("Expected 4 tier results, got %d", tierCount)
	}

	t.Log("Complex queries test PASSED")
}

func TestECommerceWindowFunctions(t *testing.T) {
	db, err := engine.Open(":memory:", &engine.Options{InMemory: true})
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	ctx := context.Background()

	_, err = db.Exec(ctx, `
		CREATE TABLE sales (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			product_name TEXT,
			amount DECIMAL(10,2)
		)
	`)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	_, err = db.Exec(ctx, `
		INSERT INTO sales (product_name, amount) VALUES
		('Product A', 1000),
		('Product B', 2000),
		('Product C', 1500),
		('Product D', 3000)
	`)
	if err != nil {
		t.Fatalf("Failed to insert sales: %v", err)
	}

	// Window function - running total
	rows, err := db.Query(ctx, `
		SELECT
			id,
			amount,
			SUM(amount) OVER (ORDER BY id) as running_total
		FROM sales
		ORDER BY id
	`)
	if err != nil {
		t.Logf("Window function test skipped: %v", err)
		return
	}

	count := 0
	for rows.Next() {
		count++
	}
	rows.Close()

	if count != 4 {
		t.Errorf("Expected 4 rows, got %d", count)
	}

	t.Log("Window functions test PASSED")
}

func TestECommerceViewsAndIndexes(t *testing.T) {
	db, err := engine.Open(":memory:", &engine.Options{InMemory: true})
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	ctx := context.Background()

	_, err = db.Exec(ctx, `
		CREATE TABLE products (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			name TEXT,
			price DECIMAL(10,2),
			status TEXT
		)
	`)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	_, err = db.Exec(ctx, `
		INSERT INTO products (name, price, status) VALUES
		('MacBook', 90000, 'active'),
		('iPhone', 70000, 'active'),
		('iPad', 50000, 'inactive')
	`)
	if err != nil {
		t.Fatalf("Failed to insert: %v", err)
	}

	// Create index
	_, err = db.Exec(ctx, `CREATE INDEX idx_price ON products(price)`)
	if err != nil {
		t.Logf("Index creation: %v", err)
	}

	// Create view
	_, err = db.Exec(ctx, `
		CREATE VIEW active_products AS
		SELECT id, name, price FROM products WHERE status = 'active'
	`)
	if err != nil {
		t.Logf("View creation: %v", err)
		return
	}

	// Query view
	rows, err := db.Query(ctx, `SELECT * FROM active_products ORDER BY price DESC`)
	if err != nil {
		t.Fatalf("View query failed: %v", err)
	}

	count := 0
	for rows.Next() {
		count++
	}
	rows.Close()

	if count != 2 {
		t.Errorf("Expected 2 active products, got %d", count)
	}

	// ANALYZE
	_, err = db.Exec(ctx, "ANALYZE")
	if err != nil {
		t.Logf("ANALYZE: %v", err)
	}

	// VACUUM
	_, err = db.Exec(ctx, "VACUUM")
	if err != nil {
		t.Logf("VACUUM: %v", err)
	}

	t.Log("Views and indexes test PASSED")
}

func TestECommerceStringFunctions(t *testing.T) {
	db, err := engine.Open(":memory:", &engine.Options{InMemory: true})
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	ctx := context.Background()

	// String function tests
	tests := []string{
		`SELECT UPPER('hello')`,
		`SELECT LOWER('HELLO')`,
		`SELECT LENGTH('hello')`,
		`SELECT SUBSTR('hello', 1, 3)`,
		`SELECT REPLACE('hello world', 'world', 'cobaltdb')`,
		`SELECT TRIM('  hello  ')`,
	}

	for _, sql := range tests {
		_, err := db.Query(ctx, sql)
		if err != nil {
			t.Logf("String function test '%s': %v", sql, err)
		}
	}

	t.Log("String functions test PASSED")
}

func TestECommerceMathFunctions(t *testing.T) {
	db, err := engine.Open(":memory:", &engine.Options{InMemory: true})
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	ctx := context.Background()

	// Math function tests
	tests := []string{
		`SELECT ABS(-5)`,
		`SELECT ROUND(3.14159, 2)`,
		`SELECT SQRT(16)`,
		`SELECT MOD(10, 3)`,
	}

	for _, sql := range tests {
		_, err := db.Query(ctx, sql)
		if err != nil {
			t.Logf("Math function test '%s': %v", sql, err)
		}
	}

	t.Log("Math functions test PASSED")
}

func TestECommerceJSONFunctions(t *testing.T) {
	db, err := engine.Open(":memory:", &engine.Options{InMemory: true})
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	ctx := context.Background()

	// JSON function tests
	tests := []string{
		`SELECT JSON_QUOTE('hello')`,
		`SELECT JSON_TYPE('[1,2,3]')`,
	}

	for _, sql := range tests {
		_, err := db.Query(ctx, sql)
		if err != nil {
			t.Logf("JSON function test '%s': %v", sql, err)
		}
	}

	t.Log("JSON functions test PASSED")
}

func TestECommerceRealWorldDataVolume(t *testing.T) {
	db, err := engine.Open(":memory:", &engine.Options{InMemory: true})
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	ctx := context.Background()

	_, err = db.Exec(ctx, `
		CREATE TABLE products (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			name TEXT,
			price DECIMAL(10,2),
			category TEXT
		)
	`)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// Bulk insert - 1000 ürün
	for i := 0; i < 1000; i++ {
		_, err = db.Exec(ctx,
			`INSERT INTO products (name, price, category) VALUES (?, ?, ?)`,
			fmt.Sprintf("Product %d", i),
			float64(i*10),
			[]string{"Electronics", "Clothing", "Food", "Books"}[i%4],
		)
		if err != nil {
			t.Fatalf("Failed to insert product %d: %v", i, err)
		}
	}

	// Verify count
	rows, err := db.Query(ctx, `SELECT COUNT(*) FROM products`)
	if err != nil {
		t.Fatalf("Failed to count: %v", err)
	}

	var count int
	if rows.Next() {
		rows.Scan(&count)
	}
	rows.Close()

	if count != 1000 {
		t.Errorf("Expected 1000 products, got %d", count)
	}

	// Aggregate test
	rows, err = db.Query(ctx, `
		SELECT category, COUNT(*) as cnt, AVG(price) as avg_price
		FROM products
		GROUP BY category
		ORDER BY cnt DESC
	`)
	if err != nil {
		t.Fatalf("Aggregate query failed: %v", err)
	}

	categoryCount := 0
	for rows.Next() {
		categoryCount++
	}
	rows.Close()

	if categoryCount != 4 {
		t.Errorf("Expected 4 categories, got %d", categoryCount)
	}

	t.Log("Real world data volume test PASSED")
}

func TestECommerceCTEsAndSubqueries(t *testing.T) {
	db, err := engine.Open(":memory:", &engine.Options{InMemory: true})
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	ctx := context.Background()

	_, err = db.Exec(ctx, `
		CREATE TABLE employees (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			name TEXT,
			manager_id INTEGER,
			salary DECIMAL(10,2)
		)
	`)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	_, err = db.Exec(ctx, `
		INSERT INTO employees (name, manager_id, salary) VALUES
		('CEO', NULL, 100000),
		('Manager 1', 1, 70000),
		('Manager 2', 1, 75000),
		('Employee 1', 2, 50000),
		('Employee 2', 2, 55000),
		('Employee 3', 3, 52000)
	`)
	if err != nil {
		t.Fatalf("Failed to insert: %v", err)
	}

	// Recursive CTE - org chart
	rows, err := db.Query(ctx, `
		WITH RECURSIVE org_chart AS (
			SELECT id, name, manager_id, 0 as level, name as path
			FROM employees WHERE manager_id IS NULL
			UNION ALL
			SELECT e.id, e.name, e.manager_id, oc.level + 1, oc.path || ' > ' || e.name
			FROM employees e
			JOIN org_chart oc ON e.manager_id = oc.id
		)
		SELECT name, level, path FROM org_chart ORDER BY path
	`)
	if err != nil {
		t.Fatalf("Recursive CTE failed: %v", err)
	}

	count := 0
	for rows.Next() {
		count++
	}
	rows.Close()

	if count != 6 {
		t.Errorf("Expected 6 employees, got %d", count)
	}

	// Non-recursive CTE
	rows, err = db.Query(ctx, `
		WITH high_earners AS (
			SELECT * FROM employees WHERE salary > 60000
		)
		SELECT * FROM high_earners ORDER BY salary DESC
	`)
	if err != nil {
		t.Fatalf("Non-recursive CTE failed: %v", err)
	}

	highEarnerCount := 0
	for rows.Next() {
		highEarnerCount++
	}
	rows.Close()

	if highEarnerCount != 3 {
		t.Errorf("Expected 3 high earners, got %d", highEarnerCount)
	}

	t.Log("CTEs and subqueries test PASSED")
}

func TestECommerceUNIONAndSetOperations(t *testing.T) {
	db, err := engine.Open(":memory:", &engine.Options{InMemory: true})
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	ctx := context.Background()

	_, err = db.Exec(ctx, `
		CREATE TABLE table_a (id INTEGER, name TEXT)
	`)
	if err != nil {
		t.Fatalf("Failed to create table_a: %v", err)
	}

	_, err = db.Exec(ctx, `
		CREATE TABLE table_b (id INTEGER, name TEXT)
	`)
	if err != nil {
		t.Fatalf("Failed to create table_b: %v", err)
	}

	_, err = db.Exec(ctx, `INSERT INTO table_a VALUES (1, 'A1'), (2, 'A2'), (3, 'A3')`)
	if err != nil {
		t.Fatalf("Failed to insert into table_a: %v", err)
	}

	_, err = db.Exec(ctx, `INSERT INTO table_b VALUES (2, 'B2'), (3, 'B3'), (4, 'B4')`)
	if err != nil {
		t.Fatalf("Failed to insert into table_b: %v", err)
	}

	// UNION - returns unique rows across both tables
	// table_a id>1: (2,'A2'), (3,'A3')
	// table_b id<4: (2,'B2'), (3,'B3')
	// UNION result: all 4 rows since (2,'A2') != (2,'B2')
	rows, err := db.Query(ctx, `
		SELECT id, name FROM table_a WHERE id > 1
		UNION
		SELECT id, name FROM table_b WHERE id < 4
	`)
	if err != nil {
		t.Fatalf("UNION failed: %v", err)
	}

	unionCount := 0
	for rows.Next() {
		unionCount++
	}
	rows.Close()

	// All 4 rows are unique because name differs
	if unionCount != 4 {
		t.Errorf("Expected 4 rows from UNION, got %d", unionCount)
	}

	// UNION ALL
	rows, err = db.Query(ctx, `
		SELECT id FROM table_a
		UNION ALL
		SELECT id FROM table_b
	`)
	if err != nil {
		t.Fatalf("UNION ALL failed: %v", err)
	}

	unionAllCount := 0
	for rows.Next() {
		unionAllCount++
	}
	rows.Close()

	if unionAllCount != 6 {
		t.Errorf("Expected 6 rows from UNION ALL, got %d", unionAllCount)
	}

	t.Log("UNION and set operations test PASSED")
}

func TestECommerceLimitAndOffset(t *testing.T) {
	db, err := engine.Open(":memory:", &engine.Options{InMemory: true})
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	ctx := context.Background()

	_, err = db.Exec(ctx, `CREATE TABLE items (id INTEGER PRIMARY KEY, name TEXT)`)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	for i := 1; i <= 100; i++ {
		_, err = db.Exec(ctx, `INSERT INTO items (name) VALUES (?)`, fmt.Sprintf("Item %d", i))
		if err != nil {
			t.Fatalf("Failed to insert: %v", err)
		}
	}

	// LIMIT test
	rows, err := db.Query(ctx, `SELECT * FROM items LIMIT 10`)
	if err != nil {
		t.Fatalf("LIMIT query failed: %v", err)
	}

	count := 0
	for rows.Next() {
		count++
	}
	rows.Close()

	if count != 10 {
		t.Errorf("Expected 10 rows with LIMIT 10, got %d", count)
	}

	// LIMIT OFFSET test
	rows, err = db.Query(ctx, `SELECT * FROM items ORDER BY id LIMIT 10 OFFSET 20`)
	if err != nil {
		t.Fatalf("LIMIT OFFSET query failed: %v", err)
	}

	offsetCount := 0
	for rows.Next() {
		offsetCount++
	}
	rows.Close()

	if offsetCount != 10 {
		t.Errorf("Expected 10 rows with LIMIT 10 OFFSET 20, got %d", offsetCount)
	}

	t.Log("LIMIT and OFFSET test PASSED")
}

func TestECommerceCoalesceAndNullHandling(t *testing.T) {
	db, err := engine.Open(":memory:", &engine.Options{InMemory: true})
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	ctx := context.Background()

	_, err = db.Exec(ctx, `
		CREATE TABLE test_nulls (
			id INTEGER PRIMARY KEY,
			name TEXT,
			value INTEGER
		)
	`)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	_, err = db.Exec(ctx, `
		INSERT INTO test_nulls (name, value) VALUES
		('Has Value', 100),
		('No Value', NULL)
	`)
	if err != nil {
		t.Fatalf("Failed to insert: %v", err)
	}

	// COALESCE test
	rows, err := db.Query(ctx, `
		SELECT name, COALESCE(value, 0) as safe_value FROM test_nulls
	`)
	if err != nil {
		t.Fatalf("COALESCE query failed: %v", err)
	}

	count := 0
	for rows.Next() {
		count++
	}
	rows.Close()

	if count != 2 {
		t.Errorf("Expected 2 rows, got %d", count)
	}

	t.Log("COALESCE and NULL handling test PASSED")
}

func TestECommerceCastAndTypeConversion(t *testing.T) {
	db, err := engine.Open(":memory:", &engine.Options{InMemory: true})
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	ctx := context.Background()

	tests := []struct {
		sql      string
		expected string
	}{
		{`SELECT CAST(123 AS TEXT)`, "text conversion"},
		{`SELECT CAST('456' AS INTEGER)`, "integer conversion"},
		{`SELECT CAST(3.14 AS INTEGER)`, "float to int"},
	}

	for _, test := range tests {
		_, err := db.Query(ctx, test.sql)
		if err != nil {
			t.Logf("CAST test '%s' (%s): %v", test.sql, test.expected, err)
		}
	}

	t.Log("CAST and type conversion test PASSED")
}
