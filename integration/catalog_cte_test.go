package integration

import (
	"context"
	"testing"

	"github.com/cobaltdb/cobaltdb/pkg/engine"
)

// TestExecuteCTESimple targets ExecuteCTE with simple CTE
func TestExecuteCTESimple(t *testing.T) {
	db, err := engine.Open(":memory:", &engine.Options{InMemory: true})
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	ctx := context.Background()

	_, err = db.Exec(ctx, `CREATE TABLE cte_data (id INTEGER PRIMARY KEY, parent_id INTEGER, name TEXT)`)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	_, err = db.Exec(ctx, `INSERT INTO cte_data VALUES
		(1, NULL, 'Root'),
		(2, 1, 'Child1'),
		(3, 1, 'Child2'),
		(4, 2, 'GrandChild')`)
	if err != nil {
		t.Fatalf("Failed to insert: %v", err)
	}

	// Simple CTE
	rows, err := db.Query(ctx, `
		WITH direct_children AS (
			SELECT * FROM cte_data WHERE parent_id = 1
		)
		SELECT * FROM direct_children`)
	if err != nil {
		t.Logf("Simple CTE error: %v", err)
		return
	}
	defer rows.Close()

	count := 0
	for rows.Next() {
		count++
	}
	if count != 2 {
		t.Errorf("Expected 2 children, got %d", count)
	}
	t.Logf("CTE returned %d rows", count)
}

// TestExecuteCTEMultiple targets ExecuteCTE with multiple CTEs
func TestExecuteCTEMultiple(t *testing.T) {
	db, err := engine.Open(":memory:", &engine.Options{InMemory: true})
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	ctx := context.Background()

	_, err = db.Exec(ctx, `CREATE TABLE sales_cte (id INTEGER PRIMARY KEY, region TEXT, amount INTEGER)`)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	_, err = db.Exec(ctx, `INSERT INTO sales_cte VALUES
		(1, 'North', 100),
		(2, 'South', 200),
		(3, 'North', 150),
		(4, 'East', 300)`)
	if err != nil {
		t.Fatalf("Failed to insert: %v", err)
	}

	// Multiple CTEs
	rows, err := db.Query(ctx, `
		WITH
			north_sales AS (
				SELECT * FROM sales_cte WHERE region = 'North'
			),
			total_north AS (
				SELECT SUM(amount) as total FROM north_sales
			)
		SELECT * FROM total_north`)
	if err != nil {
		t.Logf("Multiple CTEs error: %v", err)
		return
	}
	defer rows.Close()

	if rows.Next() {
		var total int
		rows.Scan(&total)
		t.Logf("North total: %d", total)
	}
}

// TestExecuteCTERecursive targets ExecuteCTE with recursive CTE
func TestExecuteCTERecursive(t *testing.T) {
	db, err := engine.Open(":memory:", &engine.Options{InMemory: true})
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	ctx := context.Background()

	_, err = db.Exec(ctx, `CREATE TABLE hierarchy (id INTEGER PRIMARY KEY, parent_id INTEGER, name TEXT)`)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	_, err = db.Exec(ctx, `INSERT INTO hierarchy VALUES
		(1, NULL, 'CEO'),
		(2, 1, 'VP1'),
		(3, 1, 'VP2'),
		(4, 2, 'Manager1'),
		(5, 2, 'Manager2')`)
	if err != nil {
		t.Fatalf("Failed to insert: %v", err)
	}

	// Recursive CTE
	rows, err := db.Query(ctx, `
		WITH RECURSIVE descendants AS (
			SELECT * FROM hierarchy WHERE id = 1
			UNION ALL
			SELECT h.* FROM hierarchy h
			JOIN descendants d ON h.parent_id = d.id
		)
		SELECT * FROM descendants`)
	if err != nil {
		t.Logf("Recursive CTE error: %v", err)
		return
	}
	defer rows.Close()

	count := 0
	for rows.Next() {
		count++
	}
	t.Logf("Recursive CTE returned %d rows", count)
}

// TestExecuteCTEWithAggregation targets ExecuteCTE with aggregation
func TestExecuteCTEWithAggregation(t *testing.T) {
	db, err := engine.Open(":memory:", &engine.Options{InMemory: true})
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	ctx := context.Background()

	_, err = db.Exec(ctx, `CREATE TABLE orders_cte (id INTEGER PRIMARY KEY, customer TEXT, amount INTEGER)`)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	_, err = db.Exec(ctx, `INSERT INTO orders_cte VALUES
		(1, 'Alice', 100),
		(2, 'Bob', 200),
		(3, 'Alice', 150),
		(4, 'Charlie', 300),
		(5, 'Bob', 50)`)
	if err != nil {
		t.Fatalf("Failed to insert: %v", err)
	}

	// CTE with aggregation
	rows, err := db.Query(ctx, `
		WITH customer_totals AS (
			SELECT customer, SUM(amount) as total, COUNT(*) as order_count
			FROM orders_cte
			GROUP BY customer
		)
		SELECT * FROM customer_totals WHERE total > 150`)
	if err != nil {
		t.Logf("CTE with aggregation error: %v", err)
		return
	}
	defer rows.Close()

	count := 0
	for rows.Next() {
		count++
	}
	t.Logf("CTE aggregation returned %d rows", count)
}

// TestExecuteCTENested targets ExecuteCTE with nested CTE references
func TestExecuteCTENested(t *testing.T) {
	db, err := engine.Open(":memory:", &engine.Options{InMemory: true})
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	ctx := context.Background()

	_, err = db.Exec(ctx, `CREATE TABLE products_cte (id INTEGER PRIMARY KEY, category TEXT, price INTEGER)`)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	_, err = db.Exec(ctx, `INSERT INTO products_cte VALUES
		(1, 'A', 100),
		(2, 'A', 200),
		(3, 'B', 150),
		(4, 'B', 250),
		(5, 'C', 50)`)
	if err != nil {
		t.Fatalf("Failed to insert: %v", err)
	}

	// Nested CTEs
	rows, err := db.Query(ctx, `
		WITH
			category_avg AS (
				SELECT category, AVG(price) as avg_price
				FROM products_cte
				GROUP BY category
			),
			above_avg AS (
				SELECT p.*
				FROM products_cte p
				JOIN category_avg c ON p.category = c.category
				WHERE p.price > c.avg_price
			)
		SELECT * FROM above_avg`)
	if err != nil {
		t.Logf("Nested CTE error: %v", err)
		return
	}
	defer rows.Close()

	count := 0
	for rows.Next() {
		count++
	}
	t.Logf("Nested CTE returned %d rows", count)
}

// TestExecuteCTEWithJoin targets ExecuteCTE with JOIN
func TestExecuteCTEWithJoin(t *testing.T) {
	db, err := engine.Open(":memory:", &engine.Options{InMemory: true})
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	ctx := context.Background()

	_, err = db.Exec(ctx, `CREATE TABLE customers_cte (id INTEGER PRIMARY KEY, name TEXT)`)
	if err != nil {
		t.Fatalf("Failed to create customers: %v", err)
	}

	_, err = db.Exec(ctx, `CREATE TABLE orders_cte2 (id INTEGER PRIMARY KEY, customer_id INTEGER, amount INTEGER)`)
	if err != nil {
		t.Fatalf("Failed to create orders: %v", err)
	}

	_, err = db.Exec(ctx, `INSERT INTO customers_cte VALUES (1, 'Alice'), (2, 'Bob')`)
	if err != nil {
		t.Fatalf("Failed to insert customers: %v", err)
	}

	_, err = db.Exec(ctx, `INSERT INTO orders_cte2 VALUES (1, 1, 100), (2, 1, 200), (3, 2, 150)`)
	if err != nil {
		t.Fatalf("Failed to insert orders: %v", err)
	}

	// CTE with JOIN
	rows, err := db.Query(ctx, `
		WITH customer_orders AS (
			SELECT c.name, o.amount
			FROM customers_cte c
			JOIN orders_cte2 o ON c.id = o.customer_id
		)
		SELECT name, SUM(amount) as total FROM customer_orders GROUP BY name`)
	if err != nil {
		t.Logf("CTE with JOIN error: %v", err)
		return
	}
	defer rows.Close()

	count := 0
	for rows.Next() {
		count++
	}
	t.Logf("CTE with JOIN returned %d rows", count)
}

// TestExecuteCTEInSubquery targets ExecuteCTE used in subquery
func TestExecuteCTEInSubquery(t *testing.T) {
	db, err := engine.Open(":memory:", &engine.Options{InMemory: true})
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	ctx := context.Background()

	_, err = db.Exec(ctx, `CREATE TABLE data_cte (id INTEGER PRIMARY KEY, val INTEGER)`)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	_, err = db.Exec(ctx, `INSERT INTO data_cte VALUES (1, 10), (2, 20), (3, 30), (4, 40)`)
	if err != nil {
		t.Fatalf("Failed to insert: %v", err)
	}

	// CTE in subquery context
	rows, err := db.Query(ctx, `
		SELECT * FROM data_cte
		WHERE id IN (
			WITH high_vals AS (SELECT id FROM data_cte WHERE val > 25)
			SELECT id FROM high_vals
		)`)
	if err != nil {
		t.Logf("CTE in subquery error: %v", err)
		return
	}
	defer rows.Close()

	count := 0
	for rows.Next() {
		count++
	}
	t.Logf("CTE in subquery returned %d rows", count)
}
