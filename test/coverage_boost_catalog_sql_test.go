package test

import (
	"context"
	"testing"

	"github.com/cobaltdb/cobaltdb/pkg/engine"
)

// TestSelectLocked_ComplexViewWithGroupBy tests complex view with GROUP BY
func TestSelectLocked_ComplexViewWithGroupBy(t *testing.T) {
	ctx := context.Background()
	db, err := engine.Open(":memory:", &engine.Options{InMemory: true})
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	// Create table
	_, err = db.Exec(ctx, "CREATE TABLE sales (id INTEGER PRIMARY KEY, category TEXT, amount INTEGER)")
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	_, err = db.Exec(ctx, "INSERT INTO sales VALUES (1, 'A', 100), (2, 'A', 200), (3, 'B', 150)")
	if err != nil {
		t.Fatalf("Failed to insert: %v", err)
	}

	// Create complex view with GROUP BY
	_, err = db.Exec(ctx, "CREATE VIEW sales_summary AS SELECT category, SUM(amount) as total FROM sales GROUP BY category")
	if err != nil {
		t.Fatalf("Failed to create view: %v", err)
	}

	// Query the view with WHERE
	rows, err := db.Query(ctx, "SELECT * FROM sales_summary WHERE total > 150")
	if err != nil {
		t.Errorf("Query complex view failed: %v", err)
	} else {
		count := 0
		for rows.Next() {
			count++
		}
		rows.Close()
		if count != 1 {
			t.Errorf("Expected 1 row, got %d", count)
		}
	}
}

// TestSelectLocked_ComplexViewWithOrderBy tests complex view with ORDER BY
func TestSelectLocked_ComplexViewWithOrderBy(t *testing.T) {
	ctx := context.Background()
	db, err := engine.Open(":memory:", &engine.Options{InMemory: true})
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	// Create table
	_, err = db.Exec(ctx, "CREATE TABLE items (id INTEGER PRIMARY KEY, category TEXT, val INTEGER)")
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	_, err = db.Exec(ctx, "INSERT INTO items VALUES (1, 'A', 10), (2, 'B', 20), (3, 'A', 30)")
	if err != nil {
		t.Fatalf("Failed to insert: %v", err)
	}

	// Create view with GROUP BY and aliased columns
	_, err = db.Exec(ctx, "CREATE VIEW item_totals AS SELECT category AS cat, SUM(val) AS total FROM items GROUP BY category")
	if err != nil {
		t.Fatalf("Failed to create view: %v", err)
	}

	// Query view with ORDER BY alias
	rows, err := db.Query(ctx, "SELECT * FROM item_totals ORDER BY total DESC")
	if err != nil {
		t.Errorf("Query view with ORDER BY failed: %v", err)
	} else {
		count := 0
		for rows.Next() {
			count++
		}
		rows.Close()
		if count != 2 {
			t.Errorf("Expected 2 rows, got %d", count)
		}
	}
}

// TestSelectLocked_DerivedTableWithJoin tests derived table with JOIN
func TestSelectLocked_DerivedTableWithJoin(t *testing.T) {
	ctx := context.Background()
	db, err := engine.Open(":memory:", &engine.Options{InMemory: true})
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	// Create tables
	_, err = db.Exec(ctx, "CREATE TABLE orders (id INTEGER PRIMARY KEY, customer_id INTEGER, amount INTEGER)")
	if err != nil {
		t.Fatalf("Failed to create orders: %v", err)
	}

	_, err = db.Exec(ctx, "CREATE TABLE customers (id INTEGER PRIMARY KEY, name TEXT)")
	if err != nil {
		t.Fatalf("Failed to create customers: %v", err)
	}

	_, err = db.Exec(ctx, "INSERT INTO customers VALUES (1, 'Alice'), (2, 'Bob')")
	if err != nil {
		t.Fatalf("Failed to insert customers: %v", err)
	}

	_, err = db.Exec(ctx, "INSERT INTO orders VALUES (1, 1, 100), (2, 1, 200), (3, 2, 150)")
	if err != nil {
		t.Fatalf("Failed to insert orders: %v", err)
	}

	// Derived table with JOIN
	rows, err := db.Query(ctx, `
		SELECT c.name, o.total
		FROM customers c
		JOIN (SELECT customer_id, SUM(amount) as total FROM orders GROUP BY customer_id) o
		ON c.id = o.customer_id
		WHERE o.total > 150
	`)
	if err != nil {
		t.Errorf("Derived table with JOIN failed: %v", err)
	} else {
		count := 0
		for rows.Next() {
			count++
		}
		rows.Close()
		if count != 1 {
			t.Errorf("Expected 1 row, got %d", count)
		}
	}
}

// TestSelectLocked_CTEWithJoin tests CTE with JOIN
func TestSelectLocked_CTEWithJoin(t *testing.T) {
	ctx := context.Background()
	db, err := engine.Open(":memory:", &engine.Options{InMemory: true})
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	// Create tables
	_, err = db.Exec(ctx, "CREATE TABLE employees (id INTEGER PRIMARY KEY, dept_id INTEGER, salary INTEGER)")
	if err != nil {
		t.Fatalf("Failed to create employees: %v", err)
	}

	_, err = db.Exec(ctx, "CREATE TABLE departments (id INTEGER PRIMARY KEY, name TEXT)")
	if err != nil {
		t.Fatalf("Failed to create departments: %v", err)
	}

	_, err = db.Exec(ctx, "INSERT INTO departments VALUES (1, 'Sales'), (2, 'Engineering')")
	if err != nil {
		t.Fatalf("Failed to insert departments: %v", err)
	}

	_, err = db.Exec(ctx, "INSERT INTO employees VALUES (1, 1, 50000), (2, 1, 60000), (3, 2, 70000)")
	if err != nil {
		t.Fatalf("Failed to insert employees: %v", err)
	}

	// CTE with JOIN
	rows, err := db.Query(ctx, `
		WITH dept_salaries AS (
			SELECT d.name, AVG(e.salary) as avg_salary
			FROM departments d
			JOIN employees e ON d.id = e.dept_id
			GROUP BY d.id, d.name
		)
		SELECT * FROM dept_salaries WHERE avg_salary > 55000
	`)
	if err != nil {
		t.Errorf("CTE with JOIN failed: %v", err)
	} else {
		count := 0
		for rows.Next() {
			count++
		}
		rows.Close()
		if count != 1 {
			t.Errorf("Expected 1 row, got %d", count)
		}
	}
}

// TestSelectLocked_CTEWithWindowFuncAndJoin tests CTE with window functions and JOIN
func TestSelectLocked_CTEWithWindowFuncAndJoin(t *testing.T) {
	ctx := context.Background()
	db, err := engine.Open(":memory:", &engine.Options{InMemory: true})
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	// Create table
	_, err = db.Exec(ctx, "CREATE TABLE scores (id INTEGER PRIMARY KEY, player TEXT, score INTEGER)")
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	_, err = db.Exec(ctx, "INSERT INTO scores VALUES (1, 'Alice', 100), (2, 'Bob', 200), (3, 'Alice', 150)")
	if err != nil {
		t.Fatalf("Failed to insert: %v", err)
	}

	// CTE with window function
	rows, err := db.Query(ctx, `
		WITH ranked_scores AS (
			SELECT player, score, RANK() OVER (ORDER BY score DESC) as rnk
			FROM scores
		)
		SELECT * FROM ranked_scores WHERE rnk <= 2
	`)
	if err != nil {
		t.Errorf("CTE with window function failed: %v", err)
	} else {
		count := 0
		for rows.Next() {
			count++
		}
		rows.Close()
		if count != 2 {
			t.Errorf("Expected 2 rows, got %d", count)
		}
	}
}

// TestSelectLocked_ViewWithAggregateAndWhere tests view with aggregate and WHERE
func TestSelectLocked_ViewWithAggregateAndWhere(t *testing.T) {
	ctx := context.Background()
	db, err := engine.Open(":memory:", &engine.Options{InMemory: true})
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	// Create table
	_, err = db.Exec(ctx, "CREATE TABLE sales (id INTEGER PRIMARY KEY, region TEXT, amount INTEGER)")
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	_, err = db.Exec(ctx, "INSERT INTO sales VALUES (1, 'North', 100), (2, 'North', 200), (3, 'South', 150)")
	if err != nil {
		t.Fatalf("Failed to insert: %v", err)
	}

	// Create view with aggregate
	_, err = db.Exec(ctx, "CREATE VIEW regional_totals AS SELECT region, SUM(amount) as total FROM sales GROUP BY region")
	if err != nil {
		t.Fatalf("Failed to create view: %v", err)
	}

	// Query with WHERE and ORDER BY
	rows, err := db.Query(ctx, "SELECT * FROM regional_totals WHERE total > 150 ORDER BY total DESC")
	if err != nil {
		t.Errorf("Query view with WHERE and ORDER BY failed: %v", err)
	} else {
		count := 0
		for rows.Next() {
			count++
		}
		rows.Close()
		if count != 1 {
			t.Errorf("Expected 1 row, got %d", count)
		}
	}
}

// TestSelectLocked_ViewWithHaving tests view with HAVING
func TestSelectLocked_ViewWithHaving(t *testing.T) {
	ctx := context.Background()
	db, err := engine.Open(":memory:", &engine.Options{InMemory: true})
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	// Create table
	_, err = db.Exec(ctx, "CREATE TABLE data (id INTEGER PRIMARY KEY, category TEXT, val INTEGER)")
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	_, err = db.Exec(ctx, "INSERT INTO data VALUES (1, 'A', 10), (2, 'A', 20), (3, 'B', 5)")
	if err != nil {
		t.Fatalf("Failed to insert: %v", err)
	}

	// Create view with HAVING
	_, err = db.Exec(ctx, "CREATE VIEW filtered_data AS SELECT category, COUNT(*) as cnt, SUM(val) as total FROM data GROUP BY category HAVING COUNT(*) > 1")
	if err != nil {
		t.Fatalf("Failed to create view: %v", err)
	}

	// Query the view
	rows, err := db.Query(ctx, "SELECT * FROM filtered_data")
	if err != nil {
		t.Errorf("Query view with HAVING failed: %v", err)
	} else {
		count := 0
		for rows.Next() {
			count++
		}
		rows.Close()
		if count != 1 {
			t.Errorf("Expected 1 row, got %d", count)
		}
	}
}

// TestSelectLocked_GroupByWithOrderBy tests GROUP BY with ORDER BY
func TestSelectLocked_GroupByWithOrderBy(t *testing.T) {
	ctx := context.Background()
	db, err := engine.Open(":memory:", &engine.Options{InMemory: true})
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	// Create table
	_, err = db.Exec(ctx, "CREATE TABLE items (id INTEGER PRIMARY KEY, category TEXT, subcategory TEXT, val INTEGER)")
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	_, err = db.Exec(ctx, "INSERT INTO items VALUES (1, 'A', 'X', 10), (2, 'A', 'Y', 20), (3, 'B', 'X', 30), (4, 'B', 'Y', 40)")
	if err != nil {
		t.Fatalf("Failed to insert: %v", err)
	}

	// GROUP BY with ORDER BY multiple columns
	rows, err := db.Query(ctx, "SELECT category, SUM(val) as total FROM items GROUP BY category ORDER BY total DESC, category ASC")
	if err != nil {
		t.Errorf("GROUP BY with ORDER BY failed: %v", err)
	} else {
		count := 0
		for rows.Next() {
			count++
		}
		rows.Close()
		if count != 2 {
			t.Errorf("Expected 2 rows, got %d", count)
		}
	}
}

// TestSelectLocked_GroupByWithLimitOffset tests GROUP BY with LIMIT/OFFSET
func TestSelectLocked_GroupByWithLimitOffset(t *testing.T) {
	ctx := context.Background()
	db, err := engine.Open(":memory:", &engine.Options{InMemory: true})
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	// Create table
	_, err = db.Exec(ctx, "CREATE TABLE data (id INTEGER PRIMARY KEY, category TEXT, val INTEGER)")
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	_, err = db.Exec(ctx, "INSERT INTO data VALUES (1, 'A', 10), (2, 'B', 20), (3, 'C', 30), (4, 'D', 40)")
	if err != nil {
		t.Fatalf("Failed to insert: %v", err)
	}

	// GROUP BY with LIMIT and OFFSET
	rows, err := db.Query(ctx, "SELECT category, SUM(val) as total FROM data GROUP BY category ORDER BY total LIMIT 2 OFFSET 1")
	if err != nil {
		t.Errorf("GROUP BY with LIMIT/OFFSET failed: %v", err)
	} else {
		count := 0
		for rows.Next() {
			count++
		}
		rows.Close()
		if count != 2 {
			t.Errorf("Expected 2 rows, got %d", count)
		}
	}
}

// TestSelectLocked_WhereWithComplexExpr tests WHERE with complex expressions
func TestSelectLocked_WhereWithComplexExpr(t *testing.T) {
	ctx := context.Background()
	db, err := engine.Open(":memory:", &engine.Options{InMemory: true})
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	// Create table
	_, err = db.Exec(ctx, "CREATE TABLE data (id INTEGER PRIMARY KEY, val1 INTEGER, val2 INTEGER, name TEXT)")
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	_, err = db.Exec(ctx, "INSERT INTO data VALUES (1, 10, 20, 'Alice'), (2, 30, 40, 'Bob'), (3, 50, 60, 'Charlie')")
	if err != nil {
		t.Fatalf("Failed to insert: %v", err)
	}

	// WHERE with complex AND/OR
	rows, err := db.Query(ctx, "SELECT * FROM data WHERE (val1 > 15 AND val2 < 50) OR name = 'Alice'")
	if err != nil {
		t.Errorf("Complex WHERE failed: %v", err)
	} else {
		count := 0
		for rows.Next() {
			count++
		}
		rows.Close()
		if count != 2 {
			t.Errorf("Expected 2 rows, got %d", count)
		}
	}

	// WHERE with IN
	rows, err = db.Query(ctx, "SELECT * FROM data WHERE id IN (1, 3)")
	if err != nil {
		t.Errorf("WHERE with IN failed: %v", err)
	} else {
		count := 0
		for rows.Next() {
			count++
		}
		rows.Close()
		if count != 2 {
			t.Errorf("Expected 2 rows, got %d", count)
		}
	}

	// WHERE with BETWEEN
	rows, err = db.Query(ctx, "SELECT * FROM data WHERE val1 BETWEEN 20 AND 40")
	if err != nil {
		t.Errorf("WHERE with BETWEEN failed: %v", err)
	} else {
		count := 0
		for rows.Next() {
			count++
		}
		rows.Close()
		if count != 1 {
			t.Errorf("Expected 1 row, got %d", count)
		}
	}
}

// TestSelectLocked_WhereWithLike tests WHERE with LIKE
func TestSelectLocked_WhereWithLike(t *testing.T) {
	ctx := context.Background()
	db, err := engine.Open(":memory:", &engine.Options{InMemory: true})
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	// Create table
	_, err = db.Exec(ctx, "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)")
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	_, err = db.Exec(ctx, "INSERT INTO users VALUES (1, 'Alice'), (2, 'Bob'), (3, 'Alexander'), (4, 'Charlie')")
	if err != nil {
		t.Fatalf("Failed to insert: %v", err)
	}

	// WHERE with LIKE
	rows, err := db.Query(ctx, "SELECT * FROM users WHERE name LIKE 'Al%'")
	if err != nil {
		t.Errorf("WHERE with LIKE failed: %v", err)
	} else {
		count := 0
		for rows.Next() {
			count++
		}
		rows.Close()
		if count != 2 {
			t.Errorf("Expected 2 rows, got %d", count)
		}
	}
}

// TestSelectLocked_WhereWithIsNull tests WHERE with IS NULL/IS NOT NULL
func TestSelectLocked_WhereWithIsNull(t *testing.T) {
	ctx := context.Background()
	db, err := engine.Open(":memory:", &engine.Options{InMemory: true})
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	// Create table
	_, err = db.Exec(ctx, "CREATE TABLE data (id INTEGER PRIMARY KEY, val INTEGER)")
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	_, err = db.Exec(ctx, "INSERT INTO data VALUES (1, 10), (2, NULL), (3, 30)")
	if err != nil {
		t.Fatalf("Failed to insert: %v", err)
	}

	// WHERE IS NULL
	rows, err := db.Query(ctx, "SELECT * FROM data WHERE val IS NULL")
	if err != nil {
		t.Errorf("WHERE IS NULL failed: %v", err)
	} else {
		count := 0
		for rows.Next() {
			count++
		}
		rows.Close()
		if count != 1 {
			t.Errorf("Expected 1 row, got %d", count)
		}
	}

	// WHERE IS NOT NULL
	rows, err = db.Query(ctx, "SELECT * FROM data WHERE val IS NOT NULL")
	if err != nil {
		t.Errorf("WHERE IS NOT NULL failed: %v", err)
	} else {
		count := 0
		for rows.Next() {
			count++
		}
		rows.Close()
		if count != 2 {
			t.Errorf("Expected 2 rows, got %d", count)
		}
	}
}

// TestSelectLocked_JoinWithWhereGroupBy tests JOIN with WHERE and GROUP BY
func TestSelectLocked_JoinWithWhereGroupBy(t *testing.T) {
	ctx := context.Background()
	db, err := engine.Open(":memory:", &engine.Options{InMemory: true})
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	// Create tables
	_, err = db.Exec(ctx, "CREATE TABLE orders (id INTEGER PRIMARY KEY, customer_id INTEGER, product TEXT, amount INTEGER)")
	if err != nil {
		t.Fatalf("Failed to create orders: %v", err)
	}

	_, err = db.Exec(ctx, "CREATE TABLE customers (id INTEGER PRIMARY KEY, name TEXT)")
	if err != nil {
		t.Fatalf("Failed to create customers: %v", err)
	}

	_, err = db.Exec(ctx, "INSERT INTO customers VALUES (1, 'Alice'), (2, 'Bob')")
	if err != nil {
		t.Fatalf("Failed to insert customers: %v", err)
	}

	_, err = db.Exec(ctx, "INSERT INTO orders VALUES (1, 1, 'Widget', 100), (2, 1, 'Gadget', 200), (3, 2, 'Widget', 150)")
	if err != nil {
		t.Fatalf("Failed to insert orders: %v", err)
	}

	// JOIN with WHERE and GROUP BY
	rows, err := db.Query(ctx, `
		SELECT c.name, o.product, SUM(o.amount) as total
		FROM customers c
		JOIN orders o ON c.id = o.customer_id
		WHERE o.amount > 100
		GROUP BY c.name, o.product
	`)
	if err != nil {
		t.Errorf("JOIN with WHERE and GROUP BY failed: %v", err)
	} else {
		count := 0
		for rows.Next() {
			count++
		}
		rows.Close()
		if count != 2 {
			t.Errorf("Expected 2 rows, got %d", count)
		}
	}
}

// TestSelectLocked_JoinWithHaving tests JOIN with HAVING
func TestSelectLocked_JoinWithHaving(t *testing.T) {
	ctx := context.Background()
	db, err := engine.Open(":memory:", &engine.Options{InMemory: true})
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	// Create tables
	_, err = db.Exec(ctx, "CREATE TABLE orders (id INTEGER PRIMARY KEY, customer_id INTEGER, amount INTEGER)")
	if err != nil {
		t.Fatalf("Failed to create orders: %v", err)
	}

	_, err = db.Exec(ctx, "CREATE TABLE customers (id INTEGER PRIMARY KEY, name TEXT)")
	if err != nil {
		t.Fatalf("Failed to create customers: %v", err)
	}

	_, err = db.Exec(ctx, "INSERT INTO customers VALUES (1, 'Alice'), (2, 'Bob')")
	if err != nil {
		t.Fatalf("Failed to insert customers: %v", err)
	}

	_, err = db.Exec(ctx, "INSERT INTO orders VALUES (1, 1, 100), (2, 1, 200), (3, 2, 50)")
	if err != nil {
		t.Fatalf("Failed to insert orders: %v", err)
	}

	// JOIN with GROUP BY and HAVING
	rows, err := db.Query(ctx, `
		SELECT c.name, SUM(o.amount) as total
		FROM customers c
		JOIN orders o ON c.id = o.customer_id
		GROUP BY c.name
		HAVING SUM(o.amount) > 150
	`)
	if err != nil {
		t.Errorf("JOIN with HAVING failed: %v", err)
	} else {
		count := 0
		for rows.Next() {
			count++
		}
		rows.Close()
		if count != 1 {
			t.Errorf("Expected 1 row, got %d", count)
		}
	}
}

// TestSelectLocked_AggregateSubqueryInSelect tests aggregate subquery in SELECT
func TestSelectLocked_AggregateSubqueryInSelect(t *testing.T) {
	ctx := context.Background()
	db, err := engine.Open(":memory:", &engine.Options{InMemory: true})
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	// Create table
	_, err = db.Exec(ctx, "CREATE TABLE products (id INTEGER PRIMARY KEY, category TEXT, price INTEGER)")
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	_, err = db.Exec(ctx, "INSERT INTO products VALUES (1, 'A', 100), (2, 'A', 200), (3, 'B', 150)")
	if err != nil {
		t.Fatalf("Failed to insert: %v", err)
	}

	// Aggregate subquery in SELECT
	rows, err := db.Query(ctx, `
		SELECT id, price,
			(SELECT AVG(price) FROM products) as avg_price,
			price - (SELECT AVG(price) FROM products) as diff
		FROM products
	`)
	if err != nil {
		t.Errorf("Aggregate subquery in SELECT failed: %v", err)
	} else {
		count := 0
		for rows.Next() {
			count++
		}
		rows.Close()
		if count != 3 {
			t.Errorf("Expected 3 rows, got %d", count)
		}
	}
}

// TestSelectLocked_CorrelatedSubquery tests correlated subquery
func TestSelectLocked_CorrelatedSubquery(t *testing.T) {
	ctx := context.Background()
	db, err := engine.Open(":memory:", &engine.Options{InMemory: true})
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	// Create table
	_, err = db.Exec(ctx, "CREATE TABLE employees (id INTEGER PRIMARY KEY, dept TEXT, salary INTEGER)")
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	_, err = db.Exec(ctx, "INSERT INTO employees VALUES (1, 'Sales', 50000), (2, 'Sales', 60000), (3, 'Engineering', 70000)")
	if err != nil {
		t.Fatalf("Failed to insert: %v", err)
	}

	// Correlated subquery
	rows, err := db.Query(ctx, `
		SELECT id, dept, salary
		FROM employees e
		WHERE salary > (SELECT AVG(salary) FROM employees WHERE dept = e.dept)
	`)
	if err != nil {
		t.Errorf("Correlated subquery failed: %v", err)
	} else {
		count := 0
		for rows.Next() {
			count++
		}
		rows.Close()
		t.Logf("Correlated subquery returned %d rows", count)
	}
}

// TestSelectLocked_ViewWithJoinAndGroupBy tests complex view with JOIN and GROUP BY
func TestSelectLocked_ViewWithJoinAndGroupBy(t *testing.T) {
	ctx := context.Background()
	db, err := engine.Open(":memory:", &engine.Options{InMemory: true})
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	// Create tables
	_, err = db.Exec(ctx, "CREATE TABLE orders (id INTEGER PRIMARY KEY, customer_id INTEGER, amount INTEGER)")
	if err != nil {
		t.Fatalf("Failed to create orders: %v", err)
	}

	_, err = db.Exec(ctx, "CREATE TABLE customers (id INTEGER PRIMARY KEY, region TEXT)")
	if err != nil {
		t.Fatalf("Failed to create customers: %v", err)
	}

	_, err = db.Exec(ctx, "INSERT INTO customers VALUES (1, 'North'), (2, 'South')")
	if err != nil {
		t.Fatalf("Failed to insert customers: %v", err)
	}

	_, err = db.Exec(ctx, "INSERT INTO orders VALUES (1, 1, 100), (2, 1, 200), (3, 2, 150)")
	if err != nil {
		t.Fatalf("Failed to insert orders: %v", err)
	}

	// Create complex view with JOIN and GROUP BY
	_, err = db.Exec(ctx, `
		CREATE VIEW regional_sales AS
		SELECT c.region, SUM(o.amount) as total
		FROM customers c
		JOIN orders o ON c.id = o.customer_id
		GROUP BY c.region
	`)
	if err != nil {
		t.Fatalf("Failed to create view: %v", err)
	}

	// Query the view
	rows, err := db.Query(ctx, "SELECT * FROM regional_sales WHERE total > 150 ORDER BY total DESC")
	if err != nil {
		t.Errorf("Query complex view failed: %v", err)
	} else {
		count := 0
		for rows.Next() {
			count++
		}
		rows.Close()
		if count != 1 {
			t.Errorf("Expected 1 row, got %d", count)
		}
	}
}

// TestSelectLocked_RecursiveCTEWithJoin tests recursive CTE with JOIN
func TestSelectLocked_RecursiveCTEWithJoin(t *testing.T) {
	ctx := context.Background()
	db, err := engine.Open(":memory:", &engine.Options{InMemory: true})
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	// Create table
	_, err = db.Exec(ctx, "CREATE TABLE org (id INTEGER PRIMARY KEY, name TEXT, manager_id INTEGER)")
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	_, err = db.Exec(ctx, "INSERT INTO org VALUES (1, 'CEO', NULL), (2, 'VP1', 1), (3, 'VP2', 1), (4, 'Manager1', 2)")
	if err != nil {
		t.Fatalf("Failed to insert: %v", err)
	}

	// Recursive CTE
	rows, err := db.Query(ctx, `
		WITH RECURSIVE subordinates AS (
			SELECT id, name, manager_id, 1 as level
			FROM org
			WHERE id = 1
			UNION ALL
			SELECT o.id, o.name, o.manager_id, s.level + 1
			FROM org o
			JOIN subordinates s ON o.manager_id = s.id
		)
		SELECT * FROM subordinates ORDER BY level, id
	`)
	if err != nil {
		t.Errorf("Recursive CTE with JOIN failed: %v", err)
	} else {
		count := 0
		for rows.Next() {
			count++
		}
		rows.Close()
		if count != 4 {
			t.Errorf("Expected 4 rows, got %d", count)
		}
	}
}

// TestSelectLocked_MultipleCTEs tests multiple CTEs
func TestSelectLocked_MultipleCTEs(t *testing.T) {
	ctx := context.Background()
	db, err := engine.Open(":memory:", &engine.Options{InMemory: true})
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	// Create table
	_, err = db.Exec(ctx, "CREATE TABLE employees (id INTEGER PRIMARY KEY, dept_id INTEGER, salary INTEGER)")
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	_, err = db.Exec(ctx, "INSERT INTO employees VALUES (1, 1, 50000), (2, 1, 60000), (3, 2, 70000)")
	if err != nil {
		t.Fatalf("Failed to insert: %v", err)
	}

	// Multiple CTEs
	rows, err := db.Query(ctx, `
		WITH dept_avg AS (
			SELECT dept_id, AVG(salary) as avg_sal
			FROM employees
			GROUP BY dept_id
		),
		above_avg AS (
			SELECT e.id, e.dept_id, e.salary
			FROM employees e
			JOIN dept_avg d ON e.dept_id = d.dept_id
			WHERE e.salary > d.avg_sal
		)
		SELECT id, dept_id, salary FROM above_avg
	`)
	if err != nil {
		t.Errorf("Multiple CTEs failed: %v", err)
	} else {
		count := 0
		for rows.Next() {
			count++
		}
		rows.Close()
		t.Logf("Multiple CTEs returned %d rows", count)
	}
}
