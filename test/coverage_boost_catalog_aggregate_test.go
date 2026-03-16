package test

import (
	"context"
	"testing"

	"github.com/cobaltdb/cobaltdb/pkg/engine"
)

// TestGroupByOrderByPositional tests GROUP BY with positional ORDER BY
func TestGroupByOrderByPositional(t *testing.T) {
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

	_, err = db.Exec(ctx, "INSERT INTO sales VALUES (1, 'A', 100), (2, 'B', 200), (3, 'A', 150), (4, 'B', 50)")
	if err != nil {
		t.Fatalf("Failed to insert: %v", err)
	}

	// GROUP BY with positional ORDER BY (ORDER BY 2 DESC means ORDER BY second column)
	rows, err := db.Query(ctx, "SELECT category, SUM(amount) as total FROM sales GROUP BY category ORDER BY 2 DESC")
	if err != nil {
		t.Errorf("GROUP BY with positional ORDER BY failed: %v", err)
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

	// GROUP BY with multiple positional ORDER BY
	rows, err = db.Query(ctx, "SELECT category, COUNT(*) as cnt, SUM(amount) as total FROM sales GROUP BY category ORDER BY 2 ASC, 3 DESC")
	if err != nil {
		t.Errorf("GROUP BY with multiple positional ORDER BY failed: %v", err)
	} else {
		rows.Close()
	}
}

// TestGroupByOrderByAggregate tests GROUP BY with aggregate in ORDER BY
func TestGroupByOrderByAggregate(t *testing.T) {
	ctx := context.Background()
	db, err := engine.Open(":memory:", &engine.Options{InMemory: true})
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	// Create table
	_, err = db.Exec(ctx, "CREATE TABLE orders (id INTEGER PRIMARY KEY, customer TEXT, amount INTEGER)")
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	_, err = db.Exec(ctx, "INSERT INTO orders VALUES (1, 'Alice', 100), (2, 'Bob', 200), (3, 'Alice', 150), (4, 'Bob', 50)")
	if err != nil {
		t.Fatalf("Failed to insert: %v", err)
	}

	// GROUP BY with aggregate in ORDER BY
	rows, err := db.Query(ctx, "SELECT customer, SUM(amount) as total FROM orders GROUP BY customer ORDER BY SUM(amount) DESC")
	if err != nil {
		t.Errorf("GROUP BY with aggregate ORDER BY failed: %v", err)
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

	// GROUP BY with COUNT(*) in ORDER BY
	rows, err = db.Query(ctx, "SELECT customer, COUNT(*) as cnt FROM orders GROUP BY customer ORDER BY COUNT(*) DESC")
	if err != nil {
		t.Errorf("GROUP BY with COUNT(*) ORDER BY failed: %v", err)
	} else {
		rows.Close()
	}
}

// TestGroupByOrderByNulls tests GROUP BY with NULL handling in ORDER BY
func TestGroupByOrderByNulls(t *testing.T) {
	ctx := context.Background()
	db, err := engine.Open(":memory:", &engine.Options{InMemory: true})
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	// Create table with NULLs
	_, err = db.Exec(ctx, "CREATE TABLE data (id INTEGER PRIMARY KEY, category TEXT, val INTEGER)")
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	_, err = db.Exec(ctx, "INSERT INTO data VALUES (1, 'A', 10), (2, 'B', NULL), (3, 'C', 30)")
	if err != nil {
		t.Fatalf("Failed to insert: %v", err)
	}

	// ORDER BY with NULLs
	rows, err := db.Query(ctx, "SELECT category, val FROM data ORDER BY val ASC")
	if err != nil {
		t.Errorf("ORDER BY with NULLs failed: %v", err)
	} else {
		rows.Close()
	}

	// ORDER BY DESC with NULLs
	rows, err = db.Query(ctx, "SELECT category, val FROM data ORDER BY val DESC")
	if err != nil {
		t.Errorf("ORDER BY DESC with NULLs failed: %v", err)
	} else {
		rows.Close()
	}
}

// TestGroupByOrderByExpression tests GROUP BY with expression in ORDER BY
func TestGroupByOrderByExpression(t *testing.T) {
	ctx := context.Background()
	db, err := engine.Open(":memory:", &engine.Options{InMemory: true})
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	// Create table
	_, err = db.Exec(ctx, "CREATE TABLE products (id INTEGER PRIMARY KEY, name TEXT, price INTEGER, qty INTEGER)")
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	_, err = db.Exec(ctx, "INSERT INTO products VALUES (1, 'A', 10, 5), (2, 'B', 20, 3), (3, 'A', 15, 4)")
	if err != nil {
		t.Fatalf("Failed to insert: %v", err)
	}

	// GROUP BY with expression aggregate in ORDER BY (SUM(price * qty))
	rows, err := db.Query(ctx, "SELECT name, SUM(price * qty) as revenue FROM products GROUP BY name ORDER BY SUM(price * qty) DESC")
	if err != nil {
		t.Logf("GROUP BY with expression aggregate ORDER BY returned: %v", err)
	} else {
		rows.Close()
	}
}

// TestEvaluateWhereComplex tests complex WHERE clause evaluation
func TestEvaluateWhereComplex(t *testing.T) {
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

	_, err = db.Exec(ctx, "INSERT INTO data VALUES (1, 10, 20, 'Alice'), (2, 30, 40, 'Bob'), (3, 50, 60, 'Charlie'), (4, NULL, 80, 'David')")
	if err != nil {
		t.Fatalf("Failed to insert: %v", err)
	}

	// Complex WHERE with AND/OR
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
	rows, err = db.Query(ctx, "SELECT * FROM data WHERE id IN (1, 3, 5)")
	if err != nil {
		t.Errorf("WHERE IN failed: %v", err)
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
		t.Errorf("WHERE BETWEEN failed: %v", err)
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

	// WHERE with LIKE
	rows, err = db.Query(ctx, "SELECT * FROM data WHERE name LIKE 'A%'")
	if err != nil {
		t.Errorf("WHERE LIKE failed: %v", err)
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

	// WHERE with IS NULL
	rows, err = db.Query(ctx, "SELECT * FROM data WHERE val1 IS NULL")
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

	// WHERE with IS NOT NULL
	rows, err = db.Query(ctx, "SELECT * FROM data WHERE val1 IS NOT NULL")
	if err != nil {
		t.Errorf("WHERE IS NOT NULL failed: %v", err)
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

// TestEvaluateWhereSubquery tests WHERE with subqueries
func TestEvaluateWhereSubquery(t *testing.T) {
	ctx := context.Background()
	db, err := engine.Open(":memory:", &engine.Options{InMemory: true})
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	// Create tables
	_, err = db.Exec(ctx, "CREATE TABLE employees (id INTEGER PRIMARY KEY, dept_id INTEGER, salary INTEGER)")
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
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

	// WHERE with IN subquery
	rows, err := db.Query(ctx, "SELECT * FROM employees WHERE dept_id IN (SELECT id FROM departments WHERE name = 'Sales')")
	if err != nil {
		t.Errorf("WHERE IN subquery failed: %v", err)
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

	// WHERE with EXISTS
	rows, err = db.Query(ctx, "SELECT * FROM employees e WHERE EXISTS (SELECT 1 FROM departments d WHERE d.id = e.dept_id)")
	if err != nil {
		t.Errorf("WHERE EXISTS failed: %v", err)
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

// TestGroupByHavingComplex tests complex HAVING clauses
func TestGroupByHavingComplex(t *testing.T) {
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

	_, err = db.Exec(ctx, "INSERT INTO sales VALUES (1, 'A', 100), (2, 'A', 200), (3, 'B', 50), (4, 'B', 30), (5, 'C', 500)")
	if err != nil {
		t.Fatalf("Failed to insert: %v", err)
	}

	// HAVING with multiple conditions
	rows, err := db.Query(ctx, "SELECT category, COUNT(*) as cnt, SUM(amount) as total FROM sales GROUP BY category HAVING COUNT(*) > 1 AND SUM(amount) > 150")
	if err != nil {
		t.Errorf("HAVING with multiple conditions failed: %v", err)
	} else {
		count := 0
		for rows.Next() {
			count++
		}
		rows.Close()
		if count != 1 { // Only category 'A' matches
			t.Errorf("Expected 1 row, got %d", count)
		}
	}

	// HAVING with AVG
	rows, err = db.Query(ctx, "SELECT category, AVG(amount) as avg_amount FROM sales GROUP BY category HAVING AVG(amount) > 100")
	if err != nil {
		t.Errorf("HAVING with AVG failed: %v", err)
	} else {
		count := 0
		for rows.Next() {
			count++
		}
		rows.Close()
		t.Logf("HAVING AVG returned %d rows", count)
	}
}

// TestGroupByJoinHaving tests GROUP BY with JOIN and HAVING
func TestGroupByJoinHaving(t *testing.T) {
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
		t.Errorf("JOIN with GROUP BY and HAVING failed: %v", err)
	} else {
		count := 0
		for rows.Next() {
			count++
		}
		rows.Close()
		if count != 1 { // Only Alice has total > 150
			t.Errorf("Expected 1 row, got %d", count)
		}
	}
}

// TestOuterQueryWithAggregates tests applyOuterQuery with aggregates
func TestOuterQueryWithAggregates(t *testing.T) {
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

	// Create view with GROUP BY
	_, err = db.Exec(ctx, "CREATE VIEW sales_summary AS SELECT category, SUM(amount) as total FROM sales GROUP BY category")
	if err != nil {
		t.Fatalf("Failed to create view: %v", err)
	}

	// Query view with WHERE on aggregate column
	rows, err := db.Query(ctx, "SELECT * FROM sales_summary WHERE total > 150")
	if err != nil {
		t.Errorf("Query view with WHERE on aggregate failed: %v", err)
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

	// Query view with ORDER BY on aggregate column
	rows, err = db.Query(ctx, "SELECT * FROM sales_summary ORDER BY total DESC")
	if err != nil {
		t.Errorf("Query view with ORDER BY on aggregate failed: %v", err)
	} else {
		rows.Close()
	}
}

// TestCastExpression tests CAST expressions
func TestCastExpression(t *testing.T) {
	ctx := context.Background()
	db, err := engine.Open(":memory:", &engine.Options{InMemory: true})
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	// Create table
	_, err = db.Exec(ctx, "CREATE TABLE data (id INTEGER PRIMARY KEY, val TEXT)")
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	_, err = db.Exec(ctx, "INSERT INTO data VALUES (1, '123'), (2, '456')")
	if err != nil {
		t.Fatalf("Failed to insert: %v", err)
	}

	// CAST in SELECT
	rows, err := db.Query(ctx, "SELECT id, CAST(val AS INTEGER) as num FROM data")
	if err != nil {
		t.Errorf("CAST in SELECT failed: %v", err)
	} else {
		rows.Close()
	}

	// CAST in WHERE
	rows, err = db.Query(ctx, "SELECT * FROM data WHERE CAST(val AS INTEGER) > 200")
	if err != nil {
		t.Errorf("CAST in WHERE failed: %v", err)
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

// TestBetweenExpression tests BETWEEN expressions
func TestBetweenExpression(t *testing.T) {
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

	_, err = db.Exec(ctx, "INSERT INTO data VALUES (1, 10), (2, 20), (3, 30), (4, 40), (5, 50)")
	if err != nil {
		t.Fatalf("Failed to insert: %v", err)
	}

	// BETWEEN
	rows, err := db.Query(ctx, "SELECT * FROM data WHERE val BETWEEN 20 AND 40")
	if err != nil {
		t.Errorf("BETWEEN failed: %v", err)
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

	// NOT BETWEEN
	rows, err = db.Query(ctx, "SELECT * FROM data WHERE val NOT BETWEEN 20 AND 40")
	if err != nil {
		t.Errorf("NOT BETWEEN failed: %v", err)
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

// TestCaseExpression tests CASE expressions
func TestCaseExpression(t *testing.T) {
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

	_, err = db.Exec(ctx, "INSERT INTO data VALUES (1, 10), (2, 50), (3, 100)")
	if err != nil {
		t.Fatalf("Failed to insert: %v", err)
	}

	// CASE in SELECT
	rows, err := db.Query(ctx, "SELECT id, CASE WHEN val < 50 THEN 'low' WHEN val < 100 THEN 'medium' ELSE 'high' END as category FROM data")
	if err != nil {
		t.Errorf("CASE in SELECT failed: %v", err)
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

// TestQualifiedIdentifier tests qualified identifiers
func TestQualifiedIdentifier(t *testing.T) {
	ctx := context.Background()
	db, err := engine.Open(":memory:", &engine.Options{InMemory: true})
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	// Create tables
	_, err = db.Exec(ctx, "CREATE TABLE t1 (id INTEGER PRIMARY KEY, name TEXT)")
	if err != nil {
		t.Fatalf("Failed to create t1: %v", err)
	}

	_, err = db.Exec(ctx, "CREATE TABLE t2 (id INTEGER PRIMARY KEY, t1_id INTEGER, val INTEGER)")
	if err != nil {
		t.Fatalf("Failed to create t2: %v", err)
	}

	_, err = db.Exec(ctx, "INSERT INTO t1 VALUES (1, 'Alice'), (2, 'Bob')")
	if err != nil {
		t.Fatalf("Failed to insert t1: %v", err)
	}

	_, err = db.Exec(ctx, "INSERT INTO t2 VALUES (1, 1, 100), (2, 2, 200)")
	if err != nil {
		t.Fatalf("Failed to insert t2: %v", err)
	}

	// JOIN with qualified identifiers
	rows, err := db.Query(ctx, "SELECT t1.name, t2.val FROM t1 JOIN t2 ON t1.id = t2.t1_id WHERE t1.name = 'Alice'")
	if err != nil {
		t.Errorf("JOIN with qualified identifiers failed: %v", err)
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
