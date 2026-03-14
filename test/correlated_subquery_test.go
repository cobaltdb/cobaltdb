package test

import (
	"testing"

	"github.com/cobaltdb/cobaltdb/pkg/engine"
)

func TestCorrelatedSubquery_Basic(t *testing.T) {
	db, err := engine.Open(":memory:", &engine.Options{InMemory: true})
	if err != nil {
		t.Fatalf("DB open: %v", err)
	}
	defer db.Close()
	ctx := t.Context()

	// Create test tables
	afExec(t, db, ctx, "CREATE TABLE employees (id INTEGER PRIMARY KEY, name TEXT, dept_id INTEGER, salary INTEGER)")
	afExec(t, db, ctx, "CREATE TABLE departments (id INTEGER PRIMARY KEY, dept_name TEXT)")

	// Insert test data
	afExec(t, db, ctx, "INSERT INTO employees VALUES (1, 'Alice', 1, 80000), (2, 'Bob', 1, 60000), (3, 'Charlie', 2, 70000)")
	afExec(t, db, ctx, "INSERT INTO departments VALUES (1, 'Engineering'), (2, 'Sales')")

	t.Run("Correlated subquery in SELECT", func(t *testing.T) {
		// For each employee, get the average salary in their department
		rows := afQuery(t, db, ctx, "SELECT name, (SELECT AVG(salary) FROM employees e2 WHERE e2.dept_id = employees.dept_id) as dept_avg FROM employees")
		if len(rows) != 3 {
			t.Errorf("Expected 3 rows, got %d", len(rows))
		}
	})

	t.Run("Correlated subquery in WHERE", func(t *testing.T) {
		// Find employees who earn above their department average
		rows := afQuery(t, db, ctx, "SELECT name FROM employees e1 WHERE salary > (SELECT AVG(salary) FROM employees e2 WHERE e2.dept_id = e1.dept_id)")
		// Only Alice (80000) earns above Engineering average (70000)
		if len(rows) != 1 {
			t.Errorf("Expected 1 row, got %d", len(rows))
		}
	})

	t.Run("Correlated subquery with EXISTS", func(t *testing.T) {
		// Find departments that have employees
		rows := afQuery(t, db, ctx, "SELECT dept_name FROM departments d WHERE EXISTS (SELECT 1 FROM employees e WHERE e.dept_id = d.id)")
		if len(rows) != 2 {
			t.Errorf("Expected 2 rows, got %d", len(rows))
		}
	})

	t.Run("Scalar correlated subquery", func(t *testing.T) {
		// Get each employee's department name
		rows := afQuery(t, db, ctx, "SELECT name, (SELECT dept_name FROM departments d WHERE d.id = employees.dept_id) as dept FROM employees")
		if len(rows) != 3 {
			t.Errorf("Expected 3 rows, got %d", len(rows))
		}
	})
}

func TestCorrelatedSubquery_Complex(t *testing.T) {
	db, err := engine.Open(":memory:", &engine.Options{InMemory: true})
	if err != nil {
		t.Fatalf("DB open: %v", err)
	}
	defer db.Close()
	ctx := t.Context()

	// Create test tables
	afExec(t, db, ctx, "CREATE TABLE products (id INTEGER PRIMARY KEY, name TEXT, category TEXT, price INTEGER)")
	afExec(t, db, ctx, "INSERT INTO products VALUES (1, 'Laptop', 'Electronics', 1000), (2, 'Mouse', 'Electronics', 50), (3, 'Keyboard', 'Electronics', 100)")
	afExec(t, db, ctx, "INSERT INTO products VALUES (4, 'Chair', 'Furniture', 200), (5, 'Desk', 'Furniture', 500)")

	t.Run("Correlated subquery with multiple conditions", func(t *testing.T) {
		// Find products priced above their category average
		rows := afQuery(t, db, ctx, "SELECT name FROM products p1 WHERE price > (SELECT AVG(price) FROM products p2 WHERE p2.category = p1.category)")
		// Laptop (1000) > avg(Electronics=383), Desk (500) > avg(Furniture=350)
		if len(rows) != 2 {
			t.Errorf("Expected 2 rows, got %d", len(rows))
		}
	})

	t.Run("Correlated subquery in HAVING", func(t *testing.T) {
		// Categories with more than average number of products
		rows := afQuery(t, db, ctx, "SELECT category, COUNT(*) as cnt FROM products GROUP BY category HAVING COUNT(*) > (SELECT AVG(prod_count) FROM (SELECT COUNT(*) as prod_count FROM products GROUP BY category) t)")
		// Electronics has 3 products, Furniture has 2, avg is 2.5
		if len(rows) != 1 {
			t.Errorf("Expected 1 row, got %d", len(rows))
		}
	})
}

func TestSubquery_IN_Clause(t *testing.T) {
	db, err := engine.Open(":memory:", &engine.Options{InMemory: true})
	if err != nil {
		t.Fatalf("DB open: %v", err)
	}
	defer db.Close()
	ctx := t.Context()

	afExec(t, db, ctx, "CREATE TABLE t1 (id INTEGER, value TEXT)")
	afExec(t, db, ctx, "CREATE TABLE t2 (id INTEGER, ref_id INTEGER)")
	afExec(t, db, ctx, "INSERT INTO t1 VALUES (1, 'A'), (2, 'B'), (3, 'C')")
	afExec(t, db, ctx, "INSERT INTO t2 VALUES (1, 1), (2, 2)")

	t.Run("IN with subquery", func(t *testing.T) {
		rows := afQuery(t, db, ctx, "SELECT * FROM t1 WHERE id IN (SELECT ref_id FROM t2)")
		if len(rows) != 2 {
			t.Errorf("Expected 2 rows, got %d", len(rows))
		}
	})

	t.Run("NOT IN with subquery", func(t *testing.T) {
		rows := afQuery(t, db, ctx, "SELECT * FROM t1 WHERE id NOT IN (SELECT ref_id FROM t2)")
		if len(rows) != 1 {
			t.Errorf("Expected 1 row, got %d", len(rows))
		}
	})
}
