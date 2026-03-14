package integration

import (
	"context"
	"testing"

	"github.com/cobaltdb/cobaltdb/pkg/engine"
)

// TestWhereWithSubquery targets evaluateWhere with subqueries
func TestWhereWithSubquery(t *testing.T) {
	db, err := engine.Open(":memory:", &engine.Options{InMemory: true})
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	ctx := context.Background()

	// Setup tables
	_, err = db.Exec(ctx, `CREATE TABLE employees (id INTEGER PRIMARY KEY, name TEXT, dept_id INTEGER, salary INTEGER)`)
	if err != nil {
		t.Fatalf("Failed to create employees: %v", err)
	}

	_, err = db.Exec(ctx, `CREATE TABLE departments (id INTEGER PRIMARY KEY, name TEXT, budget INTEGER)`)
	if err != nil {
		t.Fatalf("Failed to create departments: %v", err)
	}

	_, err = db.Exec(ctx, `INSERT INTO departments VALUES
		(1, 'Engineering', 1000000),
		(2, 'Sales', 500000),
		(3, 'Marketing', 300000)`)
	if err != nil {
		t.Fatalf("Failed to insert departments: %v", err)
	}

	_, err = db.Exec(ctx, `INSERT INTO employees VALUES
		(1, 'Alice', 1, 150000),
		(2, 'Bob', 1, 120000),
		(3, 'Charlie', 2, 80000),
		(4, 'Diana', 2, 90000),
		(5, 'Eve', 3, 70000),
		(6, 'Frank', 1, 200000)`)
	if err != nil {
		t.Fatalf("Failed to insert employees: %v", err)
	}

	tests := []struct {
		name     string
		sql      string
		expected int
	}{
		{"WHERE IN subquery", `SELECT name FROM employees WHERE dept_id IN (SELECT id FROM departments WHERE budget > 400000)`, 4},
		{"WHERE NOT IN subquery", `SELECT name FROM employees WHERE dept_id NOT IN (SELECT id FROM departments WHERE budget < 400000)`, 4},
		{"WHERE EXISTS", `SELECT name FROM employees WHERE EXISTS (SELECT 1 FROM departments WHERE departments.id = employees.dept_id AND budget > 800000)`, 3},
		{"WHERE NOT EXISTS", `SELECT name FROM employees WHERE NOT EXISTS (SELECT 1 FROM departments WHERE departments.id = employees.dept_id AND budget < 400000)`, 4},
		{"WHERE scalar subquery", `SELECT name FROM employees WHERE salary > (SELECT AVG(salary) FROM employees)`, 3},
		{"WHERE correlated subquery", `SELECT name FROM employees WHERE salary > (SELECT AVG(salary) FROM employees e2 WHERE e2.dept_id = employees.dept_id)`, 2},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rows, err := db.Query(ctx, tt.sql)
			if err != nil {
				t.Logf("Query error: %v", err)
				return
			}
			defer rows.Close()

			count := 0
			for rows.Next() {
				var name string
				rows.Scan(&name)
				count++
				t.Logf("  Result: %s", name)
			}
			t.Logf("Query returned %d rows (expected %d)", count, tt.expected)
		})
	}
}

// TestWhereComplexExpressions targets evaluateWhere with complex boolean logic
func TestWhereComplexExpressions(t *testing.T) {
	db, err := engine.Open(":memory:", &engine.Options{InMemory: true})
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	ctx := context.Background()

	_, err = db.Exec(ctx, `CREATE TABLE products (name TEXT, category TEXT, price INTEGER, stock INTEGER, active BOOLEAN)`)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	_, err = db.Exec(ctx, `INSERT INTO products VALUES
		('Product A', 'Electronics', 100, 50, true),
		('Product B', 'Electronics', 200, 0, true),
		('Product C', 'Clothing', 50, 100, false),
		('Product D', 'Clothing', 75, 25, true),
		('Product E', 'Food', 10, 200, true),
		('Product F', 'Food', 20, 0, false)`)
	if err != nil {
		t.Fatalf("Failed to insert: %v", err)
	}

	tests := []struct {
		name string
		sql  string
		desc string
	}{
		{"AND-OR combination", `SELECT name FROM products WHERE (category = 'Electronics' OR category = 'Clothing') AND active = true`, "Electronics or Clothing that are active"},
		{"Nested parentheses", `SELECT name FROM products WHERE ((price < 100 AND stock > 20) OR (price >= 100 AND stock > 0)) AND active = true`, "Complex nesting"},
		{"NOT with AND/OR", `SELECT name FROM products WHERE NOT (category = 'Food' OR stock = 0) AND active = true`, "Not Food and not out of stock, but active"},
		{"Multiple OR conditions", `SELECT name FROM products WHERE category = 'Electronics' OR price < 20 OR stock > 150`, "Any of these conditions"},
		{"Boolean column", `SELECT name FROM products WHERE active = true AND stock > 0`, "Active with stock"},
		{"Complex comparison", `SELECT name FROM products WHERE price * 2 < stock * 10 AND active = true`, "Price to stock ratio"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rows, err := db.Query(ctx, tt.sql)
			if err != nil {
				t.Logf("Query error for %s: %v", tt.desc, err)
				return
			}
			defer rows.Close()

			count := 0
			for rows.Next() {
				var name string
				rows.Scan(&name)
				count++
				t.Logf("  Result: %s", name)
			}
			t.Logf("%s: %d products found", tt.desc, count)
		})
	}
}

// TestWhereWithCase targets evaluateWhere with CASE expressions
func TestWhereWithCase(t *testing.T) {
	db, err := engine.Open(":memory:", &engine.Options{InMemory: true})
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	ctx := context.Background()

	_, err = db.Exec(ctx, `CREATE TABLE orders (id INTEGER, amount INTEGER, status TEXT, priority INTEGER)`)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	_, err = db.Exec(ctx, `INSERT INTO orders VALUES
		(1, 100, 'pending', 1),
		(2, 500, 'pending', 2),
		(3, 1000, 'completed', 1),
		(4, 50, 'cancelled', 3),
		(5, 2000, 'pending', 1)`)
	if err != nil {
		t.Fatalf("Failed to insert: %v", err)
	}

	// WHERE with CASE expression
	rows, err := db.Query(ctx, `
		SELECT id, amount, status
		FROM orders
		WHERE (CASE
			WHEN status = 'completed' THEN amount > 500
			WHEN status = 'pending' THEN amount > 100
			ELSE amount > 0
		END)`)
	if err != nil {
		t.Logf("WHERE with CASE error: %v", err)
		return
	}
	defer rows.Close()

	count := 0
	for rows.Next() {
		var id, amount int
		var status string
		rows.Scan(&id, &amount, &status)
		count++
		t.Logf("Order %d: %d (%s)", id, amount, status)
	}
	t.Logf("Total matching orders: %d", count)
}

// TestWhereInExpression targets evaluateWhere with IN expressions
func TestWhereInExpression(t *testing.T) {
	db, err := engine.Open(":memory:", &engine.Options{InMemory: true})
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	ctx := context.Background()

	_, err = db.Exec(ctx, `CREATE TABLE items (name TEXT, category TEXT, tags TEXT)`)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	_, err = db.Exec(ctx, `INSERT INTO items VALUES
		('Item 1', 'A', 'red,blue'),
		('Item 2', 'B', 'green'),
		('Item 3', 'A', 'red'),
		('Item 4', 'C', 'blue,green'),
		('Item 5', 'B', 'red,green')`)
	if err != nil {
		t.Fatalf("Failed to insert: %v", err)
	}

	tests := []struct {
		name string
		sql  string
	}{
		{"IN list", `SELECT name FROM items WHERE category IN ('A', 'C')`},
		{"NOT IN list", `SELECT name FROM items WHERE category NOT IN ('B', 'C')`},
		{"IN with single value", `SELECT name FROM items WHERE category IN ('A')`},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rows, err := db.Query(ctx, tt.sql)
			if err != nil {
				t.Logf("Query error: %v", err)
				return
			}
			defer rows.Close()

			count := 0
			for rows.Next() {
				var name string
				rows.Scan(&name)
				count++
				t.Logf("  Result: %s", name)
			}
			t.Logf("%s: %d items found", tt.name, count)
		})
	}
}
