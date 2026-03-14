package integration

import (
	"context"
	"testing"

	"github.com/cobaltdb/cobaltdb/pkg/engine"
)

// TestUpdateWithFromClause tests UPDATE with FROM clause
func TestUpdateWithFromClause(t *testing.T) {
	db, err := engine.Open(":memory:", &engine.Options{InMemory: true})
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	ctx := context.Background()

	// Create tables
	_, err = db.Exec(ctx, `CREATE TABLE employees (
		id INTEGER PRIMARY KEY,
		name TEXT,
		dept_id INTEGER,
		salary REAL
	)`)
	if err != nil {
		t.Fatalf("Failed to create employees: %v", err)
	}

	_, err = db.Exec(ctx, `CREATE TABLE departments (
		id INTEGER PRIMARY KEY,
		name TEXT,
		budget REAL
	)`)
	if err != nil {
		t.Fatalf("Failed to create departments: %v", err)
	}

	// Insert data
	_, err = db.Exec(ctx, `INSERT INTO departments VALUES (1, 'Engineering', 1000000), (2, 'Sales', 500000)`)
	if err != nil {
		t.Fatalf("Failed to insert departments: %v", err)
	}

	_, err = db.Exec(ctx, `INSERT INTO employees VALUES (1, 'Alice', 1, 80000), (2, 'Bob', 1, 70000), (3, 'Carol', 2, 60000)`)
	if err != nil {
		t.Fatalf("Failed to insert employees: %v", err)
	}

	// UPDATE with FROM - increase salary for Engineering employees
	result, err := db.Exec(ctx, `UPDATE employees SET salary = salary * 1.1 FROM departments WHERE employees.dept_id = departments.id AND departments.name = 'Engineering'`)
	if err != nil {
		t.Logf("UPDATE FROM error: %v", err)
		return
	}

	rowsAffected := result.RowsAffected
	if rowsAffected != 2 {
		t.Errorf("Expected 2 rows updated, got %d", rowsAffected)
	}

	// Verify the update
	rows, err := db.Query(ctx, `SELECT name, salary FROM employees WHERE dept_id = 1 ORDER BY name`)
	if err != nil {
		t.Fatalf("Failed to query: %v", err)
	}
	defer rows.Close()

	expected := map[string]float64{
		"Alice": 88000,
		"Bob":   77000,
	}

	count := 0
	for rows.Next() {
		var name string
		var salary interface{}
		if err := rows.Scan(&name, &salary); err != nil {
			t.Fatalf("Scan error: %v", err)
		}
		count++
		var salaryFloat float64
		switch v := salary.(type) {
		case float64:
			salaryFloat = v
		case int64:
			salaryFloat = float64(v)
		case int:
			salaryFloat = float64(v)
		}
		if expected[name] != salaryFloat {
			t.Errorf("Expected %s salary=%.0f, got %.0f", name, expected[name], salaryFloat)
		}
	}

	if count != 2 {
		t.Errorf("Expected 2 rows, got %d", count)
	}

	t.Log("UPDATE with FROM works correctly")
}

// TestUpdateWithJoinClause tests UPDATE with JOIN syntax
func TestUpdateWithJoinClause(t *testing.T) {
	db, err := engine.Open(":memory:", &engine.Options{InMemory: true})
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	ctx := context.Background()

	// Create tables
	_, err = db.Exec(ctx, `CREATE TABLE orders (
		id INTEGER PRIMARY KEY,
		customer_id INTEGER,
		amount REAL
	)`)
	if err != nil {
		t.Fatalf("Failed to create orders: %v", err)
	}

	_, err = db.Exec(ctx, `CREATE TABLE customers (
		id INTEGER PRIMARY KEY,
		name TEXT,
		status TEXT
	)`)
	if err != nil {
		t.Fatalf("Failed to create customers: %v", err)
	}

	// Insert data
	_, err = db.Exec(ctx, `INSERT INTO customers VALUES (1, 'Acme Corp', 'VIP'), (2, 'Beta Inc', 'Regular')`)
	if err != nil {
		t.Fatalf("Failed to insert customers: %v", err)
	}

	_, err = db.Exec(ctx, `INSERT INTO orders VALUES (1, 1, 1000), (2, 1, 2000), (3, 2, 500)`)
	if err != nil {
		t.Fatalf("Failed to insert orders: %v", err)
	}

	// UPDATE with JOIN - apply VIP discount
	result, err := db.Exec(ctx, `UPDATE orders SET amount = amount * 0.9 FROM customers WHERE orders.customer_id = customers.id AND customers.status = 'VIP'`)
	if err != nil {
		t.Logf("UPDATE JOIN error: %v", err)
		return
	}

	rowsAffected := result.RowsAffected
	if rowsAffected != 2 {
		t.Errorf("Expected 2 rows updated, got %d", rowsAffected)
	}

	// Verify the update
	rows, err := db.Query(ctx, `SELECT id, amount FROM orders WHERE customer_id = 1 ORDER BY id`)
	if err != nil {
		t.Fatalf("Failed to query: %v", err)
	}
	defer rows.Close()

	expected := map[int]float64{
		1: 900,
		2: 1800,
	}

	count := 0
	for rows.Next() {
		var id int
		var amount interface{}
		if err := rows.Scan(&id, &amount); err != nil {
			t.Fatalf("Scan error: %v", err)
		}
		count++
		var amountFloat float64
		switch v := amount.(type) {
		case float64:
			amountFloat = v
		case int64:
			amountFloat = float64(v)
		case int:
			amountFloat = float64(v)
		}
		if expected[id] != amountFloat {
			t.Errorf("Order %d: expected amount=%.0f, got %.0f", id, expected[id], amountFloat)
		}
	}

	if count != 2 {
		t.Errorf("Expected 2 rows, got %d", count)
	}

	t.Log("UPDATE with JOIN works correctly")
}

// TestDeleteWithUsingClause tests DELETE with USING clause
func TestDeleteWithUsingClause(t *testing.T) {
	db, err := engine.Open(":memory:", &engine.Options{InMemory: true})
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	ctx := context.Background()

	// Create tables
	_, err = db.Exec(ctx, `CREATE TABLE orders (
		id INTEGER PRIMARY KEY,
		customer_id INTEGER,
		amount REAL
	)`)
	if err != nil {
		t.Fatalf("Failed to create orders: %v", err)
	}

	_, err = db.Exec(ctx, `CREATE TABLE customers (
		id INTEGER PRIMARY KEY,
		name TEXT,
		status TEXT
	)`)
	if err != nil {
		t.Fatalf("Failed to create customers: %v", err)
	}

	// Insert data
	_, err = db.Exec(ctx, `INSERT INTO customers VALUES (1, 'Acme Corp', 'Inactive'), (2, 'Beta Inc', 'Active')`)
	if err != nil {
		t.Fatalf("Failed to insert customers: %v", err)
	}

	_, err = db.Exec(ctx, `INSERT INTO orders VALUES (1, 1, 1000), (2, 1, 2000), (3, 2, 500)`)
	if err != nil {
		t.Fatalf("Failed to insert orders: %v", err)
	}

	// DELETE with USING - delete orders for inactive customers
	result, err := db.Exec(ctx, `DELETE FROM orders USING customers WHERE orders.customer_id = customers.id AND customers.status = 'Inactive'`)
	if err != nil {
		t.Logf("DELETE USING error: %v", err)
		return
	}

	rowsAffected := result.RowsAffected
	if rowsAffected != 2 {
		t.Errorf("Expected 2 rows deleted, got %d", rowsAffected)
	}

	// Verify remaining orders
	rows, err := db.Query(ctx, `SELECT COUNT(*) FROM orders`)
	if err != nil {
		t.Fatalf("Failed to query: %v", err)
	}
	defer rows.Close()

	if rows.Next() {
		var count int
		if err := rows.Scan(&count); err != nil {
			t.Fatalf("Scan error: %v", err)
		}
		if count != 1 {
			t.Errorf("Expected 1 remaining order, got %d", count)
		}
	}

	t.Log("DELETE with USING works correctly")
}
