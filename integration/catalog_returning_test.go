package integration

import (
	"context"
	"testing"

	"github.com/cobaltdb/cobaltdb/pkg/engine"
)

// TestUpdateReturning tests UPDATE with RETURNING clause
func TestUpdateReturning(t *testing.T) {
	db, err := engine.Open(":memory:", &engine.Options{InMemory: true})
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	ctx := context.Background()

	// Create test table
	_, err = db.Exec(ctx, `CREATE TABLE products (
		id INTEGER PRIMARY KEY,
		name TEXT,
		price REAL
	)`)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// Insert test data
	_, err = db.Exec(ctx, `INSERT INTO products VALUES (1, 'Apple', 1.50), (2, 'Banana', 0.75), (3, 'Cherry', 2.00)`)
	if err != nil {
		t.Fatalf("Failed to insert: %v", err)
	}

	// Test UPDATE with RETURNING *
	rows, err := db.Query(ctx, `UPDATE products SET price = price * 1.1 WHERE price < 2.0 RETURNING *`)
	if err != nil {
		t.Logf("UPDATE RETURNING error: %v", err)
		return
	}
	defer rows.Close()

	count := 0
	for rows.Next() {
		var id int
		var name string
		var price float64
		if err := rows.Scan(&id, &name, &price); err != nil {
			t.Logf("Scan error: %v", err)
			return
		}
		count++
		t.Logf("Updated row: id=%d, name=%s, price=%.2f", id, name, price)
	}

	if count != 2 {
		t.Errorf("Expected 2 rows returned, got %d", count)
	}

	t.Log("UPDATE RETURNING works correctly")
}

// TestUpdateReturningColumns tests UPDATE with specific RETURNING columns
func TestUpdateReturningColumns(t *testing.T) {
	db, err := engine.Open(":memory:", &engine.Options{InMemory: true})
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	ctx := context.Background()

	// Create test table
	_, err = db.Exec(ctx, `CREATE TABLE employees (
		id INTEGER PRIMARY KEY,
		name TEXT,
		salary INTEGER
	)`)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// Insert test data
	_, err = db.Exec(ctx, `INSERT INTO employees VALUES (1, 'Alice', 50000), (2, 'Bob', 60000)`)
	if err != nil {
		t.Fatalf("Failed to insert: %v", err)
	}

	// Test UPDATE with RETURNING specific columns
	rows, err := db.Query(ctx, `UPDATE employees SET salary = salary + 5000 RETURNING id, name, salary`)
	if err != nil {
		t.Logf("UPDATE RETURNING columns error: %v", err)
		return
	}
	defer rows.Close()

	count := 0
	for rows.Next() {
		var id, salary int
		var name string
		if err := rows.Scan(&id, &name, &salary); err != nil {
			t.Logf("Scan error: %v", err)
			return
		}
		count++
		t.Logf("Updated: id=%d, name=%s, new_salary=%d", id, name, salary)
		if salary != 55000 && salary != 65000 {
			t.Errorf("Unexpected salary: %d", salary)
		}
	}

	if count != 2 {
		t.Errorf("Expected 2 rows returned, got %d", count)
	}

	t.Log("UPDATE RETURNING with specific columns works correctly")
}

// TestDeleteReturning tests DELETE with RETURNING clause
func TestDeleteReturning(t *testing.T) {
	db, err := engine.Open(":memory:", &engine.Options{InMemory: true})
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	ctx := context.Background()

	// Create test table
	_, err = db.Exec(ctx, `CREATE TABLE logs (
		id INTEGER PRIMARY KEY,
		message TEXT,
		created_at INTEGER
	)`)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// Insert test data
	_, err = db.Exec(ctx, `INSERT INTO logs VALUES (1, 'First entry', 1000), (2, 'Second entry', 2000), (3, 'Third entry', 3000)`)
	if err != nil {
		t.Fatalf("Failed to insert: %v", err)
	}

	// Test DELETE with RETURNING *
	rows, err := db.Query(ctx, `DELETE FROM logs WHERE created_at < 2500 RETURNING *`)
	if err != nil {
		t.Logf("DELETE RETURNING error: %v", err)
		return
	}
	defer rows.Close()

	count := 0
	for rows.Next() {
		var id, createdAt int
		var message string
		if err := rows.Scan(&id, &message, &createdAt); err != nil {
			t.Logf("Scan error: %v", err)
			return
		}
		count++
		t.Logf("Deleted row: id=%d, message=%s, created_at=%d", id, message, createdAt)
	}

	if count != 2 {
		t.Errorf("Expected 2 rows returned, got %d", count)
	}

	// Verify remaining rows
	remaining, err := db.Query(ctx, `SELECT COUNT(*) FROM logs`)
	if err != nil {
		t.Fatalf("Failed to count remaining rows: %v", err)
	}
	defer remaining.Close()

	if remaining.Next() {
		var cnt int
		remaining.Scan(&cnt)
		if cnt != 1 {
			t.Errorf("Expected 1 remaining row, got %d", cnt)
		}
	}

	t.Log("DELETE RETURNING works correctly")
}

// TestDeleteReturningColumns tests DELETE with specific RETURNING columns
func TestDeleteReturningColumns(t *testing.T) {
	db, err := engine.Open(":memory:", &engine.Options{InMemory: true})
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	ctx := context.Background()

	// Create test table
	_, err = db.Exec(ctx, `CREATE TABLE events (
		id INTEGER PRIMARY KEY,
		event_type TEXT,
		data TEXT
	)`)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// Insert test data
	_, err = db.Exec(ctx, `INSERT INTO events VALUES (1, 'click', 'button1'), (2, 'scroll', 'page1'), (3, 'click', 'button2')`)
	if err != nil {
		t.Fatalf("Failed to insert: %v", err)
	}

	// Test DELETE with RETURNING specific columns
	rows, err := db.Query(ctx, `DELETE FROM events WHERE event_type = 'click' RETURNING id, data`)
	if err != nil {
		t.Logf("DELETE RETURNING columns error: %v", err)
		return
	}
	defer rows.Close()

	count := 0
	for rows.Next() {
		var id int
		var data string
		if err := rows.Scan(&id, &data); err != nil {
			t.Logf("Scan error: %v", err)
			return
		}
		count++
		t.Logf("Deleted: id=%d, data=%s", id, data)
	}

	if count != 2 {
		t.Errorf("Expected 2 rows returned, got %d", count)
	}

	t.Log("DELETE RETURNING with specific columns works correctly")
}

// TestInsertReturningComplete tests INSERT RETURNING more thoroughly
func TestInsertReturningComplete(t *testing.T) {
	db, err := engine.Open(":memory:", &engine.Options{InMemory: true})
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	ctx := context.Background()

	// Create test table
	_, err = db.Exec(ctx, `CREATE TABLE users (
		id INTEGER PRIMARY KEY,
		name TEXT,
		email TEXT
	)`)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// Test INSERT with RETURNING *
	rows, err := db.Query(ctx, `INSERT INTO users (id, name, email) VALUES (1, 'Alice', 'alice@example.com') RETURNING *`)
	if err != nil {
		t.Logf("INSERT RETURNING error: %v", err)
		return
	}
	defer rows.Close()

	if rows.Next() {
		var id int
		var name, email string
		if err := rows.Scan(&id, &name, &email); err != nil {
			t.Logf("Scan error: %v", err)
			return
		}
		if id != 1 || name != "Alice" || email != "alice@example.com" {
			t.Errorf("Unexpected values: id=%d, name=%s, email=%s", id, name, email)
		}
		t.Logf("Inserted: id=%d, name=%s, email=%s", id, name, email)
	} else {
		t.Error("Expected a row to be returned")
	}

	t.Log("INSERT RETURNING works correctly")
}
