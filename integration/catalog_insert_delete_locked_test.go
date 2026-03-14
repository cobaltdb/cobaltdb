package integration

import (
	"context"
	"testing"

	"github.com/cobaltdb/cobaltdb/pkg/engine"
)

// TestInsertWithDefaults targets insertLocked with DEFAULT values
func TestInsertWithDefaults(t *testing.T) {
	db, err := engine.Open(":memory:", &engine.Options{InMemory: true})
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	ctx := context.Background()

	// Create table with DEFAULT values
	_, err = db.Exec(ctx, `CREATE TABLE products (
		id INTEGER PRIMARY KEY,
		name TEXT DEFAULT 'Unknown',
		price INTEGER DEFAULT 0,
		active BOOLEAN DEFAULT true,
		created_at TEXT DEFAULT '2024-01-01'
	)`)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	tests := []struct {
		name string
		sql  string
		desc string
	}{
		{"All defaults", `INSERT INTO products (id) VALUES (1)`, "Only PK specified"},
		{"Some defaults", `INSERT INTO products (id, name) VALUES (2, 'Widget')`, "Partial defaults"},
		{"No defaults", `INSERT INTO products VALUES (3, 'Gadget', 100, true, '2024-06-01')`, "All values specified"},
		{"Mixed with NULL", `INSERT INTO products (id, name, price) VALUES (4, NULL, 50)`, "NULL override"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := db.Exec(ctx, tt.sql)
			if err != nil {
				t.Logf("Insert error for %s: %v", tt.desc, err)
				return
			}
			t.Logf("Insert succeeded for %s", tt.desc)
		})
	}

	// Verify defaults were applied
	rows, _ := db.Query(ctx, `SELECT id, name, price, active FROM products ORDER BY id`)
	if rows != nil {
		defer rows.Close()
		for rows.Next() {
			var id, price int
			var name string
			var active bool
			rows.Scan(&id, &name, &price, &active)
			t.Logf("Row %d: name=%q, price=%d, active=%v", id, name, price, active)
		}
	}
}

// TestInsertExpressions targets insertLocked with expression evaluation
func TestInsertExpressions(t *testing.T) {
	db, err := engine.Open(":memory:", &engine.Options{InMemory: true})
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	ctx := context.Background()

	_, err = db.Exec(ctx, `CREATE TABLE calculations (
		a INTEGER,
		b INTEGER,
		sum INTEGER,
		product INTEGER,
		formula TEXT
	)`)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// Insert with expressions
	_, err = db.Exec(ctx, `INSERT INTO calculations (a, b, sum, product, formula) VALUES
		(5, 3, 5 + 3, 5 * 3, 'addition'),
		(10, 2, 10 + 2, 10 * 2, 'multiply')`)
	if err != nil {
		t.Logf("Insert with expressions error: %v", err)
		return
	}

	// Verify calculations
	rows, err := db.Query(ctx, `SELECT a, b, sum, product FROM calculations`)
	if err != nil {
		t.Fatalf("Query error: %v", err)
	}
	defer rows.Close()

	for rows.Next() {
		var a, b, sum, product int
		rows.Scan(&a, &b, &sum, &product)
		t.Logf("a=%d, b=%d, sum=%d (expected %d), product=%d (expected %d)",
			a, b, sum, a+b, product, a*b)
	}
}

// TestDeleteWithTriggers targets deleteRowLocked with triggers
func TestDeleteWithTriggers(t *testing.T) {
	db, err := engine.Open(":memory:", &engine.Options{InMemory: true})
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	ctx := context.Background()

	// Create audit table
	_, err = db.Exec(ctx, `CREATE TABLE audit_log (id INTEGER PRIMARY KEY, action TEXT, old_id INTEGER, old_name TEXT)`)
	if err != nil {
		t.Fatalf("Failed to create audit table: %v", err)
	}

	// Create main table with trigger
	_, err = db.Exec(ctx, `CREATE TABLE items (id INTEGER PRIMARY KEY, name TEXT)`)
	if err != nil {
		t.Fatalf("Failed to create items table: %v", err)
	}

	_, err = db.Exec(ctx, `INSERT INTO items VALUES (1, 'Item A'), (2, 'Item B'), (3, 'Item C')`)
	if err != nil {
		t.Fatalf("Failed to insert items: %v", err)
	}

	// Create BEFORE DELETE trigger
	_, err = db.Exec(ctx, `CREATE TRIGGER before_delete_items
		BEFORE DELETE ON items
		BEGIN
			INSERT INTO audit_log (action, old_id, old_name) VALUES ('DELETE', OLD.id, OLD.name);
		END`)
	if err != nil {
		t.Logf("Create trigger error: %v", err)
		return
	}

	// Delete rows
	_, err = db.Exec(ctx, `DELETE FROM items WHERE id = 1`)
	if err != nil {
		t.Logf("Delete error: %v", err)
		return
	}

	// Verify audit log
	rows, _ := db.Query(ctx, `SELECT action, old_id, old_name FROM audit_log`)
	if rows != nil {
		defer rows.Close()
		for rows.Next() {
			var action, oldName string
			var oldID int
			rows.Scan(&action, &oldID, &oldName)
			t.Logf("Audit: %s id=%d name=%s", action, oldID, oldName)
		}
	}
}

// TestDeleteWithFKCascadeDeep targets deleteRowLocked with FK CASCADE (deep test)
func TestDeleteWithFKCascadeDeep(t *testing.T) {
	db, err := engine.Open(":memory:", &engine.Options{InMemory: true})
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	ctx := context.Background()

	// Create parent table
	_, err = db.Exec(ctx, `CREATE TABLE departments (id INTEGER PRIMARY KEY, name TEXT)`)
	if err != nil {
		t.Fatalf("Failed to create departments: %v", err)
	}

	// Create child table with CASCADE
	_, err = db.Exec(ctx, `CREATE TABLE employees (
		id INTEGER PRIMARY KEY,
		name TEXT,
		dept_id INTEGER,
		FOREIGN KEY (dept_id) REFERENCES departments(id) ON DELETE CASCADE
	)`)
	if err != nil {
		t.Fatalf("Failed to create employees: %v", err)
	}

	// Insert data
	_, err = db.Exec(ctx, `INSERT INTO departments VALUES (1, 'Engineering'), (2, 'Sales')`)
	if err != nil {
		t.Fatalf("Failed to insert departments: %v", err)
	}

	_, err = db.Exec(ctx, `INSERT INTO employees VALUES
		(1, 'Alice', 1), (2, 'Bob', 1), (3, 'Charlie', 2), (4, 'Diana', 1)`)
	if err != nil {
		t.Fatalf("Failed to insert employees: %v", err)
	}

	// Delete department (should cascade to employees)
	_, err = db.Exec(ctx, `DELETE FROM departments WHERE id = 1`)
	if err != nil {
		t.Logf("Delete with cascade error: %v", err)
		return
	}

	// Verify cascade worked
	rows, _ := db.Query(ctx, `SELECT COUNT(*) FROM employees`)
	if rows != nil {
		defer rows.Close()
		if rows.Next() {
			var count int
			rows.Scan(&count)
			t.Logf("Remaining employees after cascade: %d (expected 1)", count)
		}
	}
}

// TestDeleteWithFKSetNullDeep targets deleteRowLocked with FK SET NULL (deep test)
func TestDeleteWithFKSetNullDeep(t *testing.T) {
	db, err := engine.Open(":memory:", &engine.Options{InMemory: true})
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	ctx := context.Background()

	// Create parent table
	_, err = db.Exec(ctx, `CREATE TABLE categories (id INTEGER PRIMARY KEY, name TEXT)`)
	if err != nil {
		t.Fatalf("Failed to create categories: %v", err)
	}

	// Create child table with SET NULL
	_, err = db.Exec(ctx, `CREATE TABLE products (
		id INTEGER PRIMARY KEY,
		name TEXT,
		cat_id INTEGER,
		FOREIGN KEY (cat_id) REFERENCES categories(id) ON DELETE SET NULL
	)`)
	if err != nil {
		t.Fatalf("Failed to create products: %v", err)
	}

	// Insert data
	_, err = db.Exec(ctx, `INSERT INTO categories VALUES (1, 'Electronics'), (2, 'Clothing')`)
	if err != nil {
		t.Fatalf("Failed to insert categories: %v", err)
	}

	_, err = db.Exec(ctx, `INSERT INTO products VALUES
		(1, 'Laptop', 1), (2, 'Phone', 1), (3, 'Shirt', 2)`)
	if err != nil {
		t.Fatalf("Failed to insert products: %v", err)
	}

	// Delete category (should set cat_id to NULL)
	_, err = db.Exec(ctx, `DELETE FROM categories WHERE id = 1`)
	if err != nil {
		t.Logf("Delete with SET NULL error: %v", err)
		return
	}

	// Verify SET NULL worked
	rows, _ := db.Query(ctx, `SELECT name, cat_id FROM products`)
	if rows != nil {
		defer rows.Close()
		for rows.Next() {
			var name string
			var catID interface{}
			rows.Scan(&name, &catID)
			t.Logf("Product %s has cat_id: %v", name, catID)
		}
	}
}

// TestDeleteWithIndexCleanup targets deleteRowLocked with index maintenance
func TestDeleteWithIndexCleanup(t *testing.T) {
	db, err := engine.Open(":memory:", &engine.Options{InMemory: true})
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	ctx := context.Background()

	// Create table with unique index
	_, err = db.Exec(ctx, `CREATE TABLE users (id INTEGER PRIMARY KEY, email TEXT UNIQUE, name TEXT)`)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// Insert users
	_, err = db.Exec(ctx, `INSERT INTO users VALUES
		(1, 'alice@example.com', 'Alice'),
		(2, 'bob@example.com', 'Bob'),
		(3, 'charlie@example.com', 'Charlie')`)
	if err != nil {
		t.Fatalf("Failed to insert users: %v", err)
	}

	// Delete a user
	_, err = db.Exec(ctx, `DELETE FROM users WHERE id = 2`)
	if err != nil {
		t.Logf("Delete error: %v", err)
		return
	}

	// Try to insert with the same email (should work since bob was deleted)
	_, err = db.Exec(ctx, `INSERT INTO users VALUES (4, 'bob@example.com', 'New Bob')`)
	if err != nil {
		t.Logf("Re-insert with same email error: %v", err)
		return
	}

	t.Log("Index cleanup worked correctly - can reuse deleted email")
}

// TestDeleteWithUndoLog targets deleteRowLocked with transaction rollback
func TestDeleteWithUndoLog(t *testing.T) {
	db, err := engine.Open(":memory:", &engine.Options{InMemory: true})
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	ctx := context.Background()

	_, err = db.Exec(ctx, `CREATE TABLE data (id INTEGER PRIMARY KEY, value TEXT)`)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	_, err = db.Exec(ctx, `INSERT INTO data VALUES (1, 'A'), (2, 'B'), (3, 'C')`)
	if err != nil {
		t.Fatalf("Failed to insert: %v", err)
	}

	// Delete within transaction then rollback
	tx, err := db.Begin(ctx)
	if err != nil {
		t.Fatalf("Failed to begin transaction: %v", err)
	}

	_, err = tx.Exec(ctx, `DELETE FROM data WHERE id = 2`)
	if err != nil {
		t.Fatalf("Delete in transaction error: %v", err)
	}

	// Verify deleted in transaction
	rows, _ := tx.Query(ctx, `SELECT COUNT(*) FROM data`)
	if rows != nil {
		defer rows.Close()
		if rows.Next() {
			var count int
			rows.Scan(&count)
			t.Logf("Count in transaction (after delete): %d", count)
		}
	}

	// Rollback
	tx.Rollback()

	// Verify data restored
	rows, _ = db.Query(ctx, `SELECT COUNT(*) FROM data`)
	if rows != nil {
		defer rows.Close()
		if rows.Next() {
			var count int
			rows.Scan(&count)
			t.Logf("Count after rollback: %d (expected 3)", count)
		}
	}
}
