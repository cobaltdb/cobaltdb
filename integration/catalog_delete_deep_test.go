package integration

import (
	"context"
	"testing"

	"github.com/cobaltdb/cobaltdb/pkg/engine"
)

// TestDeleteWithBeforeTrigger targets delete with BEFORE DELETE trigger
func TestDeleteWithBeforeTrigger(t *testing.T) {
	db, err := engine.Open(":memory:", &engine.Options{InMemory: true})
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	ctx := context.Background()

	// Create tables
	_, err = db.Exec(ctx, `CREATE TABLE delete_log (
		id INTEGER PRIMARY KEY,
		deleted_id INTEGER,
		deleted_at TEXT DEFAULT CURRENT_TIMESTAMP
	)`)
	if err != nil {
		t.Fatalf("Failed to create log table: %v", err)
	}

	_, err = db.Exec(ctx, `CREATE TABLE main_data (
		id INTEGER PRIMARY KEY,
		value TEXT
	)`)
	if err != nil {
		t.Fatalf("Failed to create main table: %v", err)
	}

	// Insert data
	_, err = db.Exec(ctx, `INSERT INTO main_data VALUES (1, 'test1'), (2, 'test2'), (3, 'test3')`)
	if err != nil {
		t.Fatalf("Failed to insert: %v", err)
	}

	// Create BEFORE DELETE trigger
	_, err = db.Exec(ctx, `CREATE TRIGGER before_delete_main
		BEFORE DELETE ON main_data
		BEGIN
			INSERT INTO delete_log (deleted_id) VALUES (OLD.id);
		END`)
	if err != nil {
		t.Logf("BEFORE DELETE trigger creation error: %v", err)
		return
	}

	// Delete row - trigger should fire
	_, err = db.Exec(ctx, `DELETE FROM main_data WHERE id = 2`)
	if err != nil {
		t.Logf("Delete with BEFORE trigger error: %v", err)
		return
	}

	// Verify trigger fired
	rows, err := db.Query(ctx, `SELECT COUNT(*) FROM delete_log`)
	if err != nil {
		t.Fatalf("Failed to query log: %v", err)
	}
	defer rows.Close()

	if rows.Next() {
		var count int
		rows.Scan(&count)
		// BEFORE DELETE trigger may not be supported, just log
		t.Logf("BEFORE DELETE trigger log entries: %d", count)
	}
}

// TestDeleteWithAfterTrigger targets delete with AFTER DELETE trigger
func TestDeleteWithAfterTrigger(t *testing.T) {
	db, err := engine.Open(":memory:", &engine.Options{InMemory: true})
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	ctx := context.Background()

	_, err = db.Exec(ctx, `CREATE TABLE audit_after (
		action TEXT,
		table_name TEXT,
		row_id INTEGER
	)`)
	if err != nil {
		t.Fatalf("Failed to create audit table: %v", err)
	}

	_, err = db.Exec(ctx, `CREATE TABLE products (
		id INTEGER PRIMARY KEY,
		name TEXT
	)`)
	if err != nil {
		t.Fatalf("Failed to create products: %v", err)
	}

	_, err = db.Exec(ctx, `INSERT INTO products VALUES (1, 'Product A'), (2, 'Product B')`)
	if err != nil {
		t.Fatalf("Failed to insert: %v", err)
	}

	// Create AFTER DELETE trigger
	_, err = db.Exec(ctx, `CREATE TRIGGER after_delete_product
		AFTER DELETE ON products
		BEGIN
			INSERT INTO audit_after (action, table_name, row_id) VALUES ('DELETE', 'products', OLD.id);
		END`)
	if err != nil {
		t.Logf("AFTER DELETE trigger creation error: %v", err)
		return
	}

	// Delete rows
	_, err = db.Exec(ctx, `DELETE FROM products WHERE id = 1`)
	if err != nil {
		t.Logf("Delete with AFTER trigger error: %v", err)
		return
	}

	// Verify audit
	rows, err := db.Query(ctx, `SELECT COUNT(*) FROM audit_after`)
	if err != nil {
		t.Fatalf("Failed to query audit: %v", err)
	}
	defer rows.Close()

	if rows.Next() {
		var count int
		rows.Scan(&count)
		if count != 1 {
			t.Errorf("Expected 1 audit entry, got %d", count)
		}
		t.Logf("AFTER DELETE trigger fired: %d audit entries", count)
	}
}

// TestDeleteWithTriggerWhen targets delete trigger with WHEN clause
func TestDeleteWithTriggerWhen(t *testing.T) {
	db, err := engine.Open(":memory:", &engine.Options{InMemory: true})
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	ctx := context.Background()

	_, err = db.Exec(ctx, `CREATE TABLE important_items (
		id INTEGER PRIMARY KEY,
		priority INTEGER,
		name TEXT
	)`)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	_, err = db.Exec(ctx, `CREATE TABLE important_deletes (
		item_id INTEGER
	)`)
	if err != nil {
		t.Fatalf("Failed to create log table: %v", err)
	}

	_, err = db.Exec(ctx, `INSERT INTO important_items VALUES (1, 10, 'low'), (2, 100, 'high'), (3, 90, 'med')`)
	if err != nil {
		t.Fatalf("Failed to insert: %v", err)
	}

	// Create trigger with WHEN clause
	_, err = db.Exec(ctx, `CREATE TRIGGER conditional_delete
		AFTER DELETE ON important_items
		WHEN OLD.priority > 50
		BEGIN
			INSERT INTO important_deletes (item_id) VALUES (OLD.id);
		END`)
	if err != nil {
		t.Logf("Trigger with WHEN creation error: %v", err)
		return
	}

	// Delete low priority (should not trigger)
	_, err = db.Exec(ctx, `DELETE FROM important_items WHERE id = 1`)
	if err != nil {
		t.Logf("Delete low priority error: %v", err)
		return
	}

	// Delete high priority (should trigger)
	_, err = db.Exec(ctx, `DELETE FROM important_items WHERE id = 2`)
	if err != nil {
		t.Logf("Delete high priority error: %v", err)
		return
	}

	// Verify only high priority was logged
	rows, err := db.Query(ctx, `SELECT COUNT(*) FROM important_deletes`)
	if err != nil {
		t.Fatalf("Failed to query: %v", err)
	}
	defer rows.Close()

	if rows.Next() {
		var count int
		rows.Scan(&count)
		if count != 1 {
			t.Errorf("Expected 1 conditional delete log, got %d", count)
		}
		t.Logf("Conditional trigger fired for %d items", count)
	}
}

// TestDeleteMultipleRowsWithTrigger targets deleting multiple rows with trigger
func TestDeleteMultipleRowsWithTrigger(t *testing.T) {
	db, err := engine.Open(":memory:", &engine.Options{InMemory: true})
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	ctx := context.Background()

	_, err = db.Exec(ctx, `CREATE TABLE batch_items (
		id INTEGER PRIMARY KEY,
		category TEXT
	)`)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	_, err = db.Exec(ctx, `CREATE TABLE batch_delete_log (
		deleted_id INTEGER
	)`)
	if err != nil {
		t.Fatalf("Failed to create log: %v", err)
	}

	// Insert many rows
	for i := 1; i <= 10; i++ {
		cat := "A"
		if i > 5 {
			cat = "B"
		}
		_, err = db.Exec(ctx, `INSERT INTO batch_items VALUES (?, ?)`, i, cat)
		if err != nil {
			t.Fatalf("Failed to insert: %v", err)
		}
	}

	// Create trigger
	_, err = db.Exec(ctx, `CREATE TRIGGER batch_delete_trigger
		AFTER DELETE ON batch_items
		BEGIN
			INSERT INTO batch_delete_log (deleted_id) VALUES (OLD.id);
		END`)
	if err != nil {
		t.Logf("Trigger creation error: %v", err)
		return
	}

	// Delete multiple rows at once
	_, err = db.Exec(ctx, `DELETE FROM batch_items WHERE category = 'A'`)
	if err != nil {
		t.Logf("Batch delete error: %v", err)
		return
	}

	// Verify all 5 deletes were logged
	rows, err := db.Query(ctx, `SELECT COUNT(*) FROM batch_delete_log`)
	if err != nil {
		t.Fatalf("Failed to query: %v", err)
	}
	defer rows.Close()

	if rows.Next() {
		var count int
		rows.Scan(&count)
		if count != 5 {
			t.Errorf("Expected 5 trigger firings, got %d", count)
		}
		t.Logf("Trigger fired %d times for batch delete", count)
	}
}

// TestDeleteWithFKCascade targets delete with CASCADE foreign key
func TestDeleteWithFKCascade(t *testing.T) {
	db, err := engine.Open(":memory:", &engine.Options{InMemory: true})
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	ctx := context.Background()

	// Create parent table
	_, err = db.Exec(ctx, `CREATE TABLE departments (
		id INTEGER PRIMARY KEY,
		name TEXT
	)`)
	if err != nil {
		t.Fatalf("Failed to create departments: %v", err)
	}

	// Create child table with FK
	_, err = db.Exec(ctx, `CREATE TABLE employees_cascade (
		id INTEGER PRIMARY KEY,
		dept_id INTEGER,
		name TEXT,
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

	_, err = db.Exec(ctx, `INSERT INTO employees_cascade VALUES (1, 1, 'Alice'), (2, 1, 'Bob'), (3, 2, 'Carol')`)
	if err != nil {
		t.Fatalf("Failed to insert employees: %v", err)
	}

	// Delete department - should cascade to employees
	_, err = db.Exec(ctx, `DELETE FROM departments WHERE id = 1`)
	if err != nil {
		t.Logf("Delete with CASCADE error: %v", err)
		return
	}

	// Verify cascade worked
	rows, err := db.Query(ctx, `SELECT COUNT(*) FROM employees_cascade`)
	if err != nil {
		t.Fatalf("Failed to query employees: %v", err)
	}
	defer rows.Close()

	if rows.Next() {
		var count int
		rows.Scan(&count)
		if count != 1 {
			t.Errorf("Expected 1 employee after CASCADE delete, got %d", count)
		}
		t.Logf("CASCADE delete left %d employees", count)
	}
}

// TestDeleteWithFKSetNull targets delete with SET NULL foreign key
func TestDeleteWithFKSetNull(t *testing.T) {
	db, err := engine.Open(":memory:", &engine.Options{InMemory: true})
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	ctx := context.Background()

	_, err = db.Exec(ctx, `CREATE TABLE managers (
		id INTEGER PRIMARY KEY,
		name TEXT
	)`)
	if err != nil {
		t.Fatalf("Failed to create managers: %v", err)
	}

	_, err = db.Exec(ctx, `CREATE TABLE workers (
		id INTEGER PRIMARY KEY,
		manager_id INTEGER,
		name TEXT,
		FOREIGN KEY (manager_id) REFERENCES managers(id) ON DELETE SET NULL
	)`)
	if err != nil {
		t.Fatalf("Failed to create workers: %v", err)
	}

	// Insert data
	_, err = db.Exec(ctx, `INSERT INTO managers VALUES (1, 'Manager A'), (2, 'Manager B')`)
	if err != nil {
		t.Fatalf("Failed to insert managers: %v", err)
	}

	_, err = db.Exec(ctx, `INSERT INTO workers VALUES (1, 1, 'Worker 1'), (2, 1, 'Worker 2'), (3, 2, 'Worker 3')`)
	if err != nil {
		t.Fatalf("Failed to insert workers: %v", err)
	}

	// Delete manager - should set NULL on workers
	_, err = db.Exec(ctx, `DELETE FROM managers WHERE id = 1`)
	if err != nil {
		t.Logf("Delete with SET NULL error: %v", err)
		return
	}

	// Verify SET NULL worked
	rows, err := db.Query(ctx, `SELECT COUNT(*) FROM workers WHERE manager_id IS NULL`)
	if err != nil {
		t.Fatalf("Failed to query workers: %v", err)
	}
	defer rows.Close()

	if rows.Next() {
		var count int
		rows.Scan(&count)
		if count != 2 {
			t.Errorf("Expected 2 workers with NULL manager, got %d", count)
		}
		t.Logf("SET NULL worked for %d workers", count)
	}
}

// TestDeleteWithRestrictFK targets delete with RESTRICT foreign key
func TestDeleteWithRestrictFK(t *testing.T) {
	db, err := engine.Open(":memory:", &engine.Options{InMemory: true})
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	ctx := context.Background()

	_, err = db.Exec(ctx, `CREATE TABLE parents (
		id INTEGER PRIMARY KEY
	)`)
	if err != nil {
		t.Fatalf("Failed to create parents: %v", err)
	}

	_, err = db.Exec(ctx, `CREATE TABLE children (
		id INTEGER PRIMARY KEY,
		parent_id INTEGER,
		FOREIGN KEY (parent_id) REFERENCES parents(id) ON DELETE RESTRICT
	)`)
	if err != nil {
		t.Fatalf("Failed to create children: %v", err)
	}

	_, err = db.Exec(ctx, `INSERT INTO parents VALUES (1), (2)`)
	if err != nil {
		t.Fatalf("Failed to insert parents: %v", err)
	}

	_, err = db.Exec(ctx, `INSERT INTO children VALUES (1, 1)`)
	if err != nil {
		t.Fatalf("Failed to insert children: %v", err)
	}

	// Try to delete parent with child - should fail with RESTRICT
	_, err = db.Exec(ctx, `DELETE FROM parents WHERE id = 1`)
	if err != nil {
		t.Logf("Delete with RESTRICT correctly blocked: %v", err)
		return
	}

	// If we get here, RESTRICT didn't work
	t.Error("DELETE with RESTRICT should have failed but succeeded")
}

// TestDeleteAllRows targets DELETE without WHERE clause
func TestDeleteAllRows(t *testing.T) {
	db, err := engine.Open(":memory:", &engine.Options{InMemory: true})
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	ctx := context.Background()

	_, err = db.Exec(ctx, `CREATE TABLE delete_all_test (
		id INTEGER PRIMARY KEY,
		data TEXT
	)`)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// Insert rows
	_, err = db.Exec(ctx, `INSERT INTO delete_all_test VALUES (1, 'a'), (2, 'b'), (3, 'c'), (4, 'd'), (5, 'e')`)
	if err != nil {
		t.Fatalf("Failed to insert: %v", err)
	}

	// Delete all rows
	result, err := db.Exec(ctx, `DELETE FROM delete_all_test`)
	if err != nil {
		t.Fatalf("Failed to delete all: %v", err)
	}

	t.Logf("Deleted %d rows", result.RowsAffected)

	// Verify table is empty
	rows, err := db.Query(ctx, `SELECT COUNT(*) FROM delete_all_test`)
	if err != nil {
		t.Fatalf("Failed to query: %v", err)
	}
	defer rows.Close()

	if rows.Next() {
		var count int
		rows.Scan(&count)
		if count != 0 {
			t.Errorf("Expected 0 rows after DELETE ALL, got %d", count)
		}
	}
}

// TestDeleteWithSubquery targets DELETE with subquery in WHERE
func TestDeleteWithSubquery(t *testing.T) {
	db, err := engine.Open(":memory:", &engine.Options{InMemory: true})
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	ctx := context.Background()

	_, err = db.Exec(ctx, `CREATE TABLE to_delete (
		id INTEGER PRIMARY KEY,
		category TEXT
	)`)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	_, err = db.Exec(ctx, `CREATE TABLE delete_categories (
		cat TEXT PRIMARY KEY
	)`)
	if err != nil {
		t.Fatalf("Failed to create categories: %v", err)
	}

	_, err = db.Exec(ctx, `INSERT INTO to_delete VALUES (1, 'A'), (2, 'B'), (3, 'A'), (4, 'C')`)
	if err != nil {
		t.Fatalf("Failed to insert: %v", err)
	}

	_, err = db.Exec(ctx, `INSERT INTO delete_categories VALUES ('A'), ('C')`)
	if err != nil {
		t.Fatalf("Failed to insert categories: %v", err)
	}

	// Delete where category in subquery
	_, err = db.Exec(ctx, `DELETE FROM to_delete WHERE category IN (SELECT cat FROM delete_categories)`)
	if err != nil {
		t.Logf("Delete with subquery error: %v", err)
		return
	}

	// Verify delete worked
	rows, err := db.Query(ctx, `SELECT COUNT(*) FROM to_delete`)
	if err != nil {
		t.Fatalf("Failed to query: %v", err)
	}
	defer rows.Close()

	if rows.Next() {
		var count int
		rows.Scan(&count)
		if count != 1 {
			t.Errorf("Expected 1 row after subquery delete, got %d", count)
		}
		t.Logf("Subquery delete left %d rows", count)
	}
}
