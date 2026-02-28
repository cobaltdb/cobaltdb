package engine

import (
	"context"
	"testing"
)

func TestOpenMemory(t *testing.T) {
	db, err := Open(":memory:", &Options{
		InMemory:  true,
		CacheSize: 1024,
	})
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	if db == nil {
		t.Fatal("Database is nil")
	}
}

func TestCreateTable(t *testing.T) {
	db, err := Open(":memory:", &Options{
		InMemory:  true,
		CacheSize: 1024,
	})
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	ctx := context.Background()

	// Create table
	result, err := db.Exec(ctx, `
		CREATE TABLE users (
			id INTEGER PRIMARY KEY,
			name TEXT NOT NULL
		)
	`)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	if result.RowsAffected != 0 {
		t.Errorf("Expected 0 rows affected, got %d", result.RowsAffected)
	}

	// Try to create same table again (should fail)
	_, err = db.Exec(ctx, `CREATE TABLE users (id INTEGER)`)
	if err == nil {
		t.Error("Expected error when creating duplicate table")
	}
}

func TestInsertAndSelect(t *testing.T) {
	db, err := Open(":memory:", &Options{
		InMemory:  true,
		CacheSize: 1024,
	})
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	ctx := context.Background()

	// Create table
	_, err = db.Exec(ctx, `CREATE TABLE users (id INTEGER, name TEXT)`)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// Insert data
	result, err := db.Exec(ctx, `INSERT INTO users (id, name) VALUES (?, ?)`, 1, "Alice")
	if err != nil {
		t.Fatalf("Failed to insert: %v", err)
	}

	if result.RowsAffected != 1 {
		t.Errorf("Expected 1 row affected, got %d", result.RowsAffected)
	}

	// Select data
	rows, err := db.Query(ctx, `SELECT id, name FROM users`)
	if err != nil {
		t.Fatalf("Failed to query: %v", err)
	}
	defer rows.Close()

	columns := rows.Columns()
	if len(columns) != 2 {
		t.Errorf("Expected 2 columns, got %d", len(columns))
	}

	count := 0
	for rows.Next() {
		var id interface{}
		var name interface{}
		if err := rows.Scan(&id, &name); err != nil {
			t.Fatalf("Failed to scan: %v", err)
		}
		count++
	}

	if count != 1 {
		t.Errorf("Expected 1 row, got %d", count)
	}
}

func TestTransaction(t *testing.T) {
	db, err := Open(":memory:", &Options{
		InMemory:  true,
		CacheSize: 1024,
	})
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	ctx := context.Background()

	// Create table
	_, err = db.Exec(ctx, `CREATE TABLE items (id INTEGER, name TEXT)`)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// Begin transaction
	tx, err := db.Begin(ctx)
	if err != nil {
		t.Fatalf("Failed to begin transaction: %v", err)
	}

	// Insert in transaction
	_, err = tx.Exec(ctx, `INSERT INTO items (id, name) VALUES (?, ?)`, 1, "Item1")
	if err != nil {
		tx.Rollback()
		t.Fatalf("Failed to insert in transaction: %v", err)
	}

	// Commit
	if err := tx.Commit(); err != nil {
		t.Fatalf("Failed to commit transaction: %v", err)
	}

	// Verify data
	rows, err := db.Query(ctx, `SELECT id FROM items`)
	if err != nil {
		t.Fatalf("Failed to query: %v", err)
	}
	defer rows.Close()

	count := 0
	for rows.Next() {
		count++
	}

	if count != 1 {
		t.Errorf("Expected 1 row after transaction, got %d", count)
	}
}

func TestMultipleInserts(t *testing.T) {
	db, err := Open(":memory:", &Options{
		InMemory:  true,
		CacheSize: 1024,
	})
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	ctx := context.Background()

	// Create table
	_, err = db.Exec(ctx, `CREATE TABLE test (id INTEGER, value TEXT)`)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// Insert multiple rows
	for i := 0; i < 10; i++ {
		_, err = db.Exec(ctx, `INSERT INTO test (id, value) VALUES (?, ?)`, i, "value")
		if err != nil {
			t.Fatalf("Failed to insert row %d: %v", i, err)
		}
	}

	// Count rows
	rows, err := db.Query(ctx, `SELECT id FROM test`)
	if err != nil {
		t.Fatalf("Failed to query: %v", err)
	}
	defer rows.Close()

	count := 0
	for rows.Next() {
		count++
	}

	if count != 10 {
		t.Errorf("Expected 10 rows, got %d", count)
	}
}

func TestQueryRow(t *testing.T) {
	db, err := Open(":memory:", &Options{
		InMemory:  true,
		CacheSize: 1024,
	})
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	ctx := context.Background()

	// Create and insert
	db.Exec(ctx, `CREATE TABLE single (id INTEGER, name TEXT)`)
	db.Exec(ctx, `INSERT INTO single (id, name) VALUES (?, ?)`, 1, "Test")

	// Query single row
	row := db.QueryRow(ctx, `SELECT id, name FROM single`)
	if row == nil {
		t.Fatal("QueryRow returned nil")
	}

	var id, name interface{}
	if err := row.Scan(&id, &name); err != nil {
		t.Fatalf("Failed to scan row: %v", err)
	}
}
