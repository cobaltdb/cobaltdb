package engine

import (
	"context"
	"os"
	"path/filepath"
	"testing"
)

// TestExecuteInsertReturning tests INSERT with RETURNING clause
func TestExecuteInsertReturning(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "test.db")

	db, err := Open(dbPath, nil)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer os.RemoveAll(dir)
	defer db.Close()

	ctx := context.Background()

	// Create table
	_, err = db.Exec(ctx, "CREATE TABLE test (id INTEGER PRIMARY KEY, name TEXT)")
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// Test INSERT RETURNING with values
	rows, err := db.Query(ctx, "INSERT INTO test (id, name) VALUES (1, 'Alice') RETURNING id, name")
	if err != nil {
		t.Fatalf("INSERT RETURNING failed: %v", err)
	}

	if rows == nil {
		t.Fatal("Expected rows, got nil")
	}

	// Verify columns
	if len(rows.columns) != 2 {
		t.Errorf("Expected 2 columns, got %d", len(rows.columns))
	}

	// Verify we have a row
	if rows.rows == nil || len(rows.rows) == 0 {
		t.Error("Expected at least one row")
	}
}

// TestExecuteInsertReturningError tests INSERT RETURNING with errors
func TestExecuteInsertReturningError(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "test.db")

	db, err := Open(dbPath, nil)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer os.RemoveAll(dir)
	defer db.Close()

	ctx := context.Background()

	// Try INSERT RETURNING on non-existent table
	_, err = db.Query(ctx, "INSERT INTO nonexistent (id) VALUES (1) RETURNING id")
	if err == nil {
		t.Error("Expected error for non-existent table")
	}
}

// TestExecuteUpdateReturning tests UPDATE with RETURNING clause
func TestExecuteUpdateReturning(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "test.db")

	db, err := Open(dbPath, nil)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer os.RemoveAll(dir)
	defer db.Close()

	ctx := context.Background()

	// Create table and insert data
	_, err = db.Exec(ctx, "CREATE TABLE test (id INTEGER PRIMARY KEY, name TEXT)")
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	_, err = db.Exec(ctx, "INSERT INTO test (id, name) VALUES (1, 'Alice')")
	if err != nil {
		t.Fatalf("Failed to insert: %v", err)
	}

	// Test UPDATE RETURNING
	rows, err := db.Query(ctx, "UPDATE test SET name = 'Bob' WHERE id = 1 RETURNING id, name")
	if err != nil {
		t.Fatalf("UPDATE RETURNING failed: %v", err)
	}

	if rows == nil {
		t.Fatal("Expected rows, got nil")
	}

	if len(rows.columns) != 2 {
		t.Errorf("Expected 2 columns, got %d", len(rows.columns))
	}
}

// TestExecuteUpdateReturningError tests UPDATE RETURNING with errors
func TestExecuteUpdateReturningError(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "test.db")

	db, err := Open(dbPath, nil)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer os.RemoveAll(dir)
	defer db.Close()

	ctx := context.Background()

	// Try UPDATE RETURNING on non-existent table
	_, err = db.Query(ctx, "UPDATE nonexistent SET id = 1 RETURNING id")
	if err == nil {
		t.Error("Expected error for non-existent table")
	}
}

// TestExecuteDeleteReturning tests DELETE with RETURNING clause
func TestExecuteDeleteReturning(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "test.db")

	db, err := Open(dbPath, nil)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer os.RemoveAll(dir)
	defer db.Close()

	ctx := context.Background()

	// Create table and insert data
	_, err = db.Exec(ctx, "CREATE TABLE test (id INTEGER PRIMARY KEY, name TEXT)")
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	_, err = db.Exec(ctx, "INSERT INTO test (id, name) VALUES (1, 'Alice')")
	if err != nil {
		t.Fatalf("Failed to insert: %v", err)
	}

	// Test DELETE RETURNING
	rows, err := db.Query(ctx, "DELETE FROM test WHERE id = 1 RETURNING id, name")
	if err != nil {
		t.Fatalf("DELETE RETURNING failed: %v", err)
	}

	if rows == nil {
		t.Fatal("Expected rows, got nil")
	}

	if len(rows.columns) != 2 {
		t.Errorf("Expected 2 columns, got %d", len(rows.columns))
	}
}

// TestExecuteDeleteReturningError tests DELETE RETURNING with errors
func TestExecuteDeleteReturningError(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "test.db")

	db, err := Open(dbPath, nil)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer os.RemoveAll(dir)
	defer db.Close()

	ctx := context.Background()

	// Try DELETE RETURNING on non-existent table
	_, err = db.Query(ctx, "DELETE FROM nonexistent WHERE id = 1 RETURNING id")
	if err == nil {
		t.Error("Expected error for non-existent table")
	}
}

// TestCreateNewErrorPaths2 tests createNew with error conditions (duplicate name avoided)
func TestCreateNewErrorPaths2(t *testing.T) {
	// Test with invalid path containing invalid characters (Windows)
	invalidPath := "CON:/invalid/path"

	db, err := Open(invalidPath, nil)
	if err == nil {
		db.Close()
		t.Log("Warning: No error for invalid path - may be platform dependent")
	}
}
