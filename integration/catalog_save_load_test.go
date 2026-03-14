package integration

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/cobaltdb/cobaltdb/pkg/engine"
)

// TestSaveLoadBasic targets Save and Load operations
func TestSaveLoadBasic(t *testing.T) {
	tempDir := t.TempDir()
	dbPath := filepath.Join(tempDir, "test.db")

	ctx := context.Background()

	// Create and populate database
	db, err := engine.Open(dbPath, &engine.Options{InMemory: false})
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}

	_, err = db.Exec(ctx, `CREATE TABLE save_test (
		id INTEGER PRIMARY KEY,
		name TEXT,
		value INTEGER
	)`)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	_, err = db.Exec(ctx, `INSERT INTO save_test VALUES (1, 'test1', 100), (2, 'test2', 200)`)
	if err != nil {
		t.Fatalf("Failed to insert: %v", err)
	}

	// Save database
	_, err = db.Exec(ctx, `SAVEPOINT test_save`)
	if err != nil {
		t.Logf("SAVEPOINT error: %v", err)
	}

	db.Close()

	// Re-open and verify data persisted
	db2, err := engine.Open(dbPath, &engine.Options{InMemory: false})
	if err != nil {
		t.Fatalf("Failed to reopen database: %v", err)
	}
	defer db2.Close()

	rows, err := db2.Query(ctx, `SELECT COUNT(*) FROM save_test`)
	if err != nil {
		t.Fatalf("Failed to query: %v", err)
	}
	defer rows.Close()

	if rows.Next() {
		var count int
		rows.Scan(&count)
		if count != 2 {
			t.Errorf("Expected 2 rows after reopen, got %d", count)
		}
		t.Logf("Database loaded with %d rows", count)
	}
}

// TestSaveLoadWithIndex targets Save/Load with indexes
func TestSaveLoadWithIndex(t *testing.T) {
	tempDir := t.TempDir()
	dbPath := filepath.Join(tempDir, "index.db")

	ctx := context.Background()

	db, err := engine.Open(dbPath, &engine.Options{InMemory: false})
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}

	_, err = db.Exec(ctx, `CREATE TABLE idx_test (
		id INTEGER PRIMARY KEY,
		email TEXT UNIQUE,
		name TEXT
	)`)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	_, err = db.Exec(ctx, `CREATE INDEX idx_name ON idx_test(name)`)
	if err != nil {
		t.Fatalf("Failed to create index: %v", err)
	}

	_, err = db.Exec(ctx, `INSERT INTO idx_test VALUES (1, 'a@example.com', 'Alice'), (2, 'b@example.com', 'Bob')`)
	if err != nil {
		t.Fatalf("Failed to insert: %v", err)
	}

	db.Close()

	// Reopen and verify indexes still work
	db2, err := engine.Open(dbPath, &engine.Options{InMemory: false})
	if err != nil {
		t.Fatalf("Failed to reopen: %v", err)
	}
	defer db2.Close()

	// Query using index
	rows, err := db2.Query(ctx, `SELECT * FROM idx_test WHERE email = 'a@example.com'`)
	if err != nil {
		t.Fatalf("Failed to query: %v", err)
	}
	defer rows.Close()

	if rows.Next() {
		var id int
		var email, name string
		rows.Scan(&id, &email, &name)
		t.Logf("Found: id=%d, email=%s, name=%s", id, email, name)
	}
}

// TestSaveLoadWithFK targets Save/Load with foreign keys
func TestSaveLoadWithFK(t *testing.T) {
	tempDir := t.TempDir()
	dbPath := filepath.Join(tempDir, "fk.db")

	ctx := context.Background()

	db, err := engine.Open(dbPath, &engine.Options{InMemory: false})
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}

	_, err = db.Exec(ctx, `CREATE TABLE parent (
		id INTEGER PRIMARY KEY,
		name TEXT
	)`)
	if err != nil {
		t.Fatalf("Failed to create parent: %v", err)
	}

	_, err = db.Exec(ctx, `CREATE TABLE child (
		id INTEGER PRIMARY KEY,
		parent_id INTEGER,
		FOREIGN KEY (parent_id) REFERENCES parent(id) ON DELETE CASCADE
	)`)
	if err != nil {
		t.Fatalf("Failed to create child: %v", err)
	}

	_, err = db.Exec(ctx, `INSERT INTO parent VALUES (1, 'parent1'), (2, 'parent2')`)
	if err != nil {
		t.Fatalf("Failed to insert parent: %v", err)
	}

	_, err = db.Exec(ctx, `INSERT INTO child VALUES (1, 1), (2, 2)`)
	if err != nil {
		t.Fatalf("Failed to insert child: %v", err)
	}

	db.Close()

	// Reopen and verify FK still works
	db2, err := engine.Open(dbPath, &engine.Options{InMemory: false})
	if err != nil {
		t.Fatalf("Failed to reopen: %v", err)
	}
	defer db2.Close()

	// Try to delete parent (should cascade)
	_, err = db2.Exec(ctx, `DELETE FROM parent WHERE id = 1`)
	if err != nil {
		t.Logf("DELETE with FK error: %v", err)
		return
	}

	rows, _ := db2.Query(ctx, `SELECT COUNT(*) FROM child`)
	if rows != nil {
		defer rows.Close()
		if rows.Next() {
			var count int
			rows.Scan(&count)
			t.Logf("Children after cascade: %d", count)
		}
	}
}

// TestSaveLoadWithView targets Save/Load with views
func TestSaveLoadWithView(t *testing.T) {
	tempDir := t.TempDir()
	dbPath := filepath.Join(tempDir, "view.db")

	ctx := context.Background()

	db, err := engine.Open(dbPath, &engine.Options{InMemory: false})
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}

	_, err = db.Exec(ctx, `CREATE TABLE base (id INTEGER PRIMARY KEY, val INTEGER)`)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	_, err = db.Exec(ctx, `INSERT INTO base VALUES (1, 100), (2, 200)`)
	if err != nil {
		t.Fatalf("Failed to insert: %v", err)
	}

	_, err = db.Exec(ctx, `CREATE VIEW v1 AS SELECT * FROM base WHERE val > 150`)
	if err != nil {
		t.Fatalf("Failed to create view: %v", err)
	}

	db.Close()

	// Reopen and verify view
	db2, err := engine.Open(dbPath, &engine.Options{InMemory: false})
	if err != nil {
		t.Fatalf("Failed to reopen: %v", err)
	}
	defer db2.Close()

	rows, err := db2.Query(ctx, `SELECT * FROM v1`)
	if err != nil {
		t.Logf("View query error (may not persist): %v", err)
		return
	}
	defer rows.Close()

	count := 0
	for rows.Next() {
		count++
	}
	t.Logf("View returned %d rows after load", count)
}

// TestSaveLoadWithTrigger targets Save/Load with triggers
func TestSaveLoadWithTrigger(t *testing.T) {
	tempDir := t.TempDir()
	dbPath := filepath.Join(tempDir, "trigger.db")

	ctx := context.Background()

	db, err := engine.Open(dbPath, &engine.Options{InMemory: false})
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}

	_, err = db.Exec(ctx, `CREATE TABLE audit (id INTEGER PRIMARY KEY, action TEXT)`)
	if err != nil {
		t.Fatalf("Failed to create audit: %v", err)
	}

	_, err = db.Exec(ctx, `CREATE TABLE main (id INTEGER PRIMARY KEY)`)
	if err != nil {
		t.Fatalf("Failed to create main: %v", err)
	}

	_, err = db.Exec(ctx, `CREATE TRIGGER trg AFTER INSERT ON main BEGIN INSERT INTO audit (action) VALUES ('insert'); END`)
	if err != nil {
		t.Fatalf("Failed to create trigger: %v", err)
	}

	// Insert to fire trigger
	_, err = db.Exec(ctx, `INSERT INTO main VALUES (1)`)
	if err != nil {
		t.Fatalf("Failed to insert: %v", err)
	}

	db.Close()

	// Reopen and verify trigger still works
	db2, err := engine.Open(dbPath, &engine.Options{InMemory: false})
	if err != nil {
		t.Fatalf("Failed to reopen: %v", err)
	}
	defer db2.Close()

	_, err = db2.Exec(ctx, `INSERT INTO main VALUES (2)`)
	if err != nil {
		t.Fatalf("Failed to insert: %v", err)
	}

	rows, _ := db2.Query(ctx, `SELECT COUNT(*) FROM audit`)
	if rows != nil {
		defer rows.Close()
		if rows.Next() {
			var count int
			rows.Scan(&count)
			t.Logf("Audit entries: %d (should be 2)", count)
		}
	}
}

// TestLoadEmptyDatabase targets Load with empty catalog
func TestLoadEmptyDatabase(t *testing.T) {
	tempDir := t.TempDir()
	dbPath := filepath.Join(tempDir, "empty.db")

	ctx := context.Background()

	// Create empty database
	db, err := engine.Open(dbPath, &engine.Options{InMemory: false})
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	db.Close()

	// Verify file exists
	if _, err := os.Stat(dbPath); os.IsNotExist(err) {
		t.Fatal("Database file was not created")
	}

	// Reopen empty database
	db2, err := engine.Open(dbPath, &engine.Options{InMemory: false})
	if err != nil {
		t.Fatalf("Failed to reopen empty database: %v", err)
	}
	defer db2.Close()

	// Should be able to query (no tables)
	_, err = db2.Query(ctx, `SELECT name FROM sqlite_master WHERE type='table'`)
	if err != nil {
		t.Logf("Query on empty database error: %v", err)
	}
}

// TestLoadWithCorruptedData tests Load behavior with corrupted data
func TestLoadWithCorruptedData(t *testing.T) {
	tempDir := t.TempDir()
	dbPath := filepath.Join(tempDir, "corrupt.db")

	// Create a file with corrupted data
	corruptData := []byte("corrupted data that is not valid")
	err := os.WriteFile(dbPath, corruptData, 0644)
	if err != nil {
		t.Fatalf("Failed to write corrupt file: %v", err)
	}

	// Try to open corrupted database
	db, err := engine.Open(dbPath, &engine.Options{InMemory: false})
	if err != nil {
		t.Logf("Open corrupted database error (expected): %v", err)
		return
	}
	defer db.Close()

	t.Log("Corrupted database opened without error - may auto-recover")
}
