package test

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/cobaltdb/cobaltdb/pkg/engine"
)

func TestPersistenceBasic(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "test.db")
	ctx := context.Background()

	// Create DB and insert data
	db, err := engine.Open(dbPath, &engine.Options{CacheSize: 64})
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}

	_, err = db.Exec(ctx, "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, email TEXT)")
	if err != nil {
		t.Fatalf("CREATE TABLE failed: %v", err)
	}

	_, err = db.Exec(ctx, "INSERT INTO users (id, name, email) VALUES (1, 'Alice', 'alice@test.com')")
	if err != nil {
		t.Fatalf("INSERT 1 failed: %v", err)
	}

	_, err = db.Exec(ctx, "INSERT INTO users (id, name, email) VALUES (2, 'Bob', 'bob@test.com')")
	if err != nil {
		t.Fatalf("INSERT 2 failed: %v", err)
	}

	// Close DB
	if err := db.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	// Verify file exists
	if _, err := os.Stat(dbPath); os.IsNotExist(err) {
		t.Fatalf("Database file not created at %s", dbPath)
	}

	// Reopen DB
	db2, err := engine.Open(dbPath, &engine.Options{CacheSize: 64})
	if err != nil {
		t.Fatalf("Reopen failed: %v", err)
	}
	defer db2.Close()

	// Verify data persisted
	rows, err := db2.Query(ctx, "SELECT id, name, email FROM users ORDER BY id")
	if err != nil {
		t.Fatalf("SELECT after reopen failed: %v", err)
	}
	defer rows.Close()

	count := 0
	for rows.Next() {
		var id int
		var name, email string
		if err := rows.Scan(&id, &name, &email); err != nil {
			t.Fatalf("Scan failed: %v", err)
		}

		switch id {
		case 1:
			if name != "Alice" || email != "alice@test.com" {
				t.Fatalf("Row 1 mismatch: %s %s", name, email)
			}
		case 2:
			if name != "Bob" || email != "bob@test.com" {
				t.Fatalf("Row 2 mismatch: %s %s", name, email)
			}
		default:
			t.Fatalf("Unexpected row id: %d", id)
		}
		count++
	}

	if count != 2 {
		t.Fatalf("Expected 2 rows after reopen, got %d", count)
	}
}

func TestPersistenceMultipleTables(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "multi.db")
	ctx := context.Background()

	// Create DB with multiple tables
	db, err := engine.Open(dbPath, &engine.Options{CacheSize: 64})
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}

	_, err = db.Exec(ctx, "CREATE TABLE departments (id INTEGER PRIMARY KEY, name TEXT)")
	if err != nil {
		t.Fatalf("CREATE departments failed: %v", err)
	}

	_, err = db.Exec(ctx, "CREATE TABLE employees (id INTEGER PRIMARY KEY, name TEXT, dept_id INTEGER, salary REAL)")
	if err != nil {
		t.Fatalf("CREATE employees failed: %v", err)
	}

	_, err = db.Exec(ctx, "INSERT INTO departments (id, name) VALUES (1, 'Engineering')")
	if err != nil {
		t.Fatalf("INSERT dept failed: %v", err)
	}

	_, err = db.Exec(ctx, "INSERT INTO departments (id, name) VALUES (2, 'Marketing')")
	if err != nil {
		t.Fatalf("INSERT dept 2 failed: %v", err)
	}

	for i := 1; i <= 10; i++ {
		deptID := 1
		if i > 5 {
			deptID = 2
		}
		sql := fmt.Sprintf("INSERT INTO employees (id, name, dept_id, salary) VALUES (%d, 'Employee_%d', %d, %d)", i, i, deptID, 50000+i*5000)
		if _, err := db.Exec(ctx, sql); err != nil {
			t.Fatalf("INSERT emp %d failed: %v", i, err)
		}
	}

	db.Close()

	// Reopen
	db2, err := engine.Open(dbPath, &engine.Options{CacheSize: 64})
	if err != nil {
		t.Fatalf("Reopen failed: %v", err)
	}
	defer db2.Close()

	// Verify tables exist
	tables := db2.Tables()
	if len(tables) < 2 {
		t.Fatalf("Expected at least 2 tables, got %d: %v", len(tables), tables)
	}

	// Verify departments
	rows, err := db2.Query(ctx, "SELECT COUNT(*) FROM departments")
	if err != nil {
		t.Fatalf("COUNT departments failed: %v", err)
	}
	rows.Next()
	var deptCount int
	rows.Scan(&deptCount)
	rows.Close()
	if deptCount != 2 {
		t.Fatalf("Expected 2 departments, got %d", deptCount)
	}

	// Verify employees
	rows, err = db2.Query(ctx, "SELECT COUNT(*) FROM employees")
	if err != nil {
		t.Fatalf("COUNT employees failed: %v", err)
	}
	rows.Next()
	var empCount int
	rows.Scan(&empCount)
	rows.Close()
	if empCount != 10 {
		t.Fatalf("Expected 10 employees, got %d", empCount)
	}

	// Verify JOIN works after reopen
	rows, err = db2.Query(ctx, "SELECT d.name, COUNT(e.id) FROM departments d JOIN employees e ON d.id = e.dept_id GROUP BY d.name ORDER BY d.name")
	if err != nil {
		t.Fatalf("JOIN query after reopen failed: %v", err)
	}
	defer rows.Close()

	joinCount := 0
	for rows.Next() {
		var dName string
		var cnt int
		rows.Scan(&dName, &cnt)
		t.Logf("Department %s: %d employees", dName, cnt)

		if dName == "Engineering" && cnt != 5 {
			t.Fatalf("Expected 5 engineering employees, got %d", cnt)
		}
		if dName == "Marketing" && cnt != 5 {
			t.Fatalf("Expected 5 marketing employees, got %d", cnt)
		}
		joinCount++
	}
	if joinCount != 2 {
		t.Fatalf("Expected 2 department groups, got %d", joinCount)
	}
}

func TestPersistenceLargeDataset(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "large.db")
	ctx := context.Background()

	// Create DB and insert many rows
	db, err := engine.Open(dbPath, &engine.Options{CacheSize: 128})
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}

	_, err = db.Exec(ctx, "CREATE TABLE items (id INTEGER PRIMARY KEY, data TEXT, value REAL)")
	if err != nil {
		t.Fatalf("CREATE TABLE failed: %v", err)
	}

	// Insert 200 rows (exceeds single page)
	for i := 1; i <= 200; i++ {
		sql := fmt.Sprintf("INSERT INTO items (id, data, value) VALUES (%d, 'item_data_%d_padding_for_size_%d', %d.%d)",
			i, i, i*100, i*10, i%100)
		if _, err := db.Exec(ctx, sql); err != nil {
			t.Fatalf("INSERT %d failed: %v", i, err)
		}
	}

	db.Close()

	// Reopen
	db2, err := engine.Open(dbPath, &engine.Options{CacheSize: 128})
	if err != nil {
		t.Fatalf("Reopen failed: %v", err)
	}
	defer db2.Close()

	// Verify count
	rows, err := db2.Query(ctx, "SELECT COUNT(*) FROM items")
	if err != nil {
		t.Fatalf("COUNT after reopen failed: %v", err)
	}
	rows.Next()
	var count int
	rows.Scan(&count)
	rows.Close()

	if count != 200 {
		t.Fatalf("Expected 200 rows after reopen, got %d", count)
	}

	// Verify specific rows
	rows, err = db2.Query(ctx, "SELECT data FROM items WHERE id = 100")
	if err != nil {
		t.Fatalf("SELECT specific row failed: %v", err)
	}
	if !rows.Next() {
		t.Fatal("Expected row with id=100")
	}
	var data string
	rows.Scan(&data)
	rows.Close()

	if data != "item_data_100_padding_for_size_10000" {
		t.Fatalf("Data mismatch for id=100: %s", data)
	}

	// Verify aggregation
	rows, err = db2.Query(ctx, "SELECT MIN(id), MAX(id) FROM items")
	if err != nil {
		t.Fatalf("MIN/MAX query failed: %v", err)
	}
	rows.Next()
	var minID, maxID int
	rows.Scan(&minID, &maxID)
	rows.Close()

	if minID != 1 || maxID != 200 {
		t.Fatalf("Expected MIN=1, MAX=200, got MIN=%d, MAX=%d", minID, maxID)
	}

	t.Logf("200 rows persisted and verified successfully")
}

func TestPersistenceUpdateDelete(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "upd.db")
	ctx := context.Background()

	// Phase 1: Create and insert
	db, err := engine.Open(dbPath, &engine.Options{CacheSize: 64})
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}

	db.Exec(ctx, "CREATE TABLE t1 (id INTEGER PRIMARY KEY, val TEXT)")
	for i := 1; i <= 5; i++ {
		db.Exec(ctx, fmt.Sprintf("INSERT INTO t1 (id, val) VALUES (%d, 'original_%d')", i, i))
	}
	db.Close()

	// Phase 2: Reopen, update and delete
	db2, err := engine.Open(dbPath, &engine.Options{CacheSize: 64})
	if err != nil {
		t.Fatalf("Reopen for update failed: %v", err)
	}

	db2.Exec(ctx, "UPDATE t1 SET val = 'updated_3' WHERE id = 3")
	db2.Exec(ctx, "DELETE FROM t1 WHERE id = 5")
	db2.Close()

	// Phase 3: Reopen and verify
	db3, err := engine.Open(dbPath, &engine.Options{CacheSize: 64})
	if err != nil {
		t.Fatalf("Reopen for verify failed: %v", err)
	}
	defer db3.Close()

	// Verify count (should be 4 after delete)
	rows, err := db3.Query(ctx, "SELECT COUNT(*) FROM t1")
	if err != nil {
		t.Fatalf("COUNT failed: %v", err)
	}
	rows.Next()
	var count int
	rows.Scan(&count)
	rows.Close()
	if count != 4 {
		t.Fatalf("Expected 4 rows after delete+reopen, got %d", count)
	}

	// Verify update persisted
	rows, err = db3.Query(ctx, "SELECT val FROM t1 WHERE id = 3")
	if err != nil {
		t.Fatalf("SELECT updated row failed: %v", err)
	}
	if !rows.Next() {
		t.Fatal("Expected row with id=3")
	}
	var val string
	rows.Scan(&val)
	rows.Close()

	if val != "updated_3" {
		t.Fatalf("Expected 'updated_3', got '%s'", val)
	}

	// Verify deleted row is gone
	rows, err = db3.Query(ctx, "SELECT id FROM t1 WHERE id = 5")
	if err != nil {
		t.Fatalf("SELECT deleted row failed: %v", err)
	}
	if rows.Next() {
		t.Fatal("Row id=5 should have been deleted")
	}
	rows.Close()
}

func TestPersistenceAutoIncrement(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "autoinc.db")
	ctx := context.Background()

	// Phase 1: Create table with autoincrement and insert
	db, err := engine.Open(dbPath, &engine.Options{CacheSize: 64})
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}

	db.Exec(ctx, "CREATE TABLE items (id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT)")
	db.Exec(ctx, "INSERT INTO items (name) VALUES ('first')")
	db.Exec(ctx, "INSERT INTO items (name) VALUES ('second')")
	db.Exec(ctx, "INSERT INTO items (name) VALUES ('third')")
	db.Close()

	// Phase 2: Reopen and insert more (should continue from 4)
	db2, err := engine.Open(dbPath, &engine.Options{CacheSize: 64})
	if err != nil {
		t.Fatalf("Reopen failed: %v", err)
	}

	db2.Exec(ctx, "INSERT INTO items (name) VALUES ('fourth')")
	db2.Exec(ctx, "INSERT INTO items (name) VALUES ('fifth')")

	// Verify all 5 rows exist with distinct IDs
	rows, err := db2.Query(ctx, "SELECT id, name FROM items ORDER BY id")
	if err != nil {
		t.Fatalf("SELECT failed: %v", err)
	}
	defer rows.Close()

	count := 0
	var prevID int
	for rows.Next() {
		var id int
		var name string
		rows.Scan(&id, &name)
		t.Logf("Row: id=%d name=%s", id, name)

		if count > 0 && id <= prevID {
			t.Fatalf("IDs not monotonically increasing: prev=%d, curr=%d", prevID, id)
		}
		prevID = id
		count++
	}

	if count != 5 {
		t.Fatalf("Expected 5 rows, got %d", count)
	}

	db2.Close()
}
